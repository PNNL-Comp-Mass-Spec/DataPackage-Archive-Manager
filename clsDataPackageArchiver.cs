using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using MyEMSLReader;
using Pacifica.Core;
using Pacifica.DMS_Metadata;
using Pacifica.Upload;
using PRISM;
using PRISM.Logging;
using PRISMDatabaseUtils;
using Utilities = Pacifica.Core.Utilities;

namespace DataPackage_Archive_Manager
{
    internal class DataPackageArchiver : EventNotifier
    {
        // Ignore Spelling: DataPkgs, misconfigured, msgfdb, pnl, pre, protoapps, UtcNow, yyyy-MM-dd

        public const string CONNECTION_STRING = "Data Source=gigasax;Initial Catalog=DMS_Data_Package;Integrated Security=SSPI;";

        private const string SP_NAME_STORE_MYEMSL_STATS = "StoreMyEMSLUploadStats";
        private const string SP_NAME_SET_MYEMSL_UPLOAD_STATUS = "SetMyEMSLUploadStatus";

        private enum UploadStatus
        {
            Success = 0,
            VerificationError = 1,
            CriticalError = 2
        }

        /// <summary>
        /// Maximum number of files to archive
        /// </summary>
        /// <remarks>
        /// <para>
        /// Since data package uploads always work with the entire data package directory and all subdirectories,
        /// there is a maximum cap on the number of files that will be stored in MyEMSL for a given data package
        /// </para>
        /// <para>
        /// If a data package has more than 600 files, the data needs to be manually zipped before this manager will auto-push into MyEMSL
        /// </para>
        /// </remarks>
        private const int MAX_FILES_TO_ARCHIVE = 600;

        private struct MyEMSLStatusInfo
        {
            public int EntryID;
            public int DataPackageID;
            public string DataPackageOwner;
            public DateTime Entered;
            public string StatusURI;

            public string SharePath;
            public string LocalPath;

            /// <summary>
            /// Show the data package ID and Status URI
            /// </summary>
            public override string ToString()
            {
                return string.Format("Data package {0}: {1}", DataPackageID, StatusURI ?? string.Empty);
            }
        }

        private struct MyEMSLUploadInfo
        {
            public string SubDir;
            public int FileCountNew;
            public int FileCountUpdated;
            public long Bytes;
            public double UploadTimeSeconds;
            public string StatusURI;
            public int ErrorCode;

            public void Clear()
            {
                SubDir = string.Empty;
                FileCountNew = 0;
                FileCountUpdated = 0;
                Bytes = 0;
                UploadTimeSeconds = 0;
                StatusURI = string.Empty;
                ErrorCode = 0;
            }

            /// <summary>
            /// Show file counts
            /// </summary>
            public override string ToString()
            {
                return string.Format(
                    "{0} new files, {1} updated files, {2:N0} total bytes",
                    FileCountNew, FileCountUpdated, Bytes);
            }
        }

        private readonly IDBTools mDBTools;
        private readonly Upload mMyEMSLUploader;
        private DateTime mLastStatusUpdate;

        /// <summary>
        /// When true, skip verifying upload status
        /// </summary>
        public bool DisableVerify { get; set; }

        /// <summary>
        /// Gigasax.DMS_Data_Package database
        /// </summary>
        public string DBConnectionString { get; }

        /// <summary>
        /// Error message
        /// </summary>
        public string ErrorMessage { get; private set; }

        /// <summary>
        /// True if previewing updates
        /// </summary>
        public bool PreviewMode { get; set; }

        /// <summary>
        /// When true, skip checking existing data packages
        /// </summary>
        public bool SkipCheckExisting { get; set; }

        /// <summary>
        /// True if trace mode is enabled
        /// </summary>
        public bool TraceMode { get; set; }

        /// <summary>
        /// Logging level; range is 1-5, where 5 is the most verbose
        /// </summary>
        /// <remarks>Levels are:
        /// DEBUG = 5,
        /// INFO = 4,
        /// WARN = 3,
        /// ERROR = 2,
        /// FATAL = 1</remarks>
        public BaseLogger.LogLevels LogLevel { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        public DataPackageArchiver(string connectionString, BaseLogger.LogLevels logLevel)
        {
            // Typically:
            // Data Source=gigasax;Initial Catalog=DMS_Data_Package;Integrated Security=SSPI;
            DBConnectionString = connectionString;
            LogLevel = logLevel;

            mDBTools = DbToolsFactory.GetDBTools(DBConnectionString);
            RegisterEvents(mDBTools);

            var pacificaConfig = new Configuration();

            mMyEMSLUploader = new Upload(pacificaConfig);
            RegisterEvents(mMyEMSLUploader);

            // Attach the events
            mMyEMSLUploader.StatusUpdate += MyEMSLUpload_StatusUpdate;
            mMyEMSLUploader.UploadCompleted += MyEMSLUpload_UploadCompleted;

            Initialize();
        }

        private static void AddFileIfArchiveRequired(
            DirectoryInfo dataPkg,
            ref MyEMSLUploadInfo uploadInfo,
            ICollection<FileInfoObject> datasetFilesToArchive,
            FileInfo localFile,
            ICollection<DatasetDirectoryOrFileInfo> archiveFiles)
        {
            if (archiveFiles.Count == 0)
            {
                // File not found; add to datasetFilesToArchive
                if (dataPkg.Parent == null)
                    throw new DirectoryNotFoundException("Unable to determine the parent directory of " + dataPkg.FullName);

                datasetFilesToArchive.Add(new FileInfoObject(localFile, dataPkg.Parent.FullName));
                uploadInfo.FileCountNew++;
                uploadInfo.Bytes += localFile.Length;
                return;
            }

            var archiveFile = archiveFiles.First();

            // File already in MyEMSL
            // Do not re-upload if it was stored in MyEMSL less than 6.75 days ago
            if (DateTime.UtcNow.Subtract(archiveFile.FileInfo.SubmissionTimeValue).TotalDays < 6.75)
            {
                return;
            }

            // Compare file size
            if (localFile.Length != archiveFile.FileInfo.FileSizeBytes)
            {
                // Sizes don't match; add to datasetFilesToArchive
                if (dataPkg.Parent == null)
                    throw new DirectoryNotFoundException("Unable to determine the parent directory of " + dataPkg.FullName);

                datasetFilesToArchive.Add(new FileInfoObject(localFile, dataPkg.Parent.FullName));
                uploadInfo.FileCountUpdated++;
                uploadInfo.Bytes += localFile.Length;
                return;
            }

            // File sizes match
            // Compare SHA-1 hash if the file is less than 1 month old or
            // if the file is less than 6 months old and less than 50 MB in size

            const int THRESHOLD_50_MB = 50 * 1024 * 1024;

            if (localFile.LastWriteTimeUtc > DateTime.UtcNow.AddMonths(-1) ||
                localFile.LastWriteTimeUtc > DateTime.UtcNow.AddMonths(-6) && localFile.Length < THRESHOLD_50_MB)
            {
                var sha1HashHex = Utilities.GenerateSha1Hash(localFile);

                if (sha1HashHex != archiveFile.FileInfo.Sha1Hash)
                {
                    if (dataPkg.Parent == null)
                        throw new DirectoryNotFoundException("Unable to determine the parent directory of " + dataPkg.FullName);

                    // Hashes don't match; add to datasetFilesToArchive
                    // We include the hash when instantiating the new FileInfoObject so that the hash will not need to be regenerated later
                    var relativeDestinationDirectory = FileInfoObject.GenerateRelativePath(localFile.DirectoryName,
                                                                                           dataPkg.Parent.FullName);

                    datasetFilesToArchive.Add(new FileInfoObject(localFile, relativeDestinationDirectory, sha1HashHex));
                    uploadInfo.FileCountUpdated++;
                    uploadInfo.Bytes += localFile.Length;
                }
            }
        }

        private short BoolToTinyInt(bool value)
        {
            return (short)(value ? 1 : 0);
        }

        private int CountFilesForDataPackage(DataPackageInfo dataPkgInfo)
        {
            var dataPkg = new DirectoryInfo(dataPkgInfo.LocalPath);
            if (!dataPkg.Exists)
                dataPkg = new DirectoryInfo(dataPkgInfo.SharePath);

            return !dataPkg.Exists ? 0 : dataPkg.GetFiles("*.*", SearchOption.AllDirectories).Length;
        }

        private bool FilePassesFilters(ICollection<string> filesToSkip, ICollection<string> extensionsToSkip, FileInfo dataPkgFile)
        {
            if (dataPkgFile.Length == 0)
                return false;

            if (filesToSkip.Contains(dataPkgFile.Name))
                return false;

            if (extensionsToSkip.Contains(dataPkgFile.Extension))
                return false;

            if (dataPkgFile.Name.StartsWith("~$") && ((dataPkgFile.Attributes & FileAttributes.Hidden) == FileAttributes.Hidden))
                return false;

            if (dataPkgFile.Name.StartsWith("SyncToy_") && dataPkgFile.Name.EndsWith(".dat"))
                return false;

            return !dataPkgFile.Name.EndsWith(".mzml.gz", StringComparison.OrdinalIgnoreCase);
        }

        private List<FileInfoObject> FindDataPackageFilesToArchive(
            DataPackageInfo dataPkgInfo,
            DirectoryInfo dataPkg,
            DateTime dateThreshold,
            DataPackageListInfo dataPackageInfoCache,
            out MyEMSLUploadInfo uploadInfo)
        {
            var datasetFilesToArchive = new List<FileInfoObject>();

            uploadInfo = new MyEMSLUploadInfo();
            uploadInfo.Clear();

            uploadInfo.SubDir = dataPkgInfo.DirectoryName;

            // Construct a list of the files on disk for this data package
            var dataPackageFilesAll = dataPkg.GetFiles("*.*", SearchOption.AllDirectories).ToList();

            if (dataPackageFilesAll.Count == 0)
            {
                // Nothing to archive; this is not an error
                ReportMessage("Data Package " + dataPkgInfo.ID + " does not have any files; nothing to archive", BaseLogger.LogLevels.DEBUG);
                return new List<FileInfoObject>();
            }

            // Look for any Auto-process directories that have recently modified files
            // These directories will be skipped until the files are at least 4 hours old
            // This list contains directory paths to skip
            var dataPackageDirectoriesToSkip = new List<string>();

            var dataPackageDirectories = dataPkg.GetDirectories("*", SearchOption.AllDirectories).ToList();
            var autoJobDirectoryMatcher = new Regex(@"^[A-Z].{1,5}\d{12,12}_Auto\d+", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            foreach (var dataPkgDirectory in dataPackageDirectories)
            {
                if (!autoJobDirectoryMatcher.IsMatch(dataPkgDirectory.Name))
                {
                    continue;
                }

                var skipDataPkg = false;
                foreach (var file in dataPkgDirectory.GetFiles("*.*", SearchOption.AllDirectories))
                {
                    if (DateTime.UtcNow.Subtract(file.LastWriteTimeUtc).TotalHours < 4)
                    {
                        skipDataPkg = true;
                        break;
                    }
                }

                if (skipDataPkg)
                    dataPackageDirectoriesToSkip.Add(dataPkgDirectory.FullName);
            }

            // Filter out files that we do not want to archive
            var filesToSkip = new SortedSet<string>(StringComparer.CurrentCultureIgnoreCase)
            {
                "Thumbs.db",
                ".DS_Store",
                ".Rproj.user"
            };

            // Also filter out Thermo .raw files, mzML files, etc.
            var extensionsToSkip = new SortedSet<string>(StringComparer.CurrentCultureIgnoreCase)
            {
                ".raw",
                ".mzXML",
                ".mzML"
            };

            var dataPackageFiles = new List<FileInfo>();

            foreach (var dataPkgFile in dataPackageFilesAll)
            {
                if (!FilePassesFilters(filesToSkip, extensionsToSkip, dataPkgFile))
                    continue;

                if (dataPkgFile.Directory != null)
                {
                    // Skip the file if it is in one of the directories in dataPackageDirectoriesToSkip
                    var keep = dataPackageDirectoriesToSkip.All(dataPkgDirectory => !dataPkgFile.Directory.FullName.StartsWith(dataPkgDirectory));

                    if (!keep)
                        continue;
                }

                dataPackageFiles.Add(dataPkgFile);
            }

            if (dataPackageFiles.Count > MAX_FILES_TO_ARCHIVE)
            {
                ReportError(string.Format(
                    " Data Package {0} has {1} files;" +
                    " the maximum number of files allowed in MyEMSL per data package is {2};" +
                    " zip up groups of files to reduce the total file count; see {3}",
                    dataPkgInfo.ID, dataPackageFiles.Count, MAX_FILES_TO_ARCHIVE, dataPkgInfo.SharePath));

                return new List<FileInfoObject>();
            }

            if (dataPackageFiles.Count == 0)
            {
                // Nothing to archive; this is not an error
                var msg = " Data Package " + dataPkgInfo.ID + " has " + dataPackageFilesAll.Count + " files, but all have been skipped";

                if (dataPackageDirectoriesToSkip.Count > 0)
                    msg += " due to recently modified files in auto-job result directories";
                else
                    msg += " since they are system or temporary files";

                ReportMessage(msg + "; nothing to archive");
                ReportMessage(" Data Package " + dataPkgInfo.ID + " path: " + dataPkg.FullName, BaseLogger.LogLevels.DEBUG);
                return new List<FileInfoObject>();
            }

            // Make sure at least one of the files was modified after the date threshold
            var passesDateThreshold = dataPackageFiles.Any(localFile => localFile.LastWriteTime >= dateThreshold);

            if (!passesDateThreshold)
            {
                // None of the modified files passes the date threshold
                string msg;

                if (dataPackageFilesAll.Count == 1)
                    msg = " Data Package " + dataPkgInfo.ID + " has 1 file, but it was modified before " + dateThreshold.ToString("yyyy-MM-dd");
                else
                    msg = " Data Package " + dataPkgInfo.ID + " has " + dataPackageFilesAll.Count + " files, but all were modified before " + dateThreshold.ToString("yyyy-MM-dd");

                ReportMessage(msg + "; nothing to archive", BaseLogger.LogLevels.DEBUG);
                return new List<FileInfoObject>();
            }

            // Note: subtracting 60 seconds from UtcNow when initializing lastProgress so that a progress message will appear as an "INFO" level log message if 5 seconds elapses
            // After that, the INFO level messages will appear every 30 seconds
            var lastProgress = DateTime.UtcNow.AddSeconds(-60);
            var lastProgressDetail = DateTime.UtcNow;

            var filesProcessed = 0;

            foreach (var localFile in dataPackageFiles)
            {
                // Note: when storing data package files in MyEMSL the SubDir path will always start with the data package directory name
                var subDir = string.Copy(uploadInfo.SubDir);

                if (localFile.Directory != null && localFile.Directory.FullName.Length > dataPkg.FullName.Length)
                {
                    // Append the subdirectory path
                    subDir = Path.Combine(subDir, localFile.Directory.FullName.Substring(dataPkg.FullName.Length + 1));
                }

                // Look for this file in MyEMSL
                var archiveFiles = dataPackageInfoCache.FindFiles(localFile.Name, subDir, dataPkgInfo.ID, recurse: false);

                if (dataPkg.Parent == null)
                    throw new DirectoryNotFoundException("Unable to determine the parent directory of " + dataPkg.FullName);

                // Possibly add the file to datasetFilesToArchive
                AddFileIfArchiveRequired(dataPkg, ref uploadInfo, datasetFilesToArchive, localFile, archiveFiles);

                filesProcessed++;
                if (DateTime.UtcNow.Subtract(lastProgressDetail).TotalSeconds < 5)
                    continue;

                lastProgressDetail = DateTime.UtcNow;

                var progressMessage = "Finding files to archive for Data Package " + dataPkgInfo.ID + ": " + filesProcessed + " / " +
                                      dataPackageFiles.Count;

                if (DateTime.UtcNow.Subtract(lastProgress).TotalSeconds >= 30)
                {
                    lastProgress = DateTime.UtcNow;
                    ReportMessage(progressMessage);
                }
                else
                {
                    ReportMessage(progressMessage, BaseLogger.LogLevels.DEBUG);
                }
            }

            if (datasetFilesToArchive.Count == 0)
            {
                // Nothing to archive; this is not an error
                ReportMessage(" All files for Data Package " + dataPkgInfo.ID + " are already in MyEMSL; FileCount=" + dataPackageFiles.Count, BaseLogger.LogLevels.DEBUG);
                return datasetFilesToArchive;
            }

            return datasetFilesToArchive;
        }

        private IEnumerable<DataPackageInfo> GetFilteredDataPackageInfoList(
            IEnumerable<DataPackageInfo> dataPkgInfo,
            IEnumerable<int> dataPkgGroup)
        {
            return (from item in dataPkgInfo
                    join dataPkgID in dataPkgGroup on item.ID equals dataPkgID
                    select item).ToList();
        }

        private Dictionary<int, MyEMSLStatusInfo> GetStatusURIs(int retryCount)
        {
            var statusURIs = new Dictionary<int, MyEMSLStatusInfo>();
            var dateThreshold = DateTime.Now.AddDays(-45);

            var sql = string.Format(
                " SELECT MU.Entry_ID, MU.Data_Package_ID, MU.Entered, MU.StatusNum, MU.Status_URI, DP.Local_Path, DP.Share_Path, DP.Owner " +
                " FROM V_MyEMSL_Uploads MU INNER JOIN V_Data_Package_Export DP ON MU.Data_Package_ID = DP.ID" +
                " WHERE MU.ErrorCode = 0 AND " +
                "       (MU.Available = 0 Or MU.Verified = 0) AND " +
                "       ISNULL(MU.StatusNum, 0) > 0 AND" +
                "       Entered >= '{0:yyyy-MM-dd}'",
                dateThreshold);

            var success = mDBTools.GetQueryResultsDataTable(sql, out var table, retryCount);

            if (!success)
                OnWarningEvent("GetQueryResultsDataTable reported false querying V_MyEMSL_Uploads");

            foreach (DataRow row in table.Rows)
            {
                var statusInfo = new MyEMSLStatusInfo
                {
                    EntryID = row[0].CastDBVal<int>(),
                    DataPackageID = row[1].CastDBVal<int>(),
                    Entered = row[2].CastDBVal(DateTime.Now)
                };

                var statusNum = row[3].CastDBVal<int>();

                if (statusURIs.ContainsKey(statusNum))
                {
                    var msg = "Error, StatusNum " + statusNum + " is defined for multiple data packages";
                    ReportError(msg, true);
                    continue;
                }

                var statusURI = row[4].CastDBVal(string.Empty);
                if (!string.IsNullOrWhiteSpace(statusURI))
                {
                    statusInfo.StatusURI = statusURI;

                    statusInfo.SharePath = row["Share_Path"].CastDBVal<string>();
                    statusInfo.LocalPath = row["Local_Path"].CastDBVal<string>();
                    statusInfo.DataPackageOwner = row["Owner"].CastDBVal<string>();

                    statusURIs.Add(statusNum, statusInfo);
                }
            }

            return statusURIs;
        }

        private void Initialize()
        {
            ErrorMessage = string.Empty;
            mLastStatusUpdate = DateTime.UtcNow;

            // Set up the loggers
            const string logFileNameBase = @"Logs\DataPkgArchiver";

            LogTools.CreateFileLogger(logFileNameBase, LogLevel);

            LogTools.CreateDbLogger(DBConnectionString, "DataPkgArchiver: " + Environment.MachineName);

            // Make initial log entry
            var msg = "=== Started Data Package Archiver V" + System.Reflection.Assembly.GetExecutingAssembly().GetName().Version + " ===== ";
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO, msg);
        }

        private List<DataPackageInfo> LookupDataPkgInfo(IReadOnlyList<KeyValuePair<int, int>> dataPkgIDs)
        {
            var dataPkgInfo = new List<DataPackageInfo>();
            var sql = new StringBuilder();

            sql.Append(" SELECT ID, Name, Owner, Instrument, " +
                       " EUS_Person_ID, EUS_Proposal_ID, EUS_Instrument_ID, Created, " +
                       " Package_File_Folder, Share_Path, Local_Path, MyEMSL_Uploads " +
                       " FROM V_Data_Package_Export");

            if (dataPkgIDs.Count > 0)
            {
                sql.Append(" WHERE ");

                for (var i = 0; i < dataPkgIDs.Count; i++)
                {
                    if (i > 0)
                    {
                        sql.Append(" OR ");
                    }

                    if (dataPkgIDs[i].Key == dataPkgIDs[i].Value)
                    {
                        sql.AppendFormat("ID = {0}", dataPkgIDs[i].Key);
                    }
                    else if (dataPkgIDs[i].Value < 0)
                    {
                        sql.AppendFormat("ID >= {0}", dataPkgIDs[i].Key);
                    }
                    else
                    {
                        sql.AppendFormat("ID BETWEEN {0} AND {1}", dataPkgIDs[i].Key, dataPkgIDs[i].Value);
                    }
                }
            }

            sql.Append(" ORDER BY ID");

            var success = mDBTools.GetQueryResultsDataTable(sql.ToString(), out var table, retryCount: 1);

            if (!success)
                OnWarningEvent("GetQueryResultsDataTable reported false querying V_Data_Package_Export");

            foreach (DataRow row in table.Rows)
            {
                var dataPkgID = row[0].CastDBVal<int>();

                var dataPkg = new DataPackageInfo(dataPkgID)
                {
                    Name = row["Name"].CastDBVal<string>(),
                    OwnerPRN = row["Owner"].CastDBVal<string>(),
                    OwnerEUSID = row["EUS_Person_ID"].CastDBVal<int>(),
                    EUSProposalID = row["EUS_Proposal_ID"].CastDBVal<string>(),
                    EUSInstrumentID = row["EUS_Instrument_ID"].CastDBVal<int>(),
                    InstrumentName = row["Instrument"].CastDBVal<string>(),
                    Created = row["Created"].CastDBVal(DateTime.Now),
                    DirectoryName = row["Package_File_Folder"].CastDBVal<string>(),
                    SharePath = row["Share_Path"].CastDBVal<string>(),
                    LocalPath = row["Local_Path"].CastDBVal<string>(),
                    MyEMSLUploads = row["MyEMSL_Uploads"].CastDBVal<int>()
                };

                dataPkgInfo.Add(dataPkg);
            }

            return dataPkgInfo;
        }

        /// <summary>
        /// Parses a list of data package IDs (or ID ranges) separated commas
        /// </summary>
        /// <remarks>
        /// To indicate a range of 300 or higher, use "300-"
        /// In that case, the KeyValuePair will be (300,-1)</remarks>
        /// <param name="dataPkgIDList"></param>
        /// <returns>List of KeyValue pairs where the key is the start ID and the value is the end ID</returns>
        public List<KeyValuePair<int, int>> ParseDataPkgIDList(string dataPkgIDList)
        {
            var values = dataPkgIDList.Split(',').ToList();
            var dataPkgIDs = new List<KeyValuePair<int, int>>();

            foreach (var item in values)
            {
                if (string.IsNullOrWhiteSpace(item))
                {
                    continue;
                }

                string startID;
                string endID;

                // Check for a range of data package IDs
                var dashIndex = item.IndexOf("-", StringComparison.Ordinal);

                if (dashIndex == 0)
                {
                    if (item.Length == 1)
                    {
                        ReportError("Invalid item; ignoring: " + item);
                        continue;
                    }

                    // Range is 0 through the number after the dash
                    startID = "0";
                    endID = item.Substring(dashIndex + 1);
                }
                else if (dashIndex > 0)
                {
                    // Range is 0 through the number after the dash
                    startID = item.Substring(0, dashIndex);

                    if (dashIndex == item.Length - 1)
                        endID = "-1";
                    else
                        endID = item.Substring(dashIndex + 1);
                }
                else
                {
                    startID = item;
                    endID = item;
                }

                if (int.TryParse(startID, out var dataPkgIDStart) && int.TryParse(endID, out var dataPkgIDEnd))
                {
                    var idRange = new KeyValuePair<int, int>(dataPkgIDStart, dataPkgIDEnd);

                    if (!dataPkgIDs.Contains(idRange))
                        dataPkgIDs.Add(idRange);
                }
                else
                {
                    ReportError("Value is not an integer or range of integers; ignoring: " + item);
                }
            }

            return dataPkgIDs;
        }

        /// <summary>
        /// Update the data packages in dataPkgIDs
        /// </summary>
        /// <param name="dataPkgIDs"></param>
        /// <param name="dateThreshold"></param>
        /// <returns>True if successful, false if an error</returns>
        public bool ProcessDataPackages(List<KeyValuePair<int, int>> dataPkgIDs, DateTime dateThreshold)
        {
            List<DataPackageInfo> dataPkgInfo;
            var successCount = 0;

            try
            {
                dataPkgInfo = LookupDataPkgInfo(dataPkgIDs);

                if (dataPkgInfo.Count == 0)
                {
                    ReportError("None of the data packages in dataPkgIDs corresponded to a known data package ID");
                    return false;
                }

                if (!PreviewMode)
                {
                    ReportMessage(@"Pushing data into MyEMSL as user pnl\" + Environment.UserName);
                }

                // List of groups of data package IDs
                var dataPkgGroups = new List<List<int>>();
                var currentGroup = new List<int>();
                var lastProgress = DateTime.UtcNow.AddSeconds(-10);
                var lastSimpleSearchVerify = DateTime.MinValue;

                var runningCount = 0;

                ReportMessage("Finding data package files for " + dataPkgInfo.Count + " data packages");

                // Determine the number of files that are associated with each data package
                // We will use this information to process the data packages in chunks
                for (var i = 0; i < dataPkgInfo.Count; i++)
                {
                    var fileCount = CountFilesForDataPackage(dataPkgInfo[i]);

                    if (runningCount + fileCount > 5000 || currentGroup.Count >= 30)
                    {
                        // Store the current group
                        if (currentGroup.Count > 0)
                            dataPkgGroups.Add(currentGroup);

                        // Make a new group
                        currentGroup = new List<int>
                        {
                            dataPkgInfo[i].ID
                        };
                        runningCount = fileCount;
                    }
                    else
                    {
                        // Use the current group
                        currentGroup.Add(dataPkgInfo[i].ID);
                        runningCount += fileCount;
                    }

                    if (DateTime.UtcNow.Subtract(lastProgress).TotalSeconds >= 20)
                    {
                        lastProgress = DateTime.UtcNow;
                        ReportMessage("Finding data package files; examined " + i + " of " + dataPkgInfo.Count + " data packages");
                    }
                }

                // Store the current group
                if (currentGroup.Count > 0)
                    dataPkgGroups.Add(currentGroup);

                var groupNumber = 0;

                foreach (var dataPkgGroup in dataPkgGroups)
                {
                    groupNumber++;

                    var dataPackageInfoCache = new DataPackageListInfo
                    {
                        ReportMetadataURLs = TraceMode || LogLevel == BaseLogger.LogLevels.DEBUG,
                        ThrowErrors = true,
                        TraceMode = TraceMode
                    };

                    RegisterEvents(dataPackageInfoCache);

                    foreach (var dataPkgID in dataPkgGroup)
                    {
                        dataPackageInfoCache.AddDataPackage(dataPkgID);
                    }

                    if (!PreviewMode && DateTime.UtcNow.Subtract(lastSimpleSearchVerify).TotalMinutes > 15)
                    {
                        // Verify that MyEMSL is returning expected results for data packages known to have been stored previously in MyEMSL
                        // If no results are found, we will presume that MyEMSL is not available, or is available but is misconfigured (or has permissions issues)
                        var success = VerifyKnownMyEMSLSearchResults();

                        // Continue only if previewing the data or if known MyEMSL results were found
                        // If known results were not found, we don't want to risk pushing 1000's of data package files into MyEMSL when those files likely are already stored in MyEMSL
                        if (!success)
                        {
                            ReportMessage("Aborting processing of data packages since the Metadata server is not available");
                            return false;
                        }

                        lastSimpleSearchVerify = DateTime.UtcNow;
                    }

                    // Pre-populate dataPackageInfoCache with the files for the current group
                    ReportMessage("Querying MyEMSL for " + dataPkgGroup.Count + " data packages in group " + groupNumber + " of " + dataPkgGroups.Count);
                    dataPackageInfoCache.RefreshInfo();

                    // Obtain the DataPackageInfo objects for the IDs in dataPkgGroup
                    var filteredDataPkgInfo = GetFilteredDataPackageInfoList(dataPkgInfo, dataPkgGroup);

                    foreach (var dataPkg in filteredDataPkgInfo)
                    {
                        var success = ProcessOneDataPackage(dataPkg, dateThreshold, dataPackageInfoCache);

                        if (success)
                            successCount++;
                    }
                }
            }
            catch (Exception ex)
            {
                ReportError("Error in ProcessDataPackages: " + ex.Message, true, ex);

                // Include the stack trace in the log
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Detail for error in ProcessDataPackages", ex);

                return false;
            }

            if (successCount == dataPkgInfo.Count)
            {
                if (successCount == 1)
                    ReportMessage("Processing complete for Data Package " + dataPkgInfo.First().ID);
                else
                    ReportMessage("Processed " + successCount + " data packages");

                // Wait 3 seconds then continue
                // The purpose for the wait is to give MyEMSL time to process the newly ingested files;
                // if the files are small, they may be fully processed and stored before the call to VerifyUploadStatus
                System.Threading.Thread.Sleep(3000);

                return true;
            }

            if (PreviewMode)
                return true;

            if (dataPkgInfo.Count == 1)
                ReportError("Failed to archive Data Package " + dataPkgIDs.First());
            else if (successCount == 0)
                ReportError("Failed to archive any of the " + dataPkgIDs.Count + " candidate data packages", true);
            else
                ReportError("Failed to archive " + (dataPkgInfo.Count - successCount) + " data package(s); successfully archived " + successCount + " data package(s)", true);

            return false;
        }

        private bool ProcessOneDataPackage(
            DataPackageInfo dataPkgInfo,
            DateTime dateThreshold,
            DataPackageListInfo dataPackageInfoCache)
        {
            var success = false;
            var uploadInfo = new MyEMSLUploadInfo();
            uploadInfo.Clear();

            var startTime = DateTime.UtcNow;

            try
            {
                var dataPkg = new DirectoryInfo(dataPkgInfo.LocalPath);
                if (!dataPkg.Exists)
                    dataPkg = new DirectoryInfo(dataPkgInfo.SharePath);

                if (!dataPkg.Exists)
                {
                    ReportMessage("Data package directory not found (also tried remote share path): " + dataPkgInfo.LocalPath, BaseLogger.LogLevels.WARN);
                    return false;
                }

                if (dataPkg.Parent == null)
                    throw new DirectoryNotFoundException("Unable to determine the parent directory of " + dataPkg.FullName);

                // Look for an existing metadata file
                // For example, \\protoapps\DataPkgs\Public\2014\MyEMSL_metadata_CaptureJob_1055.txt
                var metadataFilePath = Path.Combine(dataPkg.Parent.FullName, Utilities.GetMetadataFilenameForJob(dataPkgInfo.ID.ToString(CultureInfo.InvariantCulture)));
                var metadataFile = new FileInfo(metadataFilePath);
                if (metadataFile.Exists)
                {
                    if (DateTime.UtcNow.Subtract(metadataFile.LastWriteTimeUtc).TotalHours >= 48)
                    {
                        if (DateTime.UtcNow.Subtract(metadataFile.LastWriteTimeUtc).TotalDays > 6.5)
                        {
                            ReportError(string.Format(
                                "Data Package {0} has an existing metadata file over 6.5 days old; deleting file: {1}",
                                dataPkgInfo.ID , metadataFile.FullName), true);
                            metadataFile.Delete();
                        }
                        else
                        {
                            // This is likely an error, but we don't want to re-upload the files yet
                            // Log an error to the database
                            ReportError(string.Format(
                                "Data Package {0} has an existing metadata file between 2 and 6.5 days old: {1}",
                                dataPkgInfo.ID, metadataFile.FullName), true);

                            // This is not a fatal error; return true
                            return true;
                        }
                    }
                    else
                    {
                        ReportMessage(string.Format(
                            "Data Package {0} has an existing metadata file less than 48 hours old in {1}; skipping this data package",
                            dataPkgInfo.ID, dataPkg.Parent.FullName), BaseLogger.LogLevels.WARN);

                        ReportMessage("  " + metadataFile.FullName, BaseLogger.LogLevels.DEBUG);

                        // This is not a fatal error; return true
                        return true;
                    }
                }

                // Compare the files to those already in MyEMSL to create a list of files to be uploaded
                var unmatchedFiles = FindDataPackageFilesToArchive(
                    dataPkgInfo,
                    dataPkg,
                    dateThreshold,
                    dataPackageInfoCache,
                    out uploadInfo);

                if (unmatchedFiles.Count == 0)
                {
                    // Nothing to do
                    return true;
                }

                // Check whether the MyEMSL Metadata query returned results
                // If it did not, either this is a new data package, or we had a query error
                var archiveFileCountExisting = dataPackageInfoCache.FindFiles("*", "", dataPkgInfo.ID).Count;
                if (archiveFileCountExisting == 0)
                {
                    // Data package not in MyEMSL (or the files reported by it were filtered out by the reader)
                    // See if DMS is tracking that this data package was, in fact, uploaded to DMS at some point in time
                    // This is tracked by table T_MyEMSL_Uploads, examining rows where ErrorCode is 0 and FileCountNew or FileCountUpdated are positive

                    if (dataPkgInfo.MyEMSLUploads > 0)
                    {
                        var logToDB = !PreviewMode;

                        ReportMessage(
                            "Data package " + dataPkgInfo.ID +
                            " was previously uploaded to MyEMSL, yet the Metadata query did not return any files for this dataset." +
                            " Skipping this data package to prevent the addition of duplicate files to MyEMSL." +
                            " To allow this upload, change ErrorCode to 101 in DMS_Data_Package.dbo.T_MyEMSL_Uploads"
                            ,
                            BaseLogger.LogLevels.ERROR, logToDB);

                        return false;
                    }
                }

                if (PreviewMode)
                {
                    ReportMessage("Need to upload " + unmatchedFiles.Count + " file(s) for Data Package " + dataPkgInfo.ID);

                    // Preview the changes
                    foreach (var unmatchedFile in unmatchedFiles)
                    {
                        Console.WriteLine("  " + unmatchedFile.RelativeDestinationFullPath);
                    }
                }
                else
                {
                    // Upload the files
                    ReportMessage("Uploading " + unmatchedFiles.Count + " new/changed files for Data Package " + dataPkgInfo.ID);

                    var uploadMetadata = new Upload.UploadMetadata();
                    uploadMetadata.Clear();

                    uploadMetadata.DataPackageID = dataPkgInfo.ID;
                    uploadMetadata.SubFolder = uploadInfo.SubDir;

                    if (dataPkgInfo.OwnerEUSID == 0)
                    {
                        OnWarningEvent("Data package owner (" + dataPkgInfo.OwnerPRN + ") does not have an EUS_PersonID; using " + Upload.DEFAULT_EUS_OPERATOR_ID);
                        uploadMetadata.EUSOperatorID = Upload.DEFAULT_EUS_OPERATOR_ID;
                    }
                    else
                    {
                        // ReSharper disable once StringLiteralTypo
                        if (dataPkgInfo.OwnerEUSID == 52259 && dataPkgInfo.OwnerPRN.EndsWith("SWEN778", StringComparison.OrdinalIgnoreCase))
                        {
                            // This user has two EUS IDs and MyEMSL only recognizes the first one
                            // Override the EUS ID
                            uploadMetadata.EUSOperatorID = 45413;
                        }
                        else
                        {
                            uploadMetadata.EUSOperatorID = dataPkgInfo.OwnerEUSID;
                        }
                    }

                    if (string.IsNullOrWhiteSpace(dataPkgInfo.EUSProposalID))
                    {
                        OnWarningEvent("Data package does not have an associated EUS Proposal; using " + Upload.DEFAULT_EUS_PROJECT_ID);
                        uploadMetadata.EUSProjectID = Upload.DEFAULT_EUS_PROJECT_ID;
                    }
                    else
                    {
                        uploadMetadata.EUSProjectID = dataPkgInfo.EUSProposalID;
                    }

                    if (dataPkgInfo.EUSInstrumentID <= 0)
                    {
                        OnWarningEvent("Data package does not have an associated EUS Instrument ID; using " + Upload.UNKNOWN_INSTRUMENT_EUS_INSTRUMENT_ID);
                        uploadMetadata.EUSInstrumentID = Upload.UNKNOWN_INSTRUMENT_EUS_INSTRUMENT_ID;
                    }
                    else
                    {
                        uploadMetadata.EUSInstrumentID = dataPkgInfo.EUSInstrumentID;
                    }

                    if (string.IsNullOrWhiteSpace(dataPkgInfo.InstrumentName))
                    {
                        OnWarningEvent("Data package does not have an associated Instrument Name; using " + Upload.UNKNOWN_INSTRUMENT_NAME);
                        uploadMetadata.DMSInstrumentName = Upload.UNKNOWN_INSTRUMENT_NAME;
                    }
                    else
                    {
                        uploadMetadata.DMSInstrumentName = dataPkgInfo.InstrumentName;
                    }

                    // Instantiate the metadata object
                    var metadataObject = Upload.CreatePacificaMetadataObject(uploadMetadata, unmatchedFiles, out _);

                    var metadataDescription = Upload.GetMetadataObjectDescription(metadataObject);
                    ReportMessage("UploadMetadata: " + metadataDescription);

                    mMyEMSLUploader.TransferFolderPath = dataPkg.Parent.FullName;
                    mMyEMSLUploader.JobNumber = dataPkgInfo.ID.ToString();

                    success = mMyEMSLUploader.StartUpload(metadataObject, out var statusURL);

                    var tsElapsedTime = DateTime.UtcNow.Subtract(startTime);

                    uploadInfo.UploadTimeSeconds = tsElapsedTime.TotalSeconds;
                    uploadInfo.StatusURI = statusURL;

                    if (success)
                    {
                        var msg = "Upload of changed files for Data Package " + dataPkgInfo.ID + " completed in " + tsElapsedTime.TotalSeconds.ToString("0.0") + " seconds: " + uploadInfo.FileCountNew + " new files, " + uploadInfo.FileCountUpdated + " updated files, " + uploadInfo.Bytes + " bytes";
                        ReportMessage(msg);

                        var statusUriMsg = "  myEMSL statusURI => " + uploadInfo.StatusURI;
                        ReportMessage(statusUriMsg, BaseLogger.LogLevels.DEBUG);

                        ReportMessage(string.Format(
                            "EUS metadata: Instrument ID {0}, Project ID {1}, Uploader ID {2}",
                            uploadMetadata.EUSInstrumentID,
                            uploadMetadata.EUSProjectID,
                            uploadMetadata.EUSOperatorID));   // aka EUSUploaderID or EUSSubmitterID
                    }
                    else
                    {
                        ReportError("Upload of changed files for Data Package " + dataPkgInfo.ID + " failed: " + mMyEMSLUploader.ErrorMessage, true);

                        uploadInfo.ErrorCode = mMyEMSLUploader.ErrorMessage.GetHashCode();
                        if (uploadInfo.ErrorCode == 0)
                            uploadInfo.ErrorCode = 1;
                    }

                    // Post the StatusURI info to the database
                    StoreMyEMSLUploadStats(dataPkgInfo, uploadInfo);
                }
                return success;
            }
            catch (Exception ex)
            {
                ReportError("Error in ProcessOneDataPackage processing Data Package " + dataPkgInfo.ID + ": " + ex.Message, true, ex);

                // Include the stack trace in the log
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Detail for error in ProcessOneDataPackage for Data Package " + dataPkgInfo.ID, ex);

                uploadInfo.ErrorCode = ex.Message.GetHashCode();
                if (uploadInfo.ErrorCode == 0)
                    uploadInfo.ErrorCode = 1;

                uploadInfo.UploadTimeSeconds = DateTime.UtcNow.Subtract(startTime).TotalSeconds;

                StoreMyEMSLUploadStats(dataPkgInfo, uploadInfo);

                return false;
            }
        }

        private void ReportMessage(
            string message,
            BaseLogger.LogLevels logLevel = BaseLogger.LogLevels.INFO,
            bool logToDB = false)
        {
            if (logToDB)
                LogTools.WriteLog(LogTools.LoggerTypes.LogDb, logLevel, message.Trim());
            else
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, logLevel, message);

            switch (logLevel)
            {
                case BaseLogger.LogLevels.DEBUG:
                    OnDebugEvent(message);
                    break;
                case BaseLogger.LogLevels.ERROR:
                    OnErrorEvent(message);
                    break;
                case BaseLogger.LogLevels.WARN:
                    OnWarningEvent(message);
                    break;
                default:
                    OnStatusEvent(message);
                    break;
            }
        }

        private void ReportError(string message, bool logToDB = false, Exception ex = null)
        {
            OnErrorEvent(message, ex);

            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, message);

            if (logToDB)
                LogTools.WriteLog(LogTools.LoggerTypes.LogDb, BaseLogger.LogLevels.ERROR, message.Trim());

            ErrorMessage = string.Copy(message);
        }

        /// <summary>
        /// Update the data packages in dataPkgIDs, then verify the upload status
        /// </summary>
        /// <param name="dataPkgIDs"></param>
        /// <param name="dateThreshold"></param>
        /// <param name="previewMode"></param>
        /// <returns>True if successful, false if an error</returns>
        public bool StartProcessing(List<KeyValuePair<int, int>> dataPkgIDs, DateTime dateThreshold, bool previewMode)
        {
            PreviewMode = previewMode;

            // Upload new data
            var success = ProcessDataPackages(dataPkgIDs, dateThreshold);

            if (DisableVerify)
                return success;

            // Verify uploaded data (even if success is false)
            VerifyUploadStatus();

            return success;
        }

        // ReSharper disable once UnusedMethodReturnValue.Local
        private bool StoreMyEMSLUploadStats(DataPackageInfo dataPkgInfo, MyEMSLUploadInfo uploadInfo)
        {
            try
            {
                // Setup for execution of the stored procedure
                var cmd = mDBTools.CreateCommand(SP_NAME_STORE_MYEMSL_STATS, CommandType.StoredProcedure);

                mDBTools.AddParameter(cmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);
                mDBTools.AddParameter(cmd, "@DataPackageID", SqlType.Int).Value = Convert.ToInt32(dataPkgInfo.ID);
                mDBTools.AddParameter(cmd, "@Subfolder", SqlType.VarChar, 128, uploadInfo.SubDir);
                mDBTools.AddParameter(cmd, "@FileCountNew", SqlType.Int).Value = uploadInfo.FileCountNew;
                mDBTools.AddParameter(cmd, "@FileCountUpdated", SqlType.Int).Value = uploadInfo.FileCountUpdated;
                mDBTools.AddParameter(cmd, "@Bytes", SqlType.BigInt).Value = uploadInfo.Bytes;
                mDBTools.AddParameter(cmd, "@UploadTimeSeconds", SqlType.Real).Value = (float)uploadInfo.UploadTimeSeconds;
                mDBTools.AddParameter(cmd, "@StatusURI", SqlType.VarChar, 255, uploadInfo.StatusURI);
                mDBTools.AddParameter(cmd, "@ErrorCode", SqlType.Int).Value = uploadInfo.ErrorCode;

                ReportMessage("Calling " + SP_NAME_STORE_MYEMSL_STATS + " for Data Package " + dataPkgInfo.ID, BaseLogger.LogLevels.DEBUG);

                // Execute the SP (retry the call up to 4 times)
                cmd.CommandTimeout = 20;
                var resCode = mDBTools.ExecuteSP(cmd, 4);

                if (resCode == 0)
                {
                    return true;
                }

                var msg = "Error " + resCode + " storing MyEMSL Upload Stats";
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, msg);
                return false;
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Exception storing the MyEMSL upload stats: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Update values for Available and Verified in the Data_Package database
        /// </summary>
        /// <param name="statusInfo"></param>
        /// <param name="verified"></param>
        /// <returns>Assumes that Available = true</returns>
        // ReSharper disable once UnusedMethodReturnValue.Local
        private bool UpdateMyEMSLUploadStatus(MyEMSLStatusInfo statusInfo, bool verified)
        {
            try
            {
                var cmd = mDBTools.CreateCommand(SP_NAME_SET_MYEMSL_UPLOAD_STATUS, CommandType.StoredProcedure);

                mDBTools.AddParameter(cmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);
                mDBTools.AddTypedParameter(cmd, "@EntryID", SqlType.Int, value: statusInfo.EntryID);
                mDBTools.AddTypedParameter(cmd, "@DataPackageID", SqlType.Int, value: statusInfo.DataPackageID);
                mDBTools.AddTypedParameter(cmd, "@Available", SqlType.TinyInt, value: BoolToTinyInt(true));
                mDBTools.AddTypedParameter(cmd, "@Verified", SqlType.TinyInt, value: BoolToTinyInt(verified));
                mDBTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.Output);

                if (PreviewMode)
                {
                    Console.WriteLine("Simulate call to " + SP_NAME_SET_MYEMSL_UPLOAD_STATUS + " for Entry_ID=" + statusInfo.EntryID + ", DataPackageID=" + statusInfo.DataPackageID + " Entry_ID=" + statusInfo.EntryID);
                    return true;
                }

                ReportMessage("  Calling " + SP_NAME_SET_MYEMSL_UPLOAD_STATUS + " for Data Package " + statusInfo.DataPackageID, BaseLogger.LogLevels.DEBUG);

                cmd.CommandTimeout = 20;
                var resCode = mDBTools.ExecuteSP(cmd, 2);

                if (resCode == 0)
                    return true;

                var msg = "Error " + resCode + " calling stored procedure " + SP_NAME_SET_MYEMSL_UPLOAD_STATUS;
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, msg);
                return false;
            }
            catch (Exception ex)
            {
                const string msg = "Exception calling stored procedure " + SP_NAME_SET_MYEMSL_UPLOAD_STATUS;
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, msg, ex);
                return false;
            }
        }

        /// <summary>
        /// Search MyEMSL for expected filenames for specific data packages
        /// </summary>
        /// <returns>True if the expected results are found or if SkipCheckExisting is true; otherwise False</returns>
        public bool VerifyKnownMyEMSLSearchResults()
        {
            if (SkipCheckExisting)
            {
                OnWarningEvent("Skipping check for known data package files");
                return true;
            }

            try
            {
                // Keys in this dictionary are data package IDs.  Values are some of the expected filenames that MyEMSL should return
                var dataPackageIDs = new Dictionary<int, List<string>>();
                VerifyKnownResultsAddExpectedFiles(dataPackageIDs, 593, new List<string> { "PX_Submission_2015-10-09_16-01.px" });
                VerifyKnownResultsAddExpectedFiles(dataPackageIDs, 721, new List<string> { "AScore_AnalysisSummary.txt", "JobParameters_995425.xml", "T_Filtered_Results.txt" });
                VerifyKnownResultsAddExpectedFiles(dataPackageIDs, 1034, new List<string> { "MasterWorkflowSyn.xml", "T_Reporter_Ions_Typed.txt" });
                VerifyKnownResultsAddExpectedFiles(dataPackageIDs, 1376, new List<string> { "Concatenated_msgfdb_syn_plus_ascore.txt", "AScore_CID_0.5Da_ETD_0.5Da_HCD_0.05Da.xml" });
                VerifyKnownResultsAddExpectedFiles(dataPackageIDs, 1512, new List<string> { "AScore_CID_0.5Da_ETD_0.5Da_HCD_0.05Da.xml", "Job_to_Dataset_Map.txt" });

                var dataPackageListInfo = new DataPackageListInfo
                {
                    ReportMetadataURLs = TraceMode || LogLevel == BaseLogger.LogLevels.DEBUG,
                    ThrowErrors = true,
                    TraceMode = TraceMode
                };

                RegisterEvents(dataPackageListInfo);

                // Lookup the files tracked by MyEMSL for data packages created between 2012 and 2016
                foreach (var dataPkg in dataPackageIDs)
                {
                    dataPackageListInfo.AddDataPackage(dataPkg.Key);
                }

                var archiveFiles = dataPackageListInfo.FindFiles("*");

                if (archiveFiles.Count == 0)
                {
                    ReportError("MyEMSL did not return any files for the known data packages (" + dataPackageIDs.First().Key + "-" + dataPackageIDs.Last().Key + "); " +
                                "the Metadata service is likely disabled or broken at present.", true);
                    return false;
                }

                var dataPkgMissingFiles = new Dictionary<int, List<string>>();

                // Check for known files from each of the data packages
                foreach (var dataPkg in dataPackageIDs)
                {
                    var foundFiles = (from item in archiveFiles where item.FileInfo.DataPackageID == dataPkg.Key select item.FileInfo).ToList();

                    var missingFiles = new List<string>();

                    foreach (var fileName in dataPkg.Value)
                    {
                        var fileMatch = (from item in foundFiles where string.Equals(item.Filename, fileName) select item).ToList();

                        if (fileMatch.Count < 1)
                        {
                            missingFiles.Add(fileName);
                        }
                    }

                    if (missingFiles.Count > 0)
                        dataPkgMissingFiles.Add(dataPkg.Key, missingFiles);
                }

                if (dataPkgMissingFiles.Count == 0)
                {
                    return true;
                }

                if (dataPkgMissingFiles.Count == dataPackageIDs.Count)
                {
                    ReportError("MyEMSL did not return the expected files for any of the data packages; some search results were returned but none of the expected files were found", true);
                    return false;
                }

                var msg =
                    "MyEMSL did not return all of the expected files for the known data packages; " +
                    "some search results were returned but files were missing for data packages: " + string.Join(", ", dataPkgMissingFiles.Keys);

                ReportError(msg, true);
                return false;
            }
            catch (Exception ex)
            {
                ReportError("Exception verifying known MyEMSL search results: " + ex.Message, true);
                return false;
            }
        }

        /// <summary>
        /// Add a new data package ID and list of expected filenames to the dataPackageIDs dictionary
        /// </summary>
        /// <param name="dataPackageIDs"></param>
        /// <param name="dataPkgID"></param>
        /// <param name="fileNames"></param>
        private void VerifyKnownResultsAddExpectedFiles(IDictionary<int, List<string>> dataPackageIDs, int dataPkgID, List<string> fileNames)
        {
            dataPackageIDs.Add(dataPkgID, fileNames);
        }

        /// <summary>
        /// Query the database to find the status URIs that need to be verified
        /// Verify each one, updating the database as appropriate (if PreviewMode=false)
        /// Post an error to the DB if data has not been ingested within 24 hours or verified within 48 hours (and PreviewMode=false)
        /// </summary>
        /// <returns>True if successful, false if an error</returns>
        public bool VerifyUploadStatus()
        {
            // First obtain a list of status URIs to check
            // Keys are StatusNum integers, values are StatusURI strings
            const int retryCount = 2;
            var statusURIs = GetStatusURIs(retryCount);

            if (statusURIs.Count == 0)
            {
                // Nothing to do
                return true;
            }

            try
            {
                // Confirm that the data packages are visible in MyEMSL Metadata
                // To avoid obtaining too many results from MyEMSL, process the data packages in statusURIs in groups, 5 at a time
                // First construct a unique list of the Data Package IDs in statusURIs

                var distinctDataPackageIDs = (from item in statusURIs select item.Value.DataPackageID).Distinct().ToList();
                const int DATA_PACKAGE_GROUP_SIZE = 5;

                var statusChecker = new MyEMSLStatusCheck();
                RegisterEvents(statusChecker);

                for (var i = 0; i < distinctDataPackageIDs.Count; i += DATA_PACKAGE_GROUP_SIZE)
                {
                    var dataPackageInfoCache = new DataPackageListInfo
                    {
                        ReportMetadataURLs = TraceMode || LogLevel == BaseLogger.LogLevels.DEBUG,
                        ThrowErrors = true,
                        TraceMode = TraceMode
                    };

                    RegisterEvents(dataPackageInfoCache);

                    var statusURIsInGroup = new Dictionary<int, MyEMSLStatusInfo>();

                    for (var j = i; j < i + DATA_PACKAGE_GROUP_SIZE; j++)
                    {
                        if (j >= distinctDataPackageIDs.Count)
                            break;

                        var currentDataPackageID = distinctDataPackageIDs[j];
                        dataPackageInfoCache.AddDataPackage(currentDataPackageID);

                        // Find all of the URIs for this data package
                        var query = from item in statusURIs where item.Value.DataPackageID == currentDataPackageID select item;

                        foreach (var uriItem in query)
                        {
                            statusURIsInGroup.Add(uriItem.Key, uriItem.Value);
                        }
                    }

                    // Pre-populate dataPackageInfoCache with the files for the current group
                    dataPackageInfoCache.RefreshInfo();

                    var exceptionCount = 0;

                    foreach (var statusInfo in statusURIsInGroup)
                    {
                        var result = VerifyUploadStatusWork(statusChecker, statusInfo, dataPackageInfoCache, ref exceptionCount);

                        if (result == UploadStatus.CriticalError)
                            return false;
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                ReportError("Exception verifying data package upload status (VerifyUploadStatus): " + ex.Message, true);
                return false;
            }
        }

        private UploadStatus VerifyUploadStatusWork(
            MyEMSLStatusCheck statusChecker,
            KeyValuePair<int, MyEMSLStatusInfo> statusInfo,
            DataPackageListInfo dataPackageInfoCache,
            ref int exceptionCount)
        {
            try
            {
                // Obtain the Status XML
                var serverResponse = statusChecker.GetIngestStatus(
                    statusInfo.Value.StatusURI,
                    out _,
                    out var percentComplete,
                    out var lookupError,
                    out var errorMessage);

                // Could examine the current task and percent complete to determine the number of ingest steps completed
                // var ingestStepsCompleted = statusChecker.DetermineIngestStepsCompleted(currentTask, percentComplete, 0);

                var dataPackageAndEntryId = "Data Package " + statusInfo.Value.DataPackageID + ", Entry_ID " + statusInfo.Value.EntryID;

                if (lookupError)
                {
                    ReportError("Error looking up archive status for " + dataPackageAndEntryId + "; " + errorMessage, true);
                    return UploadStatus.VerificationError;
                }

                if (serverResponse.Keys.Count == 0)
                {
                    ReportError("Empty JSON server response for " + dataPackageAndEntryId);
                    return UploadStatus.VerificationError;
                }

                if (serverResponse.TryGetValue("state", out var ingestState))
                {
                    if (string.Equals((string)ingestState, "failed", StringComparison.InvariantCultureIgnoreCase) ||
                        !string.IsNullOrWhiteSpace(errorMessage))
                    {
                        // Error should have already been logged during the call to GetIngestStatus
                        if (string.IsNullOrWhiteSpace(errorMessage))
                        {
                            errorMessage = "Ingest failed; unknown reason";
                            ReportError(errorMessage);
                        }

                        if (errorMessage.Contains("Invalid values for submitter"))
                        {
                            ReportError(string.Format(
                                "Data package owner is not recognized by the MyEMSL system " +
                                "(or the user has two EUS IDs and MyEMSL only recognizes the first one; see https://dms2.pnl.gov/user/report). " +
                                "Have user {0} login to {1} then wait 24 hours, " +
                                "then update table DMS_Data_Package.T_MyEMSL_Uploads to change the ErrorCode to 101 for data package {2}. " +
                                "You must also delete file MyEMSL_metadata_CaptureJob_{2}.txt from a subdirectory below \\\\protoapps\\dataPkgs\\",
                                statusInfo.Value.DataPackageOwner, DMSMetadataObject.EUS_PORTAL_URL, statusInfo.Value.DataPackageID));
                        }

                        return UploadStatus.VerificationError;
                    }
                }
                else
                {
                    ReportError("State parameter not found in ingest status; see " + statusInfo.Value.StatusURI);
                    return UploadStatus.VerificationError;
                }

                if (percentComplete < 100)
                {
                    if (DateTime.Now.Subtract(statusInfo.Value.Entered).TotalHours > 24)
                    {
                        ReportError(
                        "Data package " + statusInfo.Value.DataPackageID + " is not available in MyEMSL after 24 hours; see " +
                        statusInfo.Value.StatusURI, true);
                    }

                    // Even though it is not yet available, we report Success
                    return UploadStatus.Success;
                }

                var archiveFiles = dataPackageInfoCache.FindFiles("*", "", statusInfo.Value.DataPackageID);

                if (archiveFiles.Count > 0)
                {
                    ReportMessage("Data package " + statusInfo.Value.DataPackageID + " is visible in MyEMSL Metadata",
                                    BaseLogger.LogLevels.DEBUG);
                }
                else
                {
                    ReportMessage("Data package " + statusInfo.Value.DataPackageID + " is not yet visible in MyEMSL Metadata",
                                    BaseLogger.LogLevels.DEBUG);

                    // Update values in the DB
                    UpdateMyEMSLUploadStatus(statusInfo.Value, verified: false);

                    // Even though it is not yet available, we report Success
                    return UploadStatus.Success;
                }

                var dataPkg = new DirectoryInfo(statusInfo.Value.LocalPath);
                if (!dataPkg.Exists)
                {
                    dataPkg = new DirectoryInfo(statusInfo.Value.SharePath);
                }

                if (!dataPkg.Exists)
                {
                    OnErrorEvent("Data package directory not found by VerifyUploadStatusWork, this is unexpected; see: " + dataPkg.FullName);

                    UpdateMyEMSLUploadStatus(statusInfo.Value, verified: false);

                    return UploadStatus.Success;
                }

                if (dataPkg.Parent == null)
                    throw new DirectoryNotFoundException("Unable to determine the parent directory of " + dataPkg.FullName);

                // Construct the metadata file path
                // For example, \\protoapps\dataPkgs\Public\2014\MyEMSL_metadata_CaptureJob_1055.txt
                var metadataFilePath = Path.Combine(dataPkg.Parent.FullName,
                                                    Utilities.GetMetadataFilenameForJob(
                                                        statusInfo.Value.DataPackageID.ToString(CultureInfo.InvariantCulture)));

                var metadataFile = new FileInfo(metadataFilePath);

                if (metadataFile.Exists)
                {
                    var msg = "Deleting metadata file for Data package " + statusInfo.Value.DataPackageID +
                              " since it is now available and verified: " + metadataFile.FullName;

                    if (PreviewMode)
                    {
                        ReportMessage("SIMULATE: " + msg);
                    }
                    else
                    {
                        ReportMessage(msg);
                        metadataFile.Delete();
                    }
                }

                // Update values in the DB
                UpdateMyEMSLUploadStatus(statusInfo.Value, verified: true);
            }
            catch (Exception ex)
            {
                var errorMessage = string.Format(
                        "Exception verifying archive status for Data package {0}, Entry_ID {1}: {2}",
                        statusInfo.Value.DataPackageID, statusInfo.Value.EntryID, ex.Message);

                exceptionCount++;
                if (exceptionCount < 3)
                {
                    ReportMessage(errorMessage, BaseLogger.LogLevels.WARN);
                    return UploadStatus.VerificationError;
                }

                ReportError(errorMessage, true);

                // Too many errors for this data package; move on to the next one
                return UploadStatus.CriticalError;
            }

            return UploadStatus.Success;
        }

        private void MyEMSLUpload_StatusUpdate(object sender, StatusEventArgs e)
        {
            if (DateTime.UtcNow.Subtract(mLastStatusUpdate).TotalSeconds >= 5)
            {
                mLastStatusUpdate = DateTime.UtcNow;
                ReportMessage(e.StatusMessage, BaseLogger.LogLevels.DEBUG);
            }
        }

        private void MyEMSLUpload_UploadCompleted(object sender, UploadCompletedEventArgs e)
        {
            var msg = "Upload complete";

            // Note that e.ServerResponse will simply have the StatusURL if the upload succeeded
            // If a problem occurred, then e.ServerResponse will either have the full server response, or may even be blank
            if (string.IsNullOrEmpty(e.ServerResponse))
                msg += ": empty server response";
            else
                msg += ": " + e.ServerResponse;

            ReportMessage(msg);
        }
    }
}
