using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Data.SqlClient;
using MyEMSLReader;
using Pacifica.Core;
using PRISM;
using PRISM.Logging;
using Utilities = Pacifica.Core.Utilities;

namespace DataPackage_Archive_Manager
{
    class DataPackageArchiver : EventNotifier
    {
        #region "Constants"

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
        /// Since data package uploads always work with the entire data package folder and all subfolders,
        ///   there is a maximum cap on the number of files that will be stored in MyEMSL for a given data package
        /// If a data package has more than 600 files, the data needs to be manually zipped before this manager will auto-push into MyEMSL
        /// </remarks>
        private const int MAX_FILES_TO_ARCHIVE = 600;

        #endregion

        #region "Structures"
        private struct MyEMSLStatusInfo
        {
            public int EntryID;
            public int DataPackageID;
            public DateTime Entered;
            public string StatusURI;

            public string SharePath;
            public string LocalPath;
        }

        private struct MyEMSLUploadInfo
        {
            public string SubDir;
            public int FileCountNew;
            public int FileCountUpdated;
            public Int64 Bytes;
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
        }

        #endregion

        #region "Class variables"

        private readonly ExecuteDatabaseSP m_ExecuteSP;
        private readonly Upload mMyEMSLUploader;
        private DateTime mLastStatusUpdate;

        #endregion

        #region "Auto properties"

        /// <summary>
        /// Gigasax.DMS_Data_Package database
        /// </summary>
        public string DBConnectionString { get; }

        public string ErrorMessage { get; private set; }

        public bool PreviewMode { get; set; }

        public bool SkipCheckExisting { get; set; }

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

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public DataPackageArchiver(string connectionString, BaseLogger.LogLevels logLevel)
        {
            // Typically:
            // Data Source=gigasax;Initial Catalog=DMS_Data_Package;Integrated Security=SSPI;
            DBConnectionString = connectionString;
            LogLevel = logLevel;

            m_ExecuteSP = new ExecuteDatabaseSP(DBConnectionString);
            RegisterEvents(m_ExecuteSP);

            var pacificaConfig = new Configuration();

            mMyEMSLUploader = new Upload(pacificaConfig);
            RegisterEvents(mMyEMSLUploader);

            // Attach the events
            mMyEMSLUploader.StatusUpdate += myEMSLUpload_StatusUpdate;
            mMyEMSLUploader.UploadCompleted += myEMSLUpload_UploadCompleted;

            Initialize();
        }

        private short BoolToTinyInt(bool value)
        {
            return (short)(value ? 1 : 0);
        }

        private int CountFilesForDataPackage(DataPackageInfo dataPkgInfo)
        {
            var diDataPkg = new DirectoryInfo(dataPkgInfo.LocalPath);
            if (!diDataPkg.Exists)
                diDataPkg = new DirectoryInfo(dataPkgInfo.SharePath);

            return !diDataPkg.Exists ? 0 : diDataPkg.GetFiles("*.*", SearchOption.AllDirectories).Length;
        }

        private List<FileInfoObject> FindDataPackageFilesToArchive(
            DataPackageInfo dataPkgInfo,
            DirectoryInfo diDataPkg,
            DateTime dateThreshold,
            DataPackageListInfo dataPackageInfoCache,
            out MyEMSLUploadInfo uploadInfo)
        {
            var lstDatasetFilesToArchive = new List<FileInfoObject>();

            uploadInfo = new MyEMSLUploadInfo();
            uploadInfo.Clear();

            uploadInfo.SubDir = dataPkgInfo.FolderName;

            // Construct a list of the files on disk for this data package
            var lstDataPackageFilesAll = diDataPkg.GetFiles("*.*", SearchOption.AllDirectories).ToList();

            if (lstDataPackageFilesAll.Count == 0)
            {
                // Nothing to archive; this is not an error
                ReportMessage("Data Package " + dataPkgInfo.ID + " does not have any files; nothing to archive", BaseLogger.LogLevels.DEBUG);
                return new List<FileInfoObject>();
            }

            // Look for any Auto-process folders that have recently modified files
            // These folders will be skipped until the files are at least 4 hours old
            // This list contains folder paths to skip
            var lstDataPackageFoldersToSkip = new List<string>();

            var lstDataPackageFolders = diDataPkg.GetDirectories("*", SearchOption.AllDirectories).ToList();
            var reAutoJobFolder = new Regex(@"^[A-Z].{1,5}\d{12,12}_Auto\d+", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            foreach (var dataPkgFolder in lstDataPackageFolders)
            {
                if (!reAutoJobFolder.IsMatch(dataPkgFolder.Name))
                {
                    continue;
                }

                var skipFolder = false;
                foreach (var file in dataPkgFolder.GetFiles("*.*", SearchOption.AllDirectories))
                {
                    if (DateTime.UtcNow.Subtract(file.LastWriteTimeUtc).TotalHours < 4)
                    {
                        skipFolder = true;
                        break;
                    }
                }

                if (skipFolder)
                    lstDataPackageFoldersToSkip.Add(dataPkgFolder.FullName);
            }


            // Filter out files that we do not want to archive
            var lstFilesToSkip = new SortedSet<string>(StringComparer.CurrentCultureIgnoreCase)
            {
                "Thumbs.db",
                ".DS_Store",
                ".Rproj.user"
            };

            // Also filter out Thermo .raw files, mzML files, etc.
            var lstExtensionsToSkip = new SortedSet<string>(StringComparer.CurrentCultureIgnoreCase)
            {
                ".raw",
                ".mzXML",
                ".mzML"
            };

            var lstDataPackageFiles = new List<FileInfo>();
            foreach (var dataPkgFile in lstDataPackageFilesAll)
            {
                var keep = true;
                if (lstFilesToSkip.Contains(dataPkgFile.Name))
                {
                    keep = false;
                }
                else if (lstExtensionsToSkip.Contains(dataPkgFile.Extension))
                {
                    keep = false;
                }
                else if (dataPkgFile.Name.StartsWith("~$"))
                {
                    if ((dataPkgFile.Attributes & FileAttributes.Hidden) == FileAttributes.Hidden)
                        keep = false;
                }
                else if (dataPkgFile.Name.StartsWith("SyncToy_") && dataPkgFile.Name.EndsWith(".dat"))
                {
                    keep = false;
                }
                else if (dataPkgFile.Name.ToLower().EndsWith(".mzml.gz"))
                {
                    keep = false;
                }

                var diDataPkgFileContainer = dataPkgFile.Directory;
                if (keep && diDataPkgFileContainer != null)
                {
                    foreach (var dataPkgFolder in lstDataPackageFoldersToSkip)
                    {
                        if (diDataPkgFileContainer.FullName.StartsWith(dataPkgFolder))
                        {
                            keep = false;
                            break;
                        }
                    }
                }

                if (keep)
                    lstDataPackageFiles.Add(dataPkgFile);

            }


            if (lstDataPackageFiles.Count > MAX_FILES_TO_ARCHIVE)
            {
                ReportError(" Data Package " + dataPkgInfo.ID + " has " + lstDataPackageFiles.Count + " files; the maximum number of files allowed in MyEMSL per data package is " + MAX_FILES_TO_ARCHIVE + "; zip up groups of files to reduce the total file count; see " + dataPkgInfo.SharePath, true);
                return new List<FileInfoObject>();
            }

            if (lstDataPackageFiles.Count == 0)
            {
                // Nothing to archive; this is not an error
                var msg = " Data Package " + dataPkgInfo.ID + " has " + lstDataPackageFilesAll.Count + " files, but all have been skipped";

                if (lstDataPackageFoldersToSkip.Count > 0)
                    msg += " due to recently modified files in auto-job result folders";
                else
                    msg += " since they are system or temporary files";

                ReportMessage(msg + "; nothing to archive");
                ReportMessage("  Data Package " + dataPkgInfo.ID + " path: " + diDataPkg.FullName, BaseLogger.LogLevels.DEBUG);
                return new List<FileInfoObject>();
            }

            // Make sure at least one of the files was modified after the date threshold
            var passesDateThreshold = lstDataPackageFiles.Any(fiLocalFile => fiLocalFile.LastWriteTime >= dateThreshold);

            if (!passesDateThreshold)
            {
                // None of the modified files passes the date threshold
                string msg;

                if (lstDataPackageFilesAll.Count == 1)
                    msg = " Data Package " + dataPkgInfo.ID + " has 1 file, but it was modified before " + dateThreshold.ToString("yyyy-MM-dd");
                else
                    msg = " Data Package " + dataPkgInfo.ID + " has " + lstDataPackageFilesAll.Count + " files, but all were modified before " + dateThreshold.ToString("yyyy-MM-dd");

                ReportMessage(msg + "; nothing to archive", BaseLogger.LogLevels.DEBUG);
                return new List<FileInfoObject>();
            }

            // Note: subtracting 60 seconds from UtcNow when initializing dtLastProgress so that a progress message will appear as an "INFO" level log message if 5 seconds elapses
            // After that, the INFO level messages will appear every 30 seconds
            var dtLastProgress = DateTime.UtcNow.AddSeconds(-60);
            var dtLastProgressDetail = DateTime.UtcNow;

            var filesProcessed = 0;

            foreach (var fiLocalFile in lstDataPackageFiles)
            {
                // Note: when storing data package files in MyEMSL the SubDir path will always start with the data package folder name
                var subDir = string.Copy(uploadInfo.SubDir);

                if (fiLocalFile.Directory != null && fiLocalFile.Directory.FullName.Length > diDataPkg.FullName.Length)
                {
                    // Append the subdirectory path
                    subDir = Path.Combine(subDir, fiLocalFile.Directory.FullName.Substring(diDataPkg.FullName.Length + 1));
                }

                // Look for this file in MyEMSL
                var archiveFiles = dataPackageInfoCache.FindFiles(fiLocalFile.Name, subDir, dataPkgInfo.ID, recurse: false);

                if (diDataPkg.Parent == null)
                    throw new DirectoryNotFoundException("Unable to determine the parent directory of " + diDataPkg.FullName);

                // Possibly add the file to lstDatasetFilesToArchive
                AddFileIfArchiveRequired(diDataPkg, ref uploadInfo, lstDatasetFilesToArchive, fiLocalFile, archiveFiles);

                filesProcessed++;
                if (DateTime.UtcNow.Subtract(dtLastProgressDetail).TotalSeconds >= 5)
                {
                    dtLastProgressDetail = DateTime.UtcNow;

                    var progressMessage = "Finding files to archive for Data Package " + dataPkgInfo.ID + ": " + filesProcessed + " / " + lstDataPackageFiles.Count;
                    if (DateTime.UtcNow.Subtract(dtLastProgress).TotalSeconds >= 30)
                    {
                        dtLastProgress = DateTime.UtcNow;
                        ReportMessage(progressMessage);
                    }
                    else
                    {
                        ReportMessage(progressMessage, BaseLogger.LogLevels.DEBUG);
                    }
                }
            }

            if (lstDatasetFilesToArchive.Count == 0)
            {
                // Nothing to archive; this is not an error
                ReportMessage(" All files for Data Package " + dataPkgInfo.ID + " are already in MyEMSL; FileCount=" + lstDataPackageFiles.Count, BaseLogger.LogLevels.DEBUG);
                return lstDatasetFilesToArchive;
            }

            return lstDatasetFilesToArchive;
        }

        private static void AddFileIfArchiveRequired(
            DirectoryInfo diDataPkg,
            ref MyEMSLUploadInfo uploadInfo,
            ICollection<FileInfoObject> lstDatasetFilesToArchive,
            FileInfo fiLocalFile,
            ICollection<DatasetDirectoryOrFileInfo> archiveFiles)
        {

            if (archiveFiles.Count == 0)
            {
                // File not found; add to lstDatasetFilesToArchive
                if (diDataPkg.Parent == null)
                    throw new DirectoryNotFoundException("Unable to determine the parent directory of " + diDataPkg.FullName);

                lstDatasetFilesToArchive.Add(new FileInfoObject(fiLocalFile.FullName, diDataPkg.Parent.FullName));
                uploadInfo.FileCountNew++;
                uploadInfo.Bytes += fiLocalFile.Length;
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
            if (fiLocalFile.Length != archiveFile.FileInfo.FileSizeBytes)
            {
                // Sizes don't match; add to lstDatasetFilesToArchive
                if (diDataPkg.Parent == null)
                    throw new DirectoryNotFoundException("Unable to determine the parent directory of " + diDataPkg.FullName);

                lstDatasetFilesToArchive.Add(new FileInfoObject(fiLocalFile.FullName, diDataPkg.Parent.FullName));
                uploadInfo.FileCountUpdated++;
                uploadInfo.Bytes += fiLocalFile.Length;
                return;
            }

            // File sizes match
            // Compare Sha-1 hash if the file is less than 1 month old or
            // if the file is less than 6 months old and less than 50 MB in size

            const int THRESHOLD_50_MB = 50 * 1024 * 1024;

            if (fiLocalFile.LastWriteTimeUtc > DateTime.UtcNow.AddMonths(-1) ||
                fiLocalFile.LastWriteTimeUtc > DateTime.UtcNow.AddMonths(-6) && fiLocalFile.Length < THRESHOLD_50_MB)
            {
                var sha1HashHex = Utilities.GenerateSha1Hash(fiLocalFile.FullName);

                if (sha1HashHex != archiveFile.FileInfo.Sha1Hash)
                {
                    if (diDataPkg.Parent == null)
                        throw new DirectoryNotFoundException("Unable to determine the parent directory of " + diDataPkg.FullName);

                    // Hashes don't match; add to lstDatasetFilesToArchive
                    // We include the hash when instantiating the new FileInfoObject so that the hash will not need to be regenerated later
                    var relativeDestinationDirectory = FileInfoObject.GenerateRelativePath(fiLocalFile.DirectoryName,
                                                                                           diDataPkg.Parent.FullName);

                    lstDatasetFilesToArchive.Add(new FileInfoObject(fiLocalFile.FullName, relativeDestinationDirectory, sha1HashHex));
                    uploadInfo.FileCountUpdated++;
                    uploadInfo.Bytes += fiLocalFile.Length;
                }
            }

        }

        private DateTime GetDBDate(IDataRecord reader, string columnName)
        {
            var value = reader[columnName];

            if (Convert.IsDBNull(value))
                return DateTime.Now;

            return (DateTime)value;
        }

        private int GetDBInt(IDataRecord reader, string columnName)
        {
            var value = reader[columnName];

            if (Convert.IsDBNull(value))
                return 0;

            return (int)value;
        }

        private string GetDBString(IDataRecord reader, string columnName)
        {
            var value = reader[columnName];

            if (Convert.IsDBNull(value))
                return string.Empty;

            return (string)value;
        }

        private IEnumerable<DataPackageInfo> GetFilteredDataPackageInfoList(
            IEnumerable<DataPackageInfo> lstDataPkgInfo,
            IEnumerable<int> dataPkgGroup)
        {
            var lstFilteredDataPkgInfo =
                (from item in lstDataPkgInfo
                 join dataPkgID in dataPkgGroup on item.ID equals dataPkgID
                 select item).ToList();

            return lstFilteredDataPkgInfo;
        }

        private Dictionary<int, MyEMSLStatusInfo> GetStatusURIs(int retryCount)
        {
            var dctURIs = new Dictionary<int, MyEMSLStatusInfo>();

            try
            {
                const string sql = " SELECT MU.Entry_ID, MU.Data_Package_ID, MU.Entered, MU.StatusNum, MU.Status_URI, DP.Local_Path, DP.Share_Path " +
                                   " FROM V_MyEMSL_Uploads MU INNER JOIN V_Data_Package_Export DP ON MU.Data_Package_ID = DP.ID" +
                                   " WHERE MU.ErrorCode = 0 And (MU.Available = 0 Or MU.Verified = 0) AND ISNULL(MU.StatusNum, 0) > 0";

                while (retryCount > 0)
                {
                    try
                    {
                        using (var cnDB = new SqlConnection(DBConnectionString))
                        {
                            cnDB.Open();

                            var cmd = new SqlCommand(sql, cnDB);
                            var reader = cmd.ExecuteReader();

                            while (reader.Read())
                            {
                                var statusInfo = new MyEMSLStatusInfo
                                {
                                    EntryID = reader.GetInt32(0),
                                    DataPackageID = reader.GetInt32(1),
                                    Entered = reader.GetDateTime(2)
                                };

                                var statusNum = reader.GetInt32(3);

                                if (dctURIs.ContainsKey(statusNum))
                                {
                                    var msg = "Error, StatusNum " + statusNum + " is defined for multiple data packages";
                                    ReportError(msg, true);
                                    continue;
                                }

                                if (!Convert.IsDBNull(reader.GetValue(4)))
                                {
                                    var statusURI = (string)reader.GetValue(4);
                                    if (!string.IsNullOrEmpty(statusURI))
                                    {
                                        statusInfo.StatusURI = statusURI;

                                        statusInfo.SharePath = GetDBString(reader, "Share_Path");
                                        statusInfo.LocalPath = GetDBString(reader, "Local_Path");

                                        dctURIs.Add(statusNum, statusInfo);
                                    }
                                }
                            }
                        }
                        break;
                    }
                    catch (Exception ex)
                    {
                        retryCount -= 1;
                        var msg = "Exception querying database in GetStatusURIs: " + ex.Message;
                        msg += ", RetryCount = " + retryCount;
                        ReportError(msg, true, ex);

                        // Delay for 5 second before trying again
                        System.Threading.Thread.Sleep(5000);
                    }
                }
            }
            catch (Exception ex)
            {
                var msg = "Exception connecting to database in GetStatusURIs: " + ex.Message + "; ConnectionString: " + DBConnectionString;
                ReportError(msg, ex);
            }

            return dctURIs;
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

        private List<DataPackageInfo> LookupDataPkgInfo(IReadOnlyList<KeyValuePair<int, int>> lstDataPkgIDs)
        {
            var lstDataPkgInfo = new List<DataPackageInfo>();

            try
            {

                using (var cnDB = new SqlConnection(DBConnectionString))
                {
                    cnDB.Open();

                    var sql = new StringBuilder();

                    sql.Append(" SELECT ID, Name, Owner, Instrument, " +
                                  " EUS_Person_ID, EUS_Proposal_ID, EUS_Instrument_ID, Created, " +
                                  " Package_File_Folder, Share_Path, Local_Path, MyEMSL_Uploads " +
                               " FROM V_Data_Package_Export");

                    if (lstDataPkgIDs.Count > 0)
                    {
                        sql.Append(" WHERE ");

                        for (var i = 0; i < lstDataPkgIDs.Count; i++)
                        {
                            if (i > 0)
                                sql.Append(" OR ");

                            if (lstDataPkgIDs[i].Key == lstDataPkgIDs[i].Value)
                                sql.Append("ID = " + lstDataPkgIDs[i].Key);
                            else if (lstDataPkgIDs[i].Value < 0)
                                sql.Append("ID >= " + lstDataPkgIDs[i].Key);
                            else
                                sql.Append("ID BETWEEN " + lstDataPkgIDs[i].Key + " AND " + lstDataPkgIDs[i].Value);
                        }

                    }

                    sql.Append(" ORDER BY ID");

                    using (var cmd = new SqlCommand(sql.ToString(), cnDB))
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var dataPkgID = reader.GetInt32(0);

                            var dataPkgInfo = new DataPackageInfo(dataPkgID)
                            {
                                Name = GetDBString(reader, "Name"),
                                OwnerPRN = GetDBString(reader, "Owner"),
                                OwnerEUSID = GetDBInt(reader, "EUS_Person_ID"),
                                EUSProposalID = GetDBString(reader, "EUS_Proposal_ID"),
                                EUSInstrumentID = GetDBInt(reader, "EUS_Instrument_ID"),
                                InstrumentName = GetDBString(reader, "Instrument"),
                                Created = GetDBDate(reader, "Created"),
                                FolderName = GetDBString(reader, "Package_File_Folder"),
                                SharePath = GetDBString(reader, "Share_Path"),
                                LocalPath = GetDBString(reader, "Local_Path"),
                                MyEMSLUploads = GetDBInt(reader, "MyEMSL_Uploads")
                            };

                            lstDataPkgInfo.Add(dataPkgInfo);
                        }
                    }

                }

                return lstDataPkgInfo;

            }
            catch (Exception ex)
            {
                ReportError("Error in LookupDataPkgInfo: " + ex.Message, true, ex);

                // Include the stack trace in the log
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Detail for error in LookupDataPkgInfo", ex);

                return new List<DataPackageInfo>();
            }

        }

        /// <summary>
        /// Parses a list of data package IDs (or ID ranges) separated commas
        /// </summary>
        /// <param name="dataPkgIDList"></param>
        /// <returns>List of KeyValue pairs where the key is the start ID and the value is the end ID</returns>
        /// <remarks>
        /// To indicate a range of 300 or higher, use "300-"
        /// In that case, the KeyValuePair will be (300,-1)</remarks>
        public List<KeyValuePair<int, int>> ParseDataPkgIDList(string dataPkgIDList)
        {

            var lstValues = dataPkgIDList.Split(',').ToList();
            var lstDataPkgIDs = new List<KeyValuePair<int, int>>();

            foreach (var item in lstValues)
            {
                if (!string.IsNullOrWhiteSpace(item))
                {
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

                        if (!lstDataPkgIDs.Contains(idRange))
                            lstDataPkgIDs.Add(idRange);
                    }
                    else
                    {
                        ReportError("Value is not an integer or range of integers; ignoring: " + item);
                    }
                }
            }

            return lstDataPkgIDs;

        }

        /// <summary>
        /// Update the data packages in lstDataPkgIDs
        /// </summary>
        /// <param name="lstDataPkgIDs"></param>
        /// <param name="dateThreshold"></param>
        /// <returns></returns>
        public bool ProcessDataPackages(List<KeyValuePair<int, int>> lstDataPkgIDs, DateTime dateThreshold)
        {

            List<DataPackageInfo> lstDataPkgInfo;
            var successCount = 0;

            try
            {
                lstDataPkgInfo = LookupDataPkgInfo(lstDataPkgIDs);

                if (lstDataPkgInfo.Count == 0)
                {
                    ReportError("None of the data packages in lstDataPkgIDs corresponded to a known data package ID");
                    return false;
                }

                if (!PreviewMode)
                {
                    ReportMessage(@"Pushing data into MyEMSL as user pnl\" + Environment.UserName);
                }

                // List of groups of data package IDs
                var lstDataPkgGroups = new List<List<int>>();
                var lstCurrentGroup = new List<int>();
                var dtLastProgress = DateTime.UtcNow.AddSeconds(-10);
                var dtLastSimpleSearchVerify = DateTime.MinValue;

                var runningCount = 0;

                ReportMessage("Finding data package files for " + lstDataPkgInfo.Count + " data packages");

                // Determine the number of files that are associated with each data package
                // We will use this information to process the data packages in chunks
                for (var i = 0; i < lstDataPkgInfo.Count; i++)
                {
                    var fileCount = CountFilesForDataPackage(lstDataPkgInfo[i]);

                    if (runningCount + fileCount > 5000 || lstCurrentGroup.Count >= 30)
                    {
                        // Store the current group
                        if (lstCurrentGroup.Count > 0)
                            lstDataPkgGroups.Add(lstCurrentGroup);

                        // Make a new group
                        lstCurrentGroup = new List<int>
                        {
                            lstDataPkgInfo[i].ID
                        };
                        runningCount = fileCount;
                    }
                    else
                    {
                        // Use the current group
                        lstCurrentGroup.Add(lstDataPkgInfo[i].ID);
                        runningCount += fileCount;
                    }

                    if (DateTime.UtcNow.Subtract(dtLastProgress).TotalSeconds >= 20)
                    {
                        dtLastProgress = DateTime.UtcNow;
                        ReportMessage("Finding data package files; examined " + i + " of " + lstDataPkgInfo.Count + " data packages");
                    }
                }

                // Store the current group
                if (lstCurrentGroup.Count > 0)
                    lstDataPkgGroups.Add(lstCurrentGroup);

                var groupNumber = 0;

                foreach (var dataPkgGroup in lstDataPkgGroups)
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

                    if (!PreviewMode && DateTime.UtcNow.Subtract(dtLastSimpleSearchVerify).TotalMinutes > 15)
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

                        dtLastSimpleSearchVerify = DateTime.UtcNow;

                    }

                    // Pre-populate lstDataPackageInfoCache with the files for the current group
                    ReportMessage("Querying MyEMSL for " + dataPkgGroup.Count + " data packages in group " + groupNumber + " of " + lstDataPkgGroups.Count);
                    dataPackageInfoCache.RefreshInfo();

                    // Obtain the DataPackageInfo objects for the IDs in dataPkgGroup
                    var lstFilteredDataPkgInfo = GetFilteredDataPackageInfoList(lstDataPkgInfo, dataPkgGroup);

                    foreach (var dataPkgInfo in lstFilteredDataPkgInfo)
                    {
                        var success = ProcessOneDataPackage(dataPkgInfo, dateThreshold, dataPackageInfoCache);

                        if (success)
                            successCount += 1;

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

            if (successCount == lstDataPkgInfo.Count)
            {
                if (successCount == 1)
                    ReportMessage("Processing complete for Data Package " + lstDataPkgInfo.First().ID);
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

            if (lstDataPkgInfo.Count == 1)
                ReportError("Failed to archive Data Package " + lstDataPkgIDs.First());
            else if (successCount == 0)
                ReportError("Failed to archive any of the " + lstDataPkgIDs.Count + " candidate data packages", true);
            else
                ReportError("Failed to archive " + (lstDataPkgInfo.Count - successCount) + " data package(s); successfully archived " + successCount + " data package(s)", true);

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

            var dtStartTime = DateTime.UtcNow;

            try
            {
                var diDataPkg = new DirectoryInfo(dataPkgInfo.LocalPath);
                if (!diDataPkg.Exists)
                    diDataPkg = new DirectoryInfo(dataPkgInfo.SharePath);

                if (!diDataPkg.Exists)
                {
                    ReportMessage("Data package folder not found (also tried remote share path): " + dataPkgInfo.LocalPath, BaseLogger.LogLevels.WARN);
                    return false;
                }

                if (diDataPkg.Parent == null)
                    throw new DirectoryNotFoundException("Unable to determine the parent directory of " + diDataPkg.FullName);

                // Look for an existing metadata file
                // For example, \\protoapps\dataPkgs\Public\2014\MyEMSL_metadata_CaptureJob_1055.txt
                var metadataFilePath = Path.Combine(diDataPkg.Parent.FullName, Utilities.GetMetadataFilenameForJob(dataPkgInfo.ID.ToString(CultureInfo.InvariantCulture)));
                var fiMetadataFile = new FileInfo(metadataFilePath);
                if (fiMetadataFile.Exists)
                {
                    if (DateTime.UtcNow.Subtract(fiMetadataFile.LastWriteTimeUtc).TotalHours >= 48)
                    {
                        if (DateTime.UtcNow.Subtract(fiMetadataFile.LastWriteTimeUtc).TotalDays > 6.5)
                        {
                            ReportError("Data Package " + dataPkgInfo.ID + " has an existing metadata file over 6.5 days old; deleting file: " + fiMetadataFile.FullName, true);
                            fiMetadataFile.Delete();
                        }
                        else
                        {
                            // This is likely an error, but we don't want to re-upload the files yet
                            // Log an error to the database
                            ReportError("Data Package " + dataPkgInfo.ID + " has an existing metadata file between 2 and 6.5 days old: " + fiMetadataFile.FullName, true);

                            // This is not a fatal error; return true
                            return true;
                        }
                    }
                    else
                    {
                        ReportMessage("Data Package " + dataPkgInfo.ID + " has an existing metadata file less than 48 hours old; skipping this data package", BaseLogger.LogLevels.WARN);
                        ReportMessage("  " + fiMetadataFile.FullName, BaseLogger.LogLevels.DEBUG);

                        // This is not a fatal error; return true
                        return true;
                    }
                }

                // Compare the files to those already in MyEMSL to create a list of files to be uploaded
                var lstUnmatchedFiles = FindDataPackageFilesToArchive(
                    dataPkgInfo,
                    diDataPkg,
                    dateThreshold,
                    dataPackageInfoCache,
                    out uploadInfo);

                if (lstUnmatchedFiles.Count == 0)
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
                    ReportMessage("Need to upload " + lstUnmatchedFiles.Count + " file(s) for Data Package " + dataPkgInfo.ID);

                    // Preview the changes
                    foreach (var unmatchedFile in lstUnmatchedFiles)
                    {
                        Console.WriteLine("  " + unmatchedFile.RelativeDestinationFullPath);
                    }
                }
                else
                {
                    // Upload the files
                    ReportMessage("Uploading " + lstUnmatchedFiles.Count + " new/changed files for Data Package " + dataPkgInfo.ID);

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
                        uploadMetadata.EUSOperatorID = dataPkgInfo.OwnerEUSID;
                    }

                    if (string.IsNullOrWhiteSpace(dataPkgInfo.EUSProposalID))
                    {
                        OnWarningEvent("Data package does not have an associated EUS Proposal; using " + Upload.DEFAULT_EUS_PROPOSAL_ID);
                        uploadMetadata.EUSProposalID = Upload.DEFAULT_EUS_PROPOSAL_ID;
                    }
                    else
                    {
                        uploadMetadata.EUSProposalID = dataPkgInfo.EUSProposalID;
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
                    var metadataObject = Upload.CreatePacificaMetadataObject(uploadMetadata, lstUnmatchedFiles, out _);

                    var metadataDescription = Upload.GetMetadataObjectDescription(metadataObject);
                    ReportMessage("UploadMetadata: " + metadataDescription);

                    mMyEMSLUploader.TransferFolderPath = diDataPkg.Parent.FullName;
                    mMyEMSLUploader.JobNumber = dataPkgInfo.ID.ToString();

                    success = mMyEMSLUploader.StartUpload(metadataObject, out var statusURL);

                    var tsElapsedTime = DateTime.UtcNow.Subtract(dtStartTime);

                    uploadInfo.UploadTimeSeconds = tsElapsedTime.TotalSeconds;
                    uploadInfo.StatusURI = statusURL;

                    if (success)
                    {

                        var msg = "Upload of changed files for Data Package " + dataPkgInfo.ID + " completed in " + tsElapsedTime.TotalSeconds.ToString("0.0") + " seconds: " + uploadInfo.FileCountNew + " new files, " + uploadInfo.FileCountUpdated + " updated files, " + uploadInfo.Bytes + " bytes";
                        ReportMessage(msg);

                        var statusUriMsg = "  myEMSL statusURI => " + uploadInfo.StatusURI;
                        ReportMessage(statusUriMsg, BaseLogger.LogLevels.DEBUG);
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

                uploadInfo.UploadTimeSeconds = DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds;

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

        private void ReportError(string message, Exception ex)
        {
            ReportError(message, false, ex);
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
        /// Update the data packages in lstDataPkgIDs, then verify the upload status
        /// </summary>
        /// <param name="lstDataPkgIDs"></param>
        /// <param name="dateThreshold"></param>
        /// <param name="previewMode"></param>
        /// <returns></returns>
        public bool StartProcessing(List<KeyValuePair<int, int>> lstDataPkgIDs, DateTime dateThreshold, bool previewMode)
        {

            PreviewMode = previewMode;

            // Upload new data
            var success = ProcessDataPackages(lstDataPkgIDs, dateThreshold);

            // Verify uploaded data (even if success is false)
            VerifyUploadStatus();

            return success;
        }

        private bool StoreMyEMSLUploadStats(DataPackageInfo dataPkgInfo, MyEMSLUploadInfo uploadInfo)
        {

            try
            {

                // Setup for execution of the stored procedure
                var cmd = new SqlCommand();
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = SP_NAME_STORE_MYEMSL_STATS;

                    cmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;

                    cmd.Parameters.Add(new SqlParameter("@DataPackageID", SqlDbType.Int)).Value = Convert.ToInt32(dataPkgInfo.ID);

                    cmd.Parameters.Add(new SqlParameter("@Subfolder", SqlDbType.VarChar, 128)).Value = uploadInfo.SubDir;

                    cmd.Parameters.Add(new SqlParameter("@FileCountNew", SqlDbType.Int)).Value = uploadInfo.FileCountNew;

                    cmd.Parameters.Add(new SqlParameter("@FileCountUpdated", SqlDbType.Int)).Value = uploadInfo.FileCountUpdated;

                    cmd.Parameters.Add(new SqlParameter("@Bytes", SqlDbType.BigInt)).Value = uploadInfo.Bytes;

                    cmd.Parameters.Add(new SqlParameter("@UploadTimeSeconds", SqlDbType.Real)).Value = (float)uploadInfo.UploadTimeSeconds;

                    cmd.Parameters.Add(new SqlParameter("@StatusURI", SqlDbType.VarChar, 255)).Value = uploadInfo.StatusURI;

                    cmd.Parameters.Add(new SqlParameter("@ErrorCode", SqlDbType.Int)).Value = uploadInfo.ErrorCode;
                }

                ReportMessage("Calling " + SP_NAME_STORE_MYEMSL_STATS + " for Data Package " + dataPkgInfo.ID, BaseLogger.LogLevels.DEBUG);

                // Execute the SP (retry the call up to 4 times)
                m_ExecuteSP.TimeoutSeconds = 20;
                var resCode = m_ExecuteSP.ExecuteSP(cmd, 4);

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
        private bool UpdateMyEMSLUploadStatus(MyEMSLStatusInfo statusInfo, bool verified)
        {

            try
            {
                var cmd = new SqlCommand(SP_NAME_SET_MYEMSL_UPLOAD_STATUS)
                {
                    CommandType = CommandType.StoredProcedure
                };

                cmd.Parameters.Add("@Return", SqlDbType.Int).Direction = ParameterDirection.ReturnValue;

                cmd.Parameters.Add("@EntryID", SqlDbType.Int).Value = statusInfo.EntryID;

                cmd.Parameters.Add("@DataPackageID", SqlDbType.Int).Value = statusInfo.DataPackageID;

                cmd.Parameters.Add("@Available", SqlDbType.TinyInt).Value = BoolToTinyInt(true);

                cmd.Parameters.Add("@Verified", SqlDbType.TinyInt).Value = BoolToTinyInt(verified);

                cmd.Parameters.Add("@message", SqlDbType.VarChar, 512).Direction = ParameterDirection.Output;

                if (PreviewMode)
                {
                    Console.WriteLine("Simulate call to " + SP_NAME_SET_MYEMSL_UPLOAD_STATUS + " for Entry_ID=" + statusInfo.EntryID + ", DataPackageID=" + statusInfo.DataPackageID + " Entry_ID=" + statusInfo.EntryID);
                    return true;
                }

                ReportMessage("  Calling " + SP_NAME_SET_MYEMSL_UPLOAD_STATUS + " for Data Package " + statusInfo.DataPackageID, BaseLogger.LogLevels.DEBUG);

                m_ExecuteSP.TimeoutSeconds = 20;
                var resCode = m_ExecuteSP.ExecuteSP(cmd, 2);

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
        /// <returns></returns>
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
                                "the Metadata service must be disabled or broken at present.", true);
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

                if (dataPkgMissingFiles.Count <= 0)
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
        /// <returns></returns>
        public bool VerifyUploadStatus()
        {

            // First obtain a list of status URIs to check
            // Keys are StatusNum integers, values are StatusURI strings
            const int retryCount = 2;
            var dctURIs = GetStatusURIs(retryCount);

            if (dctURIs.Count == 0)
            {
                // Nothing to do
                return true;
            }

            try
            {

                // Confirm that the data packages are visible in MyEMSL Metadata
                // To avoid obtaining too many results from MyEMSL, process the data packages in dctURIs in groups, 5 at a time
                // First construct a unique list of the Data Package IDs in dctURIs

                var distinctDataPackageIDs = (from item in dctURIs select item.Value.DataPackageID).Distinct().ToList();
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

                    var dctURIsInGroup = new Dictionary<int, MyEMSLStatusInfo>();

                    for (var j = i; j < i + DATA_PACKAGE_GROUP_SIZE; j++)
                    {
                        if (j >= distinctDataPackageIDs.Count)
                            break;

                        var currentDataPackageID = distinctDataPackageIDs[j];
                        dataPackageInfoCache.AddDataPackage(currentDataPackageID);

                        // Find all of the URIs for this data package
                        foreach (
                            var uriItem in
                                (from item in dctURIs where item.Value.DataPackageID == currentDataPackageID select item)
                            )
                            dctURIsInGroup.Add(uriItem.Key, uriItem.Value);

                    }

                    // Pre-populate lstDataPackageInfoCache with the files for the current group
                    dataPackageInfoCache.RefreshInfo();

                    foreach (var statusInfo in dctURIsInGroup)
                    {
                        var eResult = VerifyUploadStatusWork(statusChecker, statusInfo, dataPackageInfoCache);

                        if (eResult == UploadStatus.CriticalError)
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
            DataPackageListInfo dataPackageInfoCache)
        {
            var exceptionCount = 0;

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

                var diDataPkg = new DirectoryInfo(statusInfo.Value.LocalPath);
                if (!diDataPkg.Exists)
                {
                    diDataPkg = new DirectoryInfo(statusInfo.Value.SharePath);
                }

                if (!diDataPkg.Exists)
                {
                    OnErrorEvent("Data package folder not found by VerifyUploadStatusWork, this is unexpected; see: " + diDataPkg.FullName);

                    UpdateMyEMSLUploadStatus(statusInfo.Value, verified: false);

                    return UploadStatus.Success;
                }

                if (diDataPkg.Parent == null)
                    throw new DirectoryNotFoundException("Unable to determine the parent directory of " + diDataPkg.FullName);

                // Construct the metadata file path
                // For example, \\protoapps\dataPkgs\Public\2014\MyEMSL_metadata_CaptureJob_1055.txt
                var metadataFilePath = Path.Combine(diDataPkg.Parent.FullName,
                                                    Utilities.GetMetadataFilenameForJob(
                                                        statusInfo.Value.DataPackageID.ToString(CultureInfo.InvariantCulture)));

                var fiMetadataFile = new FileInfo(metadataFilePath);

                if (fiMetadataFile.Exists)
                {
                    var msg = "Deleting metadata file for Data package " + statusInfo.Value.DataPackageID +
                              " since it is now available and verified: " + fiMetadataFile.FullName;

                    if (PreviewMode)
                    {
                        ReportMessage("SIMULATE: " + msg);
                    }
                    else
                    {
                        ReportMessage(msg);
                        fiMetadataFile.Delete();
                    }
                }

                // Update values in the DB
                UpdateMyEMSLUploadStatus(statusInfo.Value, verified: true);

            }
            catch (Exception ex)
            {
                exceptionCount++;
                if (exceptionCount < 3)
                {
                    ReportMessage(
                        "Exception verifying archive status for Data package " + statusInfo.Value.DataPackageID + ", Entry_ID " +
                        statusInfo.Value.EntryID + ": " + ex.Message, BaseLogger.LogLevels.WARN);
                }
                else
                {
                    ReportError(
                        "Exception verifying archive status for for Data Package " + statusInfo.Value.DataPackageID + ", Entry_ID " +
                        statusInfo.Value.EntryID + ": " + ex.Message, true);

                    // Too many errors for this data package; move on to the next one
                    return UploadStatus.VerificationError;
                }
            }

            return UploadStatus.Success;
        }

        #region "Event Handlers"

        void myEMSLUpload_StatusUpdate(object sender, StatusEventArgs e)
        {
            if (DateTime.UtcNow.Subtract(mLastStatusUpdate).TotalSeconds >= 5)
            {
                mLastStatusUpdate = DateTime.UtcNow;
                ReportMessage(e.StatusMessage, BaseLogger.LogLevels.DEBUG);
            }

        }

        void myEMSLUpload_UploadCompleted(object sender, UploadCompletedEventArgs e)
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

        #endregion

    }
}
