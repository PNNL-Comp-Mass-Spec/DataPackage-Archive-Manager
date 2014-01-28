﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Data.SqlClient;
using Pacifica.Core;

namespace DataPackage_Archive_Manager
{
	class clsDataPackageArchiver
	{
		#region "Constants"

		public const string CONNECTION_STRING = "Data Source=gigasax;Initial Catalog=DMS_Data_Package;Integrated Security=SSPI;";

		protected const string SP_NAME_STORE_MYEMSL_STATS = "StoreMyEMSLUploadStats";
		protected const string SP_NAME_SET_MYEMSL_UPLOAD_STATUS = "SetMyEMSLUploadStatus";


		/// <summary>
		/// Maximum number of files to archive
		/// </summary>
		/// <remarks>
		/// Since data package uploads always work with the entire data package folder and all subfolders,
		///   this a maximum cap on the number of files that will be stored in MyEMSL for a given data package
		/// If a data package has more than 600 files, then zip up groups of files before archiving to MyEMSL
		/// </remarks>
		protected const int MAX_FILES_TO_ARCHIVE = 600;

		#endregion

		#region "Structures"
		protected struct udtMyEMSLStatusInfo
		{
			public int EntryID;
			public int DataPackageID;
			public DateTime Entered;
			public string StatusURI;

			public string SharePath;
			public string LocalPath;
		}

		protected struct udtMyEMSLUploadInfo
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

		protected PRISM.DataBase.clsExecuteDatabaseSP m_ExecuteSP;
		protected Upload mMyEMSLUploader;
		protected DateTime mLastStatusUpdate;

		#endregion

		#region "Auto properties"

		public string DBConnectionString
		{ get; private set; }

		public string ErrorMessage
		{ get; private set; }

		public bool PreviewMode
		{ get; set; }

		/// <summary>
		/// Logging level; range is 1-5, where 5 is the most verbose
		/// </summary>
		/// <remarks>Levels are:
		/// DEBUG = 5,
		/// INFO = 4,
		/// WARN = 3,
		/// ERROR = 2,
		/// FATAL = 1</remarks>
		public clsLogTools.LogLevels LogLevel
		{ get; set; }

		#endregion

		/// <summary>
		/// Constructor
		/// </summary>
		public clsDataPackageArchiver(string connectionString, clsLogTools.LogLevels logLevel)
		{
			this.DBConnectionString = connectionString;
			this.LogLevel = logLevel;

			Initialize();
		}

		protected short BoolToTinyInt(bool value)
		{
			if (value)
				return 1;
			else
				return 0;
		}


		protected int CountFilesForDataPackage(clsDataPackageInfo dataPkgInfo)
		{
			var diDataPkg = new DirectoryInfo(dataPkgInfo.LocalPath);
			if (!diDataPkg.Exists)
				diDataPkg = new DirectoryInfo(dataPkgInfo.SharePath);

			if (!diDataPkg.Exists)
				return 0;
			else
				return diDataPkg.GetFiles("*.*", SearchOption.AllDirectories).Length;

		}

		protected List<FileInfoObject> FindDataPackageFilesToArchive(
			clsDataPackageInfo dataPkgInfo,
			DirectoryInfo diDataPkg,
			DateTime dateThreshold,
			MyEMSLReader.DataPackageListInfo dataPackageInfoCache,
			out udtMyEMSLUploadInfo uploadInfo)
		{
			var lstDatasetFilesToArchive = new List<FileInfoObject>();

			uploadInfo = new udtMyEMSLUploadInfo();
			uploadInfo.Clear();

			uploadInfo.SubDir = dataPkgInfo.FolderName;

			// Construct a list of the files on disk for this data package
			var lstDataPackageFilesAll = diDataPkg.GetFiles("*.*", SearchOption.AllDirectories).ToList();

			if (lstDataPackageFilesAll.Count == 0)
			{
				// Nothing to archive; this is not an error
				ReportMessage("Data package " + dataPkgInfo.ID + " does not have any files; nothing to archive", clsLogTools.LogLevels.DEBUG);
				return new List<FileInfoObject>();
			}

			// Look for any Auto-process folders that have recently modified files
			// These folders will be skipped until the files are at least 60 minutes old
			// This list contains folder paths to skip
			var lstDataPackageFoldersToSkip = new List<string>();

			var lstDataPackageFolders = diDataPkg.GetDirectories("*", SearchOption.AllDirectories).ToList();
			var reAutoJobFolder = new Regex(@"^[A-Z].{1,5}\d{12,12}_Auto\d+", RegexOptions.Compiled | RegexOptions.IgnoreCase);

			foreach (var dataPkgFolder in lstDataPackageFolders)
			{
				if (reAutoJobFolder.IsMatch(dataPkgFolder.Name))
				{
					bool skipFolder = false;
					foreach (var file in dataPkgFolder.GetFiles("*.*", SearchOption.AllDirectories))
					{
						if (DateTime.UtcNow.Subtract(file.LastWriteTimeUtc).TotalHours < 1)
						{
							skipFolder = true;
							break;
						}
					}

					if (skipFolder)
						lstDataPackageFoldersToSkip.Add(dataPkgFolder.FullName);
				}
			}


			// Filter out files that we do not want to archive
			var lstFilesToSkip = new SortedSet<string>(StringComparer.CurrentCultureIgnoreCase);
			lstFilesToSkip.Add("Thumbs.db");
			lstFilesToSkip.Add(".DS_Store");
			lstFilesToSkip.Add(".Rproj.user");

			var lstDataPackageFiles = new List<FileInfo>();
			foreach (var dataPkgFile in lstDataPackageFilesAll)
			{
				bool keep = true;
				if (lstFilesToSkip.Contains(dataPkgFile.Name))
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

				foreach (var dataPkgFolder in lstDataPackageFoldersToSkip)
				{
					if (dataPkgFile.Directory.FullName.StartsWith(dataPkgFolder))
					{
						keep = false;
						break;
					}
				}

				if (keep)
					lstDataPackageFiles.Add(dataPkgFile);

			}


			if (lstDataPackageFiles.Count > MAX_FILES_TO_ARCHIVE)
			{
				ReportError("Data package " + dataPkgInfo.ID + " has " + lstDataPackageFiles.Count + " files; the maximum number of files allowed in MyEMSL per data package is " + MAX_FILES_TO_ARCHIVE + "; zip up groups of files to reduce the total file count; see " + dataPkgInfo.SharePath, true);
				return new List<FileInfoObject>();
			}
			else if (lstDataPackageFiles.Count == 0)
			{
				// Nothing to archive; this is not an error
				string msg = "Data package " + dataPkgInfo.ID + " has " + lstDataPackageFilesAll.Count + " files, but all have been skipped";

				if (lstDataPackageFoldersToSkip.Count > 0)
					msg += " due to recently modified files in auto-job result folders";
				else
					msg += " since they are system or temporary files";

				ReportMessage(msg + "; nothing to archive", clsLogTools.LogLevels.INFO);
				ReportMessage("  Data package " + dataPkgInfo.ID + " path: " + diDataPkg.FullName, clsLogTools.LogLevels.DEBUG);
				return new List<FileInfoObject>();
			}

			// Make sure at least one of the files was modified after the date threshold
			bool passesDateThreshold = false;
			foreach (var fiLocalFile in lstDataPackageFiles)
			{
				if (fiLocalFile.LastWriteTime >= dateThreshold)
				{
					passesDateThreshold = true;
					break;
				}
			}

			if (!passesDateThreshold)
			{
				// None of the modified files passes the date threshold
				string msg;

				if (lstDataPackageFilesAll.Count == 1)
					msg = "Data package " + dataPkgInfo.ID + " has 1 file, but it was modified before " + dateThreshold.ToString("yyyy-MM-dd");
				else
					msg = "Data package " + dataPkgInfo.ID + " has " + lstDataPackageFilesAll.Count + " files, but all were modified before " + dateThreshold.ToString("yyyy-MM-dd");

				ReportMessage(msg + "; nothing to archive", clsLogTools.LogLevels.DEBUG);
				return new List<FileInfoObject>();
			}

			// Note: subtracting 60 seconds from UtcNow when initialize dtLastProgress so that a progress message will appear as an "INFO" level log message if 5 seconds elapses
			// After that, the INFO level messages will appear every 30 seconds
			DateTime dtLastProgress = System.DateTime.UtcNow.AddSeconds(-60);
			DateTime dtLastProgressDetail = System.DateTime.UtcNow;

			int filesProcessed = 0;

			foreach (var fiLocalFile in lstDataPackageFiles)
			{
				// Note: when storing data package files in MyEMSL the SubDir path will always start with the data package folder name
				string subDir = string.Copy(uploadInfo.SubDir);

				if (fiLocalFile.Directory.FullName.Length > diDataPkg.FullName.Length)
				{
					// Append the subdirectory path
					subDir = Path.Combine(subDir, fiLocalFile.Directory.FullName.Substring(diDataPkg.FullName.Length + 1));
				}

				// Look for this file in MyEMSL
				var archiveFiles = dataPackageInfoCache.FindFiles(fiLocalFile.Name, subDir, dataPkgInfo.ID);

				if (diDataPkg.Parent == null)
					throw new DirectoryNotFoundException("Unable to determine the parent folder for directory " + diDataPkg.Name);

				if (archiveFiles.Count == 0)
				{
					lstDatasetFilesToArchive.Add(new FileInfoObject(fiLocalFile.FullName, diDataPkg.Parent.FullName));
					uploadInfo.FileCountNew++;
					uploadInfo.Bytes += fiLocalFile.Length;
				}
				else
				{
					var archiveFile = archiveFiles.First();

					// Compare file size
					if (fiLocalFile.Length != archiveFile.FileInfo.FileSizeBytes)
					{
						lstDatasetFilesToArchive.Add(new FileInfoObject(fiLocalFile.FullName, diDataPkg.Parent.FullName));
						uploadInfo.FileCountUpdated++;
						uploadInfo.Bytes += fiLocalFile.Length;
					}
					else
					{
						// Compare Sha-1 hash
						string sha1HashHex = Utilities.GenerateSha1Hash(fiLocalFile.FullName);

						if (sha1HashHex != archiveFile.FileInfo.Sha1Hash)
						{
							string relativeDestinationDirectory = FileInfoObject.GenerateRelativePath(fiLocalFile.Directory.FullName, diDataPkg.Parent.FullName);

							lstDatasetFilesToArchive.Add(new FileInfoObject(fiLocalFile.FullName, relativeDestinationDirectory, sha1HashHex));
							uploadInfo.FileCountUpdated++;
							uploadInfo.Bytes += fiLocalFile.Length;
						}
					}

				}

				filesProcessed++;
				if (DateTime.UtcNow.Subtract(dtLastProgressDetail).TotalSeconds >= 5)
				{
					dtLastProgressDetail = DateTime.UtcNow;

					string progressMessage = "Finding files to archive for data package " + dataPkgInfo.ID + ": " + filesProcessed + " / " + lstDataPackageFiles.Count;
					if (DateTime.UtcNow.Subtract(dtLastProgress).TotalSeconds >= 30)
					{
						dtLastProgress = DateTime.UtcNow;
						ReportMessage(progressMessage, clsLogTools.LogLevels.INFO);
					}
					else
					{
						ReportMessage(progressMessage, clsLogTools.LogLevels.DEBUG);
					}
				}
			}

			if (lstDatasetFilesToArchive.Count == 0)
			{
				// Nothing to archive; this is not an error
				ReportMessage("All files for data package " + dataPkgInfo.ID + " are already in MyEMSL; FileCount=" + lstDataPackageFiles.Count, clsLogTools.LogLevels.DEBUG);
				return lstDatasetFilesToArchive;
			}

			return lstDatasetFilesToArchive;
		}

		protected DateTime GetDBDate(SqlDataReader reader, string columnName)
		{
			object value = reader[columnName];

			if (Convert.IsDBNull(value))
				return DateTime.Now;
			else
				return (DateTime)value;

		}

		protected string GetDBString(SqlDataReader reader, string columnName)
		{
			object value = reader[columnName];

			if (Convert.IsDBNull(value))
				return string.Empty;
			else
				return (string)value;

		}

		protected List<clsDataPackageInfo> GetFilteredDataPackageInfoList(List<clsDataPackageInfo> lstDataPkgInfo, List<int> dataPkgGroup)
		{
			var lstFilteredDataPkgInfo = 
				(from item in lstDataPkgInfo
				 join dataPkgID in dataPkgGroup on item.ID equals dataPkgID
				 select item).ToList();

			return lstFilteredDataPkgInfo;
		}

		protected Dictionary<int, udtMyEMSLStatusInfo> GetStatusURIs(int retryCount)
		{
			var dctURIs = new Dictionary<int, udtMyEMSLStatusInfo>();

			try
			{

				string sql = " SELECT MU.Entry_ID, MU.Data_Package_ID, MU.Entered, MU.StatusNum, MU.Status_URI, DP.Local_Path, DP.Share_Path " +
							 " FROM V_MyEMSL_Uploads MU INNER JOIN V_Data_Package_Export DP ON MU.Data_Package_ID = DP.ID" +
							 " WHERE MU.ErrorCode = 0 And (MU.Available = 0 Or MU.Verified = 0) AND ISNULL(MU.StatusNum, 0) > 0";

				while (retryCount > 0)
				{
					try
					{
						using (var cnDB = new SqlConnection(this.DBConnectionString))
						{
							cnDB.Open();

							var cmd = new SqlCommand(sql, cnDB);
							var reader = cmd.ExecuteReader();

							while (reader.Read())
							{
								var statusInfo = new udtMyEMSLStatusInfo();

								statusInfo.EntryID = reader.GetInt32(0);
								statusInfo.DataPackageID = reader.GetInt32(1);
								statusInfo.Entered = reader.GetDateTime(2);

								int statusNum = reader.GetInt32(3);

								if (!Convert.IsDBNull(reader.GetValue(4)))
								{
									string value = (string)reader.GetValue(4);
									if (!string.IsNullOrEmpty(value))
									{
										statusInfo.StatusURI = value;

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
						string msg = "Exception querying database in GetStatusURIs: " + ex.Message;
						msg += ", RetryCount = " + retryCount.ToString();
						ReportError(msg, true);

						//Delay for 5 second before trying again
						System.Threading.Thread.Sleep(5000);
					}
				}
			}
			catch (Exception ex)
			{
				string msg = "Exception connecting to database in GetStatusURIs: " + ex.Message + "; ConnectionString: " + this.DBConnectionString;
				ReportError(msg, false);
			}

			return dctURIs;
		}

		protected void Initialize()
		{
			this.ErrorMessage = string.Empty;
			this.mLastStatusUpdate = DateTime.UtcNow;

			// Set up the loggers
			string logFileName = @"Logs\DataPkgArchiver";
			clsLogTools.CreateFileLogger(logFileName, this.LogLevel);

			clsLogTools.CreateDbLogger(this.DBConnectionString, "DataPkgArchiver: " + Environment.MachineName);

			// Make initial log entry
			string msg = "=== Started Data Package Archiver V" + System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString() + " ===== ";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

			m_ExecuteSP = new PRISM.DataBase.clsExecuteDatabaseSP(this.DBConnectionString);
			m_ExecuteSP.DBErrorEvent += new PRISM.DataBase.clsExecuteDatabaseSP.DBErrorEventEventHandler(m_ExecuteSP_DBErrorEvent);

			mMyEMSLUploader = new Upload();

			// Attach the events			
			mMyEMSLUploader.DebugEvent += new Pacifica.Core.MessageEventHandler(myEMSLUpload_DebugEvent);
			mMyEMSLUploader.ErrorEvent += new Pacifica.Core.MessageEventHandler(myEMSLUpload_ErrorEvent);

			mMyEMSLUploader.StatusUpdate += new Pacifica.Core.StatusUpdateEventHandler(myEMSLUpload_StatusUpdate);
			mMyEMSLUploader.UploadCompleted += new UploadCompletedEventHandler(myEMSLUpload_UploadCompleted);


		}

		protected List<clsDataPackageInfo> LookupDataPkgInfo(List<KeyValuePair<int, int>> lstDataPkgIDs)
		{
			var lstDataPkgInfo = new List<clsDataPackageInfo>();

			try
			{

				using (var cnDB = new SqlConnection(this.DBConnectionString))
				{
					cnDB.Open();

					var sql = new StringBuilder();

					sql.Append(" SELECT ID, Name, Created, Package_File_Folder, Share_Path, Local_Path FROM V_Data_Package_Export");

					if (lstDataPkgIDs.Count > 0)
					{
						sql.Append(" WHERE ");

						for (int i = 0; i < lstDataPkgIDs.Count; i++)
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

					var cmd = new SqlCommand(sql.ToString(), cnDB);
					var reader = cmd.ExecuteReader();

					while (reader.Read())
					{
						int dataPkgID = reader.GetInt32(0);

						var dataPkgInfo = new clsDataPackageInfo(dataPkgID);

						dataPkgInfo.Name = GetDBString(reader, "Name");

						dataPkgInfo.Created = GetDBDate(reader, "Created");

						dataPkgInfo.FolderName = GetDBString(reader, "Package_File_Folder");
						dataPkgInfo.SharePath = GetDBString(reader, "Share_Path");
						dataPkgInfo.LocalPath = GetDBString(reader, "Local_Path");

						lstDataPkgInfo.Add(dataPkgInfo);
					}
				}

				return lstDataPkgInfo;

			}
			catch (Exception ex)
			{
				ReportError("Error in LookupDataPkgInfo: " + ex.Message, true);
				return new List<clsDataPackageInfo>();
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
					int dashIndex = item.IndexOf("-");
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

					int dataPkgIDStart;
					int dataPkgIDEnd;
					if (int.TryParse(startID, out dataPkgIDStart) && int.TryParse(endID, out dataPkgIDEnd))
					{
						KeyValuePair<int, int> idRange = new KeyValuePair<int, int>(dataPkgIDStart, dataPkgIDEnd);

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
		/// <returns></returns>
		public bool ProcessDataPackages(List<KeyValuePair<int, int>> lstDataPkgIDs, DateTime dateThreshold)
		{

			List<clsDataPackageInfo> lstDataPkgInfo = new List<clsDataPackageInfo>();
			int successCount = 0;

			try
			{
				lstDataPkgInfo = LookupDataPkgInfo(lstDataPkgIDs);

				if (lstDataPkgInfo.Count == 0)
				{
					ReportError("None of the data packages in lstDataPkgIDs corresponded to a known data package ID");
					return false;
				}

				if (Environment.UserName.ToLower() != "svc-dms")
				{
					// The current user is not svc-dms
					// Uploaded files would be associated with the wrong username and thus would not be visible to all DMS Users
					if (!this.PreviewMode)
					{
						this.PreviewMode = true;
						ReportMessage(@"Current user is not pnl\svc-dms; auto-enabling PreviewMode");
					}
				}

				// List of groups of data package IDs
				var lstDataPkgGroups = new List<List<int>>();
				var lstCurrentGroup = new List<int>();
				DateTime dtLastProgress = DateTime.UtcNow.AddSeconds(-15);

				int runningCount = 0;

				// Determine the number of files that are associated with each data package
				// We will use this information to process the data packages in chunks
				for (int i = 0; i < lstDataPkgInfo.Count; i++)
				{
					int fileCount = CountFilesForDataPackage(lstDataPkgInfo[i]);

					if (runningCount + fileCount > 5000)
					{
						// Store the current group
						if (lstCurrentGroup.Count > 0)
							lstDataPkgGroups.Add(lstCurrentGroup);

						// Make a new group
						lstCurrentGroup = new List<int>();
						lstCurrentGroup.Add(lstDataPkgInfo[i].ID);
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

				foreach (var dataPkgGroup in lstDataPkgGroups)
				{
					var dataPackageInfoCache = new MyEMSLReader.DataPackageListInfo();
					foreach (int dataPkgID in dataPkgGroup)
					{
						dataPackageInfoCache.AddDataPackage(dataPkgID);
					}

					// Pre-populate lstDataPackageInfoCache with the files for the current group
					dataPackageInfoCache.RefreshInfo();

					// Obtain the clsDataPackageInfo objects for the IDs in dataPkgGroup
					var lstFilteredDataPkgInfo = GetFilteredDataPackageInfoList(lstDataPkgInfo, dataPkgGroup);

					foreach (var dataPkgInfo in lstFilteredDataPkgInfo)
					{
						bool success = ProcessOneDataPackage(dataPkgInfo, dateThreshold, dataPackageInfoCache);

						if (success)
							successCount += 1;

					}
				}

			}
			catch (Exception ex)
			{
				ReportError("Error in ProcessDataPackages: " + ex.Message, true);
				return false;
			}

			if (successCount == lstDataPkgInfo.Count)
			{
				if (successCount == 1)
					ReportMessage("Processing complete for data package " + lstDataPkgInfo.First().ID);
				else
					ReportMessage("Processed " + successCount + " data packages");

				return true;
			}
			else
			{
				if (this.PreviewMode)
					return true;

				if (lstDataPkgInfo.Count == 1)
					ReportError("Failed to archive data package " + lstDataPkgIDs.First());
				else if (successCount == 0)
					ReportError("Failed to archive any of the " + lstDataPkgIDs.Count + " candidate data packages", true);
				else
					ReportError("Failed to archive " + (lstDataPkgInfo.Count - successCount).ToString() + " data package(s); successfully archived " + successCount + " data package(s)", true);

				return false;
			}

		}

		protected bool ProcessOneDataPackage(clsDataPackageInfo dataPkgInfo, DateTime dateThreshold, MyEMSLReader.DataPackageListInfo dataPackageInfoCache)
		{
			bool success = false;
			udtMyEMSLUploadInfo uploadInfo = new udtMyEMSLUploadInfo();
			uploadInfo.Clear();

			DateTime dtStartTime = System.DateTime.UtcNow;

			try
			{
				var diDataPkg = new DirectoryInfo(dataPkgInfo.LocalPath);
				if (!diDataPkg.Exists)
					diDataPkg = new DirectoryInfo(dataPkgInfo.SharePath);

				if (!diDataPkg.Exists)
				{
					ReportMessage("Data package folder not found (also tried remote share path): " + dataPkgInfo.LocalPath, clsLogTools.LogLevels.WARN, false);
					return false;
				}

				// Look for an existing metadata file
				string metadataFilePath = Path.Combine(diDataPkg.Parent.FullName, Utilities.GetMetadataFilenameForJob(dataPkgInfo.ID.ToString()));
				var fiMetadataFile = new FileInfo(metadataFilePath);
				if (fiMetadataFile.Exists)
				{
					if (DateTime.UtcNow.Subtract(fiMetadataFile.LastWriteTimeUtc).TotalHours > 48)
					{
						ReportError("Data Package " + dataPkgInfo.ID + " has an existing metadata file over 48 hours old; deleting file: " + fiMetadataFile.FullName, true);
						fiMetadataFile.Delete();
					}
					else
					{
						ReportMessage("Data Package " + dataPkgInfo.ID + " has an existing metadata file less than 48 hours old; skipping this data package", clsLogTools.LogLevels.WARN);
						ReportMessage("  " + fiMetadataFile.FullName, clsLogTools.LogLevels.DEBUG);

						// This is not a fatal error; return true
						return true;
					}
				}

				// Compare the files to those already in MyEMSL to create a list of files to be uploaded
				List<FileInfoObject> lstUnmatchedFiles = FindDataPackageFilesToArchive(dataPkgInfo, diDataPkg, dateThreshold, dataPackageInfoCache, out uploadInfo);

				if (lstUnmatchedFiles.Count == 0)
				{
					// Nothing to do
					return true;
				}

				if (this.PreviewMode)
				{
					if (lstUnmatchedFiles.Count == 0)
					{
						ReportMessage("All files for Data Package " + dataPkgInfo.ID + " are already up-to-date in the archive");
					}
					else
					{
						ReportMessage("Need to upload " + lstUnmatchedFiles.Count + " file(s) for Data Package " + dataPkgInfo.ID);

						// Preview the changes
						foreach (var unmatchedFile in lstUnmatchedFiles)
						{
							Console.WriteLine("  " + unmatchedFile.RelativeDestinationFullPath);
						}
					}

				}
				else
				{
					// Upload the files
					ReportMessage("Uploading " + lstUnmatchedFiles.Count + " new/changed files for Data Package " + dataPkgInfo.ID);

					Upload.udtUploadMetadata uploadMetadata = new Upload.udtUploadMetadata();
					uploadMetadata.Clear();

					uploadMetadata.DataPackageID = dataPkgInfo.ID;
					uploadMetadata.SubFolder = uploadInfo.SubDir;

					// Instantiate the metadata object
					Dictionary<string, object> metadataObject = Upload.CreateMetadataObject(uploadMetadata, lstUnmatchedFiles);
					string statusURL;

					mMyEMSLUploader.TransferFolderPath = diDataPkg.Parent.FullName;
					mMyEMSLUploader.JobNumber = dataPkgInfo.ID.ToString();

					success = mMyEMSLUploader.StartUpload(metadataObject, out statusURL);

					var tsElapsedTime = System.DateTime.UtcNow.Subtract(dtStartTime);

					uploadInfo.UploadTimeSeconds = tsElapsedTime.TotalSeconds;
					uploadInfo.StatusURI = statusURL;

					if (success)
					{

						string msg = "Upload of changed files for Data Package " + dataPkgInfo.ID + " completed in " + tsElapsedTime.TotalSeconds.ToString("0.0") + " seconds: " + uploadInfo.FileCountNew + " new files, " + uploadInfo.FileCountUpdated + " updated files, " + uploadInfo.Bytes + " bytes";
						ReportMessage(msg);

						msg = "  myEMSL statusURI => " + uploadInfo.StatusURI;
						ReportMessage(msg, clsLogTools.LogLevels.DEBUG);
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
				ReportError("Error in ProcessOneDataPackage processing Data Package " + dataPkgInfo.ID + ": " + ex.Message, true);

				uploadInfo.ErrorCode = ex.Message.GetHashCode();
				if (uploadInfo.ErrorCode == 0)
					uploadInfo.ErrorCode = 1;

				uploadInfo.UploadTimeSeconds = System.DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds;

				StoreMyEMSLUploadStats(dataPkgInfo, uploadInfo);

				return false;
			}

		}

		protected void ReportMessage(string message)
		{
			ReportMessage(message, clsLogTools.LogLevels.INFO, logToDB: false);
		}

		protected void ReportMessage(string message, clsLogTools.LogLevels logLevel)
		{
			ReportMessage(message, logLevel, logToDB: false);
		}

		protected void ReportMessage(string message, clsLogTools.LogLevels logLevel, bool logToDB)
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, logLevel, message);

			if (logToDB)
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, logLevel, message);

			OnMessage(new MessageEventArgs(message));
		}

		protected void ReportError(string message)
		{
			ReportError(message, false);
		}

		protected void ReportError(string message, bool logToDB)
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, message);

			if (logToDB)
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, message);

			OnErrorMessage(new MessageEventArgs(message));

			this.ErrorMessage = string.Copy(message);
		}

		/// <summary>
		/// Update the data packages in lstDataPkgIDs, then verify the upload status
		/// </summary>
		/// <param name="lstDataPkgIDs"></param>
		/// <param name="previewMode"></param>
		/// <returns></returns>
		public bool StartProcessing(List<KeyValuePair<int, int>> lstDataPkgIDs, DateTime dateThreshold, bool previewMode)
		{

			this.PreviewMode = previewMode;

			// Upload new data
			bool success = ProcessDataPackages(lstDataPkgIDs, dateThreshold);

			// Verify uploaded data (even if success is false)
			// We're setting PreviewMode again in case it was auto-set to True because the current user is not svc-dms
			this.PreviewMode = previewMode;
			VerifyUploadStatus();

			return success;
		}

		protected bool StoreMyEMSLUploadStats(clsDataPackageInfo dataPkgInfo, udtMyEMSLUploadInfo uploadInfo)
		{

			bool Outcome = false;

			try
			{

				//Setup for execution of the stored procedure
				var cmd = new SqlCommand();
				{
					cmd.CommandType = System.Data.CommandType.StoredProcedure;
					cmd.CommandText = SP_NAME_STORE_MYEMSL_STATS;

					cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@Return", System.Data.SqlDbType.Int));
					cmd.Parameters["@Return"].Direction = System.Data.ParameterDirection.ReturnValue;

					cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@DataPackageID", System.Data.SqlDbType.Int));
					cmd.Parameters["@DataPackageID"].Direction = System.Data.ParameterDirection.Input;
					cmd.Parameters["@DataPackageID"].Value = Convert.ToInt32(dataPkgInfo.ID);

					cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@Subfolder", System.Data.SqlDbType.VarChar, 128));
					cmd.Parameters["@Subfolder"].Direction = System.Data.ParameterDirection.Input;
					cmd.Parameters["@Subfolder"].Value = uploadInfo.SubDir;

					cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@FileCountNew", System.Data.SqlDbType.Int));
					cmd.Parameters["@FileCountNew"].Direction = System.Data.ParameterDirection.Input;
					cmd.Parameters["@FileCountNew"].Value = uploadInfo.FileCountNew;

					cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@FileCountUpdated", System.Data.SqlDbType.Int));
					cmd.Parameters["@FileCountUpdated"].Direction = System.Data.ParameterDirection.Input;
					cmd.Parameters["@FileCountUpdated"].Value = uploadInfo.FileCountUpdated;

					cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@Bytes", System.Data.SqlDbType.BigInt));
					cmd.Parameters["@Bytes"].Direction = System.Data.ParameterDirection.Input;
					cmd.Parameters["@Bytes"].Value = uploadInfo.Bytes;

					cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@UploadTimeSeconds", System.Data.SqlDbType.Real));
					cmd.Parameters["@UploadTimeSeconds"].Direction = System.Data.ParameterDirection.Input;
					cmd.Parameters["@UploadTimeSeconds"].Value = (float)uploadInfo.UploadTimeSeconds;

					cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@StatusURI", System.Data.SqlDbType.VarChar, 255));
					cmd.Parameters["@StatusURI"].Direction = System.Data.ParameterDirection.Input;
					cmd.Parameters["@StatusURI"].Value = uploadInfo.StatusURI;

					cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@ErrorCode", System.Data.SqlDbType.Int));
					cmd.Parameters["@ErrorCode"].Direction = System.Data.ParameterDirection.Input;
					cmd.Parameters["@ErrorCode"].Value = uploadInfo.ErrorCode;
				}

				ReportMessage("Calling " + SP_NAME_STORE_MYEMSL_STATS + " for Data Package " + dataPkgInfo.ID, clsLogTools.LogLevels.DEBUG);

				//Execute the SP (retry the call up to 4 times)
				m_ExecuteSP.TimeoutSeconds = 20;
				var resCode = m_ExecuteSP.ExecuteSP(cmd, 4);

				if (resCode == 0)
				{
					Outcome = true;
				}
				else
				{
					string Msg = "Error " + resCode.ToString() + " storing MyEMSL Upload Stats";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg);
					Outcome = false;
				}

			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception storing the MyEMSL upload stats: " + ex.Message);
				Outcome = false;
			}

			return Outcome;

		}

		protected bool UpdateMyEMSLUploadStatus(udtMyEMSLStatusInfo statusInfo, bool available, bool verified)
		{

			try
			{
				var cmd = new SqlCommand(SP_NAME_SET_MYEMSL_UPLOAD_STATUS);
				cmd.CommandType = System.Data.CommandType.StoredProcedure;

				cmd.Parameters.Add("@Return", System.Data.SqlDbType.Int);
				cmd.Parameters["@Return"].Direction = System.Data.ParameterDirection.ReturnValue;

				cmd.Parameters.Add("@EntryID", System.Data.SqlDbType.Int);
				cmd.Parameters["@EntryID"].Direction = System.Data.ParameterDirection.Input;
				cmd.Parameters["@EntryID"].Value = statusInfo.EntryID;

				cmd.Parameters.Add("@DataPackageID", System.Data.SqlDbType.Int);
				cmd.Parameters["@DataPackageID"].Direction = System.Data.ParameterDirection.Input;
				cmd.Parameters["@DataPackageID"].Value = statusInfo.DataPackageID;

				cmd.Parameters.Add("@Available", System.Data.SqlDbType.TinyInt);
				cmd.Parameters["@Available"].Direction = System.Data.ParameterDirection.Input;
				cmd.Parameters["@Available"].Value = BoolToTinyInt(available);

				cmd.Parameters.Add("@Verified", System.Data.SqlDbType.TinyInt);
				cmd.Parameters["@Verified"].Direction = System.Data.ParameterDirection.Input;
				cmd.Parameters["@Verified"].Value = BoolToTinyInt(verified);

				cmd.Parameters.Add("@message", System.Data.SqlDbType.VarChar, 512);
				cmd.Parameters["@message"].Direction = System.Data.ParameterDirection.Output;

				var paramDatasetID = cmd.CreateParameter();

				if (this.PreviewMode)
				{
					Console.WriteLine("Simulate call to " + SP_NAME_SET_MYEMSL_UPLOAD_STATUS + " for Entry_ID=" + statusInfo.EntryID + ", DataPackageID=" + statusInfo.DataPackageID + " Entry_ID=" + statusInfo.EntryID);
					return true;
				}
				else
				{
					ReportMessage("  Calling " + SP_NAME_SET_MYEMSL_UPLOAD_STATUS + " for Data Package " + statusInfo.DataPackageID, clsLogTools.LogLevels.DEBUG);
				}

				m_ExecuteSP.TimeoutSeconds = 20;
				var resCode = m_ExecuteSP.ExecuteSP(cmd, 2);

				if (resCode == 0)
					return true;
				else
				{
					var msg = "Error " + resCode + " calling stored procedure " + SP_NAME_SET_MYEMSL_UPLOAD_STATUS;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return false;
				}

			}
			catch (Exception ex)
			{
				var msg = "Exception calling stored procedure " + SP_NAME_SET_MYEMSL_UPLOAD_STATUS;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
				return false;
			}
		}

		public bool VerifyUploadStatus()
		{
			// Query the database to find the status URIs that need to be verified
			// Verify each one, updating the database as appropriate (if PreviewMode=false)

			// Post an error to the DB if data has not been ingested within 24 hours or verified within 48 hours (and PreviewMode=false)

			// First obtain a list of status URIs to check
			// Keys are StatusNum integers, values are StatusURI strings
			int retryCount = 2;
			var dctURIs = GetStatusURIs(retryCount);

			if (dctURIs.Count == 0)
			{
				// Nothing to do
				return true;
			}

			// Call the testauth service to obtain a cookie for this session
			string authURL = Configuration.TestAuthUri;
			Auth auth = new Auth(new Uri(authURL));

			CookieContainer cookieJar = null;
			if (!auth.GetAuthCookies(out cookieJar))
			{
				string msg = "Auto-login to " + Configuration.TestAuthUri + " failed authentication";
				ReportError(msg);
				return false;
			}

			int exceptionCount = 0;

			// Confirm that the data package is visible in Elastic Search
			var dataPackageInfoCache = new MyEMSLReader.DataPackageListInfo();
			foreach (var statusInfo in dctURIs)
			{
				dataPackageInfoCache.AddDataPackage(statusInfo.Value.DataPackageID);
			}

			// Prepopulate lstDataPackageInfoCache with the files for the current group
			dataPackageInfoCache.RefreshInfo();

			var statusChecker = new MyEMSLStatusCheck();

			foreach (var statusInfo in dctURIs)
			{
				bool accessDenied;
				string statusMessage;

				try
				{
					bool available = false;
					bool verified = false;

					// First check step 5 (Available in MyEMSL)
					available = statusChecker.IngestStepCompleted(statusInfo.Value.StatusURI, MyEMSLStatusCheck.StatusStep.Available, cookieJar, out accessDenied, out statusMessage);

					if (accessDenied)
					{
						ReportError("Error looking up archive status for Data Package " + statusInfo.Value.DataPackageID + ", Entry_ID " + statusInfo.Value.EntryID + "; " + statusMessage);
						Utilities.Logout(cookieJar);
						return false;
					}

					if (available)
					{
						var archiveFiles = dataPackageInfoCache.FindFiles("*", "", statusInfo.Value.DataPackageID);

						if (archiveFiles.Count > 0)
							ReportMessage("Data package " + statusInfo.Value.DataPackageID + " is available in MyEMSL Elastic Search", clsLogTools.LogLevels.DEBUG);
						else
						{
							ReportMessage("Data package " + statusInfo.Value.DataPackageID + " is not yet available in MyEMSL Elastic Search", clsLogTools.LogLevels.DEBUG);
							available = false;
						}
					}

					if (!available && System.DateTime.Now.Subtract(statusInfo.Value.Entered).TotalHours > 24)
					{
						ReportError("Data package " + statusInfo.Value.DataPackageID + " is not available in MyEMSL after 24 hours; see " + statusInfo.Value.StatusURI, true);
					}

					if (available)
					{
						// Next check step 6 (Archived and Sha-1 hash values checked)
						verified = statusChecker.IngestStepCompleted(statusInfo.Value.StatusURI, MyEMSLStatusCheck.StatusStep.Archived, cookieJar, out accessDenied, out statusMessage);

						if (verified)
							ReportMessage("Data package " + statusInfo.Value.DataPackageID + " has been verified against expected hash values", clsLogTools.LogLevels.DEBUG);
					}

					if (!verified && System.DateTime.Now.Subtract(statusInfo.Value.Entered).TotalHours > 48)
					{
						ReportError("Data package " + statusInfo.Value.DataPackageID + " has not been validated in the archive after 48 hours; see " + statusInfo.Value.StatusURI, true);
					}

					if (available || verified)
					{
						// Update values in the DB
						UpdateMyEMSLUploadStatus(statusInfo.Value, available, verified);

						if (available && verified)
						{
							var diDataPkg = new DirectoryInfo(statusInfo.Value.LocalPath);
							if (!diDataPkg.Exists)
								diDataPkg = new DirectoryInfo(statusInfo.Value.SharePath);

							if (diDataPkg.Exists)
							{
								string metadataFilePath = Path.Combine(diDataPkg.Parent.FullName, Utilities.GetMetadataFilenameForJob(statusInfo.Value.DataPackageID.ToString()));
								var fiMetadataFile = new FileInfo(metadataFilePath);

								if (fiMetadataFile.Exists)
								{
									string msg = "Deleting metadata file for Data Package " + statusInfo.Value.DataPackageID + " since it is now available and verified: " + fiMetadataFile.FullName;

									if (this.PreviewMode)
									{
										ReportMessage("SIMULATE: " + msg);
									}
									else
									{
										ReportMessage(msg);
										fiMetadataFile.Delete();
									}
								}
							}

						}
					}

				}
				catch (Exception ex)
				{
					exceptionCount++;
					if (exceptionCount < 3)
					{
						ReportMessage("Exception verifying archive status for Data Package " + statusInfo.Value.DataPackageID + ", Entry_ID " + statusInfo.Value.EntryID + ": " + ex.Message, clsLogTools.LogLevels.WARN);
					}
					else
					{
						ReportError("Exception verifying archive status for for Data Package " + statusInfo.Value.DataPackageID + ", Entry_ID " + statusInfo.Value.EntryID + ": " + ex.Message, true);
						break;
					}
				}

				exceptionCount = 0;

			}

			Utilities.Logout(cookieJar);

			return true;


		}

		#region "Events"

		public event MessageEventHandler ErrorEvent;
		public event MessageEventHandler MessageEvent;
		public event ProgressEventHandler ProgressEvent;

		#endregion

		#region "Event Handlers"


		private void m_ExecuteSP_DBErrorEvent(string Message)
		{
			ReportError("Stored procedure execution error: " + Message, true);
		}


		void myEMSLUpload_DebugEvent(object sender, Pacifica.Core.MessageEventArgs e)
		{
			if (!string.IsNullOrWhiteSpace(e.Message))
				ReportMessage(e.Message, clsLogTools.LogLevels.DEBUG);
		}

		void myEMSLUpload_ErrorEvent(object sender, Pacifica.Core.MessageEventArgs e)
		{
			ReportError(e.Message, true);
		}

		void myEMSLUpload_StatusUpdate(object sender, Pacifica.Core.StatusEventArgs e)
		{
			if (DateTime.UtcNow.Subtract(mLastStatusUpdate).TotalSeconds >= 5)
			{
				mLastStatusUpdate = DateTime.UtcNow;
				ReportMessage(e.StatusMessage, clsLogTools.LogLevels.DEBUG);
			}

		}

		void myEMSLUpload_UploadCompleted(object sender, Pacifica.Core.UploadCompletedEventArgs e)
		{
			string msg = "Upload complete";

			// Note that e.ServerResponse will simply have the StatusURL if the upload succeeded
			// If a problem occurred, then e.ServerResponse will either have the full server reponse, or may even be blank
			if (string.IsNullOrEmpty(e.ServerResponse))
				msg += ": empty server reponse";
			else
				msg += ": " + e.ServerResponse;

			ReportMessage(msg, clsLogTools.LogLevels.INFO);
		}

		public void OnErrorMessage(MessageEventArgs e)
		{
			if (ErrorEvent != null)
				ErrorEvent(this, e);
		}

		public void OnMessage(MessageEventArgs e)
		{
			if (MessageEvent != null)
				MessageEvent(this, e);
		}

		public void OnProgressUpdate(ProgressEventArgs e)
		{
			if (ProgressEvent != null)
				ProgressEvent(this, e);
		}

		#endregion

	}
}
