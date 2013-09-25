using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
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

		#endregion

		#region "Structures"
		protected struct udtMyEMSLStatusInfo
		{
			public int EntryID;
			public int DataPackageID;
			public DateTime Entered;
			public string StatusURI;
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
		}

		#endregion

		#region "Class variables"
		protected PRISM.DataBase.clsExecuteDatabaseSP m_ExecuteSP;
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
		public int LogLevel
		{ get; set; }

		#endregion

		/// <summary>
		/// Constructor
		/// </summary>
		public clsDataPackageArchiver(string connectionString)
		{
			this.DBConnectionString = connectionString;
			this.ErrorMessage = string.Empty;

			Initialize();
		}

		protected short BoolToTinyInt(bool value)
		{
			if (value)
				return 1;
			else
				return 0;
		}

		protected string GetDBString(SqlDataReader reader, string columnName)
		{
			object value = reader[columnName];

			if (Convert.IsDBNull(value))
				return string.Empty;
			else
				return (string)value;

		}

		protected Dictionary<int, udtMyEMSLStatusInfo> GetStatusURIs(int retryCount)
		{
			var dctURIs = new Dictionary<int, udtMyEMSLStatusInfo>();

			try
			{

				string sql = "SELECT Entry_ID, Data_Package_ID, Entered, StatusNum, Status_URI FROM V_MyEMSL_Uploads WHERE ErrorCode = 0 And (Available = 0 Or Verified = 0) AND ISNULL(StatusNum, 0) > 0";

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
			// Set up the loggers
			string logFileName = "DataPkgArchiver";
			this.LogLevel = 4;
			clsLogTools.CreateFileLogger(logFileName, this.LogLevel);

			clsLogTools.CreateDbLogger(this.DBConnectionString, "DataPkgArchiver: " + Environment.MachineName);

			// Make initial log entry
			string msg = "=== Started Data Package Archiver V" + System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString() + " ===== ";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

			m_ExecuteSP = new PRISM.DataBase.clsExecuteDatabaseSP(this.DBConnectionString);
			m_ExecuteSP.DBErrorEvent += new PRISM.DataBase.clsExecuteDatabaseSP.DBErrorEventEventHandler(m_ExecuteSP_DBErrorEvent);

		}


		protected List<clsDataPackageInfo> LookupDataPkgInfo(List<int> lstDataPkgIDs)
		{
			var lstDataPkgInfo = new List<clsDataPackageInfo>();

			try
			{

				using (var cnDB = new SqlConnection(this.DBConnectionString))
				{
					cnDB.Open();

					string sql = " SELECT ID, Name, Created, Package_File_Folder, Share_Path, Local_Path " +
								 " FROM S_V_Data_Package_Export";

					if (lstDataPkgIDs.Count > 0)
						sql += " WHERE ID IN (" + string.Join(",", lstDataPkgIDs) + ")";

					sql += " ORDER BY ID";

					var cmd = new SqlCommand(sql, cnDB);
					var reader = cmd.ExecuteReader();

					while (reader.Read())
					{
						int dataPkgID = reader.GetInt32(0);

						var dataPkgInfo = new clsDataPackageInfo(dataPkgID);


						dataPkgInfo.Name = GetDBString(reader, "Name");

						string pkgCreatedText = GetDBString(reader, "Created");
						DateTime pkgCreated;
						if (System.DateTime.TryParse(pkgCreatedText, out pkgCreated))
						{
							dataPkgInfo.Created = pkgCreated;
						}

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
				ReportError("Error in LookupDataPkgInfo: " + ex.Message);
				return new List<clsDataPackageInfo>();
			}

		}

		public List<int> ParseDataPkgIDList(string dataPkgIDList)
		{

			if (Environment.UserName.ToLower() != "svc-dms")
			{
				// The current user is not svc-dms
				// Uploaded files would be associated with the wrong username and thus would not be visible to all DMS Users
				this.PreviewMode = true;
				ReportMessage(@"Current user is not pnl\svc-dms; auto-enabling PreviewMode");
			}

			List<string> lstValues = dataPkgIDList.Split(',').ToList();
			List<int> lstDataPkgIDs = new List<int>();

			foreach (var item in lstValues)
			{
				if (!string.IsNullOrWhiteSpace(item))
				{
					int dataPkgID;
					if (int.TryParse(item, out dataPkgID))
					{
						if (!lstDataPkgIDs.Contains(dataPkgID))
							lstDataPkgIDs.Add(dataPkgID);
					}
					else
					{
						ReportError("Value is not an integer in ParseDataPkgIDList; ignoring: " + item);
					}
				}
			}

			return lstDataPkgIDs;

		}

		public bool ProcessDataPackages(List<int> lstDataPkgIDs)
		{

			int dataPackagesToProcess = 0;
			int successCount = 0;

			try
			{
				List<clsDataPackageInfo> lstDataPkgInfo = LookupDataPkgInfo(lstDataPkgIDs);
				dataPackagesToProcess = lstDataPkgInfo.Count;

				if (dataPackagesToProcess == 0)
				{
					ReportError("None of the data packages in lstDataPkgIDs corresponded to a known data package ID");
					return false;
				}

				// List of groups of data package IDs
				var lstDataPkgGroups = new List<List<int>>();
				var lstCurrentGroup = new List<int>();

				int runningCount = 0;

				// Determine the number of files that are associated with each data package
				// We will use this information to process the data packages in chunks
				foreach (var dataPkgInfo in lstDataPkgInfo)
				{
					int fileCount = CountFilesForDataPackage(dataPkgInfo);

					if (runningCount + fileCount > 5000)
					{
						// Store the current group
						if (lstCurrentGroup.Count > 0)
							lstDataPkgGroups.Add(lstCurrentGroup);

						// Make a new group
						lstCurrentGroup = new List<int>();
						lstCurrentGroup.Add(dataPkgInfo.ID);
						runningCount = fileCount;
					}
					else
					{
						// Use the current group
						lstCurrentGroup.Add(dataPkgInfo.ID);
						runningCount += fileCount;
					}

				}

				// Store the current group
				if (lstCurrentGroup.Count > 0)
					lstDataPkgGroups.Add(lstCurrentGroup);

				foreach (var dataPkgGroup in lstDataPkgGroups)
				{
					var dataPackageInfoCache = new MyEMSLReader.DatasetPackageListInfo();
					foreach (int dataPkgID in dataPkgGroup)
					{
						dataPackageInfoCache.AddDataPackage(dataPkgID);
					}

					// Prepopulate lstDataPackageInfoCache with the files for the current group
					dataPackageInfoCache.RefreshInfo();

					foreach (var dataPkgInfo in lstDataPkgInfo)
					{
						bool success = ProcessOneDataPackage(dataPkgInfo, dataPackageInfoCache);

						if (success)
							successCount += 1;

					}
				}

			}
			catch (Exception ex)
			{
				ReportError("Error in ProcessDataPackages: " + ex.Message);
				return false;
			}

			if (successCount == dataPackagesToProcess)
				return true;
			else
			{
				if (dataPackagesToProcess == 1)
					ReportError("Failed to archive data package " + lstDataPkgIDs.First());
				else if (successCount == 0)
					ReportError("Failed to archive any of the " + lstDataPkgIDs.Count + " candidate data packages", true);
				else
					ReportError("Failed to archive " + (dataPackagesToProcess - successCount).ToString() + " data package(s); successfully archived " + successCount + " data package(s)", true);

				return false;
			}

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

		protected bool ProcessOneDataPackage(clsDataPackageInfo dataPkgInfo, MyEMSLReader.DatasetPackageListInfo dataPackageInfoCache)
		{
			bool success = false;
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

				// Construct a list of the files on disk for this data package
				var fileList = diDataPkg.GetFiles("*.*", SearchOption.AllDirectories).ToList<FileInfo>();

				if (fileList.Count == 0)
				{
					// Nothing to archive; this is not an error
					ReportMessage("Data package folder is empty; nothing to archive: " + diDataPkg.FullName, clsLogTools.LogLevels.DEBUG);
					return true;
				}

				// Look for files tracked in MyEMSL for this data package
				dataPackageInfoCache

				if (this.PreviewMode)
				{
					// Preview the changes
				}
				else
				{
					// Upload the files

					udtMyEMSLUploadInfo uploadInfo = StartUpload(dataPkgInfo, filesToUpload);

					// Post the StatusURI info to the database
					StoreMyEMSLUploadStats(dataPkgInfo, uploadInfo);

				}
				return success;
			}
			catch (Exception ex)
			{
				ReportError("Error in ProcessOneDataPackage: " + ex.Message);
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

		public bool StartProcessing(List<int> lstDataPkgIDs, bool previewMode)
		{

			this.PreviewMode = previewMode;

			// Upload new data
			bool success = ProcessDataPackages(lstDataPkgIDs);

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
				var msg = "Exceptiong calling stored procedure " + SP_NAME_SET_MYEMSL_UPLOAD_STATUS;
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

					if (!available && System.DateTime.Now.Subtract(statusInfo.Value.Entered).TotalHours > 24)
					{
						ReportError("Data package " + statusInfo.Value.DataPackageID + " is not available in MyEMSL after 24 hours; see " + statusInfo.Value.StatusURI, true);
					}

					if (available)
					{
						// Next check step 6 (Archived and Sha-1 hash values checked)
						verified = statusChecker.IngestStepCompleted(statusInfo.Value.StatusURI, MyEMSLStatusCheck.StatusStep.Archived, cookieJar, out accessDenied, out statusMessage);
					}

					if (!verified && System.DateTime.Now.Subtract(statusInfo.Value.Entered).TotalHours > 48)
					{
						ReportError("Data package " + statusInfo.Value.DataPackageID + " has not been validated in the archive after 48 hours; see " + statusInfo.Value.StatusURI, true);
					}

					if (available || verified)
					{
						// Update values in the DB
						UpdateMyEMSLUploadStatus(statusInfo.Value, available, verified);
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

		private void m_ExecuteSP_DBErrorEvent(string Message)
		{
			ReportError("Stored procedure execution error: " + Message, true);
		}

		#endregion

	}
}
