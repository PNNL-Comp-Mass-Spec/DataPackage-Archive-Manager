using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Data.SqlClient;

namespace DataPackage_Archive_Manager
{
	class clsDataPackageArchiver
	{
		public const string CONNECTION_STRING = "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI;";

		#region "Auto properties"

		public string DBConnectionString
		{ get; private set; }

		public string ErrorMessage
		{ get; private set; }

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

		protected string GetDBString(SqlDataReader reader, string columnName)
		{
			object value = reader[columnName];

			if (Convert.IsDBNull(value))
				return string.Empty;
			else
				return (string)value;

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

				foreach (var dataPkgInfo in lstDataPkgInfo)
				{
					bool success = ProcessOneDataPackage(dataPkgInfo);

					if (success)
						successCount += 1;

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

		protected bool ProcessOneDataPackage(clsDataPackageInfo dataPkgInfo)
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
		#endregion

	}
}
