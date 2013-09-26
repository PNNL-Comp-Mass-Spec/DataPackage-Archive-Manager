﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataPackage_Archive_Manager
{
	// This program uploads new/changed data package files to MyEMSL
	//
	// -------------------------------------------------------------------------------
	// Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
	//
	// E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com
	// Website: http://panomics.pnnl.gov/ or http://omics.pnl.gov or http://www.sysbio.org/resources/staff/
	// -------------------------------------------------------------------------------
	// 

	internal class Program
	{

		public const string PROGRAM_DATE = "September 26, 2013";

		protected static string mDBConnectionString;
		protected static clsLogTools.LogLevels mLogLevel;

		protected static string mDataPkgIDList;
		protected static bool mPreviewMode;

		public static int Main(string[] args)
		{
			FileProcessor.clsParseCommandLine objParseCommandLine = new FileProcessor.clsParseCommandLine();
			bool success = false;

			mDBConnectionString = clsDataPackageArchiver.CONNECTION_STRING;
			mLogLevel = clsLogTools.LogLevels.INFO;

			mDataPkgIDList = string.Empty;
			mPreviewMode = false;

			try
			{
				success = false;

				if (objParseCommandLine.ParseCommandLine())
				{
					if (SetOptionsUsingCommandLineParameters(objParseCommandLine))
						success = true;
				}

				if (!success ||
					objParseCommandLine.NeedToShowHelp ||
					objParseCommandLine.ParameterCount + objParseCommandLine.NonSwitchParameterCount == 0 ||
					mDataPkgIDList.Length == 0)
				{
					ShowProgramHelp();
					return -1;

				}
				else
				{
					clsDataPackageArchiver archiver = new clsDataPackageArchiver(mDBConnectionString, mLogLevel);

					// Attach the events
					archiver.ErrorEvent	+=new MessageEventHandler(archiver_ErrorEvent);
					archiver.MessageEvent+=new MessageEventHandler(archiver_MessageEvent);
				
					List<int> lstDataPkgIDs;
					if (mDataPkgIDList.StartsWith("*"))
						// Process all Data Packages
						lstDataPkgIDs= new List<int>();
					else
					{
						// Parse the data package ID list
						lstDataPkgIDs = archiver.ParseDataPkgIDList(mDataPkgIDList);

						if (lstDataPkgIDs.Count == 0)
						{
							// Data Package IDs not defined							
							ShowErrorMessage("DataPackageIDList was empty; should contain integers or '*'");
							ShowProgramHelp();
							return -2;
						}
					}

					// Upload new data, then verify previously updated data
					success = archiver.StartProcessing(lstDataPkgIDs, mPreviewMode);

					if (!success)
					{
						ShowErrorMessage("Error archiving the data packages: " + archiver.ErrorMessage);
						return -3;
					}
				}

			}
			catch (Exception ex)
			{
				Console.WriteLine("Error occurred in Program->Main: " + Environment.NewLine + ex.Message);
				return -1;
			}

			return 0;
		}

		private static string GetAppVersion()
		{
			return System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString() + " (" + PROGRAM_DATE + ")";
		}

		private static bool SetOptionsUsingCommandLineParameters(FileProcessor.clsParseCommandLine objParseCommandLine)
		{
			// Returns True if no problems; otherwise, returns false

			string strValue = string.Empty;
			List<string> lstValidParameters = new List<string> { "Preview", "DB", "Debug" };

			try
			{
				// Make sure no invalid parameters are present
				if (objParseCommandLine.InvalidParametersPresent(lstValidParameters))
				{
					List<string> badArguments = new List<string>();
					foreach (string item in objParseCommandLine.InvalidParameters(lstValidParameters))
					{
						badArguments.Add("/" + item);
					}

					ShowErrorMessage("Invalid commmand line parameters", badArguments);

					return false;
				}
				else
				{
					{
						// Query objParseCommandLine to see if various parameters are present						
						if (objParseCommandLine.NonSwitchParameterCount > 0)
						{
							mDataPkgIDList = objParseCommandLine.RetrieveNonSwitchParameter(0);
						}

						if (objParseCommandLine.IsParameterPresent("Preview"))
						{
							mPreviewMode = true;
						}

						if (objParseCommandLine.IsParameterPresent("Debug"))
						{
							mLogLevel = clsLogTools.LogLevels.DEBUG;

						}						

						if (objParseCommandLine.RetrieveValueForParameter("DB", out strValue))
						{
							if (string.IsNullOrWhiteSpace(strValue))
								ShowErrorMessage("/DB does not have a value; not overriding the connection string");
							else
								mDBConnectionString = strValue;								
						}
						
					}

					return true;
				}

			}
			catch (Exception ex)
			{
				ShowErrorMessage("Error parsing the command line parameters: " + Environment.NewLine + ex.Message);
			}

			return false;
		}


		private static void ShowErrorMessage(string strMessage)
		{
			string strSeparator = "------------------------------------------------------------------------------";

			Console.WriteLine();
			Console.WriteLine(strSeparator);
			Console.WriteLine(strMessage);
			Console.WriteLine(strSeparator);
			Console.WriteLine();

			WriteToErrorStream(strMessage);
		}

		private static void ShowErrorMessage(string strTitle, List<string> items)
		{
			string strSeparator = "------------------------------------------------------------------------------";
			string strMessage = null;

			Console.WriteLine();
			Console.WriteLine(strSeparator);
			Console.WriteLine(strTitle);
			strMessage = strTitle + ":";

			foreach (string item in items)
			{
				Console.WriteLine("   " + item);
				strMessage += " " + item;
			}
			Console.WriteLine(strSeparator);
			Console.WriteLine();

			WriteToErrorStream(strMessage);
		}


		private static void ShowProgramHelp()
		{
			string exeName = System.IO.Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location);

			try
			{
				Console.WriteLine();
				Console.WriteLine("This program uploads new/changed data package files to MyEMSL");
				Console.WriteLine();
				Console.WriteLine("Program syntax:" + Environment.NewLine + exeName);

				Console.WriteLine(" DataPackageIDList [/Preview] [/DB:ConnectionString] [/Debug]");

				Console.WriteLine();
				Console.WriteLine("DataPackageIDList can be a single Data package ID, a comma-separated list of IDs, or * to process all Data Packages");
				Console.WriteLine();
				Console.WriteLine("Use /Preview to preview any files that would be uploaded");
				Console.WriteLine("Note that when uploading files this program must be run as user pnl\\svc-dms");
				Console.WriteLine("");
				Console.WriteLine("Use /DB to override the default connection string of " + clsDataPackageArchiver.CONNECTION_STRING);
				Console.WriteLine();
				Console.WriteLine("Use /Debug to enable the display (and logging) of debug messages");
				Console.WriteLine();
				Console.WriteLine("Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in 2013");
				Console.WriteLine("Version: " + GetAppVersion());
				Console.WriteLine();

				Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com");
				Console.WriteLine("Website: http://panomics.pnnl.gov/ or http://omics.pnl.gov or http://www.sysbio.org/resources/staff/");
				Console.WriteLine();

				// Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
				System.Threading.Thread.Sleep(750);

			}
			catch (Exception ex)
			{
				Console.WriteLine("Error displaying the program syntax: " + ex.Message);
			}

		}

		private static void WriteToErrorStream(string strErrorMessage)
		{
			try
			{
				using (System.IO.StreamWriter swErrorStream = new System.IO.StreamWriter(Console.OpenStandardError()))
				{
					swErrorStream.WriteLine(strErrorMessage);
				}
			}
			catch
			{
				// Ignore errors here
			}
		}

		static void ShowErrorMessage(string message, bool pauseAfterError)
		{
			Console.WriteLine();
			Console.WriteLine("===============================================");

			Console.WriteLine(message);

			if (pauseAfterError)
			{
				Console.WriteLine("===============================================");
				System.Threading.Thread.Sleep(1500);
			}
		}

		#region "Event Handlers"

		static void archiver_ErrorEvent(object sender, MessageEventArgs e)
		{
			ShowErrorMessage(e.Message);
		}

		static void archiver_MessageEvent(object sender, MessageEventArgs e)
		{
 			Console.WriteLine(e.Message);
		}

		#endregion

	}
}