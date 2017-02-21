using System;
using System.Collections.Generic;
using PRISM;

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

        public const string PROGRAM_DATE = "February 21, 2017";

        /// <summary>
        /// Gigasax.DMS_Data_Package
        /// </summary>
        private static string mDBConnectionString;
        private static clsLogTools.LogLevels mLogLevel;

        private static string mDataPkgIDList;
        private static DateTime mDateThreshold;

        private static bool mPreviewMode;
        private static bool mVerifyOnly;

        public static int Main(string[] args)
        {
            var objParseCommandLine = new FileProcessor.clsParseCommandLine();

            mDBConnectionString = clsDataPackageArchiver.CONNECTION_STRING;
            mLogLevel = clsLogTools.LogLevels.INFO;

            mDataPkgIDList = string.Empty;
            mDateThreshold = DateTime.MinValue;
            mPreviewMode = false;
            mVerifyOnly = false;

            try
            {
                var success = false;

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

                string pendingWindowsUpdateMessage;
                var updatesArePending = clsWindowsUpdateStatus.UpdatesArePending(out pendingWindowsUpdateMessage);

                if (updatesArePending)
                {
                    Console.WriteLine(pendingWindowsUpdateMessage);
                    Console.WriteLine("Will not contact archive any data packages");
                    return 0;
                }

                var archiver = new clsDataPackageArchiver(mDBConnectionString, mLogLevel);

                // Attach the events
                archiver.DebugEvent += Archiver_DebugEvent;
                archiver.ErrorEvent += Archiver_ErrorEvent;
                archiver.StatusEvent += Archiver_StatusEvent;
                archiver.WarningEvent += Archiver_WarningEvent;

                if (mVerifyOnly)
                {
                    //Verify previously updated data
                    success = archiver.VerifyUploadStatus();
                }
                else
                {
                    List<KeyValuePair<int, int>> lstDataPkgIDs;
                    if (mDataPkgIDList.StartsWith("*"))
                        // Process all Data Packages by passing an empty list to ParseDataPkgIDList
                        lstDataPkgIDs = new List<KeyValuePair<int, int>>();
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
                    success = archiver.StartProcessing(lstDataPkgIDs, mDateThreshold, mPreviewMode);
                    
                }

                if (!success)
                {
                    ShowErrorMessage("Error archiving the data packages: " + archiver.ErrorMessage);
                    return -3;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error occurred in Program->Main: " + Environment.NewLine + ex.Message);
                Console.WriteLine(ex.StackTrace);
                return -1;
            }

            return 0;
        }

        private static string GetAppVersion()
        {
            return System.Reflection.Assembly.GetExecutingAssembly().GetName().Version + " (" + PROGRAM_DATE + ")";
        }

        private static bool SetOptionsUsingCommandLineParameters(FileProcessor.clsParseCommandLine objParseCommandLine)
        {
            // Returns True if no problems; otherwise, returns false
            var lstValidParameters = new List<string> { "D", "Preview", "V", "Debug", "DB" };

            try
            {
                // Make sure no invalid parameters are present
                if (objParseCommandLine.InvalidParametersPresent(lstValidParameters))
                {
                    var badArguments = new List<string>();
                    foreach (var item in objParseCommandLine.InvalidParameters(lstValidParameters))
                    {
                        badArguments.Add("/" + item);
                    }

                    ShowErrorMessage("Invalid commmand line parameters", badArguments);

                    return false;
                }

                // Query objParseCommandLine to see if various parameters are present						
                if (objParseCommandLine.NonSwitchParameterCount > 0)
                {
                    mDataPkgIDList = objParseCommandLine.RetrieveNonSwitchParameter(0);
                }

                string strValue;
                if (objParseCommandLine.RetrieveValueForParameter("D", out strValue))
                {
                    if (string.IsNullOrWhiteSpace(strValue))
                        ShowErrorMessage("/D does not have a date; date threshold will not be used");
                    else
                    {
                        DateTime dtThreshold;
                        if (DateTime.TryParse(strValue, out dtThreshold))
                        {
                            mDateThreshold = dtThreshold;
                        }
                        else
                            ShowErrorMessage("Invalid date specified with /D:" + strValue);
                    }

                }

                if (objParseCommandLine.IsParameterPresent("Preview"))
                {
                    mPreviewMode = true;
                }

                if (objParseCommandLine.IsParameterPresent("V"))
                {
                    mVerifyOnly = true;
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



                return true;
            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error parsing the command line parameters: " + Environment.NewLine + ex.Message);
            }

            return false;
        }


        private static void ShowErrorMessage(string strMessage)
        {
            const string strSeparator = "------------------------------------------------------------------------------";

            Console.WriteLine();
            Console.WriteLine(strSeparator);
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(strMessage);
            Console.ResetColor();
            Console.WriteLine(strSeparator);
            Console.WriteLine();

            WriteToErrorStream(strMessage);
        }

        private static void ShowErrorMessage(string strTitle, IEnumerable<string> items)
        {
            const string strSeparator = "------------------------------------------------------------------------------";

            Console.WriteLine();
            Console.WriteLine(strSeparator);
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(strTitle);
            var strMessage = strTitle + ":";

            foreach (var item in items)
            {
                Console.WriteLine("   " + item);
                strMessage += " " + item;
            }
            Console.ResetColor();
            Console.WriteLine(strSeparator);
            Console.WriteLine();

            WriteToErrorStream(strMessage);
        }


        private static void ShowProgramHelp()
        {
            var exeName = System.IO.Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location);

            try
            {
                Console.WriteLine();
                Console.WriteLine("This program uploads new/changed data package files to MyEMSL");
                Console.WriteLine();
                Console.WriteLine("Program syntax:" + Environment.NewLine + exeName);

                Console.WriteLine(" DataPackageIDList [/D:DateThreshold] [/Preview] [/V] [/DB:ConnectionString] [/Debug]");

                Console.WriteLine();
                Console.WriteLine("DataPackageIDList can be a single Data package ID, a comma-separated list of IDs, or * to process all Data Packages");
                Console.WriteLine("Items in DataPackageIDList can be ID ranges, for example 880-885 or even 892-");
                Console.WriteLine();
                Console.WriteLine("Use /D to specify a date threshold for finding modified data packages; if a data package does not have any files modified on/after the /D date, then the data package will not be uploaded to MyEMSL");
                Console.WriteLine();
                Console.WriteLine("Use /Preview to preview any files that would be uploaded");
                Console.WriteLine("Note that when uploading files this program must be run as user pnl\\svc-dms");
                Console.WriteLine("");
                Console.WriteLine("Use /V to verify recently uploaded data packages and skip looking for new/changed files");
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
                System.Threading.Thread.Sleep(1500);

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
                using (var swErrorStream = new System.IO.StreamWriter(Console.OpenStandardError()))
                {
                    swErrorStream.WriteLine(strErrorMessage);
                }
            }
            // ReSharper disable once EmptyGeneralCatchClause
            catch
            {
                // Ignore errors here
            }
        }

        #region "Event Handlers"

        private static void Archiver_DebugEvent(string strMessage)
        {
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.WriteLine("  " + strMessage);
            Console.ResetColor();
        }

        private static void Archiver_ErrorEvent(string errorMessage, Exception ex)
        {
            string formattedError;
            if (ex == null || errorMessage.EndsWith(ex.Message))
            {
                formattedError = errorMessage;
            }
            else
            {
                formattedError = errorMessage + ": " + ex.Message;
            }

            ShowErrorMessage(formattedError);

            if (ex != null)
            {
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine(clsStackTraceFormatter.GetExceptionStackTraceMultiLine(ex));
            }

            Console.ResetColor();
        }

        private static void Archiver_StatusEvent(string strMessage)
        {
            Console.WriteLine(strMessage);
        }
        private static void Archiver_WarningEvent(string strMessage)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(strMessage);
            Console.ResetColor();
        }

        #endregion

    }
}