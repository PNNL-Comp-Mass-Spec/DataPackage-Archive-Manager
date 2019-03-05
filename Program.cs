using System;
using System.Collections.Generic;
using System.Threading;
using PRISM;
using PRISM.Logging;

namespace DataPackage_Archive_Manager
{
    // This program uploads new/changed data package files to MyEMSL
    //
    // -------------------------------------------------------------------------------
    // Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
    //
    // E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov
    // Website: https://panomics.pnnl.gov/ or https://omics.pnl.gov
    // -------------------------------------------------------------------------------
    //

    internal static class Program
    {

        public const string PROGRAM_DATE = "March 5, 2019";

        /// <summary>
        /// Gigasax.DMS_Data_Package
        /// </summary>
        private static string mDBConnectionString;
        private static BaseLogger.LogLevels mLogLevel;

        private static string mDataPkgIDList;
        private static DateTime mDateThreshold;

        private static bool mPreviewMode;
        private static bool mSkipCheckExisting;
        private static bool mTraceMode;
        private static bool mVerifyOnly;

        public static int Main(string[] args)
        {
            var commandLineParser = new clsParseCommandLine();

            mDBConnectionString = DataPackageArchiver.CONNECTION_STRING;
            mLogLevel = BaseLogger.LogLevels.INFO;

            mDataPkgIDList = string.Empty;
            mDateThreshold = DateTime.MinValue;
            mPreviewMode = false;
            mSkipCheckExisting = false;
            mTraceMode = false;
            mVerifyOnly = false;

            try
            {
                var success = false;

                if (commandLineParser.ParseCommandLine())
                {
                    if (SetOptionsUsingCommandLineParameters(commandLineParser))
                        success = true;
                }

                if (!success ||
                    commandLineParser.NeedToShowHelp ||
                    commandLineParser.ParameterCount + commandLineParser.NonSwitchParameterCount == 0 ||
                    mDataPkgIDList.Length == 0)
                {
                    ShowProgramHelp();
                    return -1;

                }

                var updatesArePending = WindowsUpdateStatus.UpdatesArePending(out var pendingWindowsUpdateMessage);

                if (updatesArePending)
                {
                    Console.WriteLine(pendingWindowsUpdateMessage);
                    Console.WriteLine("Will not contact archive any data packages");
                    return 0;
                }

                var archiver = new DataPackageArchiver(mDBConnectionString, mLogLevel)
                {
                    SkipCheckExisting = mSkipCheckExisting,
                    TraceMode = mTraceMode
                };

                // Attach the events
                archiver.DebugEvent += Archiver_DebugEvent;
                archiver.ErrorEvent += Archiver_ErrorEvent;
                archiver.StatusEvent += Archiver_StatusEvent;
                archiver.WarningEvent += Archiver_WarningEvent;

                if (mVerifyOnly)
                {
                    // Verify previously updated data
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

                FileLogger.FlushPendingMessages();

                if (!success)
                {
                    ShowErrorMessage("Error archiving the data packages: " + archiver.ErrorMessage);
                    return -3;
                }
            }
            catch (Exception ex)
            {
                FileLogger.FlushPendingMessages();

                Console.WriteLine("Error occurred in Program->Main: " + Environment.NewLine + ex.Message);
                Console.WriteLine(ex.StackTrace);
                Thread.Sleep(1500);
                return -1;
            }

            return 0;
        }

        private static string GetAppVersion()
        {
            return System.Reflection.Assembly.GetExecutingAssembly().GetName().Version + " (" + PROGRAM_DATE + ")";
        }

        private static bool SetOptionsUsingCommandLineParameters(clsParseCommandLine commandLineParser)
        {
            // Returns True if no problems; otherwise, returns false
            var lstValidParameters = new List<string> { "D", "Preview", "V", "Trace", "Debug", "DB", "SkipCheckExisting" };

            try
            {
                // Make sure no invalid parameters are present
                if (commandLineParser.InvalidParametersPresent(lstValidParameters))
                {
                    var badArguments = new List<string>();
                    foreach (var item in commandLineParser.InvalidParameters(lstValidParameters))
                    {
                        badArguments.Add("/" + item);
                    }

                    ShowErrorMessage("Invalid command line parameters", badArguments);

                    return false;
                }

                // Query commandLineParser to see if various parameters are present
                if (commandLineParser.NonSwitchParameterCount > 0)
                {
                    mDataPkgIDList = commandLineParser.RetrieveNonSwitchParameter(0);
                }

                if (commandLineParser.RetrieveValueForParameter("D", out var strValue))
                {
                    if (string.IsNullOrWhiteSpace(strValue))
                        ShowErrorMessage("/D does not have a date; date threshold will not be used");
                    else
                    {
                        if (DateTime.TryParse(strValue, out var dtThreshold))
                        {
                            mDateThreshold = dtThreshold;
                        }
                        else
                        {
                            ShowErrorMessage("Invalid date specified with /D:" + strValue);
                        }
                    }

                }

                if (commandLineParser.IsParameterPresent("Preview"))
                {
                    mPreviewMode = true;
                }

                if (commandLineParser.IsParameterPresent("SkipCheckExisting"))
                {
                    mSkipCheckExisting = true;
                }

                if (commandLineParser.IsParameterPresent("Trace"))
                {
                    mTraceMode = true;
                }

                if (commandLineParser.IsParameterPresent("V"))
                {
                    mVerifyOnly = true;
                }

                if (commandLineParser.IsParameterPresent("Debug"))
                {
                    mLogLevel = BaseLogger.LogLevels.DEBUG;
                    mTraceMode = true;
                }

                if (commandLineParser.RetrieveValueForParameter("DB", out strValue))
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

        private static void ShowErrorMessage(string message, Exception ex = null)
        {
            ConsoleMsgUtils.ShowError(message, ex);
        }

        private static void ShowErrorMessage(string message, IReadOnlyCollection<string> additionalInfo)
        {
            if (additionalInfo == null || additionalInfo.Count == 0)
            {
                ConsoleMsgUtils.ShowError(message);
                return;
            }

            var formattedMessage = message + ":";

            foreach (var item in additionalInfo)
            {
                formattedMessage += Environment.NewLine + "  " + item;
            }

            ConsoleMsgUtils.ShowError(formattedMessage, true, false);
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

                Console.WriteLine(
                    " DataPackageIDList [/D:DateThreshold] [/Preview] [/V] " +
                    "[/DB:ConnectionString] [/Trace] [/Debug]");

                Console.WriteLine();
                Console.WriteLine("DataPackageIDList can be a single Data package ID, a comma-separated list of IDs, or * to process all Data Packages");
                Console.WriteLine("Items in DataPackageIDList can be ID ranges, for example 880-885 or even 892-");
                Console.WriteLine();
                Console.WriteLine("Use /D to specify a date threshold for finding modified data packages; if a data package does not have any files modified on/after the /D date, then the data package will not be uploaded to MyEMSL");
                Console.WriteLine();
                Console.WriteLine("Use /Preview to preview any files that would be uploaded");
                Console.WriteLine("");
                Console.WriteLine("Use /V to verify recently uploaded data packages and skip looking for new/changed files");
                Console.WriteLine("Use /DB to override the default connection string of " + DataPackageArchiver.CONNECTION_STRING);
                Console.WriteLine();
                Console.WriteLine("Use /SkipCheckExisting to skip the check for data package files that are known to exist in MyEMSL and should be visible by a metadata query; if this switch is used, you risk pushing duplicate data files into MyEMSL");
                Console.WriteLine();
                Console.WriteLine("Use /Trace to show additional log messages");
                Console.WriteLine("Use /Debug to enable the display (and logging) of debug messages; auto-enables /Trace");
                Console.WriteLine();
                Console.WriteLine("Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in 2013");
                Console.WriteLine("Version: " + GetAppVersion());
                Console.WriteLine();

                Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov");
                Console.WriteLine("Website: https://panomics.pnnl.gov/ or https://omics.pnl.gov");
                Console.WriteLine();

                // Delay for 1500 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
                Thread.Sleep(1500);

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error displaying the program syntax: " + ex.Message);
            }

        }

        #region "Event Handlers"

        private static void Archiver_DebugEvent(string strMessage)
        {
            ConsoleMsgUtils.ShowDebug(strMessage);
        }

        private static void Archiver_ErrorEvent(string errorMessage, Exception ex)
        {
            ConsoleMsgUtils.ShowError(errorMessage, ex);
        }

        private static void Archiver_StatusEvent(string strMessage)
        {
            Console.WriteLine(strMessage);
        }

        private static void Archiver_WarningEvent(string strMessage)
        {
            ConsoleMsgUtils.ShowWarning(strMessage);
        }

        #endregion

    }
}