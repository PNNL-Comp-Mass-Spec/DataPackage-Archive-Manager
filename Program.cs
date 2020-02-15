using System;
using System.Collections.Generic;
using System.Threading;
using PRISM;
using PRISM.FileProcessor;
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

        public const string PROGRAM_DATE = "February 15, 2020";

        private static BaseLogger.LogLevels mLogLevel;

        public static int Main(string[] args)
        {
            mLogLevel = BaseLogger.LogLevels.INFO;

            try
            {
                var exeName = System.IO.Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().GetName().Name);

                var cmdLineParser = new CommandLineParser<CommandLineOptions>(exeName,
                    ProcessFilesOrDirectoriesBase.GetAppVersion(PROGRAM_DATE))
                {
                    ProgramInfo = "This program uploads new/changed data package files to MyEMSL",
                    ContactInfo =
                        "Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in 2013" +
                        Environment.NewLine +
                        "E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov" + Environment.NewLine +
                        "Website: https://panomics.pnnl.gov/ or https://omics.pnl.gov"
                };

                var parsed = cmdLineParser.ParseArgs(args, false, false);
                var options = parsed.ParsedResults;
                if (!parsed.Success || !options.Validate())
                {
                    cmdLineParser.PrintHelp();
                    // Delay for 1500 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
                    Thread.Sleep(1500);
                    return -1;
                }

                if (options.DebugMode)
                {
                    mLogLevel = BaseLogger.LogLevels.DEBUG;
                }

                var updatesArePending = WindowsUpdateStatus.UpdatesArePending(out var pendingWindowsUpdateMessage);

                if (updatesArePending)
                {
                    Console.WriteLine(pendingWindowsUpdateMessage);
                    Console.WriteLine("Will not contact archive any data packages");
                    return 0;
                }

                var archiver = new DataPackageArchiver(options.DBConnectionString, mLogLevel)
                {
                    DisableVerify = options.DisableVerify,
                    SkipCheckExisting = options.SkipCheckExisting,
                    TraceMode = options.TraceMode
                };

                // Attach the events
                archiver.DebugEvent += Archiver_DebugEvent;
                archiver.ErrorEvent += Archiver_ErrorEvent;
                archiver.StatusEvent += Archiver_StatusEvent;
                archiver.WarningEvent += Archiver_WarningEvent;

                bool success;

                if (options.VerifyOnly)
                {
                    // Verify previously updated data
                    success = archiver.VerifyUploadStatus();
                }
                else
                {
                    List<KeyValuePair<int, int>> lstDataPkgIDs;
                    if (options.PackageIds.StartsWith("*"))
                        // Process all Data Packages by passing an empty list to ParseDataPkgIDList
                        lstDataPkgIDs = new List<KeyValuePair<int, int>>();
                    else
                    {
                        // Parse the data package ID list
                        lstDataPkgIDs = archiver.ParseDataPkgIDList(options.PackageIds);

                        if (lstDataPkgIDs.Count == 0)
                        {
                            // Data Package IDs not defined
                            ShowErrorMessage("DataPackageIDList was empty; should contain integers or '*'");
                            cmdLineParser.PrintHelp();
                            return -2;
                        }
                    }

                    // Upload new data, then verify previously updated data
                    success = archiver.StartProcessing(lstDataPkgIDs, options.DateThreshold, options.PreviewMode);
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

        private static void ShowErrorMessage(string message, Exception ex = null)
        {
            ConsoleMsgUtils.ShowError(message, ex);
        }

        #region "Event Handlers"

        private static void Archiver_DebugEvent(string message)
        {
            ConsoleMsgUtils.ShowDebug(message);
        }

        private static void Archiver_ErrorEvent(string errorMessage, Exception ex)
        {
            ConsoleMsgUtils.ShowError(errorMessage, ex);
        }

        private static void Archiver_StatusEvent(string message)
        {
            Console.WriteLine(message);
        }

        private static void Archiver_WarningEvent(string message)
        {
            ConsoleMsgUtils.ShowWarning(message);
        }

        #endregion

    }
}