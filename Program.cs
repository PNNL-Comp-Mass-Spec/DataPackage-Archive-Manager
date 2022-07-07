using System;
using System.Collections.Generic;
using System.Threading;
using PRISM;
using PRISM.FileProcessor;
using PRISM.Logging;
using PRISMDatabaseUtils;

namespace DataPackage_Archive_Manager
{
    /// <summary>
    /// This program uploads new/changed data package files to MyEMSL
    /// </summary>
    /// <remarks>
    /// <para>
    /// Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
    /// </para>
    /// <para>
    /// E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov
    /// Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://panomics.pnnl.gov/ or https://www.pnnl.gov/integrative-omics
    /// </para>
    /// </remarks>
    internal static class Program
    {
        public const string PROGRAM_DATE = "July 7, 2022";

        private static BaseLogger.LogLevels mLogLevel;

        public static int Main(string[] args)
        {
            mLogLevel = BaseLogger.LogLevels.INFO;

            try
            {
                var exeName = System.IO.Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().GetName().Name);

                var parser = new CommandLineParser<CommandLineOptions>(exeName,
                    ProcessFilesOrDirectoriesBase.GetAppVersion(PROGRAM_DATE))
                {
                    ProgramInfo = "This program uploads new/changed data package files to MyEMSL",
                    ContactInfo =
                        "Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)" + Environment.NewLine +
                        "E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov" + Environment.NewLine +
                        "Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://panomics.pnnl.gov/ or https://www.pnnl.gov/integrative-omics"
                };

                var result = parser.ParseArgs(args, false, false);
                var options = result.ParsedResults;

                if (!result.Success || !options.Validate())
                {
                    if (parser.CreateParamFileProvided)
                    {
                        return 0;
                    }

                    parser.PrintHelp();

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

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(options.DBConnectionString, "DataPackageArchiveMgr");

                var archiver = new DataPackageArchiver(connectionStringToUse, mLogLevel)
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
                    List<KeyValuePair<int, int>> dataPkgIDs;
                    if (options.PackageIds.StartsWith("*"))
                    {
                        // Process all Data Packages by passing an empty list to ParseDataPkgIDList
                        dataPkgIDs = new List<KeyValuePair<int, int>>();
                    }
                    else
                    {
                        // Parse the data package ID list
                        dataPkgIDs = archiver.ParseDataPkgIDList(options.PackageIds);

                        if (dataPkgIDs.Count == 0)
                        {
                            // Data Package IDs not defined
                            ShowErrorMessage("DataPackageIDList was empty; should contain integers or '*'");
                            parser.PrintHelp();
                            return -2;
                        }
                    }

                    // Upload new data, then verify previously updated data
                    success = archiver.StartProcessing(dataPkgIDs, options.DateThreshold, options.PreviewMode);
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
    }
}
