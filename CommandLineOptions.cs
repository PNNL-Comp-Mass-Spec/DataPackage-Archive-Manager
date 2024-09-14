using System;
using PRISM;

namespace DataPackage_Archive_Manager
{
    internal class CommandLineOptions
    {
        [Option("IDs", ArgPosition = 1, Required = true, HelpShowsDefault = false, HelpText = "Data package ID list; can be a single data package ID, a comma-separated list of IDs, or * to process all Data Packages. Items in the ID list can be ID ranges, for example 880-885 or even 892-")]
        public string PackageIds { get; set; }

        [Option("Date", "D", HelpText = "Date threshold for finding modified data packages; if a data package does not have any files modified on/after this date, the data package will not be uploaded to MyEMSL")]
        public string DateThresholdString { get; set; }

        [Option("Preview", HelpText = "Preview any files that would be uploaded")]
        public bool PreviewMode { get; set; }

        [Option("VerifyOnly", "V", HelpText = "Verify recently uploaded data packages and skip looking for new/changed files")]
        public bool VerifyOnly { get; set; }

        [Option("DB", HelpText = "Use to override the default connection string")]
        public string DBConnectionString { get; set; }

        [Option("SkipCheckExisting", HelpText = "Skip the check for data package files that are known to exist in MyEMSL and should be visible by a metadata query; if this switch is used, you risk pushing duplicate data files into MyEMSL")]
        public bool SkipCheckExisting { get; set; }

        [Option("DisableVerify", "noVerify", HelpText = "Skip verifying the upload status of data previously uploaded to MyEMSL but not yet verified")]
        public bool DisableVerify { get; set; }

        [Option("Trace", "TraceMode", HelpText = "Show additional log messages")]
        public bool TraceMode { get; set; }

        [Option("Debug", HelpText = "Enable the display (and logging) of debug messages; implies 'trace'")]
        public bool DebugMode { get; set; }

        /// <summary>
        /// Date threshold
        /// </summary>
        public DateTime DateThreshold { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public CommandLineOptions()
        {
            DateThreshold = DateTime.MinValue;
            DateThresholdString = string.Empty;

            // This connection string points to the DMS database on prismdb2 (previously, DMS_Data_Package on Gigasax)
            DBConnectionString = DataPackageArchiver.CONNECTION_STRING;
        }

        /// <summary>
        /// Validate options
        /// </summary>
        public bool Validate()
        {
            if (string.IsNullOrWhiteSpace(PackageIds))
            {
                ConsoleMsgUtils.ShowError("Data package list was empty/blank!");
                return false;
            }

            if (!string.IsNullOrWhiteSpace(DateThresholdString))
            {
                if (DateTime.TryParse(DateThresholdString, out var date))
                {
                    DateThreshold = date;
                }
                else
                {
                    ConsoleMsgUtils.ShowError($"Invalid date specified with -d: \"{DateThresholdString}\"");
                    return false;
                }
            }
            else
            {
                ConsoleMsgUtils.ShowWarning("-d was not provided or was empty; date threshold will not be used.");
            }

            if (string.IsNullOrWhiteSpace(DBConnectionString))
            {
                ConsoleMsgUtils.ShowWarning("-d was empty/blank; default connection string will be used.");
                DBConnectionString = DataPackageArchiver.CONNECTION_STRING;
            }

            if (DebugMode)
            {
                TraceMode = true;
            }

            return true;
        }
    }
}
