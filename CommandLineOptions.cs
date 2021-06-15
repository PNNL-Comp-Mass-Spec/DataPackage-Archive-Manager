using System;
using PRISM;

namespace DataPackage_Archive_Manager
{
    internal class CommandLineOptions
    {
        [Option("ids", ArgPosition = 1, Required = true, HelpShowsDefault = false, HelpText = "Data package ID list; can be a single Data package ID, a comma-separated list of IDs, or * to process all Data Packages. Items in DataPackageIDList can be ID ranges, for example 880-885 or even 892-")]
        public string PackageIds { get; set; }

        [Option("d", "date", HelpText = "Date threshold for finding modified data packages; if a data package does not have any files modified on/after this date, then the data package will not be uploaded to MyEMSL")]
        public string DateThresholdString { get; set; }

        [Option("preview", HelpText = "Preview any files that would be uploaded")]
        public bool PreviewMode { get; set; }

        [Option("v", HelpText = "Verify recently uploaded data packages and skip looking for new/changed files")]
        public bool VerifyOnly { get; set; }

        [Option("db", HelpText = "Use to override the default connection string")]
        public string DBConnectionString { get; set; }

        [Option("skipCheckExisting", HelpText = "Skip the check for data package files that are known to exist in MyEMSL and should be visible by a metadata query; if this switch is used, you risk pushing duplicate data files into MyEMSL")]
        public bool SkipCheckExisting { get; set; }

        [Option("disableVerify", "noVerify", HelpText = "Skip verifying the upload status of data previously uploaded to MyEMSL but not yet verified")]
        public bool DisableVerify { get; set; }

        [Option("trace", HelpText = "Show additional log messages")]
        public bool TraceMode { get; set; }

        [Option("debug", HelpText = "Enable the display (and logging) of debug messages; implies 'trace'")]
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

            // Gigasax.DMS_Data_Package
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
