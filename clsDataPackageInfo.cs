using System;

namespace DataPackage_Archive_Manager
{
    internal class DataPackageInfo
    {
        // Ignore Spelling: DataPkgs, protoapps, username

        /// <summary>
        /// Data Package ID
        /// </summary>
        public int ID { get; }

        /// <summary>
        /// Data Package Name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Data Package Owner's username
        /// </summary>
        public string OwnerPRN { get; set; }

        /// <summary>
        /// EUS ID of the data package owner
        /// </summary>
        public int OwnerEUSID { get; set; }

        /// <summary>
        /// EUS Proposal ID (most common one in use by the datasets or jobs associated with the data package)
        /// </summary>
        public string EUSProposalID { get; set; }

        /// <summary>
        /// EUS Instrument ID (most common one in use by the datasets or jobs associated with the data package)
        /// </summary>
        public int EUSInstrumentID { get; set; }

        /// <summary>
        /// Instrument Name (most common one in use by the datasets or jobs associated with the data package)
        /// </summary>
        public string InstrumentName { get; set; }

        /// <summary>
        /// Time that the data package info was created
        /// </summary>
        public DateTime Created { get; set; }

        /// <summary>
        /// Example: 894_CPTAC_Batch4_Global_CompRef
        /// </summary>
        public string FolderName { get; set; }

        /// <summary>
        /// Example: \\protoapps\DataPkgs\Public\2013\894_CPTAC_Batch4_Global_CompRef
        /// </summary>
        public string SharePath { get; set; }

        /// <summary>
        /// Example: F:\DataPkgs\Public\2013\894_CPTAC_Batch4_Global_CompRef
        /// </summary>
        public string LocalPath { get; set; }

        /// <summary>
        /// Number of successful uploads for this data package
        /// </summary>
        public int MyEMSLUploads { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public DataPackageInfo(int dataPkgID)
        {
            ID = dataPkgID;
            Name = string.Empty;
            OwnerPRN = string.Empty;
            OwnerEUSID = 0;
            EUSProposalID = string.Empty;
            InstrumentName = string.Empty;
            Created = DateTime.Now;
            FolderName = string.Empty;
            SharePath = string.Empty;
            LocalPath = string.Empty;
        }

        /// <summary>
        /// Show the data package ID
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return ID.ToString();
        }
    }
}
