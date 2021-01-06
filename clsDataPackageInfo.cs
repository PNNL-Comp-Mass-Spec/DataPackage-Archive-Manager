using System;

namespace DataPackage_Archive_Manager
{
    internal class DataPackageInfo
    {
        public int ID { get; }			            // Data Package ID
        public string Name { get; set; }            // Data Package Name
        public string OwnerPRN { get; set; }        // Data Package Owner's username
        public int OwnerEUSID { get; set; }         // EUS ID of the data package owner
        public string EUSProposalID { get; set; }   // EUS Proposal ID (most common one in use by the datasets or jobs associated with the data package)
        public int EUSInstrumentID { get; set; }    // EUS Instrument ID (most common one in use by the datasets or jobs associated with the data package)
        public string InstrumentName { get; set; }  // Instrument Name (most common one in use by the datasets or jobs associated with the data package)
        public DateTime Created { get; set; }
        public string FolderName { get; set; }		// Example: 894_CPTAC_Batch4_Global_CompRef
        public string SharePath { get; set; }		// Example: \\protoapps\DataPkgs\Public\2013\894_CPTAC_Batch4_Global_CompRef
        public string LocalPath { get; set; }		// Example: F:\DataPkgs\Public\2013\894_CPTAC_Batch4_Global_CompRef
        public int MyEMSLUploads { get; set; }		// Number of successful uploads for this data package

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

        public override string ToString()
        {
            return ID.ToString();
        }
    }
}
