using System;

namespace DataPackage_Archive_Manager
{
    class clsDataPackageInfo
    {
        public int ID { get; private set; }			// Data Package ID
        public string Name { get; set; }            // Data Package Name
        public string OwnerPRN { get; set; }        // Data Package Owner's username
        public int OwnerEUSID { get; set; }         // EUS ID of the data package owner
        public string EUSProposalID { get; set; }      // EUS Proposal ID (most common one in use by the datasets or jobs associated with the data package)
        public DateTime Created { get; set; }
        public string FolderName { get; set; }		// Example: 894_CPTAC_Batch4_Global_CompRef
        public string SharePath { get; set; }		// Example: \\protoapps\DataPkgs\Public\2013\894_CPTAC_Batch4_Global_CompRef
        public string LocalPath { get; set; }		// Example: F:\DataPkgs\Public\2013\894_CPTAC_Batch4_Global_CompRef
        public int MyEMSLUploads { get; set; }		// Number of successful uploads for this data package

        /// <summary>
        /// Constructor
        /// </summary>
        public clsDataPackageInfo(int dataPkgID)
        {
            ID = dataPkgID;
            Name = string.Empty;
            OwnerPRN = string.Empty;
            OwnerEUSID = 0;
            EUSProposalID = string.Empty;
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
