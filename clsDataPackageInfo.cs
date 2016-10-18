using System;

namespace DataPackage_Archive_Manager
{
    class clsDataPackageInfo
    {
        public int ID { get; private set; }			// Data Package ID
        public string Name { get; set; }            // Data Package Name
        public string OwnerPRN { get; set; }        // Data Package Owner's username
        public int OwnerEUSID { get; set; }         // EUS ID of the data package owner
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
            this.ID = dataPkgID;
            this.Name = string.Empty;
            this.OwnerPRN = string.Empty;
            this.OwnerEUSID = 0;
            this.Created = DateTime.Now;
            this.FolderName = string.Empty;
            this.SharePath = string.Empty;
            this.LocalPath = string.Empty;

        }

        public override string ToString()
        {
            return this.ID.ToString();
        }
    }
}
