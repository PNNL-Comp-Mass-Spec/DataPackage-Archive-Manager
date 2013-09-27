using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataPackage_Archive_Manager
{
	class clsDataPackageInfo
	{
		public int ID { get; private set; }			// Data Package ID
		public string Name { get; set; }
		public DateTime Created { get; set; }
		public string FolderName { get; set; }		// Example: 894_CPTAC_Batch4_Global_CompRef
		public string SharePath { get; set; }		// Example: \\protoapps\DataPkgs\Public\2013\894_CPTAC_Batch4_Global_CompRef
		public string LocalPath { get; set; }		// Example: F:\DataPkgs\Public\2013\894_CPTAC_Batch4_Global_CompRef

		/// <summary>
		/// Constructor
		/// </summary>
		public clsDataPackageInfo(int dataPkgID)
		{
			this.ID = dataPkgID;

			this.Name = string.Empty;
			this.Created = System.DateTime.Now;
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
