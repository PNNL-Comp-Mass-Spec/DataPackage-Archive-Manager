# Data Package Archive Manager

The Data Package Archive Manager is a part of PRISM, the Proteomics Research Information and Management System.
The software scans data package folders and files, pushing new or changed files into MyEMSL.

This program uploads new/changed data package files to MyEMSL

## Syntax 

```
DataPackage_Archive_Manager.exe 
  [/IDs:Start-End] [/Date] [/Preview] [/VerifyOnly] 
  [/DB] [/SkipCheckExisting] [/DisableVerify] [/Trace] [/Debug]
```

## Command Line Arguments

Use `/IDs` to define the data package ID list to process
* Can be a single data package ID, a comma-separated list of IDs, or * to process all Data Packages
* Items in ID list can be ID ranges, for example `880-885` or even `892-`

Use `/Date` or `/D` to define a date threshold for finding modified data packages
* If a data package does not have any files modified on/after this date, the data package will not be uploaded to MyEMSL

Use `/Preview` to preview any files that would be uploaded

Use `/VerifyOnly` or `/V` to verify recently uploaded data packages and skip looking for new/changed files

Use `/DB` to override the default connection string
* Defaults to: `Data Source=gigasax;Initial Catalog=DMS_Data_Package;Integrated Security=SSPI;`

Use `/SkipCheckExisting` to skip the check for data package files that are known to exist in MyEMSL and should be visible by a metadata query
* If this switch is used, you risk pushing duplicate data files into MyEMSL

Use `/DisableVerify` to skip verifying the upload status of data previously uploaded to MyEMSL but not yet verified

Use `/Trace` to show additional log messages

Use `/Debug` to enable the display (and logging) of debug messages; auto-enables `/Trace`

## Contacts

Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) \
E-mail: proteomics@pnnl.gov \
Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://www.pnnl.gov/integrative-omics

## License

The Data Package Archive Manager is licensed under the Apache License, Version 2.0; 
you may not use this program except in compliance with the License.  You may obtain 
a copy of the License at https://opensource.org/licenses/Apache-2.0
