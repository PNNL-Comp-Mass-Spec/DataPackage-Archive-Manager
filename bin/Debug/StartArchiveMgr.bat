c:
cd C:\DMS_Programs\DataPackage_Archive_Manager

call DMSUpdateManager.exe /P:DMSUpdateManagerOptions.xml

rem Upload files from data package ID 388 or higher
rem Filtering to only trigger an upload if the data package has a file modified after 9/26/2013
rem Files for earlier packages can be found at \\a1.emsl.pnl.gov\prismarch\DataPkgs\Public\2013
rem You can selectively upload earlier data packages on an ad hoc basis as needed
DataPackage_Archive_Manager.exe "388-" /D:"2013-09-26"
sleep 3

rem Upload files from data package ID 891 or higher, regardless of file modification date
DataPackage_Archive_Manager.exe "891-" 
