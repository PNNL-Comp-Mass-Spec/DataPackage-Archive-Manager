@echo off
c:
cd C:\DMS_Programs\DataPackage_Archive_Manager

call ..\DMSUpdateManager\DMSUpdateManagerConsole.exe /P:..\DMSUpdateManagerOptions_DataPkgArchiveMgr.xml %1 %2 %3

echo Upload files from data package ID 388 or higher
echo Filtering to only trigger an upload if the data package has a file modified after 9/26/2013
echo Files for earlier packages can be found at \\a1.emsl.pnl.gov\prismarch\DataPkgs\Public\2013
echo You can selectively upload earlier data packages on an ad hoc basis as needed

@echo on
DataPackage_Archive_Manager.exe "388-941" /D:"2015-01-01"
@echo off
sleep 3

echo Upload files from data package ID 942 or higher, regardless of file modification date
@echo on
DataPackage_Archive_Manager.exe "942-" 
