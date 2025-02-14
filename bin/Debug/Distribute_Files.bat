@echo off

set TargetBase=\\pnl\projects\OmicsSW\DMS_Programs\AnalysisToolManagerDistribution\DataPackage_Archive_Manager
echo Copying to %TargetBase%

@echo on
xcopy net8.0\DataPackage_Archive_Manager.exe %TargetBase% /D /Y /S
xcopy net8.0\DataPackage_Archive_Manager.dll %TargetBase% /D /Y /S
xcopy net8.0\*.dll %TargetBase% /D /Y /S
@echo off

echo.
set TargetBase=\\protoapps\DMS_Programs\DataPackage_Archive_Manager
echo Copying to %TargetBase%

@echo on
xcopy net8.0\DataPackage_Archive_Manager.exe %TargetBase% /D /Y /S
xcopy net8.0\DataPackage_Archive_Manager.dll %TargetBase% /D /Y /S
xcopy net8.0\*.dll %TargetBase% /D /Y /S
@echo off

echo.
if not "%1"=="NoPause" pause
