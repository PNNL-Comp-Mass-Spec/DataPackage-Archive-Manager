@echo off

set TargetBase=\\pnl\projects\OmicsSW\DMS_Programs\AnalysisToolManagerDistribution\DataPackage_Archive_Manager
set Iteration=1

:Loop
echo Copying to %TargetBase%

@echo on
xcopy net8.0\DataPackage_Archive_Manager.exe                %TargetBase% /D /Y
xcopy net8.0\DataPackage_Archive_Manager.dll                %TargetBase% /D /Y
xcopy net8.0\DataPackage_Archive_Manager.deps.json          %TargetBase% /D /Y
xcopy net8.0\DataPackage_Archive_Manager.pdb                %TargetBase% /D /Y
xcopy net8.0\DataPackage_Archive_Manager.runtimeconfig.json %TargetBase% /D /Y
xcopy ..\..\README.md                                       %TargetBase% /D /Y
xcopy net8.0\*.dll                                          %TargetBase% /D /Y /S

@echo off

if %Iteration%==2 Goto Done

echo.

set Iteration=2
set TargetBase=\\protoapps\DMS_Programs\DataPackage_Archive_Manager

goto Loop

:Done

echo.
if not "%1"=="NoPause" pause
