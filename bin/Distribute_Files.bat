@echo on

xcopy Debug\net8.0\DataPackage_Archive_Manager.exe \\pnl\projects\OmicsSW\DMS_Programs\AnalysisToolManagerDistribution\DataPackage_Archive_Manager /D /Y
xcopy Debug\net8.0\DataPackage_Archive_Manager.dll \\pnl\projects\OmicsSW\DMS_Programs\AnalysisToolManagerDistribution\DataPackage_Archive_Manager /D /Y
xcopy Debug\net8.0\*.dll \\pnl\projects\OmicsSW\DMS_Programs\AnalysisToolManagerDistribution\DataPackage_Archive_Manager /D /Y

xcopy Debug\net8.0\DataPackage_Archive_Manager.exe \\protoapps\DMS_Programs\DataPackage_Archive_Manager /D /Y
xcopy Debug\net8.0\DataPackage_Archive_Manager.dll \\protoapps\DMS_Programs\DataPackage_Archive_Manager /D /Y
xcopy Debug\net8.0\*.dll \\protoapps\DMS_Programs\DataPackage_Archive_Manager /D /Y

if not "%1"=="NoPause" pause
