@echo on

xcopy Debug\DataPackage_Archive_Manager.exe \\pnl\projects\OmicsSW\DMS_Programs\AnalysisToolManagerDistribution\DataPackage_Archive_Manager /D /Y
xcopy Debug\DataPackage_Archive_Manager.pdb \\pnl\projects\OmicsSW\DMS_Programs\AnalysisToolManagerDistribution\DataPackage_Archive_Manager /D /Y
xcopy Debug\*.dll \\pnl\projects\OmicsSW\DMS_Programs\AnalysisToolManagerDistribution\DataPackage_Archive_Manager /D /Y

xcopy Debug\DataPackage_Archive_Manager.exe \\protoapps\DMS_Programs\DataPackage_Archive_Manager /D /Y
xcopy Debug\DataPackage_Archive_Manager.pdb \\protoapps\DMS_Programs\DataPackage_Archive_Manager /D /Y
xcopy Debug\*.dll \\protoapps\DMS_Programs\DataPackage_Archive_Manager /D /Y

if not "%1"=="NoPause" pause
