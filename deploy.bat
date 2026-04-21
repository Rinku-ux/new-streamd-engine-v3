@echo off
set VERSION=2.1.3
echo [DEPLOY] Building update package for version %VERSION%...

REM 1. ZIP the dist folder (You'll need a zip command like 7z or use powershell)
cd main.dist
powershell -Command "Compress-Archive -Path * -DestinationPath ..\update.zip -Force"
cd ..

REM 2. Create version.json info
echo { > version.json
echo   "version": "%VERSION%", >> version.json
echo   "url": "https://drive.google.com/uc?export=download&id=YOUR_FILE_ID_HERE", >> version.json
echo   "changelog": "Update to version %VERSION%" >> version.json
echo } >> version.json

echo.
echo [DONE] update.zip and version.json are ready.
echo Please upload them to Google Drive and update the ID in version.json.
pause
