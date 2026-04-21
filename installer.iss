; Inno Setup Script for Streamd BI Native Engine
; Required to build: Inno Setup (https://jrsoftware.org/isdl.php)

[Setup]
AppId={{D6B5A1F0-8E1F-4E8B-B0D1-7A3B4C5D6E7F}}
AppName=Streamd BI Native Engine
AppVersion=2.1.1
AppPublisher=Streamd Project
DefaultDirName={autopf}\StreamdBI
DefaultGroupName=Streamd BI
AllowNoIcons=yes
; Output installer name
OutputDir=Output
OutputBaseFilename=StreamdBI_Setup_v211
Compression=lzma
SolidCompression=yes
WizardStyle=modern
; Icon for the installer wizard
SetupIconFile=icon.ico
UninstallDisplayIcon={app}\main.exe

[Languages]
Name: "japanese"; MessagesFile: "compiler:Languages\Japanese.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
; The build.bat output is in dist/main.dist/
Source: "dist\main.dist\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs
; Also copy necessary but not bundled files if any (config.json etc - but usually app creates them)
Source: "config.json"; DestDir: "{app}"; Flags: ignoreversion onlyifdoesntexist

[Icons]
Name: "{group}\Streamd BI Native Engine"; Filename: "{app}\main.exe"
Name: "{group}\{cm:UninstallProgram,Streamd BI Native Engine}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\Streamd BI Native Engine"; Filename: "{app}\main.exe"; Tasks: desktopicon

[Run]
Filename: "{app}\main.exe"; Description: "{cm:LaunchProgram,Streamd BI Native Engine}"; Flags: nowait postinstall skipifsilent
