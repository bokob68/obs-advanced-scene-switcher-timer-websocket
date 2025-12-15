# Building on Windows

## Prerequisites

- Windows 10/11
- Python 3.11
- PowerShell 7 (recommended)
- Inno Setup 6 (optional; for the installer)

## Install Python dependencies

```powershell
py -3.11 -m pip install --upgrade pip
py -3.11 -m pip install -r requirements.txt
```

## Build the EXEs (PyInstaller)

From the project root:

```powershell
Remove-Item -Recurse -Force .\build, .\dist -ErrorAction SilentlyContinue
py -3.11 -m PyInstaller --noconfirm ASS_WS_Server.spec
py -3.11 -m PyInstaller --noconfirm ASS_Timer_UI.spec
```

Outputs:

- `dist\\ASS_WS_Server\\ASS_WS_Server.exe`
- `dist\\ASS_Timer_UI\\ASS_Timer_UI.exe`

## Build the installer (Inno Setup)

Open `installer\\ASS_Timer_Setup.iss` in Inno Setup Compiler and build.

The installer expects the EXEs to exist in `dist\\...` as listed above.

