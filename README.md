# ASS Timer (UI + WebSocket Server)

## Download (Windows)

Go to **Releases** and download the latest installer:
- **v0.9.0-beta** → `ASS_Timer_Setup_0.9.0-beta_20251215-105154.exe`

- Direct download (installer .exe): [ASS_Timer_Setup_0.9.0-beta_20251215-105154.exe](https://github.com/bokob68/obs-advanced-scene-switcher-timer-websocket/releases/download/v0.9.0-beta/ASS_Timer_Setup_0.9.0-beta_20251215-105154.exe)
- Or open: [Releases](https://github.com/bokob68/obs-advanced-scene-switcher-timer-websocket/releases)


ASS Timer is a small Windows app that sends the current **minute-of-hour** and/or **second-of-minute** to **OBS Advanced Scene Switcher (ASS)** using a **generic WebSocket connection** (not the OBS WebSocket protocol).

It enables ASS macros to react to where you are within the current hour (e.g., run different macros depending on the current minute, or every 5 seconds).

## What it sends

- Minutes: a plain-text number `0..59` (example: `34`)
- Seconds: a plain-text number `0..59` (example: `25`)

The server only sends the number (no JSON) so it works well with ASS “Generic websocket message”.

## Components

- **UI** (`ASS_Timer_UI.exe`): manages settings, starts/stops the server, and shows status.
- **Server** (`ASS_WS_Server.exe`): WebSocket server that clients connect to (ASS is a client).

Settings and logs are stored in:

- `%LOCALAPPDATA%\\ASS_Timer\\`

## How to use (end users)

1. Start **ASS Timer UI**.
2. Enable **Send minutes** and/or **Send seconds**.
3. Click **Start**.
4. In OBS → Advanced Scene Switcher, create a WebSocket connection to:
   - `ws://<your-ip>:5678`
   - Make sure **“Use the obs-websocket protocol” is OFF**.

## Build (developers)

See `BUILDING.md` and `requirements.txt`.

