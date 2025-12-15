# Protocol (Generic WebSocket)

This project is designed for **Advanced Scene Switcher “Generic websocket message”**.

It does **not** implement the OBS WebSocket protocol.

## Connection

Clients connect to:

`ws://<host>:<port>`

Default: `ws://127.0.0.1:5678`

## Messages sent to normal clients

The server sends plain-text numbers:

- Minutes: `0..59`
- Seconds: `0..59`

No JSON is required for ASS.

## UI monitoring connection

The UI connects as a “monitor” client and receives JSON status events for display only.
Those events are not required for the OBS/ASS workflow.

