# Office Credits - Live Chat Credits Stack

Self-contained Docker stack that monitors live chat on:
- Twitch: https://www.twitch.tv/nightattack
- YouTube: Scam School, Modern Rogue, Great Night Pod

It collects **only users who send a message**, deduplicates across all streams, and exposes:
- Control panel at `/` — live status, editable chatter list, roll controls
- OBS output at `/output` — transparent auto-scrolling credits

## Features
- Auto-start collecting when any stream goes live (no API keys)
- Combined first-seen ordering across all chats
- Persists session to ./data/state.json
- Auto-purge if ALL streams are offline for >30 minutes
- Edit names before rolling; Start/Reset from control panel
- Lightweight: ~1 Python process, ~150MB RAM, handles thousands of chatters

## Quick Start
```bash
cd officecredits
docker compose up -d --build
```

Open:
- Control: http://localhost:8000/
- Output: http://localhost:8000/output

For production with Traefik, the compose file includes labels for `officecredits.hive.scyne.com`.

## OBS Setup
1. Add Browser Source
2. URL: https://officecredits.hive.scyne.com/output
3. Width: 1920, Height: 1080
4. Check "Shutdown source when not visible"
5. Check "Refresh browser when scene becomes active"
Background is transparent by default.

## Operation
1. Streams go live → chatters appear automatically in control panel (polls every 10s)
2. Review list, click **Save Edits** if you modify names
3. Click **Start Roll** → output page scrolls
4. **Reset Roll** returns to preview
5. **Clear Session** immediately wipes memory

## Maintenance
- Logs: `docker logs -f officecredits`
- State: `./data/state.json` (back up to preserve sessions)
- Update: `docker compose pull && docker compose up -d --build`
- Health: `curl http://localhost:8000/api/status`

## How it works
- `chat-downloader` runs 4 threads, one per channel, parsing live chat without keys
- Each message author is added to an in-memory dict keyed by lowercased name
- `last_message_ts` updated on every message
- Purge task runs every 60s; if all streams offline and now - last_message_ts > 1800s → clear
- Control panel and output communicate via simple REST API (`/api/command`)

## Customization
- Edit STREAMS in `app/main.py` to add/remove channels
- Adjust purge timeout (1800 seconds) in `purge_task`
- Modify titles in control panel JS if you want default titles instead of blank
