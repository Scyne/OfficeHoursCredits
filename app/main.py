import asyncio, json, os, time, threading
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from chat_downloader import ChatDownloader

DATA_DIR = "/app/data"
STATE_FILE = os.path.join(DATA_DIR, "state.json")
os.makedirs(DATA_DIR, exist_ok=True)

STREAMS = {
    "nightattack": {"name": "Night Attack (Twitch)", "url": "https://www.twitch.tv/nightattack"},
    "scamschool": {"name": "Scam School (YouTube)", "url": "https://www.youtube.com/scamschool"},
    "modernrogue": {"name": "Modern Rogue (YouTube)", "url": "https://www.youtube.com/@ModernRogue"},
    "greatnight": {"name": "Great Night (YouTube)", "url": "https://www.youtube.com/@GreatNightPod"},
}

chatters = {}
stream_state = {sid: {"live": False, "last_seen": 0} for sid in STREAMS}
last_message_ts = 0
session_start = time.time()
command_state = {"action": "preview", "participants": [], "duration": 70, "updatedAt": 0}
state_lock = threading.Lock()

def load_state():
    global chatters, last_message_ts, session_start
    if os.path.exists(STATE_FILE):
        try:
            data = json.load(open(STATE_FILE))
            chatters = data.get("chatters", {})
            last_message_ts = data.get("last_message_ts", 0)
            session_start = data.get("session_start", time.time())
        except Exception:
            pass

def save_state():
    with state_lock:
        data = {"chatters": chatters, "last_message_ts": last_message_ts, "session_start": session_start}
        with open(STATE_FILE, "w") as f:
            json.dump(data, f)

def add_chatter(name):
    global last_message_ts
    key = name.lower().strip()
    if not key:
        return
    now = time.time()
    if key not in chatters:
        chatters[key] = {"name": name.strip(), "first_seen": now}
        save_state()
    last_message_ts = now

def chat_worker(sid, url):
    while True:
        try:
            stream_state[sid]["live"] = False
            downloader = ChatDownloader()
            chat = downloader.get_chat(url)
            stream_state[sid]["live"] = True
            for msg in chat:
                author = msg.get("author", {})
                name = author.get("display_name") or author.get("name")
                if name:
                    add_chatter(name)
                    stream_state[sid]["last_seen"] = time.time()
        except Exception:
            time.sleep(15)
        finally:
            stream_state[sid]["live"] = False
            time.sleep(10)

async def purge_task():
    global chatters, last_message_ts, session_start
    while True:
        await asyncio.sleep(60)
        now = time.time()
        all_offline = all(not s["live"] for s in stream_state.values())
        if all_offline and last_message_ts and (now - last_message_ts > 1800):
            with state_lock:
                chatters = {}
                last_message_ts = 0
                session_start = now
                save_state()

app = FastAPI()
app.mount("/static", StaticFiles(directory="/app/app/static"), name="static")

@app.on_event("startup")
async def startup():
    load_state()
    for sid, cfg in STREAMS.items():
        t = threading.Thread(target=chat_worker, args=(sid, cfg["url"]), daemon=True)
        t.start()
    asyncio.create_task(purge_task())

@app.get("/", response_class=HTMLResponse)
async def control():
    return FileResponse("/app/app/static/control.html")

@app.get("/output", response_class=HTMLResponse)
async def output():
    return FileResponse("/app/app/static/output.html")

@app.get("/api/status")
async def api_status():
    total = len(chatters)
    streams = []
    for sid in STREAMS:
        streams.append({"id": sid, **STREAMS[sid], **stream_state[sid]})
    return {"streams": streams, "total_chatters": total, "last_message_ts": last_message_ts, "session_start": session_start}

@app.get("/api/chatters")
async def api_get_chatters():
    sorted_list = sorted(chatters.values(), key=lambda x: x["first_seen"])
    return {"chatters": [{"name": c["name"], "title": "", "first_seen": c["first_seen"]} for c in sorted_list]}

@app.post("/api/chatters")
async def api_set_chatters(request: Request):
    global chatters
    data = await request.json()
    names = data.get("names", [])
    new = {}
    now = time.time()
    for n in names:
        key = n.lower().strip()
        if not key:
            continue
        if key in chatters:
            new[key] = chatters[key]
        else:
            new[key] = {"name": n.strip(), "first_seen": now}
    chatters = new
    save_state()
    return {"ok": True, "total": len(chatters)}

@app.get("/api/command")
async def api_get_command():
    return command_state

@app.post("/api/command")
async def api_set_command(request: Request):
    data = await request.json()
    command_state.update({
        "action": data.get("action", "preview"),
        "participants": data.get("participants", []),
        "duration": data.get("duration", 70),
        "updatedAt": int(time.time() * 1000)
    })
    return {"ok": True}
