import asyncio, json, os, time, threading
import urllib.request
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from app.chat_utils import TwitchChat, YouTubeChat
from app.titles import get_random_title

DATA_DIR = "/app/data"
STATE_FILE = os.path.join(DATA_DIR, "state.json")
os.makedirs(DATA_DIR, exist_ok=True)

STREAMS = {
    "nightattack": {"name": "Twitch", "url": "https://www.twitch.tv/nightattack"},
    "greatnight": {"name": "Great Night", "url": "https://www.youtube.com/@GreatNightPod/live"},
    "scamschool": {"name": "Scam School", "url": "https://www.youtube.com/scamschool/live"},
    "modernrogue": {"name": "Modern Rogue", "url": "https://www.youtube.com/@ModernRogue/live"},
}

chatters = {}
stream_state = {sid: {"live": False, "last_seen": 0, "title": None} for sid in STREAMS}
last_message_ts = 0
session_start = time.time()
command_state = {"action": "preview", "participants": [], "duration": 70, "finfin_enabled": False, "finfin_intensity": 4, "updatedAt": 0}
state_lock = threading.Lock()


# ── Durable state in Upstash Redis ──────────────────────────────────────────
# Fly machines have ephemeral disks: state.json dies on every redeploy or
# machine replacement, and command_state was memory-only. When the Upstash
# REST credentials are present, mirror both state blobs to Redis and treat
# Redis as the source of truth at startup. Everything degrades gracefully
# to the old local-file behavior when the env vars are absent or Redis is
# unreachable.
REDIS_REST_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "").rstrip("/")
REDIS_REST_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
REDIS_STATE_KEY = "officecredits:state"
REDIS_COMMAND_KEY = "officecredits:command"

def _redis_pipeline(commands):
    if not REDIS_REST_URL or not REDIS_REST_TOKEN:
        return None
    try:
        req = urllib.request.Request(
            REDIS_REST_URL + "/pipeline",
            data=json.dumps(commands).encode(),
            headers={"Authorization": f"Bearer {REDIS_REST_TOKEN}",
                     "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None

def _redis_get(key):
    out = _redis_pipeline([["GET", key]])
    if out and isinstance(out, list) and out and "result" in out[0]:
        return out[0]["result"]
    return None

def _redis_set(key, value):
    _redis_pipeline([["SET", key, value]])

def load_state():
    global chatters, last_message_ts, session_start, command_state
    raw = _redis_get(REDIS_STATE_KEY)
    if raw:
        try:
            data = json.loads(raw)
            chatters = data.get("chatters", {})
            last_message_ts = data.get("last_message_ts", 0)
            session_start = data.get("session_start", time.time())
        except Exception:
            pass
    elif os.path.exists(STATE_FILE):
        try:
            data = json.load(open(STATE_FILE))
            chatters = data.get("chatters", {})
            last_message_ts = data.get("last_message_ts", 0)
            session_start = data.get("session_start", time.time())
        except Exception:
            pass
    raw_cmd = _redis_get(REDIS_COMMAND_KEY)
    if raw_cmd:
        try:
            saved = json.loads(raw_cmd)
            if isinstance(saved, dict):
                command_state.update(saved)
        except Exception:
            pass

def save_state():
    with state_lock:
        data = {"chatters": chatters, "last_message_ts": last_message_ts, "session_start": session_start}
        try:
            with open(STATE_FILE, "w") as f:
                json.dump(data, f)
        except Exception:
            pass
        payload = json.dumps(data)
    _redis_set(REDIS_STATE_KEY, payload)

def save_command_state():
    _redis_set(REDIS_COMMAND_KEY, json.dumps(command_state))

EXCLUDED_NAMES = {"nightbot", "nightattack", "scamschool", "modernrogue", "greatnight"}

def add_chatter(name):
    global last_message_ts
    name = name.strip().lstrip('@')
    key = name.lower()
    if not key or key in EXCLUDED_NAMES:
        return
    now = time.time()
    if key not in chatters:
        used_titles = {c.get("title", "") for c in chatters.values() if c.get("title")}
        chatters[key] = {"name": name, "title": get_random_title(used_titles), "first_seen": now}
        save_state()
    last_message_ts = now

def callback_wrapper(name, sid):
    stream_state[sid]["last_seen"] = time.time()
    add_chatter(name)

async def chat_worker_async(sid, cfg):
    url = cfg["url"]
    if "twitch.tv" in url:
        channel = url.split('/')[-1]
        chat = TwitchChat(channel, callback_wrapper, sid, stream_state)
    else:
        chat = YouTubeChat(url, callback_wrapper, sid, stream_state)
    await chat.run()

def start_chat_worker(sid, cfg):
    asyncio.run(chat_worker_async(sid, cfg))

async def purge_task():
    global chatters, last_message_ts, session_start
    while True:
        await asyncio.sleep(60)
        # Disabled auto clear based on user request.
        # now = time.time()
        # all_offline = all(not s["live"] for s in stream_state.values())
        # if all_offline and last_message_ts and (now - last_message_ts > 1800):
        #     with state_lock:
        #         chatters = {}
        #         last_message_ts = 0
        #         session_start = now
        #         save_state()

app = FastAPI()
app.mount("/static", StaticFiles(directory="/app/app/static"), name="static")

@app.on_event("startup")
async def startup():
    load_state()
    for sid, cfg in STREAMS.items():
        t = threading.Thread(target=start_chat_worker, args=(sid, cfg), daemon=True)
        t.start()
    asyncio.create_task(purge_task())

@app.get("/", response_class=HTMLResponse)
async def control():
    return FileResponse("/app/app/static/control.html")

@app.get("/output", response_class=HTMLResponse)
async def output():
    return FileResponse("/app/app/static/output.html")

@app.get("/greatnightcredits", response_class=HTMLResponse)
async def greatnightcredits():
    return FileResponse("/app/app/static/greatnightcredits.html")

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
    return {"chatters": [{"name": c["name"], "title": c.get("title", ""), "first_seen": c["first_seen"]} for c in sorted_list]}

@app.post("/api/chatters")
async def api_set_chatters(request: Request):
    global chatters
    data = await request.json()
    names_data = data.get("names", [])
    new = {}
    now = time.time()

    used_titles = {c.get("title", "") for c in chatters.values() if c.get("title")}

    for item in names_data:
        # Check if item is a dictionary or a string (for backwards compatibility if needed)
        if isinstance(item, dict):
            n = item.get("name", "")
            t = item.get("title", "")
        else:
            n = item
            t = ""

        n = n.strip().lstrip('@')
        key = n.lower()
        if not key:
            continue

        if not t:
            # Reassign a random title if one is missing but it's new
            # If it already exists in chatters and has a title, we keep it
            if key in chatters and chatters[key].get("title"):
                t = chatters[key]["title"]
            else:
                t = get_random_title(used_titles)
                used_titles.add(t)

        if key in chatters:
            new[key] = chatters[key]
            # Update title in case it was explicitly changed
            if isinstance(item, dict) and "title" in item:
                 new[key]["title"] = t
        else:
            new[key] = {"name": n, "title": t, "first_seen": now}

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
        "finfin_enabled": data.get("finfin_enabled", command_state.get("finfin_enabled")),
        "finfin_intensity": data.get("finfin_intensity", command_state.get("finfin_intensity")),
        "updatedAt": int(time.time() * 1000)
    })
    save_command_state()
    return {"ok": True}

@app.post("/api/autostart")
async def api_autostart():
    sorted_list = sorted(chatters.values(), key=lambda x: x["first_seen"])
    participants = [{"name": c["name"], "title": c.get("title", "")} for c in sorted_list]
    count = len(participants)

    # Base duration for "fast" speed is 34, plus 2.4 per participant
    duration = max(34, 34 + count * 2.4)

    command_state.update({
        "action": "start",
        "participants": participants,
        "duration": duration,
        # Preserve current finfin settings if they exist
        "updatedAt": int(time.time() * 1000)
    })
    save_command_state()
    return {"ok": True, "duration": duration, "participants_count": count}
