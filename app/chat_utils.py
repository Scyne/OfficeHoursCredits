import asyncio
import json
import time
import websockets
import urllib.request
import re

from app.yt_live_chat import iter_chat, LiveStreamNotFound, ChatNotAvailable


TWITCH_GQL_URL = "https://gql.twitch.tv/gql"
# Twitch's own public web client id (used by twitch.tv in the browser).
TWITCH_WEB_CLIENT_ID = "kimne78kx3ncx6brgo4mv6wki5h1ko"

def twitch_stream_is_live(channel):
    """True only if the channel is broadcasting right now.

    Twitch chat rooms are joinable 24/7, so a connected IRC socket says
    nothing about whether the stream is up. Ask Twitch directly: the
    public GQL endpoint returns a stream object only while broadcasting
    (type "live"; "rerun" does not count).
    """
    body = json.dumps({
        "query": "query($login:String!){user(login:$login){stream{type}}}",
        "variables": {"login": channel.lower()},
    }).encode()
    req = urllib.request.Request(
        TWITCH_GQL_URL, data=body,
        headers={"Client-Id": TWITCH_WEB_CLIENT_ID,
                 "Content-Type": "application/json",
                 "User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    user = (data.get("data") or {}).get("user") or {}
    stream = user.get("stream")
    return bool(stream) and stream.get("type") == "live"

class TwitchChat:
    def __init__(self, channel, callback, sid, stream_state_ref):
        self.channel = channel.lower()
        self.callback = callback
        self.sid = sid
        self.stream_state_ref = stream_state_ref

    async def run(self):
        uri = "wss://irc-ws.chat.twitch.tv:443"
        while True:
            try:
                self.stream_state_ref[self.sid]["live"] = False
                try:
                    is_live = await asyncio.to_thread(twitch_stream_is_live, self.channel)
                except Exception:
                    is_live = False
                if not is_live:
                    await asyncio.sleep(30)
                    continue
                async with websockets.connect(uri) as websocket:
                    # Do not request tags since we don't need them and they mess up the parsing
                    await websocket.send("PASS SCHMOOPIIE")
                    await websocket.send("NICK justinfan12345")
                    await websocket.send(f"JOIN #{self.channel}")

                    self.stream_state_ref[self.sid]["live"] = True
                    last_check = time.time()

                    while True:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=60.0)
                            for line in message.split('\r\n'):
                                if line.startswith('PING'):
                                    await websocket.send("PONG :tmi.twitch.tv")
                                elif "PRIVMSG" in line:
                                    parts = line.split(':', 2)
                                    if len(parts) >= 3:
                                        user = parts[1].split('!')[0]
                                        self.callback(user, self.sid)
                        except asyncio.TimeoutError:
                            await websocket.send("PING :tmi.twitch.tv")
                        except websockets.exceptions.ConnectionClosed:
                            break
                        # Re-verify the broadcast is still up about once a minute;
                        # the chat room outlives the stream, so don't trust the socket.
                        if time.time() - last_check >= 60:
                            last_check = time.time()
                            try:
                                still_live = await asyncio.to_thread(twitch_stream_is_live, self.channel)
                            except Exception:
                                still_live = True
                            if not still_live:
                                break
            except Exception as e:
                pass
            self.stream_state_ref[self.sid]["live"] = False
            await asyncio.sleep(30)

class YouTubeChat:
    def __init__(self, url, callback, sid, stream_state_ref):
        self.url = url
        self.callback = callback
        self.sid = sid
        self.stream_state_ref = stream_state_ref

    def get_yt_live_id(self):
        try:
            # Use the /live URL if possible
            url = self.url if self.url.endswith('/live') else f"{self.url.rstrip('/')}/live"
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req) as response:
                html = response.read().decode('utf-8')

            # Only trust the player response's own broadcast state. A /live
            # page can resolve to an ended stream's VOD, an upcoming
            # premiere, or the channel page, all of which carry video IDs
            # that are NOT live. isLiveNow is set by YouTube only while the
            # broadcast is actually on air.
            m = re.search(r'ytInitialPlayerResponse\s*=\s*(\{.+?\});\s*</script>', html, re.S)
            if m:
                try:
                    pr = json.loads(m.group(1))
                    micro = pr.get("microformat", {}).get("playerMicroformatRenderer", {})
                    lbd = micro.get("liveBroadcastDetails", {})
                    if lbd.get("isLiveNow"):
                        vid = pr.get("videoDetails", {}).get("videoId")
                        if vid:
                            return vid
                    return None
                except Exception:
                    pass

            # Fallback: canonical watch URL plus an explicit isLiveNow marker.
            canonical_match = re.search(r'<link rel="canonical" href="([^"]+)">', html)
            if canonical_match and 'watch?v=' in canonical_match.group(1) and '"isLiveNow":true' in html:
                return canonical_match.group(1).split('watch?v=')[1].split('&')[0]
        except Exception as e:
            pass
        return None

    def _run_sync(self, vid):
        # This generator yields forever until the stream ends or errors out.
        for msg in iter_chat(self.url, video_id=vid):
            self.callback(msg.author, self.sid)

    async def run(self):
        while True:
            try:
                self.stream_state_ref[self.sid]["live"] = False
                vid = self.get_yt_live_id()
                if vid:
                    self.stream_state_ref[self.sid]["live"] = True
                    # Run the blocking iter_chat in a separate thread so it doesn't block the async loop.
                    await asyncio.to_thread(self._run_sync, vid)
            except (LiveStreamNotFound, ChatNotAvailable):
                pass
            except Exception as e:
                pass

            self.stream_state_ref[self.sid]["live"] = False
            await asyncio.sleep(60)
