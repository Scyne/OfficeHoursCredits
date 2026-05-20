import asyncio
import websockets
import urllib.request
import re

from app.yt_live_chat import iter_chat, LiveStreamNotFound, ChatNotAvailable

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
                async with websockets.connect(uri) as websocket:
                    # Do not request tags since we don't need them and they mess up the parsing
                    await websocket.send("PASS SCHMOOPIIE")
                    await websocket.send("NICK justinfan12345")
                    await websocket.send(f"JOIN #{self.channel}")

                    self.stream_state_ref[self.sid]["live"] = True

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
            except Exception as e:
                pass
            self.stream_state_ref[self.sid]["live"] = False
            await asyncio.sleep(60)

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

                # Check canonical URL to determine if we are on a live video or redirected to channel page
                canonical_match = re.search(r'<link rel="canonical" href="([^"]+)">', html)
                if canonical_match:
                    canonical = canonical_match.group(1)
                    if 'watch?v=' in canonical:
                        return canonical.split('watch?v=')[1]
                    elif '/channel/' in canonical or '/@' in canonical:
                        # Redirected to a channel page implies they are not live
                        return None

                # Fallback to look specifically for the videoId if canonical is undefined or missing
                match = re.search(r'"videoId":"([^"]+)"', html)
                if match:
                    return match.group(1)
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
