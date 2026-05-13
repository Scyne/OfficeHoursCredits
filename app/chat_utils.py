import asyncio
import websockets
import urllib.request
import re
import pytchat

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
            await asyncio.sleep(15)

class YouTubeChat:
    def __init__(self, url, callback, sid, stream_state_ref):
        self.url = url
        self.callback = callback
        self.sid = sid
        self.stream_state_ref = stream_state_ref

    def get_yt_live_id(self):
        try:
            req = urllib.request.Request(self.url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req) as response:
                html = response.read().decode('utf-8')
                if 'isLiveNow":true' not in html and 'isLiveBroadcast":true' not in html:
                    return None
                match = re.search(r'"videoId":"([^"]+)"', html)
                if match:
                    return match.group(1)
        except Exception as e:
            pass
        return None

    async def run(self):
        # Patch pytchat to bypass get_channelid error
        original_get_channelid = getattr(pytchat.util, 'get_channelid', None)
        if original_get_channelid:
            pytchat.util.get_channelid = lambda client, video_id: "fake_channel_id"

        while True:
            try:
                self.stream_state_ref[self.sid]["live"] = False
                vid = self.get_yt_live_id()
                if vid:
                    self.stream_state_ref[self.sid]["live"] = True
                    chat = pytchat.LiveChat(video_id=vid)
                    while chat.is_alive():
                        for c in chat.get().sync_items():
                            self.callback(c.author.name, self.sid)
                        await asyncio.sleep(1)
            except Exception as e:
                pass

            self.stream_state_ref[self.sid]["live"] = False
            await asyncio.sleep(15)
