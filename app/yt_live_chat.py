"""
yt_live_chat.py
───────────────
Stream a YouTube channel's live chat without an API key or prior knowledge
of the video link.

How it works
────────────
1. Resolve the live video ID from a channel handle/URL by hitting the
   /{channel}/live redirect endpoint and parsing ytInitialData.
2. Bootstrap a continuation token from the watch page's embedded JSON.
3. Poll YouTube's internal Innertube API for chat batches, honouring the
   server-supplied timeout between requests.

Dependencies
────────────
    pip install requests

Usage
─────
    # As a script:
    python yt_live_chat.py @MrBeast
    python yt_live_chat.py https://www.youtube.com/@NASA

    # As a library:
    from yt_live_chat import watch_live_chat

    def handle(msg):
        print(f"[{msg['type']}] {msg['author']}: {msg['text']}")
        if msg['type'] == 'superchat':
            print(f"  └─ 💰 {msg['amount']}")

    watch_live_chat("@NASA", callback=handle)
"""

import json
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Iterator, Optional
import requests

# ── Constants ──────────────────────────────────────────────────────────────────

_BASE_URL = "https://www.youtube.com"
_LIVE_CHAT_API = f"{_BASE_URL}/youtubei/v1/live_chat/get_live_chat"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Innertube context sent with every API request
_INNERTUBE_CONTEXT = {
    "client": {
        "clientName": "WEB",
        "clientVersion": "2.20240510.01.00",
        "hl": "en",
        "gl": "US",
    }
}

# Default poll interval (ms) when the server doesn't supply one
_DEFAULT_TIMEOUT_MS = 5000

# ── Data model ─────────────────────────────────────────────────────────────────

@dataclass
class ChatMessage:
    type: str                          # 'message' | 'superchat' | 'membership' | 'sticker'
    author: str
    text: str
    timestamp: Optional[datetime] = None
    amount: Optional[str] = None       # Super Chat / Super Sticker purchase amount
    badge: Optional[str] = None        # e.g. "Member (6 months)"
    raw: dict = field(default_factory=dict, repr=False)

    def __str__(self):
        parts = []
        if self.badge:
            parts.append(f"[{self.badge}]")
        parts.append(f"{self.author}: {self.text}")
        if self.amount:
            parts.append(f"({self.amount})")
        return " ".join(parts)


# ── Internal helpers ────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(_HEADERS)
    return s


def _extract_json_blob(html: str, var_name: str) -> Optional[dict]:
    """Pull a JS variable assignment like `var FOO = {...};` out of a page."""
    pattern = rf"var {re.escape(var_name)}\s*=\s*"
    match = re.search(pattern, html)
    if not match:
        return None
    start = match.end()
    # Walk forward to find the matching closing brace
    depth = 0
    in_string = False
    escape_next = False
    for i, ch in enumerate(html[start:], start):
        if escape_next:
            escape_next = False
            continue
        if ch == "\\" and in_string:
            escape_next = True
            continue
        if ch == '"' and not escape_next:
            in_string = not in_string
        if not in_string:
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(html[start : i + 1])
                    except json.JSONDecodeError:
                        return None
    return None


def _extract_innertube_api_key(html: str) -> Optional[str]:
    """Extract the Innertube API key embedded in ytcfg.set({...})."""
    match = re.search(r'"INNERTUBE_API_KEY"\s*:\s*"([^"]+)"', html)
    return match.group(1) if match else None


def _resolve_channel_to_live_url(channel: str) -> str:
    """
    Accepts any of:
      '@Handle'
      'channel/UCxxxxxxxxxxxxxxxx'
      'https://www.youtube.com/@Handle'
      'https://www.youtube.com/channel/UCxxxxxxxxxxxxxxxx'
    Returns the /live URL.
    """
    channel = channel.strip().rstrip("/")
    if channel.endswith("/live"):
        return channel
    if channel.startswith("http"):
        return channel + "/live"
    if channel.startswith("@") or channel.startswith("channel/"):
        return f"{_BASE_URL}/{channel}/live"
    # Bare handle without @
    return f"{_BASE_URL}/@{channel}/live"


def _get_continuation_from_initial_data(data: dict) -> Optional[str]:
    """
    Navigate ytInitialData to find the first live-chat continuation token.
    Multiple nesting paths exist depending on YouTube's A/B layout.
    """
    # Path 1: standard two-column watch page
    try:
        bar = data["contents"]["twoColumnWatchNextResults"]["conversationBar"]
        continuations = bar["liveChatRenderer"]["continuations"]
        return _pick_continuation(continuations[0])
    except (KeyError, IndexError, TypeError):
        pass

    # Path 2: some mobile / alternate layouts embed it differently
    try:
        for tab in (
            data.get("contents", {})
            .get("twoColumnBrowseResultsRenderer", {})
            .get("tabs", [])
        ):
            renderer = tab.get("tabRenderer", {}).get("content", {})
            lc = renderer.get("liveChatRenderer", {})
            continuations = lc.get("continuations", [])
            if continuations:
                return _pick_continuation(continuations[0])
    except (KeyError, TypeError):
        pass

    return None


def _pick_continuation(cont_obj: dict) -> Optional[str]:
    """
    A continuation object can carry the token under several key names.
    Try them all in preference order.
    """
    for key in (
        "reloadContinuationData",
        "timedContinuationData",
        "invalidationContinuationData",
        "liveChatReplayContinuationData",
    ):
        token = cont_obj.get(key, {}).get("continuation")
        if token:
            return token
    return None


def _pick_next_continuation(continuations: list) -> tuple[Optional[str], int]:
    """Returns (token, timeout_ms) from a continuations list."""
    if not continuations:
        return None, _DEFAULT_TIMEOUT_MS
    obj = continuations[0]
    for key in (
        "timedContinuationData",
        "invalidationContinuationData",
        "reloadContinuationData",
        "liveChatReplayContinuationData",
    ):
        inner = obj.get(key, {})
        token = inner.get("continuation")
        if token:
            timeout_ms = inner.get("timeoutMs", _DEFAULT_TIMEOUT_MS)
            return token, timeout_ms
    return None, _DEFAULT_TIMEOUT_MS


def _runs_to_text(runs: list) -> str:
    """Flatten a YouTube 'runs' array (text + emoji) into a plain string."""
    parts = []
    for run in runs:
        if "text" in run:
            parts.append(run["text"])
        elif "emoji" in run:
            # Prefer the short name; fall back to the first label
            emoji = run["emoji"]
            shortcuts = emoji.get("shortcuts", [])
            if shortcuts:
                parts.append(shortcuts[0])
            else:
                labels = (
                    emoji.get("accessibility", {})
                    .get("accessibilityData", {})
                    .get("label", "")
                )
                parts.append(f":{labels}:" if labels else "")
    return "".join(parts)


def _parse_actions(actions: list) -> list[ChatMessage]:
    messages: list[ChatMessage] = []
    for action in actions:
        item = action.get("addChatItemAction", {}).get("item", {})
        if not item:
            continue

        # ── Regular chat message ──────────────────────────────────────────
        r = item.get("liveChatTextMessageRenderer")
        if r:
            ts_us = int(r.get("timestampUsec", 0))
            badge = _get_member_badge(r)
            messages.append(
                ChatMessage(
                    type="message",
                    author=r.get("authorName", {}).get("simpleText", ""),
                    text=_runs_to_text(r.get("message", {}).get("runs", [])),
                    timestamp=datetime.fromtimestamp(ts_us / 1e6) if ts_us else None,
                    badge=badge,
                    raw=r,
                )
            )
            continue

        # ── Super Chat ────────────────────────────────────────────────────
        r = item.get("liveChatPaidMessageRenderer")
        if r:
            ts_us = int(r.get("timestampUsec", 0))
            messages.append(
                ChatMessage(
                    type="superchat",
                    author=r.get("authorName", {}).get("simpleText", ""),
                    text=_runs_to_text(r.get("message", {}).get("runs", [])),
                    timestamp=datetime.fromtimestamp(ts_us / 1e6) if ts_us else None,
                    amount=r.get("purchaseAmountText", {}).get("simpleText", ""),
                    raw=r,
                )
            )
            continue

        # ── Super Sticker ─────────────────────────────────────────────────
        r = item.get("liveChatPaidStickerRenderer")
        if r:
            messages.append(
                ChatMessage(
                    type="sticker",
                    author=r.get("authorName", {}).get("simpleText", ""),
                    text="[Super Sticker]",
                    amount=r.get("purchaseAmountText", {}).get("simpleText", ""),
                    raw=r,
                )
            )
            continue

        # ── New Member / Membership milestone ────────────────────────────
        r = item.get("liveChatMembershipItemRenderer")
        if r:
            header_runs = r.get("headerSubtext", {}).get("runs", [])
            messages.append(
                ChatMessage(
                    type="membership",
                    author=r.get("authorName", {}).get("simpleText", ""),
                    text=_runs_to_text(header_runs) or "[New Member]",
                    raw=r,
                )
            )
            continue

    return messages


def _get_member_badge(renderer: dict) -> Optional[str]:
    """Extract the member badge label if present."""
    for badge in renderer.get("authorBadges", []):
        label = (
            badge.get("liveChatAuthorBadgeRenderer", {})
            .get("accessibility", {})
            .get("accessibilityData", {})
            .get("label")
        )
        if label:
            return label
    return None


# ── Public API ─────────────────────────────────────────────────────────────────

class LiveStreamNotFound(Exception):
    """Raised when the channel has no active live stream."""


class ChatNotAvailable(Exception):
    """Raised when live chat is disabled or the continuation can't be found."""


def get_live_video_id(channel: str, session: Optional[requests.Session] = None) -> str:
    """
    Resolve a channel identifier to the currently-live video ID.

    Parameters
    ----------
    channel : str
        Any of: '@Handle', 'channel/UCxxx', full YouTube URL.

    Returns
    -------
    str
        The 11-character video ID.

    Raises
    ------
    LiveStreamNotFound
    """
    s = session or _make_session()
    live_url = _resolve_channel_to_live_url(channel)

    resp = s.get(live_url, allow_redirects=True, timeout=15)
    resp.raise_for_status()

    # After the redirect the URL itself usually contains ?v=VIDEO_ID
    match = re.search(r"[?&]v=([A-Za-z0-9_-]{11})", resp.url)
    if match:
        return match.group(1)

    # Fallback: parse from the page (needed when YouTube doesn't redirect)
    match = re.search(r'"videoId"\s*:\s*"([A-Za-z0-9_-]{11})"', resp.text)
    if match:
        return match.group(1)

    raise LiveStreamNotFound(
        f"No live stream found for '{channel}'. "
        "The channel may not be live right now."
    )


def get_chat_bootstrap(
    video_id: str, session: Optional[requests.Session] = None
) -> tuple[str, str]:
    """
    Fetch the watch page and extract:
        - the first chat continuation token
        - the Innertube API key

    Returns
    -------
    (continuation_token, innertube_api_key)

    Raises
    ------
    ChatNotAvailable
    """
    s = session or _make_session()
    url = f"{_BASE_URL}/watch?v={video_id}"

    resp = s.get(url, timeout=15)
    resp.raise_for_status()

    api_key = _extract_innertube_api_key(resp.text) or ""

    data = _extract_json_blob(resp.text, "ytInitialData")
    if data is None:
        raise ChatNotAvailable(f"Could not extract ytInitialData for video {video_id}.")

    token = _get_continuation_from_initial_data(data)
    if token is None:
        raise ChatNotAvailable(
            f"No live chat continuation found for video {video_id}. "
            "Live chat may be disabled on this stream."
        )

    return token, api_key


def iter_chat(
    channel: str,
    *,
    session: Optional[requests.Session] = None,
    video_id: Optional[str] = None,
) -> Iterator[ChatMessage]:
    """
    Generator that yields ChatMessage objects indefinitely until the stream ends
    or an unrecoverable error occurs.

    Parameters
    ----------
    channel : str
        Channel handle / URL (ignored if video_id is supplied).
    session : requests.Session, optional
        Reuse an existing session (e.g. with custom proxies / cookies).
    video_id : str, optional
        Skip the channel-resolution step if you already have the video ID.

    Yields
    ------
    ChatMessage
    """
    s = session or _make_session()

    vid = video_id or get_live_video_id(channel, session=s)
    continuation, api_key = get_chat_bootstrap(vid, session=s)

    api_url = _LIVE_CHAT_API
    if api_key:
        api_url = f"{_LIVE_CHAT_API}?key={api_key}"

    while True:
        payload = {
            "context": _INNERTUBE_CONTEXT,
            "continuation": continuation,
        }

        try:
            resp = s.post(
                api_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            # Transient error — back off and retry
            print(f"[yt_live_chat] Request error: {exc}. Retrying in 10 s …", file=sys.stderr)
            time.sleep(10)
            continue

        try:
            lcc = data["continuationContents"]["liveChatContinuation"]
        except KeyError:
            # Stream likely ended
            print("[yt_live_chat] Stream ended or chat no longer available.", file=sys.stderr)
            return

        # Yield parsed messages
        actions = lcc.get("actions", [])
        yield from _parse_actions(actions)

        # Advance continuation
        next_token, timeout_ms = _pick_next_continuation(lcc.get("continuations", []))
        if not next_token:
            print("[yt_live_chat] No continuation token — stream has ended.", file=sys.stderr)
            return

        continuation = next_token
        # Honour the server-supplied poll interval (convert ms → s)
        time.sleep(timeout_ms / 1000)


def watch_live_chat(
    channel: str,
    callback: Optional[Callable[[ChatMessage], None]] = None,
    *,
    session: Optional[requests.Session] = None,
    video_id: Optional[str] = None,
) -> None:
    """
    Block and call `callback` for every incoming chat message.
    If `callback` is None, messages are printed to stdout.

    Parameters
    ----------
    channel : str
        '@Handle', 'channel/UCxxx', or full YouTube channel URL.
    callback : callable, optional
        Called with a ChatMessage for every event.
    session : requests.Session, optional
    video_id : str, optional
        Skip channel resolution if you already have the video ID.
    """
    for msg in iter_chat(channel, session=session, video_id=video_id):
        if callback:
            callback(msg)
        else:
            _default_printer(msg)


# ── CLI printer ────────────────────────────────────────────────────────────────

_TYPE_PREFIX = {
    "message":    "💬",
    "superchat":  "💰",
    "sticker":    "🎯",
    "membership": "🌟",
}

def _default_printer(msg: ChatMessage) -> None:
    icon = _TYPE_PREFIX.get(msg.type, "❓")
    ts = msg.timestamp.strftime("%H:%M:%S") if msg.timestamp else "--:--:--"
    badge = f" [{msg.badge}]" if msg.badge else ""
    amount = f" ({msg.amount})" if msg.amount else ""
    print(f"{icon} {ts}{badge} {msg.author}{amount}: {msg.text}")


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python yt_live_chat.py <channel>")
        print("  channel: @Handle | channel/UCxxx | full YouTube URL")
        sys.exit(1)

    channel_arg = sys.argv[1]
    print(f"[yt_live_chat] Resolving live stream for: {channel_arg}")

    try:
        vid = get_live_video_id(channel_arg)
        print(f"[yt_live_chat] Live video ID: {vid}  →  https://youtube.com/watch?v={vid}")
        print("[yt_live_chat] Connecting to live chat…\n")
        watch_live_chat(channel_arg, video_id=vid)
    except LiveStreamNotFound as e:
        print(f"[yt_live_chat] ✗ {e}", file=sys.stderr)
        sys.exit(1)
    except ChatNotAvailable as e:
        print(f"[yt_live_chat] ✗ {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n[yt_live_chat] Stopped.")
