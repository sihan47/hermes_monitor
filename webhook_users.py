from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv
load_dotenv()

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_USERS_PATH = Path(__file__).resolve().parent / "line_users.json"
DEFAULT_USER_FIELDS = {
    "include_keywords": [],
    "exclude_keywords": [],
    "regions": [],
    "require_available": True,
    "only_bags": True,
    "notify_until": "",
    "NAME": "",
}


def _default_user_value(key):
    if key == "notify_until":
        return (
            datetime.now() - timedelta(days=1)
        ).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    default = DEFAULT_USER_FIELDS.get(key)
    return default[:] if isinstance(default, list) else default


def get_display_name(user_id):
    """Fetch the LINE display name for a given user ID."""
    if not LINE_CHANNEL_ACCESS_TOKEN or not user_id:
        return None

    try:
        resp = requests.get(
            f"https://api.line.me/v2/bot/profile/{user_id}",
            headers={"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"},
            timeout=5,
        )
        if resp.status_code == 200:
            return resp.json().get("displayName")
    except Exception as exc:
        print(f"profile lookup failed for {user_id}: {exc}", flush=True)

    return None


def send_reply(reply_token, text):
    """Reply to the user with a simple text message."""
    if not LINE_CHANNEL_ACCESS_TOKEN or not reply_token:
        return
    try:
        resp = requests.post(
            "https://api.line.me/v2/bot/message/reply",
            headers={
                "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
                "Content-Type": "application/json",
            },
            json={
                "replyToken": reply_token,
                "messages": [{"type": "text", "text": text}],
            },
            timeout=5,
        )
        if resp.status_code != 200:
            print(f"reply failed {resp.status_code}: {resp.text}", flush=True)
    except Exception as exc:
        print(f"reply exception: {exc}", flush=True)


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body_bytes = self.rfile.read(length)
        try:
            data = json.loads(body_bytes.decode("utf-8"))
            for event in data.get("events", []):
                src = event.get("source", {})
                msg = event.get("message", {})
                reply_token = event.get("replyToken")

                user_id = src.get("userId")
                display_name = get_display_name(user_id) if user_id else None
                self._maybe_update_name(user_id, display_name)

                if isinstance(msg, dict):
                    message_content = msg.get("text") or json.dumps(msg, ensure_ascii=False)
                else:
                    message_content = str(msg) if msg is not None else "None"

                print(
                    f"USER ID: {user_id or 'unknown'} | "
                    f"NAME: {display_name or 'unknown'} | "
                    f"MESSAGE: {message_content}",
                    flush=True,
                )

                if reply_token:
                    send_reply(reply_token, "歡迎使用本服務")
        except Exception as e:
            print("parse error:", e, flush=True)

        self.send_response(200)
        self.end_headers()

    def _maybe_update_name(self, user_id, display_name):
        if not user_id:
            return

        try:
            if LINE_USERS_PATH.exists():
                with LINE_USERS_PATH.open("r", encoding="utf-8") as f:
                    users = json.load(f)
                if not isinstance(users, list):
                    users = []
            else:
                users = []

            changed = False
            found = False
            for entry in users:
                if entry.get("user_id") == user_id:
                    found = True
                    if self._ensure_all_tags(entry, display_name):
                        changed = True
                    break

            if not found:
                new_entry = {"user_id": user_id}
                self._ensure_all_tags(new_entry, display_name)
                users.append(new_entry)
                changed = True
                print(f"Added new user to line_users.json: {user_id}", flush=True)

            if changed:
                with LINE_USERS_PATH.open("w", encoding="utf-8") as f:
                    json.dump(users, f, ensure_ascii=False, indent=2)
                if display_name:
                    print(f"Updated NAME for {user_id} -> {display_name}", flush=True)
        except Exception as exc:
            print(f"failed to update line_users.json: {exc}", flush=True)

    def _ensure_all_tags(self, entry, display_name):
        """Ensure a user entry contains all required fields; returns True if mutated."""
        changed = False
        for key, default in DEFAULT_USER_FIELDS.items():
            if key not in entry:
                entry[key] = _default_user_value(key)
                changed = True
        if display_name and not entry.get("NAME"):
            entry["NAME"] = display_name
            changed = True
        return changed


if __name__ == "__main__":
    print("Listening on 0.0.0.0:8000")
    HTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
