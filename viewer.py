#!/usr/bin/env python3
"""Slack Export Viewer - Flask app with SQLite FTS5 search."""

import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

from flask import Flask, g, render_template, request, send_from_directory, abort, redirect, url_for, jsonify
from markupsafe import Markup, escape

BASE_DIR = Path(__file__).parent.resolve()
DB_PATH = BASE_DIR / "slack_archive.db"

app = Flask(__name__)
app.config["BASE_DIR"] = str(BASE_DIR)

# ---------------------------------------------------------------------------
# Emoji map (common Slack emoji name -> Unicode)
# ---------------------------------------------------------------------------
EMOJI_MAP = {
    "thumbsup": "\U0001f44d", "+1": "\U0001f44d", "thumbsdown": "\U0001f44e", "-1": "\U0001f44e",
    "heart": "\u2764\ufe0f", "heavy_heart_exclamation": "\u2764\ufe0f", "heart_eyes": "\U0001f60d",
    "joy": "\U0001f602", "sob": "\U0001f62d", "cry": "\U0001f622", "laughing": "\U0001f606",
    "satisfied": "\U0001f606", "grinning": "\U0001f600", "grin": "\U0001f601", "smile": "\U0001f604",
    "smiley": "\U0001f603", "sweat_smile": "\U0001f605", "rolling_on_the_floor_laughing": "\U0001f923",
    "rofl": "\U0001f923", "slightly_smiling_face": "\U0001f642", "upside_down_face": "\U0001f643",
    "wink": "\U0001f609", "blush": "\U0001f60a", "innocent": "\U0001f607",
    "smiling_face_with_3_hearts": "\U0001f970", "kissing_heart": "\U0001f618",
    "kissing": "\U0001f617", "relaxed": "\u263a\ufe0f", "kissing_closed_eyes": "\U0001f61a",
    "kissing_smiling_eyes": "\U0001f619", "yum": "\U0001f60b", "stuck_out_tongue": "\U0001f61b",
    "stuck_out_tongue_winking_eye": "\U0001f61c", "zany_face": "\U0001f92a",
    "stuck_out_tongue_closed_eyes": "\U0001f61d", "money_mouth_face": "\U0001f911",
    "hugging_face": "\U0001f917", "thinking_face": "\U0001f914", "thinking": "\U0001f914",
    "zipper_mouth_face": "\U0001f910", "raised_eyebrow": "\U0001f928",
    "neutral_face": "\U0001f610", "expressionless": "\U0001f611", "no_mouth": "\U0001f636",
    "smirk": "\U0001f60f", "unamused": "\U0001f612", "roll_eyes": "\U0001f644",
    "grimacing": "\U0001f62c", "lying_face": "\U0001f925", "relieved": "\U0001f60c",
    "pensive": "\U0001f614", "sleepy": "\U0001f62a", "sleeping": "\U0001f634",
    "mask": "\U0001f637", "face_with_thermometer": "\U0001f912",
    "nerd_face": "\U0001f913", "sunglasses": "\U0001f60e",
    "star_struck": "\U0001f929", "partying_face": "\U0001f973",
    "confused": "\U0001f615", "worried": "\U0001f61f", "slightly_frowning_face": "\U0001f641",
    "frowning_face": "\u2639\ufe0f", "open_mouth": "\U0001f62e", "hushed": "\U0001f62f",
    "astonished": "\U0001f632", "flushed": "\U0001f633", "pleading_face": "\U0001f97a",
    "frowning": "\U0001f626", "anguished": "\U0001f627", "fearful": "\U0001f628",
    "cold_sweat": "\U0001f630", "disappointed_relieved": "\U0001f625",
    "persevere": "\U0001f623", "confounded": "\U0001f616",
    "tired_face": "\U0001f62b", "weary": "\U0001f629", "triumph": "\U0001f624",
    "rage": "\U0001f621", "angry": "\U0001f620", "skull": "\U0001f480",
    "skull_and_crossbones": "\u2620\ufe0f", "ghost": "\U0001f47b",
    "clown_face": "\U0001f921", "poop": "\U0001f4a9", "hankey": "\U0001f4a9",
    "robot_face": "\U0001f916", "see_no_evil": "\U0001f648",
    "hear_no_evil": "\U0001f649", "speak_no_evil": "\U0001f64a",
    "wave": "\U0001f44b", "raised_hands": "\U0001f64c", "clap": "\U0001f44f",
    "pray": "\U0001f64f", "handshake": "\U0001f91d", "ok_hand": "\U0001f44c",
    "muscle": "\U0001f4aa", "point_up": "\u261d\ufe0f", "point_down": "\U0001f447",
    "point_left": "\U0001f448", "point_right": "\U0001f449",
    "middle_finger": "\U0001f595", "raised_hand": "\u270b",
    "v": "\u270c\ufe0f", "crossed_fingers": "\U0001f91e",
    "fire": "\U0001f525", "100": "\U0001f4af", "star": "\u2b50", "star2": "\U0001f31f",
    "sparkles": "\u2728", "boom": "\U0001f4a5", "collision": "\U0001f4a5",
    "hearts": "\u2665\ufe0f", "yellow_heart": "\U0001f49b", "green_heart": "\U0001f49a",
    "blue_heart": "\U0001f499", "purple_heart": "\U0001f49c", "black_heart": "\U0001f5a4",
    "broken_heart": "\U0001f494", "two_hearts": "\U0001f495", "revolving_hearts": "\U0001f49e",
    "sparkling_heart": "\U0001f496", "heartpulse": "\U0001f497", "cupid": "\U0001f498",
    "gift_heart": "\U0001f49d", "heart_decoration": "\U0001f49f",
    "eyes": "\U0001f440", "eye": "\U0001f441\ufe0f", "brain": "\U0001f9e0",
    "tongue": "\U0001f445", "lips": "\U0001f444",
    "baby": "\U0001f476", "woman": "\U0001f469", "man": "\U0001f468",
    "boy": "\U0001f466", "girl": "\U0001f467",
    "sunny": "\u2600\ufe0f", "cloud": "\u2601\ufe0f", "umbrella": "\u2614",
    "snowflake": "\u2744\ufe0f", "rainbow": "\U0001f308",
    "dog": "\U0001f436", "cat": "\U0001f431", "mouse": "\U0001f42d",
    "bear": "\U0001f43b", "panda_face": "\U0001f43c",
    "monkey_face": "\U0001f435", "chicken": "\U0001f414",
    "pig": "\U0001f437", "frog": "\U0001f438", "butterfly": "\U0001f98b",
    "cherry_blossom": "\U0001f338", "rose": "\U0001f339", "sunflower": "\U0001f33b",
    "seedling": "\U0001f331", "evergreen_tree": "\U0001f332",
    "apple": "\U0001f34e", "banana": "\U0001f34c", "watermelon": "\U0001f349",
    "grapes": "\U0001f347", "strawberry": "\U0001f353", "peach": "\U0001f351",
    "pizza": "\U0001f355", "hamburger": "\U0001f354", "fries": "\U0001f35f",
    "popcorn": "\U0001f37f", "cake": "\U0001f370", "cookie": "\U0001f36a",
    "chocolate_bar": "\U0001f36b", "candy": "\U0001f36c", "ice_cream": "\U0001f368",
    "coffee": "\u2615", "tea": "\U0001f375", "beer": "\U0001f37a", "wine_glass": "\U0001f377",
    "champagne": "\U0001f37e", "cocktail": "\U0001f378",
    "tada": "\U0001f389", "confetti_ball": "\U0001f38a", "balloon": "\U0001f388",
    "gift": "\U0001f381", "trophy": "\U0001f3c6", "medal": "\U0001f3c5",
    "crown": "\U0001f451", "gem": "\U0001f48e", "ring": "\U0001f48d",
    "bell": "\U0001f514", "mega": "\U0001f4e3", "loudspeaker": "\U0001f4e2",
    "bulb": "\U0001f4a1", "mag": "\U0001f50d", "lock": "\U0001f512", "key": "\U0001f511",
    "hammer": "\U0001f528", "wrench": "\U0001f527", "gear": "\u2699\ufe0f",
    "link": "\U0001f517", "paperclip": "\U0001f4ce", "scissors": "\u2702\ufe0f",
    "pencil2": "\u270f\ufe0f", "memo": "\U0001f4dd", "book": "\U0001f4d6",
    "bookmark": "\U0001f516", "clipboard": "\U0001f4cb",
    "calendar": "\U0001f4c5", "chart_with_upwards_trend": "\U0001f4c8",
    "phone": "\U0001f4de", "email": "\U0001f4e7", "computer": "\U0001f4bb",
    "tv": "\U0001f4fa", "camera": "\U0001f4f7", "movie_camera": "\U0001f3a5",
    "musical_note": "\U0001f3b5", "headphones": "\U0001f3a7", "art": "\U0001f3a8",
    "video_game": "\U0001f3ae", "dart": "\U0001f3af",
    "car": "\U0001f697", "taxi": "\U0001f695", "bus": "\U0001f68c",
    "airplane": "\u2708\ufe0f", "rocket": "\U0001f680", "ship": "\U0001f6a2",
    "bike": "\U0001f6b2", "warning": "\u26a0\ufe0f", "no_entry": "\u26d4",
    "x": "\u274c", "white_check_mark": "\u2705", "heavy_check_mark": "\u2714\ufe0f",
    "question": "\u2753", "exclamation": "\u2757", "interrobang": "\u2049\ufe0f",
    "red_circle": "\U0001f534", "orange_circle": "\U0001f7e0",
    "yellow_circle": "\U0001f7e1", "green_circle": "\U0001f7e2",
    "blue_circle": "\U0001f535", "purple_circle": "\U0001f7e3",
    "black_circle": "\u26ab", "white_circle": "\u26aa",
    "flag-us": "\U0001f1fa\U0001f1f8", "flag-in": "\U0001f1ee\U0001f1f3",
    "us": "\U0001f1fa\U0001f1f8",
    "zzz": "\U0001f4a4", "speech_balloon": "\U0001f4ac",
    "thought_balloon": "\U0001f4ad", "sweat_drops": "\U0001f4a6",
    "dash": "\U0001f4a8", "hole": "\U0001f573\ufe0f",
    "raised_hand_with_fingers_splayed": "\U0001f590\ufe0f",
    "palms_up_together": "\U0001f932", "open_hands": "\U0001f450",
    "writing_hand": "\u270d\ufe0f", "nail_care": "\U0001f485",
    "selfie": "\U0001f933", "leg": "\U0001f9b5", "foot": "\U0001f9b6",
    "ear": "\U0001f442", "nose": "\U0001f443",
    "place_of_worship": "\U0001f6d0", "atom_symbol": "\u269b\ufe0f",
    "om_symbol": "\U0001f549\ufe0f", "dove_of_peace": "\U0001f54a\ufe0f",
    "spider": "\U0001f577\ufe0f", "spider_web": "\U0001f578\ufe0f",
    "bouquet": "\U0001f490", "lotus": "\U0001fab7",
    "fallen_leaf": "\U0001f342", "leaves": "\U0001f343", "maple_leaf": "\U0001f341",
    "earth_americas": "\U0001f30e", "earth_africa": "\U0001f30d", "earth_asia": "\U0001f30f",
    "new_moon": "\U0001f311", "full_moon": "\U0001f315",
    "crescent_moon": "\U0001f319", "dizzy": "\U0001f4ab",
    "zap": "\u26a1", "ocean": "\U0001f30a", "volcano": "\U0001f30b",
    "milky_way": "\U0001f30c",
    "camping": "\U0001f3d5\ufe0f", "beach_with_umbrella": "\U0001f3d6\ufe0f",
    "building_construction": "\U0001f3d7\ufe0f",
    "house": "\U0001f3e0", "office": "\U0001f3e2", "school": "\U0001f3eb",
    "hospital": "\U0001f3e5", "church": "\u26ea",
    "watch": "\u231a", "hourglass": "\u231b",
    "alarm_clock": "\u23f0", "stopwatch": "\u23f1\ufe0f",
    "clock1": "\U0001f550", "clock12": "\U0001f55b",
}


def emoji_to_unicode(name):
    """Convert a Slack emoji name to unicode, stripping skin tone suffixes."""
    base = re.split(r"::skin-tone-\d", name)[0]
    return EMOJI_MAP.get(base, None)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(str(DB_PATH))
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


# ---------------------------------------------------------------------------
# User cache (loaded once per request)
# ---------------------------------------------------------------------------

def get_users():
    if "users" not in g:
        db = get_db()
        rows = db.execute("SELECT id, display_name, real_name, avatar_url, color FROM users").fetchall()
        g.users = {r["id"]: dict(r) for r in rows}
    return g.users


# ---------------------------------------------------------------------------
# Slack message rendering
# ---------------------------------------------------------------------------

def render_slack_message(text, users=None):
    """Convert Slack mrkdwn to HTML."""
    if not text:
        return ""
    if users is None:
        users = get_users()

    t = escape(text)
    t = str(t)

    # User mentions: <@U1234> or <@U1234|name>
    def replace_mention(m):
        uid = m.group(1)
        u = users.get(uid)
        name = u["display_name"] or u["real_name"] if u else uid
        return f'<span class="mention">@{escape(name)}</span>'
    t = re.sub(r'&lt;@(U[A-Z0-9]+)(?:\|[^&]*)?\&gt;', replace_mention, t)

    # Channel mentions: <#C1234|name>
    def replace_channel(m):
        cname = m.group(2)
        return f'<span class="mention">#{escape(cname)}</span>'
    t = re.sub(r'&lt;#(C[A-Z0-9]+)\|([^&]*?)&gt;', replace_channel, t)

    # URLs: <URL|label> or <URL>
    def replace_url(m):
        url = m.group(1)
        label = m.group(3) or url
        return f'<a href="{url}" target="_blank" rel="noopener">{escape(label)}</a>'
    t = re.sub(r'&lt;(https?://[^|&]+?)(\|([^&]*?))?&gt;', replace_url, t)

    # Code blocks: ```code```
    def replace_codeblock(m):
        code = m.group(1)
        return f'<pre class="code-block">{code}</pre>'
    t = re.sub(r'```(.*?)```', replace_codeblock, t, flags=re.DOTALL)

    # Inline code: `code`
    t = re.sub(r'`([^`]+?)`', r'<code>\1</code>', t)

    # Bold: *text*
    t = re.sub(r'(?<![a-zA-Z0-9])\*([^\*\n]+?)\*(?![a-zA-Z0-9])', r'<strong>\1</strong>', t)

    # Italic: _text_
    t = re.sub(r'(?<![a-zA-Z0-9])_([^_\n]+?)_(?![a-zA-Z0-9])', r'<em>\1</em>', t)

    # Strikethrough: ~text~
    t = re.sub(r'(?<![a-zA-Z0-9])~([^~\n]+?)~(?![a-zA-Z0-9])', r'<del>\1</del>', t)

    # Emoji: :name: (with optional skin tone)
    def replace_emoji(m):
        name = m.group(1)
        uni = emoji_to_unicode(name)
        if uni:
            return f'<span class="emoji">{uni}</span>'
        return f'<span class="emoji-text">:{name}:</span>'
    t = re.sub(r':([a-z0-9_+\-]+(?:::skin-tone-\d)?):',  replace_emoji, t)

    # Newlines
    t = t.replace("\n", "<br>")

    return Markup(t)


app.jinja_env.filters["slack_render"] = render_slack_message
app.jinja_env.globals["get_users"] = get_users


def ts_to_datetime(ts):
    """Convert Slack ts string to datetime."""
    try:
        return datetime.fromtimestamp(float(ts))
    except (ValueError, TypeError, OSError):
        return datetime(2000, 1, 1)


app.jinja_env.filters["ts_to_datetime"] = ts_to_datetime
app.jinja_env.filters["ts_to_time"] = lambda ts: ts_to_datetime(ts).strftime("%-I:%M %p")
app.jinja_env.filters["ts_to_date"] = lambda ts: ts_to_datetime(ts).strftime("%B %-d, %Y")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    db = get_db()
    channels = db.execute("""
        SELECT c.*, COUNT(m.ts) as msg_count
        FROM channels c
        LEFT JOIN messages m ON m.channel_id = c.id AND m.thread_ts IS NULL OR m.thread_ts = m.ts
        GROUP BY c.id
        ORDER BY c.type, c.name
    """).fetchall()
    grouped = {"channel": [], "dm": [], "group": [], "mpim": []}
    for ch in channels:
        grouped.setdefault(ch["type"], []).append(ch)
    return render_template("index.html", grouped=grouped)


@app.route("/api/channels")
def api_channels():
    db = get_db()
    channels = db.execute("""
        SELECT c.id, c.name, c.type, COUNT(m.ts) as msg_count
        FROM channels c
        LEFT JOIN messages m ON m.channel_id = c.id AND (m.thread_ts IS NULL OR m.thread_ts = m.ts)
        GROUP BY c.id
        ORDER BY c.name
    """).fetchall()
    grouped = {"channel": [], "dm": [], "group": [], "mpim": []}
    for ch in channels:
        grouped.setdefault(ch["type"], []).append(
            {"id": ch["id"], "name": ch["name"], "msg_count": ch["msg_count"]}
        )
    return jsonify(grouped)


@app.route("/channel/<channel_id>")
@app.route("/channel/<channel_id>/<date>")
def channel_view(channel_id, date=None):
    db = get_db()
    channel = db.execute("SELECT * FROM channels WHERE id = ?", (channel_id,)).fetchone()
    if not channel:
        abort(404)

    # Get available dates for this channel
    dates = db.execute("""
        SELECT DISTINCT date(datetime(CAST(ts AS FLOAT), 'unixepoch', 'localtime')) as d
        FROM messages WHERE channel_id = ? AND (thread_ts IS NULL OR thread_ts = ts)
        ORDER BY d
    """, (channel_id,)).fetchall()
    date_list = [r["d"] for r in dates]

    if not date_list:
        return render_template("channel.html", channel=channel, messages=[], dates=date_list,
                               current_date=None, prev_date=None, next_date=None)

    if date is None:
        date = date_list[-1]  # most recent

    # Navigation
    try:
        idx = date_list.index(date)
    except ValueError:
        idx = len(date_list) - 1
        date = date_list[idx]
    prev_date = date_list[idx - 1] if idx > 0 else None
    next_date = date_list[idx + 1] if idx < len(date_list) - 1 else None

    # Messages for this date (top-level only: no thread_ts, or thread_ts == ts)
    messages = db.execute("""
        SELECT * FROM messages
        WHERE channel_id = ? AND date(datetime(CAST(ts AS FLOAT), 'unixepoch', 'localtime')) = ?
          AND (thread_ts IS NULL OR thread_ts = ts)
        ORDER BY CAST(ts AS FLOAT)
    """, (channel_id, date)).fetchall()

    return render_template("channel.html", channel=channel, messages=messages,
                           dates=date_list, current_date=date,
                           prev_date=prev_date, next_date=next_date)


@app.route("/thread/<channel_id>/<thread_ts>")
def thread_view(channel_id, thread_ts):
    db = get_db()
    channel = db.execute("SELECT * FROM channels WHERE id = ?", (channel_id,)).fetchone()
    if not channel:
        abort(404)

    # Get parent message
    parent = db.execute("""
        SELECT * FROM messages WHERE channel_id = ? AND ts = ?
    """, (channel_id, thread_ts)).fetchone()

    # Get replies
    replies = db.execute("""
        SELECT * FROM messages
        WHERE channel_id = ? AND thread_ts = ? AND ts != ?
        ORDER BY CAST(ts AS FLOAT)
    """, (channel_id, thread_ts, thread_ts)).fetchall()

    return render_template("thread.html", channel=channel, parent=parent, replies=replies)


@app.route("/search")
def search():
    q = request.args.get("q", "").strip()
    search_type = request.args.get("type", "all")
    page = int(request.args.get("page", 1))
    per_page = 50

    if not q:
        return render_template("search.html", q=q, search_type=search_type,
                               messages=[], files=[], msg_count=0, file_count=0, page=page, per_page=per_page)

    db = get_db()

    # Prepare FTS query - strip special chars that break FTS5, add * for prefix matching
    def sanitize_fts(term):
        # Remove FTS5 special characters, keep alphanumeric and underscores
        cleaned = re.sub(r'[^\w\s]', ' ', term)
        words = [w for w in cleaned.split() if w]
        if not words:
            # Fall back to LIKE query if nothing left after sanitizing
            return None
        return " ".join(w + "*" for w in words)

    fts_q = sanitize_fts(q)

    # If query is only special chars (like ".mp3"), use LIKE fallback
    if fts_q is None:
        like_q = f"%{q}%"
        messages = []
        files = []
        msg_count = 0
        file_count = 0
        if search_type in ("all", "messages"):
            msg_count = db.execute(
                "SELECT COUNT(*) as c FROM messages WHERE text LIKE ?", (like_q,)
            ).fetchone()["c"]
            messages = db.execute("""
                SELECT m.*, m.text as snippet, c.name as channel_name, c.type as channel_type
                FROM messages m JOIN channels c ON m.channel_id = c.id
                WHERE m.text LIKE ? ORDER BY CAST(m.ts AS FLOAT) DESC LIMIT ? OFFSET ?
            """, (like_q, per_page, (page - 1) * per_page)).fetchall()
        if search_type in ("all", "files"):
            file_count = db.execute(
                "SELECT COUNT(*) as c FROM files WHERE name LIKE ? OR context_text LIKE ?", (like_q, like_q)
            ).fetchone()["c"]
            files = db.execute("""
                SELECT f.*, f.name as snippet, c.name as channel_name
                FROM files f JOIN channels c ON f.channel_id = c.id
                WHERE f.name LIKE ? OR f.context_text LIKE ? LIMIT ? OFFSET ?
            """, (like_q, like_q, per_page, (page - 1) * per_page)).fetchall()
        return render_template("search.html", q=q, search_type=search_type,
                               messages=messages, files=files,
                               msg_count=msg_count, file_count=file_count,
                               page=page, per_page=per_page)

    messages = []
    files = []
    msg_count = 0
    file_count = 0

    if search_type in ("all", "messages"):
        msg_count = db.execute(
            "SELECT COUNT(*) as c FROM messages_fts WHERE messages_fts MATCH ?", (fts_q,)
        ).fetchone()["c"]
        messages = db.execute("""
            SELECT m.*, snippet(messages_fts, 0, '<mark>', '</mark>', '...', 48) as snippet,
                   c.name as channel_name, c.type as channel_type
            FROM messages_fts
            JOIN messages m ON messages_fts.rowid = m.rowid
            JOIN channels c ON m.channel_id = c.id
            WHERE messages_fts MATCH ?
            ORDER BY rank
            LIMIT ? OFFSET ?
        """, (fts_q, per_page, (page - 1) * per_page)).fetchall()

    if search_type in ("all", "files"):
        file_count = db.execute(
            "SELECT COUNT(*) as c FROM files_fts WHERE files_fts MATCH ?", (fts_q,)
        ).fetchone()["c"]
        files = db.execute("""
            SELECT f.*, snippet(files_fts, 0, '<mark>', '</mark>', '...', 48) as snippet,
                   c.name as channel_name
            FROM files_fts
            JOIN files f ON files_fts.rowid = f.rowid
            JOIN channels c ON f.channel_id = c.id
            WHERE files_fts MATCH ?
            ORDER BY rank
            LIMIT ? OFFSET ?
        """, (fts_q, per_page, (page - 1) * per_page)).fetchall()

    return render_template("search.html", q=q, search_type=search_type,
                           messages=messages, files=files,
                           msg_count=msg_count, file_count=file_count,
                           page=page, per_page=per_page)


@app.route("/file/<path:filepath>")
def serve_file(filepath):
    """Serve a local attachment file."""
    full = BASE_DIR / filepath
    if not full.exists():
        abort(404)
    return send_from_directory(str(full.parent), full.name)


# ---------------------------------------------------------------------------
# Indexer
# ---------------------------------------------------------------------------

def build_index():
    """Parse all Slack export JSON and build SQLite database with FTS5."""
    import time
    start = time.time()

    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA cache_size=-64000")

    # --- Schema ---
    conn.executescript("""
        CREATE TABLE users (
            id TEXT PRIMARY KEY,
            name TEXT,
            display_name TEXT,
            real_name TEXT,
            avatar_url TEXT,
            color TEXT,
            is_bot INTEGER DEFAULT 0
        );

        CREATE TABLE channels (
            id TEXT PRIMARY KEY,
            name TEXT,
            type TEXT,  -- 'channel', 'dm', 'group', 'mpim'
            is_archived INTEGER DEFAULT 0,
            topic TEXT,
            purpose TEXT,
            dir_name TEXT  -- directory name in export
        );

        CREATE TABLE messages (
            rowid INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT,
            ts TEXT,
            user_id TEXT,
            text TEXT,
            subtype TEXT,
            thread_ts TEXT,
            reply_count INTEGER DEFAULT 0,
            reactions_json TEXT,
            files_json TEXT,
            raw_json TEXT
        );

        CREATE TABLE files (
            rowid INTEGER PRIMARY KEY AUTOINCREMENT,
            id TEXT,
            channel_id TEXT,
            message_ts TEXT,
            name TEXT,
            title TEXT,
            mimetype TEXT,
            filetype TEXT,
            size INTEGER,
            local_path TEXT,
            context_text TEXT  -- message text + surrounding messages for contextual search
        );

        CREATE INDEX idx_messages_channel_ts ON messages(channel_id, ts);
        CREATE INDEX idx_messages_thread ON messages(channel_id, thread_ts);
        CREATE INDEX idx_files_channel ON files(channel_id);
    """)

    # --- Load users ---
    users_path = BASE_DIR / "users.json"
    users = {}
    if users_path.exists():
        with open(users_path) as f:
            for u in json.load(f):
                uid = u["id"]
                profile = u.get("profile", {})
                display_name = profile.get("display_name") or ""
                real_name = profile.get("real_name") or u.get("real_name", "")
                avatar_url = profile.get("image_72") or profile.get("image_48") or ""
                color = u.get("color", "999999")
                is_bot = 1 if u.get("is_bot") else 0
                users[uid] = {
                    "id": uid, "name": u.get("name", ""),
                    "display_name": display_name, "real_name": real_name,
                    "avatar_url": avatar_url, "color": color, "is_bot": is_bot,
                }
                conn.execute(
                    "INSERT OR REPLACE INTO users VALUES (?,?,?,?,?,?,?)",
                    (uid, u.get("name", ""), display_name, real_name, avatar_url, color, is_bot)
                )
    # Add USLACKBOT
    if "USLACKBOT" not in users:
        conn.execute(
            "INSERT OR REPLACE INTO users VALUES (?,?,?,?,?,?,?)",
            ("USLACKBOT", "slackbot", "Slackbot", "Slackbot", "", "999999", 1)
        )
        users["USLACKBOT"] = {"id": "USLACKBOT", "display_name": "Slackbot", "real_name": "Slackbot"}

    conn.commit()
    print(f"  Loaded {len(users)} users")

    # --- Load channels ---
    channel_map = {}  # id -> {name, dir_name, type}

    # Regular channels
    channels_path = BASE_DIR / "channels.json"
    if channels_path.exists():
        with open(channels_path) as f:
            for ch in json.load(f):
                cid = ch["id"]
                name = ch["name"]
                channel_map[cid] = {"name": name, "dir_name": name, "type": "channel",
                                    "is_archived": ch.get("is_archived", False),
                                    "topic": (ch.get("topic") or {}).get("value", ""),
                                    "purpose": (ch.get("purpose") or {}).get("value", "")}

    # DMs
    dms_path = BASE_DIR / "dms.json"
    if dms_path.exists():
        with open(dms_path) as f:
            for dm in json.load(f):
                cid = dm["id"]
                members = dm.get("members", [])
                # Name DM by the other person(s)
                names = []
                for mid in members:
                    u = users.get(mid, {})
                    names.append(u.get("display_name") or u.get("real_name") or mid)
                name = " & ".join(names) if names else cid
                channel_map[cid] = {"name": name, "dir_name": cid, "type": "dm",
                                    "is_archived": False, "topic": "", "purpose": ""}

    # Groups (private channels)
    groups_path = BASE_DIR / "groups.json"
    if groups_path.exists():
        with open(groups_path) as f:
            for gr in json.load(f):
                cid = gr["id"]
                name = gr.get("name", cid)
                channel_map[cid] = {"name": name, "dir_name": name, "type": "group",
                                    "is_archived": gr.get("is_archived", False),
                                    "topic": (gr.get("topic") or {}).get("value", ""),
                                    "purpose": (gr.get("purpose") or {}).get("value", "")}

    # MPIMs (multi-party IMs)
    mpims_path = BASE_DIR / "mpims.json"
    if mpims_path.exists():
        with open(mpims_path) as f:
            data = json.load(f)
            if isinstance(data, list):
                for mp in data:
                    cid = mp["id"]
                    name = mp.get("name", cid)
                    channel_map[cid] = {"name": name, "dir_name": name, "type": "mpim",
                                        "is_archived": mp.get("is_archived", False),
                                        "topic": (mp.get("topic") or {}).get("value", ""),
                                        "purpose": (mp.get("purpose") or {}).get("value", "")}

    # Check for directories that aren't in any JSON (DM dirs start with D)
    for entry in BASE_DIR.iterdir():
        if entry.is_dir() and entry.name != "templates" and entry.name != "__pycache__":
            # Check if this directory is already mapped by dir_name
            known_dirs = {v["dir_name"] for v in channel_map.values()}
            if entry.name not in known_dirs:
                # Check if it has JSON message files
                has_messages = any(f.suffix == ".json" and re.match(r"\d{4}-\d{2}-\d{2}", f.stem)
                                   for f in entry.iterdir() if f.is_file())
                if has_messages:
                    # Might be a DM dir by ID
                    cid = entry.name
                    if cid.startswith("D"):
                        channel_map[cid] = {"name": cid, "dir_name": cid, "type": "dm",
                                            "is_archived": False, "topic": "", "purpose": ""}
                    else:
                        channel_map[cid] = {"name": cid, "dir_name": cid, "type": "channel",
                                            "is_archived": False, "topic": "", "purpose": ""}

    for cid, ch in channel_map.items():
        conn.execute(
            "INSERT OR REPLACE INTO channels VALUES (?,?,?,?,?,?,?)",
            (cid, ch["name"], ch["type"], 1 if ch["is_archived"] else 0,
             ch["topic"], ch["purpose"], ch["dir_name"])
        )
    conn.commit()
    print(f"  Loaded {len(channel_map)} channels")

    # --- Load messages ---
    msg_count = 0
    file_count = 0

    for cid, ch in channel_map.items():
        ch_dir = BASE_DIR / ch["dir_name"]
        if not ch_dir.is_dir():
            continue

        # Collect all messages sorted by date for context window
        all_msgs_in_channel = []

        date_files = sorted(ch_dir.glob("????-??-??.json"))
        for date_file in date_files:
            try:
                with open(date_file) as f:
                    msgs = json.load(f)
            except (json.JSONDecodeError, IOError):
                continue

            for msg in msgs:
                if msg.get("type") != "message":
                    continue

                ts = msg.get("ts", "")
                user_id = msg.get("user", msg.get("bot_id", ""))
                text = msg.get("text", "")
                subtype = msg.get("subtype")
                thread_ts = msg.get("thread_ts")
                reply_count = msg.get("reply_count", 0)

                reactions = msg.get("reactions")
                reactions_json = json.dumps(reactions) if reactions else None

                files = msg.get("files")
                files_json = json.dumps(files) if files else None

                conn.execute(
                    "INSERT INTO messages (channel_id, ts, user_id, text, subtype, thread_ts, reply_count, reactions_json, files_json, raw_json) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (cid, ts, user_id, text, subtype, thread_ts, reply_count,
                     reactions_json, files_json, json.dumps(msg))
                )
                msg_count += 1

                all_msgs_in_channel.append({"ts": ts, "text": text, "files": files})

                # Index files
                if files:
                    for fi in files:
                        if fi.get("mode") == "tombstone":
                            continue
                        fid = fi.get("id", "")
                        local_path = fi.get("local_path", "")
                        if local_path:
                            local_path = ch["dir_name"] + "/" + local_path

                        conn.execute(
                            "INSERT INTO files (id, channel_id, message_ts, name, title, mimetype, filetype, size, local_path, context_text) VALUES (?,?,?,?,?,?,?,?,?,?)",
                            (fid, cid, ts,
                             fi.get("name", ""), fi.get("title", ""),
                             fi.get("mimetype", ""), fi.get("filetype", ""),
                             fi.get("size", 0), local_path, "")
                        )
                        file_count += 1

        # Now build contextual text for files
        # For each file, grab its message text + 3 before + 3 after
        texts = [(m["ts"], m["text"], m.get("files")) for m in all_msgs_in_channel]
        for i, (ts, text, files) in enumerate(texts):
            if files:
                context_parts = []
                for j in range(max(0, i - 3), min(len(texts), i + 4)):
                    if texts[j][1]:
                        context_parts.append(texts[j][1])
                context = " ".join(context_parts)

                # Update all files for this message
                for fi in files:
                    if fi.get("mode") == "tombstone":
                        continue
                    fid = fi.get("id", "")
                    fname = fi.get("name", "")
                    ftitle = fi.get("title", "")
                    full_context = f"{fname} {ftitle} {context}"
                    conn.execute(
                        "UPDATE files SET context_text = ? WHERE id = ? AND channel_id = ?",
                        (full_context, fid, cid)
                    )

        if msg_count % 10000 == 0:
            conn.commit()

    conn.commit()
    print(f"  Loaded {msg_count} messages, {file_count} files")

    # --- Build FTS5 indexes ---
    print("  Building full-text search indexes...")
    conn.executescript("""
        CREATE VIRTUAL TABLE messages_fts USING fts5(text, content=messages, content_rowid=rowid);
        INSERT INTO messages_fts(rowid, text) SELECT rowid, text FROM messages;

        CREATE VIRTUAL TABLE files_fts USING fts5(context_text, content=files, content_rowid=rowid);
        INSERT INTO files_fts(rowid, context_text) SELECT rowid, context_text FROM files;
    """)
    conn.commit()

    elapsed = time.time() - start
    print(f"  Done! Indexed in {elapsed:.1f}s. Database: {DB_PATH}")
    conn.close()


# ---------------------------------------------------------------------------
# Template helpers exposed to Jinja
# ---------------------------------------------------------------------------

@app.context_processor
def inject_helpers():
    def file_exists(path):
        return (BASE_DIR / path).exists() if path else False

    def is_image(mimetype):
        return mimetype and mimetype.startswith("image/")

    def is_video(mimetype):
        return mimetype and mimetype.startswith("video/")

    def is_audio(mimetype):
        return mimetype and mimetype.startswith("audio/")

    def reaction_emoji(name):
        uni = emoji_to_unicode(name)
        return uni if uni else f":{name}:"

    return dict(file_exists=file_exists, is_image=is_image, is_video=is_video,
                is_audio=is_audio, reaction_emoji=reaction_emoji, json_loads=json.loads)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if "--index" in sys.argv:
        print("Building index...")
        build_index()
    else:
        if not DB_PATH.exists():
            print("Database not found. Run with --index first:")
            print(f"  python3 {sys.argv[0]} --index")
            sys.exit(1)
        port = int(sys.argv[sys.argv.index("--port") + 1]) if "--port" in sys.argv else 5001
        print(f"Starting viewer at http://localhost:{port}")
        app.run(debug=True, port=port)
