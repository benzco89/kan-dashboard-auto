"""
Weekly Deck Generator - סיכום סושיאל שבועי כמצגת 16:9 ל-PDF.

הזרימה מפוצלת לשלושה שלבים, כדי שהשכבה העריכתית תהיה בידיים אנושיות:

    extract  ->  weekly_deck/out/deck_content.json  (+ out/thumbs/*.jpg)
                 ^ עורכים את הקובץ הזה ביד (טקסט ומספרים בלבד)
    render   ->  weekly_deck/out/deck.html + deck.pdf (+ צילומי QA)

    python weekly_deck/generate_deck.py --extract   # שיטס + תמונות + חישובים
    python weekly_deck/generate_deck.py --render    # רק רינדור מה-JSON (אופליין)
    python weekly_deck/generate_deck.py             # extract ואז render
    python weekly_deck/generate_deck.py --mock      # נתוני דמה, בלי creds/רשת

ברירת המחדל אינה קוראת ל-Gemini כלל: כל המספרים והטקסטים נגזרים דטרמיניסטית
ומיועדים לעריכה ידנית ב-deck_content.json. עם --gemini אפשר לבקש ניסוח אוטומטי.
אינו כותב לשום גיליון ואינו נוגע בקולקטורים. העיצוב קבוע
(weekly_deck/design/weekly-social-light.dc.html); הקוד מזין אותו בנתונים בלבד.
"""

import os
import sys
import io
import re
import json
import base64
import argparse
from datetime import datetime, timedelta

import pytz

# Windows console encoding (כמו בשאר הסקריפטים)
try:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- Config ---
SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
IL_TZ = pytz.timezone("Asia/Jerusalem")
FB_API_VERSION = "v24.0"
FOLLOWERS_SHEET = "מעקב עוקבים"

HERE = os.path.dirname(os.path.abspath(__file__))
# הסקריפט יושב בתת-תיקייה; ה-utils של הריפו (http_get_json) יושב בשורש
sys.path.insert(0, os.path.dirname(HERE))
FONTS_DIR = os.path.join(HERE, "design", "fonts")
ASSETS_DIR = os.path.join(HERE, "design", "assets")
OUT_DIR = os.path.join(HERE, "out")
THUMBS_DIR = os.path.join(OUT_DIR, "thumbs")
CONTENT_PATH = os.path.join(OUT_DIR, "deck_content.json")
REPORTERS_MAP_PATH = os.path.join(HERE, "reporters_map.json")
REPORTERS_OVERRIDES_PATH = os.path.join(HERE, "reporters_overrides.json")
PROGRAMS_MAP_PATH = os.path.join(HERE, "programs_map.json")
TODO_PATH = os.path.join(OUT_DIR, "reporters_todo.txt")
TEMPLATE = "template.html.j2"

HEB_MONTHS = ['', 'ינואר', 'פברואר', 'מרץ', 'אפריל', 'מאי', 'יוני', 'יולי',
              'אוגוסט', 'ספטמבר', 'אוקטובר', 'נובמבר', 'דצמבר']

GREEN = "#3FB950"
RED = "#F7381B"

# SVG icons lifted verbatim from the design (rendered at any size).
_ICON_INNER = {
    'youtube': '<rect x="1" y="4.5" width="22" height="15" rx="4.5" fill="#FF0000"/><path d="M10 8.5l6 3.5-6 3.5z" fill="#fff"/>',
    'tiktok': '<rect width="24" height="24" rx="6" fill="#111"/><path d="M14 4c.3 1.9 1.5 3.1 3.4 3.3v2.4c-1.1 0-2.2-.3-3.1-.9v4.7c0 2.6-2 4.5-4.5 4.5S5.3 20.1 5.3 17.6c0-2.3 1.7-4.2 4-4.4v2.5c-.9.2-1.6 1-1.6 1.9 0 1.1.9 2 2 2s2-.9 2-2V4z" fill="#FBBF24"/>',
    'instagram': '<rect x="3" y="3" width="18" height="18" rx="5.4"/><circle cx="12" cy="12" r="4"/><circle cx="17.4" cy="6.6" r="1.1" fill="#E4405F" stroke="none"/>',
    'facebook': '<rect width="24" height="24" rx="6" fill="#1877F2"/><path d="M15.5 20v-6.4h2.1l.32-2.5h-2.42V9.5c0-.72.2-1.2 1.24-1.2h1.32V6.06c-.64-.08-1.36-.12-2.06-.12-2.02 0-3.4 1.24-3.4 3.5v1.96H10.1v2.5h2.14V20z" fill="#fff"/>',
    'x': '<rect width="24" height="24" rx="6" fill="#000" stroke="#2c2f36" stroke-width="1"/><path d="M6.3 6h2.7l3.1 4.2L15.6 6h2.1l-4.3 5.7L18 18h-2.7l-3.3-4.5L8.4 18H6.3l4.5-6z" fill="#fff"/>',
}


def icon_svg(key, size):
    if key == 'instagram':
        return (f'<svg width="{size}" height="{size}" viewBox="0 0 24 24" fill="none" '
                f'stroke="#E4405F" stroke-width="1.9">{_ICON_INNER[key]}</svg>')
    fill = ' fill="none"' if key == 'youtube' else ''
    return f'<svg width="{size}" height="{size}" viewBox="0 0 24 24"{fill}>{_ICON_INNER[key]}</svg>'


PLATFORMS = {
    'youtube':   dict(name='יוטיוב', sheet='נתוני יוטיוב', date_col='published_at',
                      title_col='title', id_col='video_id', follower_col='yt_subscribers',
                      colors=dict(accent='#E11900', bar='#E11900', tint='255,0,0')),
    'tiktok':    dict(name='טיקטוק', sheet='נתוני טיקטוק', date_col='date',
                      title_col='title', id_col='video_id', follower_col='tt_followers',
                      colors=dict(accent='#B45309', bar='#B45309', tint='251,191,36')),
    'instagram': dict(name='אינסטגרם', sheet='נתוני אינסטגרם', date_col='date',
                      title_col='caption', id_col='media_id', follower_col='ig_followers',
                      colors=dict(accent='#E4405F', bar='#E4405F', tint='228,64,95')),
    'facebook':  dict(name='פייסבוק', sheet='נתוני פייסבוק', date_col='date',
                      title_col='title', id_col='post_id', follower_col='fb_followers',
                      colors=dict(accent='#1877F2', bar='#1877F2', tint='24,119,242')),
    'x':         dict(name='X', sheet='נתוני טוויטר', date_col='date',
                      title_col='text', id_col='tweet_id', follower_col='tw_followers',
                      colors=dict(accent='#111', bar='#111', tint='17,17,17')),
}
PLATFORM_ORDER = ['youtube', 'tiktok', 'instagram', 'facebook', 'x']
# X highlight cards are text-forward: no cheap thumbnail source and the media is
# usually irrelevant, so the design shows the tweet instead of a grey box.
NO_THUMBS = {'x'}


# ---------------------------------------------------------------- formatting

def fmt_num(x):
    """2_900_000 -> '2.9M', 821500 -> '822K', 512 -> '512'. Design-style."""
    try:
        x = float(x)
    except (TypeError, ValueError):
        return "0"
    n = abs(x)
    if n >= 1_000_000:
        s = f"{x/1_000_000:.1f}M".replace(".0M", "M")
    elif n >= 1000:
        s = f"{round(x/1000)}K"
    else:
        s = str(int(round(x)))
    return s


def split_suffix(s):
    """'24.8M' -> ('24.8','M'); '+34%' -> ('+34','%'); '512' -> ('512','')."""
    s = str(s or "")
    i = len(s)
    while i > 0 and not (s[i-1].isdigit() or s[i-1] == '.'):
        i -= 1
    return s[:i], s[i:]


def build_delta(pct):
    """Weekly change presentation. pct None -> no trend shown."""
    if pct is None:
        return dict(has_delta=False, str_signed="—", str_abs="—",
                    arrow="", color="#808080")
    r = int(round(pct))
    up = r >= 0
    sign = "+" if up else "−"  # U+2212 minus, matches the design
    return dict(has_delta=True,
                str_signed=f"{sign}{abs(r)}%",
                str_abs=f"{abs(r)}%",
                arrow="▲" if up else "▼",
                color=GREEN if up else RED)


def fmt_date_range(d1, d2):
    if d1.month == d2.month:
        return f"{d1.day}–{d2.day} ב{HEB_MONTHS[d2.month]} {d2.year}"
    return (f"{d1.day} ב{HEB_MONTHS[d1.month]} – "
            f"{d2.day} ב{HEB_MONTHS[d2.month]} {d2.year}")


# RTL captions wrap handles in bidi controls ("‪@itamar.margalit‬"),
# which silently break handle matching. Stripped everywhere text is parsed.
_BIDI_RE = re.compile('[​-‏‪-‮⁦-⁩؜]')


def _strip_bidi(text):
    return _BIDI_RE.sub('', str(text or ''))


def clean_title(s, cap=100):
    s = _strip_bidi(s).replace("\n", " ").replace("\r", " ").strip()
    s = " ".join(s.split())
    return s[:cap] if len(s) > cap else s


def compute_window(today=None):
    """Last COMPLETE Israeli week: Sunday..Saturday (Asia/Jerusalem). Running on
    Tue 2026-07-21 -> 2026-07-12..2026-07-18; prior week (for deltas) =
    2026-07-05..2026-07-11. Running on a Sunday -> the just-finished Sun..Sat."""
    today = today or datetime.now(IL_TZ)
    days_since_sunday = (today.weekday() + 1) % 7   # Python: Mon=0..Sun=6
    this_week_sunday = today - timedelta(days=days_since_sunday)
    we = this_week_sunday - timedelta(days=1)        # last complete Saturday
    ws = we - timedelta(days=6)                      # its Sunday
    lwe = ws - timedelta(days=1)                     # prior Saturday
    lws = lwe - timedelta(days=6)                    # prior Sunday
    fmt = lambda d: d.strftime('%Y-%m-%d')
    return dict(this=(fmt(ws), fmt(we)), last=(fmt(lws), fmt(lwe)),
                d1=ws, d2=we)


# ---------------------------------------------------------------- sheets

def get_client():
    import gspread
    from google.oauth2.service_account import Credentials
    creds_json = os.environ.get("GCP_SERVICE_ACCOUNT") or os.environ.get("GOOGLE_CREDENTIALS")
    creds_dict = json.loads(creds_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)


def load_sheet(gc, name):
    import pandas as pd
    try:
        ws = gc.open_by_key(SPREADSHEET_ID).worksheet(name)
        return pd.DataFrame(ws.get_all_records())
    except Exception as e:
        print(f"   ⚠️ could not read '{name}': {e}")
        return pd.DataFrame()


def to_num(series):
    import pandas as pd
    return pd.to_numeric(series, errors='coerce').fillna(0)


# ---------------------------------------------------------------- thumbnails

RENDERABLE = {'image/jpeg', 'image/png', 'image/webp', 'image/gif'}
_EXT = {'image/jpeg': '.jpg', 'image/png': '.png', 'image/webp': '.webp', 'image/gif': '.gif'}
_MIME = {'.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png',
         '.webp': 'image/webp', '.gif': 'image/gif'}


def _to_renderable(content, ct):
    """דפדפן לא מרנדר heic/avif (ה-CDN של טיקטוק מגיש heic גם בכתובות .jpeg,
    לפי ה-content-type בפועל). ממירים ל-JPEG; אם אין ממיר - עדיף placeholder
    מעוצב מתמונה שבורה, אז מחזירים None."""
    if ct in RENDERABLE:
        return content, ct
    try:
        from pillow_heif import register_heif_opener
        register_heif_opener()
        from PIL import Image
        img = Image.open(io.BytesIO(content)).convert('RGB')
        buf = io.BytesIO()
        img.save(buf, format='JPEG', quality=85)
        return buf.getvalue(), 'image/jpeg'
    except Exception as e:
        print(f"      cannot convert {ct} to jpeg: {e}")
        return None, None


def _download_bytes(url, headers=None, timeout=15):
    """Fetch an image -> (bytes, content_type) in a browser-renderable format."""
    import requests
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.ok and r.content and r.headers.get('content-type', '').startswith('image'):
            ct = r.headers.get('content-type', 'image/jpeg').split(';')[0]
            if len(r.content) <= 4_000_000:
                content, ct = _to_renderable(r.content, ct)
                if content:
                    return content, ct
    except Exception as e:
        print(f"      thumb download failed ({url[:60]}...): {e}")
    return None


def save_thumb(platform, item_id, payload):
    """Write a fetched thumbnail into out/thumbs/. Returns the bare filename
    (deck_content.json references thumbnails by name only — never base64)."""
    if not payload:
        return None
    content, ct = payload
    safe = re.sub(r'[^A-Za-z0-9_-]', '_', str(item_id))[:60] or 'item'
    fname = f"{platform}_{safe}{_EXT.get(ct, '.jpg')}"
    os.makedirs(THUMBS_DIR, exist_ok=True)
    with open(os.path.join(THUMBS_DIR, fname), 'wb') as f:
        f.write(content)
    return fname


def thumb_data_uri(fname):
    """Read a cached thumbnail back as a data URI (render time, offline)."""
    if not fname:
        return None
    path = os.path.join(THUMBS_DIR, os.path.basename(fname))
    if not os.path.exists(path):
        return None
    mime = _MIME.get(os.path.splitext(path)[1].lower(), 'image/jpeg')
    with open(path, 'rb') as f:
        return f"data:{mime};base64," + base64.b64encode(f.read()).decode()


def thumb_youtube(vid):
    return _download_bytes(f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg")


def thumb_fb(pid, token):
    if not token:
        return None
    try:
        from utils import http_get_json
        data = http_get_json(f"https://graph.facebook.com/{FB_API_VERSION}/{pid}",
                             params={'fields': 'full_picture', 'access_token': token},
                             timeout=15, max_retries=2)
        url = data.get('full_picture')
        return _download_bytes(url) if url else None
    except Exception as e:
        print(f"      fb thumb failed: {e}")
        return None


def thumb_ig(mid, token):
    if not token:
        return None
    try:
        from utils import http_get_json
        data = http_get_json(f"https://graph.facebook.com/{FB_API_VERSION}/{mid}",
                             params={'fields': 'media_url,thumbnail_url', 'access_token': token},
                             timeout=15, max_retries=2)
        url = data.get('thumbnail_url') or data.get('media_url')
        return _download_bytes(url) if url else None
    except Exception as e:
        print(f"      ig thumb failed: {e}")
        return None


TIKTOK_SEC_UID = ("MS4wLjABAAAA3p5tyX2Z3cacCWU34-nHbK-dpVBO5Y6"
                  "IGvTj9xufL60rC6ItchtdzkEe-0frXJZX")


def tiktok_cover_map(wanted_ids, token, max_pages=8):
    """Map aweme_id -> (bytes, ct) cover via TikHub."""
    covers = {}
    if not token or not wanted_ids:
        return covers
    try:
        from utils import http_get_json
    except Exception:
        return covers
    wanted = set(str(x) for x in wanted_ids)
    cursor = 0
    headers = {"Authorization": f"Bearer {token}"}
    for _ in range(max_pages):
        if not wanted:
            break
        try:
            data = http_get_json(
                "https://api.tikhub.io/api/v1/tiktok/app/v3/fetch_user_post_videos",
                headers=headers,
                params={"sec_user_id": TIKTOK_SEC_UID, "max_cursor": cursor, "count": 20},
                timeout=15, max_retries=2)
        except Exception as e:
            print(f"      tiktok cover fetch failed: {e}")
            break
        payload = data.get("data") or {}
        vids = payload.get("aweme_list")
        if not isinstance(vids, list) or not vids:
            break
        for v in vids:
            aid = str(v.get("aweme_id", ""))
            if aid in wanted:
                vid = v.get("video") or {}
                # ה-CDN מגיש heic (שהדפדפן לא מרנדר) וגם jpeg - אוספים את כל
                # הווריאנטים מכל סוגי העטיפות ומעדיפים jpeg מאיזשהו סוג
                all_urls = []
                for k in ("cover", "origin_cover", "dynamic_cover"):
                    all_urls.extend((vid.get(k) or {}).get("url_list") or [])
                url = next((u for u in all_urls if '.jpeg' in u or '.jpg' in u),
                           all_urls[0] if all_urls else None)
                if url:
                    got = _download_bytes(url)
                    if got:
                        covers[aid] = got
                wanted.discard(aid)
        if not payload.get("has_more"):
            break
        cursor = payload.get("max_cursor", 0)
    return covers


# ---------------------------------------------------------------- reporters

# 2-3 Hebrew words, apostrophes/geresh/hyphen allowed inside a word.
_NAME_RE = re.compile(r"^[֐-׿]+(?:[ ׳״'\"\-][֐-׿]+){1,2}$")


# Roles that are credited like a reporter but are NOT the reporter.
_NOT_REPORTER = ('צילום', 'עריכה', 'עיבוד', 'גרפיקה', 'הפקה', 'תרגום', 'אנימציה')
# "כתב: X" / "כתבת: X" / "תחקיר: X" style credits.
_CREDIT_PREFIX_RE = re.compile(
    r"(?:כתב/ת|כתבת|כתבנו|כתב|תחקיר|דיווח)\s*:\s*([^\n,|)\]#]{2,40})")
# The possessive form, which carries NO colon and so was invisible to the rule
# above: "כתבתו של X", "בכתבתה של X", "מתוך תחקירו של X". This is how Kan credits
# in YouTube descriptions and at the end of reels — the single most common shape
# among items the extractor used to miss.
_CREDIT_POSSESSIVE_RE = re.compile(
    r"(?:כתבת[והםן]?|כתבה|תחקיר[והםן]?|דיווח[והםן]?|ראיון|ריאיון)\s+של\s+"
    r"([^\n,|)\]#]{2,40})")


_LATIN_FULLNAME_RE = re.compile(r"^[A-Z][A-Za-z'’-]+(?:\s+[A-Z][A-Za-z'’-]+){1,2}$")


def _looks_like_person(s):
    """_looks_like_name, but also accepts a 2-3 word capitalised Latin name."""
    s = (s or "").strip()
    if not s or any(ch.isdigit() for ch in s):
        return False
    if ':' in s or '@' in s:
        return False
    if any(w in s for w in _NOT_REPORTER):
        return False
    return bool(_NAME_RE.match(s)) or bool(_LATIN_FULLNAME_RE.match(s))


# Generic references that fit the shape of a name but are not a person. The
# possessive credit form makes these reachable — "כתבתו של הכתב שלנו" would
# otherwise resolve to "הכתב שלנו".
_GENERIC_REF = {'הכתב', 'הכתבת', 'הכתבים', 'שלנו', 'שלהם', 'המערכת', 'הצוות',
                'העיתון', 'הערוץ', 'התוכנית', 'הסוכנות', 'הכתבה'}


def _looks_like_name(s):
    s = (s or "").strip()
    if not s or any(ch.isdigit() for ch in s):
        return False
    if ':' in s or '@' in s:
        return False
    if any(w in s for w in _NOT_REPORTER):
        return False
    if any(w in _GENERIC_REF for w in s.split()):
        return False
    return bool(_NAME_RE.match(s))


# The account's own handles/name are never a reporter credit (YouTube
# descriptions sign off with @kan_news).
_BRAND_HANDLES = {'kan_news', 'kann_news', 'kannews', 'kan11', 'kan',
                  'כאן חדשות', 'כאן 11', 'כאן11', 'כאן'}


def _is_brand(s):
    return str(s or '').strip().lstrip('@').lower() in _BRAND_HANDLES


def reporter_fallback(text):
    """Deterministic reporter credit from the FULL caption. Kan credits at the
    end, so we look for: an explicit 'כתב: X' credit, a parenthesised person
    name anywhere (last one wins — that's where the credit sits), or an
    @handle. 'צילום:'/'עריכה:' are photographers/editors, not reporters.
    Never invents: returns '' when nothing credible is present."""
    text = str(text or "")

    for rx in (_CREDIT_PREFIX_RE, _CREDIT_POSSESSIVE_RE):
        for m in rx.finditer(text):
            cand = m.group(1).strip().strip('.,;')
            # The capture runs to the next comma/newline, which in the possessive
            # form ("תחקירה של גילי כהן ששודר אמש") keeps going past the name.
            # SHORTEST first: Hebrew bylines are two words almost without
            # exception, and three words would happily swallow the next one.
            words = cand.split()
            for n in (2, 3, len(words)):
                part = " ".join(words[:n]).strip('.,;')
                if part and _looks_like_name(part):
                    return part

    found = [m.group(1).strip() for m in re.finditer(r"\(([^)]{2,40})\)", text)]
    named = [c for c in found if _looks_like_name(c)]
    if named:
        return named[-1]

    for hm in re.finditer(r"@([A-Za-z0-9._]{2,30})", text):
        handle = hm.group(1).rstrip('._')      # not the sentence's full stop
        if len(handle) >= 2 and not _is_brand(handle):
            return "@" + handle
    return ""


# Headline stops: a sentence end (not a decimal point), the 👇 pointer Kan uses
# before a link, or a hard line break.
_HEADLINE_STOP_RE = re.compile(r"[.!?](?=\s|$)")
_HEADLINE_CUT_CHARS = ('👇', '⬇️', '⬇', '🔗')

# --- headline shortening ---------------------------------------------------
# An earlier version cut a headline at its first clause boundary, on the theory
# that Kan writes <hook>: <elaboration> and the hook is the headline. On real
# captions it removed the strongest half more often than not — "הטרנד החדש שכובש
# את חטיבות הביניים" without "בני נוער משקיעים עשרות אלפי דולרים בבורסה", or a
# family's quote dropped from the story it belongs to. In Kan's style the part
# before the colon is usually a TEASER and the news sits after it.
#
# So nothing is dropped any more. The headline is only capped to what the table
# column can show, and the cut is placed at the nearest clause or comma boundary
# so it never lands mid-phrase. Truncated headlines are listed after an extract
# and rewritten in the editorial pass, where a human can see which half matters.
_BOUNDARY_RE = re.compile(r"[,;]| [-–—] | \| ")
_QUOTE_CHARS = "\"“”״‘’'"


def trim_to_clause(s, cap):
    """Cap a headline at `cap` chars, breaking at the last clause boundary that
    still leaves a substantial headline. Never drops a leading or trailing part
    of the text: what fits, fits, and the rest is elided."""
    s = " ".join(str(s or "").split()).strip().rstrip(" -–—|:,")
    if len(s) <= cap:
        return s
    # last boundary comfortably inside the cap — a slightly short headline that
    # ends on a phrase reads far better than a full-width one cut mid-phrase
    best = -1
    for m in _BOUNDARY_RE.finditer(s):
        if m.start() > cap:
            break
        if m.start() >= cap * 0.55:
            best = m.start()
    if best != -1:
        # the ellipsis is not decoration: text WAS dropped, and without it a
        # headline that stops on a clause boundary reads as a complete thought
        return s[:best].rstrip(" -–—|,;") + "…"
    cut = s[:cap]
    sp = cut.rfind(" ")
    return (cut[:sp] if sp > cap * 0.6 else cut).rstrip(" -–—|,:;") + "…"


def headline_of(text, reporter="", cap=62):
    """The leading headline of a caption — what the table and cards show.
    Deliberately does NOT cut at ':' unconditionally — in Kan headlines the colon
    is often part of the headline ('פרסום ראשון: ...'); see trim_to_clause."""
    lines = [l for l in _strip_bidi(text).replace("\r", "\n").split("\n") if l.strip()]
    s = lines[0] if lines else ""
    for ch in _HEADLINE_CUT_CHARS:
        i = s.find(ch)
        if i != -1:
            s = s[:i]
    m = _HEADLINE_STOP_RE.search(s)
    if m:
        s = s[:m.start()]
    s = " ".join(s.split())
    # a trailing credit duplicates the כתב/ת column. Once the reporter is known
    # the handle it came from is noise in the headline — and RTL captions often
    # glue it straight onto the last word ("...הוביל לאש@hadasgrinberg"), so the
    # separator is optional. With no reporter the handle STAYS: it is the visible
    # hint that reporters_map is missing a line.
    if reporter:
        if s.endswith("(" + reporter + ")"):
            s = s[:-(len(reporter) + 2)].rstrip()
        s = re.sub(r"\s*@[A-Za-z0-9._]{2,30}[.,;:!?]*\s*$", "", s).rstrip()
        if s.endswith(reporter):
            s = s[:-len(reporter)].rstrip(" -–—|·,")
    return trim_to_clause(s, cap).strip()


def load_reporters_map():
    """{"@handle": "שם בעברית"} — repo-tracked, hand-maintained, meant to grow.
    Normalized to lowercase without the leading @, so lookups are
    case-insensitive and work whether or not the caption wrote the @."""
    try:
        with open(REPORTERS_MAP_PATH, encoding='utf-8') as f:
            data = json.load(f)
    except Exception:
        return {}
    out, derived = {}, {}
    for k, v in data.items():
        k = str(k).strip()
        if not v or k.startswith('_'):
            continue
        name = str(v)
        out[k.lstrip('@').lower()] = name
        if k.startswith('@'):
            for dk in _derived_name_keys(k):
                derived[dk] = name
    for k, v in derived.items():
        out.setdefault(k, v)          # an explicit entry always wins
    return out


# --- which programme a clip was cut from -----------------------------------
# Kan tags the source programme as a hashtag on the post: #כאןבשלוש, #בחציהיום,
# #מהדורתכאןחדשות. The tag has no spaces and Hebrew cannot be word-split
# deterministically, so — exactly like reporter handles — a hand-maintained map
# turns the tag into a readable name and an unmapped tag is REPORTED rather than
# guessed at. Topic hashtags (#מונטנגרו) must never become programmes.
_HASHTAG_RE = re.compile(r"#([\w֐-׿]{2,40})")
# the radio station signs its items on the news page; a station is not a
# programme, so it is only used when no programme tag is present
_RESHETB_RE = re.compile(r"כאן חדשות ב?רשת ב")
# an explicitly quoted programme name — quotes required, so it never fires on prose
_PROGRAM_PROSE_RE = re.compile(r'(?:ב?תוכנית|בפינה|במהדורת)\s*["״”]([^"״”\n]{2,30})["״”]')


def load_programs_map():
    """{"#כאןבשלוש": "כאן בשלוש"} — repo-tracked and meant to grow, like
    reporters_map. Keys are matched without the '#' and case-insensitively."""
    try:
        with open(PROGRAMS_MAP_PATH, encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"   ⚠️ could not read programs_map.json: {e}")
        return {}
    return {str(k).lstrip('#').lower(): str(v) for k, v in data.items()
            if v and not str(k).startswith('_')}


def resolve_program(text, pmap):
    """(name, source) for the programme a clip came from. ('', '') when the post
    carries no programme signal — most social-native items genuinely have none,
    and inventing one would be worse than an empty cell."""
    s = _strip_bidi(text)
    for m in _HASHTAG_RE.finditer(s):
        name = pmap.get(m.group(1).lower())
        if name:
            return name, 'hashtag'
    m = _PROGRAM_PROSE_RE.search(s)
    if m:
        return m.group(1).strip(), 'quoted'
    if _RESHETB_RE.search(s):
        return "רשת ב׳", 'station'
    return '', ''


def unmapped_hashtags(text, pmap):
    """Hashtags the map does not know — printed after an extract so a real
    programme tag is easy to spot and add (topic tags are simply ignored)."""
    return [t for t in _HASHTAG_RE.findall(_strip_bidi(text))
            if t.lower() not in pmap]


def load_reporter_overrides():
    """{"<platform>:<item id>": "שם הכתב/ת"} — hand-filled credits for items whose
    text carries no byline at all (most YouTube titles, agency posts, quotes).
    Unlike reporters_map, which teaches a rule, this pins ONE item — so it is
    applied on every --extract AND every --render: a credit filled in after the
    extract lands without re-fetching a thing, and a re-extract never loses it.
    An empty value means "checked, genuinely uncredited" and retires the item
    from the TODO list instead of leaving it to be re-checked every week."""
    try:
        with open(REPORTERS_OVERRIDES_PATH, encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"   ⚠️ could not read reporters_overrides.json: {e}")
        return {}
    return {str(k).strip(): str(v or '').strip()
            for k, v in data.items() if not str(k).startswith('_')}


# An editor filling credits by hand knows the programme too, and writes it where
# it is natural to write it: "אריה גולן (מתוך הבוקר הזה)". Splitting it back out
# here means the deck's programme column gets it even when the post carried no
# hashtag — and the credit column shows the name alone instead of a run-on.
_OVERRIDE_PROGRAM_RE = re.compile(
    r"\s*\(\s*(?:מתוך|מ)\s*(?:התוכנית\s*|תוכנית\s*)?([^)]{2,40})\)\s*$")


def split_override(value):
    """'אריה גולן (מתוך הבוקר הזה)' -> ('אריה גולן', 'הבוקר הזה')."""
    value = str(value or '').strip()
    m = _OVERRIDE_PROGRAM_RE.search(value)
    if not m:
        return value, ''
    return value[:m.start()].strip(), m.group(1).strip()


def apply_reporter_overrides(content, overrides=None):
    """Overwrite per-item credits from the overrides file. Returns (set, vetoed)."""
    overrides = load_reporter_overrides() if overrides is None else overrides
    if not overrides:
        return 0, 0
    filled = vetoed = 0
    for p in content.get('platforms', []):
        for it in p.get('top', []):
            key = "%s:%s" % (p.get('key', ''), it.get('id', ''))
            if key not in overrides:
                continue
            name, program = split_override(overrides[key])
            it['_override'] = True
            # a programme the editor states outranks one parsed from a hashtag
            if program:
                it['program'] = program
                it['_prog_src'] = 'override'
            if name != (it.get('reporter') or ''):
                it['reporter'] = name
                it['reporter_source'] = 'override' if name else 'override-none'
                filled += 1 if name else 0
                vetoed += 0 if name else 1
    return filled, vetoed


def _derived_name_keys(handle):
    """Byline spellings implied by a handle, so one map line covers both:
    '@ItayBlumental' -> 'itay blumental', '@moav_vardi' -> 'moav vardi'.
    An all-lowercase run-together handle ('@gilicohen10') cannot be split
    deterministically, so it gets no derived key — map that byline explicitly."""
    h = re.sub(r'\d+', ' ', handle.lstrip('@'))
    h = re.sub(r'[._-]+', ' ', h)
    h = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', h)          # camelCase
    h = re.sub(r'(?<=[A-Z])(?=[A-Z][a-z])', ' ', h)     # ACRONYMWord
    words = [w for w in h.split() if w]
    return [" ".join(words).lower()] if len(words) >= 2 else []


# --- bare trailing credits (Facebook: "... | https://bit.ly/x Vered Pelman") ---
_URL_RE = re.compile(r'(?:https?://|www\.)\S+', re.IGNORECASE)
_LATIN_NAME_W = r"[A-Z][A-Za-z'’\-]+"
_HEB_W = r"[֐-׿][֐-׿'\"׳״\-]*"
# Prose words that must never be mistaken for a name in a trailing run.
_HEB_STOP = {
    'של', 'את', 'על', 'עם', 'אל', 'גם', 'כבר', 'לא', 'כן', 'זה', 'זו', 'הוא', 'היא',
    'הם', 'הן', 'אחרי', 'לפני', 'בתוך', 'מתוך', 'בגלל', 'כדי', 'לאחר', 'בעקבות',
    'בלבד', 'עוד', 'ועוד', 'המלא', 'המלאה', 'בכתבה', 'לכתבה', 'הכתבה', 'לצפייה',
    'קישור', 'בתגובות', 'כאן', 'חדשות', 'בסרטון', 'הסרטון', 'צפו', 'האזינו', 'קראו',
    'פרטים', 'נוספים', 'ראיון', 'תיעוד', 'דיווח', 'משפטי', 'ייעוץ', 'בייעוץ',
    'מאת', 'עם', 'מפי', 'לפי',
}


_URL_MARK = '␟'          # stands in for a stripped link


def _strip_urls(text):
    """Replace links with a sentinel so a trailing name can be tested for a
    separator/link IMMEDIATELY before it, rather than anywhere in the caption."""
    s = _URL_RE.sub(' ' + _URL_MARK + ' ', str(text or ''))
    return re.sub(r'\s+', ' ', s).strip()


def _trailing_latin_names(s):
    """1-2 Latin person names at the very end. Latin words are structurally
    distinctive inside a Hebrew caption, so this is low risk."""
    m = re.search(r'((?:' + _LATIN_NAME_W + r'\s+){1,3}' + _LATIN_NAME_W + r')$', s)
    if not m:
        return []
    w = m.group(1).split()
    if _URL_MARK in m.group(1):
        return []
    if any(x.lower() in ('kan', 'news', 'photo', 'photos', 'editing', 'video') for x in w):
        return []
    if len(w) == 4:
        return [' '.join(w[:2]), ' '.join(w[2:])]
    if len(w) in (2, 3):
        return [' '.join(w[:2])]
    return []


def _trailing_hebrew_name(s, rmap):
    """A bare Hebrew name at the end. Every Hebrew caption ends in Hebrew words,
    so accepting one blindly would invent a credit on almost every post. We
    accept only when the run is a person we already know (reporters_map doubles
    as a roster) or when a separator/URL clearly set it apart from the prose."""
    m = re.search(r'((?:' + _HEB_W + r'\s+){1,3}' + _HEB_W + r')$', s)
    if not m:
        return None
    words = m.group(1).split()
    # a role word anywhere in the trailing run poisons it: "צילום מוטי מילרוד"
    # has a clean-looking 2-word tail but is a photographer credit
    if any(any(bad in w for bad in _NOT_REPORTER) for w in words):
        return None
    for n in (3, 2):
        if len(words) >= n:
            cand = ' '.join(words[-n:])
            if _looks_like_name(cand) and cand.lower() in rmap:
                return cand
    # otherwise only when a separator or a link sits DIRECTLY before the name -
    # a dash elsewhere in the caption must not unlock this rule
    lead = s[:m.start()].rstrip()
    if lead and lead[-1] in ('|–—·•' + _URL_MARK) and len(words) >= 2:
        cand = ' '.join(words[-2:])
        if _looks_like_name(cand) and not any(w in _HEB_STOP for w in cand.split()):
            return cand
    return None


# A camera/clapper emoji opens a media credit that runs to the end of the
# caption ("📸: אבי דישי, פלאש90"). Everything from the marker on is a
# photographer/agency, never the reporter, so it is cut before any search.
_MEDIA_TAIL_RE = re.compile(r'[\U0001F4F8\U0001F4F7\U0001F3A5\U0001F3AC].*$', re.S)

# A role phrase right after the name is the strongest byline signal Kan uses:
# "איציק זוארץ, כתב כאן11 בדרום" / "Ketty Dor, כתבת כאן חדשות".
_ROLE_WORDS = r'(?:כתב/ת|כתבת|כתבנו|כתבתנו|כתב|פרשנית|פרשן|עורכת|עורך)'
_ROLE_SUFFIX_HEB_RE = re.compile(
    r'((?:' + _HEB_W + r'\s+){1,2}' + _HEB_W + r')\s*[,،]\s*' + _ROLE_WORDS + r'\b')
_ROLE_SUFFIX_LATIN_RE = re.compile(
    r'((?:' + _LATIN_NAME_W + r'\s+){1,2}' + _LATIN_NAME_W + r')\s*[,،]\s*' + _ROLE_WORDS + r'\b')


def _strip_media_tail(text):
    return _MEDIA_TAIL_RE.sub(' ', str(text or ''))


def _role_suffix_name(text):
    """A name immediately followed by ', <role>' — outranks every guess."""
    for rx in (_ROLE_SUFFIX_HEB_RE, _ROLE_SUFFIX_LATIN_RE):
        for m in rx.finditer(text):
            words = m.group(1).split()
            while len(words) > 2 and words[0] in _HEB_STOP:
                words.pop(0)                      # "מאת רן כהן" -> "רן כהן"
            cand = " ".join(words)
            if _looks_like_person(cand):
                return cand
    return None


def resolve_reporter_detailed(text, rmap):
    """Brand-safe wrapper: the account's own handle is never a credit."""
    res = _resolve_detailed(text, rmap)
    return dict(name='', source='', others=[]) if _is_brand(res['name']) else res


def _resolve_detailed(text, rmap):
    """-> dict(name, source, others). `source` records HOW the credit was found
    so low-confidence guesses can be reviewed; `others` holds any additional
    trailing name (Kan usually lists reporter first, then photographer)."""
    text = _strip_bidi(_strip_media_tail(text))
    blank = dict(name="", source="", others=[])

    role = _role_suffix_name(text)
    if role:
        return dict(name=rmap.get(role.lower(), role), source='role-suffix', others=[])

    rep = reporter_fallback(text)
    if rep:
        if rep.startswith('@'):
            return dict(name=rmap.get(rep[1:].lower(), rep), source='handle', others=[])
        src = 'credit-prefix' if _CREDIT_PREFIX_RE.search(text) else 'parens'
        return dict(name=rep, source=src, others=[])

    # a known person named anywhere (handle, Latin name or Hebrew name in the map)
    for known, name in rmap.items():
        if re.search(r'(?<![A-Za-z0-9_@])' + re.escape(known) + r'(?![A-Za-z0-9_])',
                     text, re.IGNORECASE):
            return dict(name=name, source='map', others=[])

    # The bare-trailing-name rules below only fire at the very END of the text,
    # so anything Kan appends AFTER the credit hides it. Programme hashtags and
    # the radio sign-off are exactly that, and they are appended often enough to
    # cost real credits — so they come off first.
    tail = _strip_urls(text)
    tail = re.sub(r"(?:\s*(?:#[\w֐-׿]{2,40}|🎙️|\|))+\s*$", '', tail)
    tail = _RESHETB_RE.sub('', tail)
    tail = tail.rstrip('.,;:!?*•·–—-| \t')

    latin = _trailing_latin_names(tail)
    if latin:
        first = latin[0]
        return dict(name=rmap.get(first.lower(), first),
                    source='trailing-latin', others=latin[1:])

    heb = _trailing_hebrew_name(tail, rmap)
    if heb:
        return dict(name=rmap.get(heb.lower(), heb), source='trailing-hebrew', others=[])

    return blank


def resolve_reporter(text, rmap):
    """Name only — the widely used entry point."""
    return resolve_reporter_detailed(text, rmap)['name']


def youtube_descriptions(video_ids, api_key):
    """Batched YouTube Data API lookup (one call per 50 ids). Kan puts the
    reporter credit in the video DESCRIPTION, not the title. Best-effort: any
    failure returns {} and the extract carries on."""
    out = {}
    ids = [str(v) for v in video_ids if v]
    if not api_key or not ids:
        return out
    try:
        from utils import http_get_json
        for i in range(0, len(ids), 50):
            chunk = ids[i:i + 50]
            data = http_get_json("https://www.googleapis.com/youtube/v3/videos",
                                 params={'part': 'snippet', 'id': ','.join(chunk),
                                         'key': api_key},
                                 timeout=15, max_retries=2)
            for it in (data.get('items') or []):
                vid = it.get('id')
                desc = ((it.get('snippet') or {}).get('description') or '')
                if vid and desc:
                    out[vid] = desc
    except Exception as e:
        print(f"      youtube descriptions unavailable: {e}")
    return out


def graph_full_text(ids, token, field):
    """Full, untruncated caption for the deck's own items, straight from the
    Graph API. The daily collectors store a CUT copy — Facebook at 700 chars,
    Instagram at 500 — because a sheet cell is not an archive. Kan credits the
    reporter at the very END of the caption, so on a long post the credit is
    already gone before the deck ever reads the sheet. Re-fetching the ~10 items
    per platform that actually reach the deck costs one batched call a week and
    puts the credit back.

    Best-effort by design: no token, a failed call or a missing id just leaves
    that item on the sheet text it already had."""
    out = {}
    ids = [str(i) for i in ids if i]
    if not token or not ids:
        return out
    try:
        from utils import http_get_json
        for i in range(0, len(ids), 25):       # the Graph ?ids= batch limit is 50
            chunk = ids[i:i + 25]
            data = http_get_json(f"https://graph.facebook.com/{FB_API_VERSION}/",
                                 params={'ids': ','.join(chunk), 'fields': field,
                                         'access_token': token},
                                 timeout=20, max_retries=2)
            for oid, obj in (data or {}).items():
                text = (obj or {}).get(field) or ''
                if text:
                    out[str(oid)] = text
    except Exception as e:
        print(f"      full {field} unavailable: {e}")
    return out


def enrich_full_text(key, items, fb_token, yt_key, rmap, pmap):
    """Replace each item's sheet text with the full original where we can get it,
    and re-resolve everything that is parsed out of the text. Returns how many
    items gained a credit, so the extract can say whether the round trip paid."""
    full = {}
    if key == 'facebook':
        full = graph_full_text([i['id'] for i in items], fb_token, 'message')
    elif key == 'instagram':
        full = graph_full_text([i['id'] for i in items], fb_token, 'caption')
    elif key == 'youtube':
        # a YouTube "caption" is the video TITLE, which never carries a byline —
        # the credit lives in the description, so this is not a longer copy of
        # the same text but the only text that can hold a credit at all
        full = youtube_descriptions([i['id'] for i in items], yt_key)
    # TikTok and X captions are short enough that the 500-char store never cuts
    # them, so neither is worth a round trip.
    if not full:
        return 0, 0
    gained = grew = 0
    for it in items:
        text = full.get(it['id'])
        if not text:
            continue
        if key == 'youtube':
            # keep the title as the headline; the description is only searched
            it['_raw'] = (it.get('_raw') or '') + "\n" + text
        elif len(text) > len(it.get('_raw') or ''):
            it['_raw'] = text
            grew += 1
        else:
            continue
        if not it.get('reporter'):
            res = resolve_reporter_detailed(it['_raw'], rmap)
            if res['name']:
                it['reporter'] = res['name']
                it['reporter_source'] = res['source'] + '+full'
                it['_others'] = res['others']
                gained += 1
                # the headline drops the credit it came from, so it has to be
                # re-derived now that the credit is finally known
                it['title'] = headline_of(it['_raw'], res['name'])
        if not it.get('program'):
            name, src = resolve_program(it['_raw'], pmap)
            if name:
                it['program'] = name
                it['_prog_src'] = src + '+full'
        it['_tags'] = unmapped_hashtags(it['_raw'], pmap)
    return gained, grew


# ---------------------------------------------------------------- per platform

def engagement_series(key, df):
    """Percent engagement per row. Uses engagement_rate when present, else
    derives it from the interaction columns the platform actually has."""
    import pandas as pd
    if 'engagement_rate' in df.columns:
        er = to_num(df['engagement_rate'])
        if er.sum() > 0:
            return er
    views = to_num(df['views']).replace(0, pd.NA)
    inter = pd.Series(0, index=df.index, dtype='float64')
    for col in ('likes', 'comments', 'shares', 'retweets', 'replies', 'quotes', 'saved', 'saves'):
        if col in df.columns:
            inter = inter + to_num(df[col])
    return (inter / views * 100).fillna(0)


def _cand(label, value, suffix, text):
    return dict(label=label, value=str(value), suffix=suffix, text=text)


# Only for the TODO list an editor fills reporters from — the collectors already
# store a permalink for FB/IG/X, and YouTube/TikTok IDs build a URL on their own.
_ITEM_URL_TPL = {
    'youtube': 'https://youtu.be/{id}',
    # a Facebook post_id is already "<page>_<post>", which resolves as a path
    'facebook': 'https://www.facebook.com/{id}',
    'tiktok': 'https://www.tiktok.com/@%s/video/{id}' % os.environ.get('TIKTOK_USERNAME', 'kan_news'),
    'x': 'https://x.com/%s/status/{id}' % os.environ.get('TWITTER_USERNAME', 'kann_news'),
}


def _item_url(key, row, item_id):
    link = str(row.get('permalink', '') or '').strip()
    if link.startswith('http'):
        return link
    tpl = _ITEM_URL_TPL.get(key)
    return tpl.format(id=item_id) if (tpl and item_id) else ''


def fun_fact_candidates(key, df):
    """4-6 deterministic candidate facts per platform. candidates[0] is the
    default `chosen`; the editor can switch `chosen` or rewrite any text.

    All of them describe WHAT HAPPENED. None compares one content format against
    another: this deck goes to the whole newsroom, and a line like "X% of the
    views came from Reels" reads as an instruction to make more Reels — a
    news desk covers the news it has, and next week's editorial calls should not
    be argued from last week's format mix."""
    c = []
    try:
        v = to_num(df['views'])
        n = len(df)
        eng = engagement_series(key, df)
        med = float(v.median()) if n else 0.0

        if key == 'youtube':
            if 'likes' in df.columns:
                c.append(_cand('likes_total', fmt_num(to_num(df['likes']).sum()), '', 'לייקים על סרטוני יוטיוב השבוע'))
            if 'comments' in df.columns:
                c.append(_cand('comments_total', fmt_num(to_num(df['comments']).sum()), '', 'תגובות על סרטוני יוטיוב השבוע'))
        elif key == 'tiktok':
            if 'whatsapp_shares' in df.columns and 'shares' in df.columns and float(to_num(df['shares']).sum()) > 0:
                wa = float(to_num(df['whatsapp_shares']).sum()); sh = float(to_num(df['shares']).sum())
                c.append(_cand('whatsapp', int(round(wa / sh * 100)), '%', 'מהשיתופים בטיקטוק הופנו לוואטסאפ'))
            if 'saves' in df.columns:
                c.append(_cand('saves', fmt_num(to_num(df['saves']).sum()), '', 'שמירות על סרטוני טיקטוק השבוע'))
            if 'shares' in df.columns:
                c.append(_cand('shares', fmt_num(to_num(df['shares']).sum()), '', 'שיתופים של סרטוני טיקטוק השבוע'))
        elif key == 'instagram':
            if 'saved' in df.columns:
                c.append(_cand('saves', fmt_num(to_num(df['saved']).sum()), '', 'שמירות על תכני אינסטגרם השבוע'))
            if 'reach' in df.columns:
                c.append(_cand('reach', fmt_num(to_num(df['reach']).sum()), '', 'חשיפה לתכני אינסטגרם השבוע'))
        elif key == 'facebook':
            if 'reach' in df.columns:
                c.append(_cand('reach', fmt_num(to_num(df['reach']).sum()), '', 'סך החשיפה של פוסטי פייסבוק השבוע'))
            if 'shares' in df.columns:
                c.append(_cand('shares', fmt_num(to_num(df['shares']).sum()), '', 'שיתופים של פוסטי פייסבוק השבוע'))
            if 'comments' in df.columns:
                c.append(_cand('comments', fmt_num(to_num(df['comments']).sum()), '', 'תגובות על פוסטי פייסבוק השבוע'))
        elif key == 'x':
            cols = [c2 for c2 in ('likes', 'retweets', 'replies', 'quotes') if c2 in df.columns]
            if 'replies' in df.columns and cols and sum(float(to_num(df[c2]).sum()) for c2 in cols) > 0:
                total = sum(float(to_num(df[c2]).sum()) for c2 in cols)
                c.append(_cand('reply_share', int(round(float(to_num(df['replies']).sum()) / total * 100)), '%', 'מהמעורבות ב‑X הגיעה מתגובות'))
            if 'retweets' in df.columns:
                c.append(_cand('retweets', fmt_num(to_num(df['retweets']).sum()), '', 'ריטוויטים על ציוצי כאן השבוע'))
            if 'quotes' in df.columns:
                c.append(_cand('quotes', fmt_num(to_num(df['quotes']).sum()), '', 'ציטוטים (quote tweets) של כאן השבוע'))

        if n >= 3 and med > 0 and float(v.max()) / med >= 1.5:
            r = float(v.max()) / med
            c.append(_cand('overperformer', f"{r:.1f}", '×', f"הפריט המוביל השבוע עשה פי {r:.1f} צפיות מפוסט רגיל בפלטפורמה"))
        if len(eng) and float(eng.max()) > 0:
            e = float(eng.max())
            c.append(_cand('eng_leader', f"{e:.1f}", '%', f"שיא המעורבות השבוע — {e:.1f}% על פריט בודד"))
    except Exception as ex:
        print(f"      candidates failed for {key}: {ex}")
    return c


def _row_int(row, *cols):
    """First present/non-zero of `cols` as an int (X calls them retweets/replies)."""
    for c in cols:
        try:
            v = int(float(row.get(c, 0) or 0))
        except (TypeError, ValueError):
            v = 0
        if v:
            return v
    return 0


def _rate_median(df, views, *cols):
    """Median interactions-per-view over the week's items that have views."""
    for c in cols:
        if c in df.columns:
            col = to_num(df[c])
            if col.sum() > 0:
                r = col[views > 0] / views[views > 0]
                if len(r):
                    return round(float(r.median()), 6)
    return 0.0


def _median_of(df, *cols):
    """Median of the first column that exists and has data. 0.0 when none."""
    for c in cols:
        if c in df.columns:
            s = to_num(df[c])
            if s.sum() > 0:
                return round(float(s.median()), 2)
    return 0.0


def extract_platform(key, df_all, window, thumbs_enabled, fb_token, tikhub_token, rmap, pmap):
    """Plain-data content for one platform (no markup, no base64). Thumbnails are
    written to out/thumbs/ and referenced by filename."""
    meta = PLATFORMS[key]
    dc, tc, idc = meta['date_col'], meta['title_col'], meta['id_col']
    (ws, we), (lws, lwe) = window['this'], window['last']

    out = dict(key=key, name=meta['name'], weekly_views=0, delta_pct=None,
               followers=None, fun_fact=dict(chosen=None, candidates=[]), top=[])

    if df_all is None or df_all.empty or dc not in df_all.columns or 'views' not in df_all.columns:
        return out

    d = df_all.copy()
    d['_date'] = d[dc].astype(str).str.slice(0, 10)
    d['views'] = to_num(d['views'])

    this_df = d[(d['_date'] >= ws) & (d['_date'] <= we)]
    last_df = d[(d['_date'] >= lws) & (d['_date'] <= lwe)]

    this_views = float(this_df['views'].sum())
    last_views = float(last_df['views'].sum())
    out['weekly_views'] = int(this_views)
    out['delta_pct'] = round((this_views - last_views) / last_views * 100, 1) if last_views > 0 else None

    if this_df.empty:
        return out

    this_df = this_df.copy()
    this_df['_eng'] = engagement_series(key, this_df)
    this_df = this_df.sort_values('views', ascending=False)

    items = []
    for _, r in this_df.head(10).iterrows():
        # the credit lives at the END of the caption, so resolve on the FULL raw
        # text and only then cut down to a headline for display
        raw = str(r.get(tc, '') or '')
        res = resolve_reporter_detailed(raw, rmap)
        reporter = res['name']
        # the programme tag sits at the END of the caption with the credit, so
        # both are resolved on the FULL raw text, never on the stored 400-char cut
        program, prog_src = resolve_program(raw, pmap)
        items.append(dict(id=str(r.get(idc, '')),
                          title=headline_of(raw, reporter),
                          caption=clean_title(raw, 400),
                          reporter=reporter,
                          reporter_source=res['source'],
                          program=program,
                          url=_item_url(key, r, str(r.get(idc, ''))),
                          _others=res['others'], _raw=raw,
                          _prog_src=prog_src,
                          _tags=unmapped_hashtags(raw, pmap),
                          views=int(r['views']),
                          likes=_row_int(r, 'likes'),
                          comments=_row_int(r, 'comments', 'replies'),
                          shares=_row_int(r, 'shares', 'retweets'),
                          engagement=round(float(r['_eng']), 1),
                          thumb=None))

    # Second pass over the deck's own items against the ORIGINAL text, because
    # the sheet holds a truncated copy and Kan credits at the end of the caption.
    gained, grew = enrich_full_text(key, items, fb_token,
                                    os.environ.get('YOUTUBE_API_KEY', ''), rmap, pmap)
    if gained or grew:
        print(f"      {key}: full text recovered for {grew} items, "
              f"+{gained} credits ({sum(1 for i in items if i['reporter'])}/{len(items)} now)")

    # Typical RATES (interactions per view) for the week. Comparing absolute
    # counts would just re-flag the biggest posts - which the views column
    # already says - so the חריג column compares each item against a normal
    # post's rate instead.
    _v = to_num(this_df['views'])
    out['median_rates'] = dict(
        likes=_rate_median(this_df, _v, 'likes'),
        comments=_rate_median(this_df, _v, 'comments', 'replies'),
        shares=_rate_median(this_df, _v, 'shares', 'retweets'),
        engagement=round(float(this_df['_eng'].median()), 3) if len(this_df) else 0.0,
    )

    # thumbnails for all top-10 (X excluded by design)
    if thumbs_enabled and key not in NO_THUMBS:
        ids = [i['id'] for i in items]
        payloads = {}
        if key == 'youtube':
            payloads = {i: thumb_youtube(i) for i in ids}
        elif key == 'facebook':
            payloads = {i: thumb_fb(i, fb_token) for i in ids}
        elif key == 'instagram':
            payloads = {i: thumb_ig(i, fb_token) for i in ids}
        elif key == 'tiktok':
            payloads = tiktok_cover_map(ids, tikhub_token)
        got = 0
        for it in items:
            fname = save_thumb(key, it['id'], payloads.get(it['id']))
            it['thumb'] = fname
            got += 1 if fname else 0
        print(f"      {key}: {got}/{len(items)} thumbnails")

    cands = fun_fact_candidates(key, this_df)
    out['fun_fact'] = dict(chosen=(cands[0]['label'] if cands else None), candidates=cands)
    out['top'] = items
    return out


def followers_map(gc):
    out = {}
    if gc is None:
        return out
    df = load_sheet(gc, FOLLOWERS_SHEET)
    if df.empty or 'date' not in df.columns:
        return out
    df = df.sort_values('date')
    for key, meta in PLATFORMS.items():
        col = meta['follower_col']
        if col in df.columns:
            vals = to_num(df[col])
            nz = vals[vals > 0]
            if len(nz):
                out[key] = int(nz.iloc[-1])
    return out


# ---------------------------------------------------------------- learnings

def default_learnings(platforms):
    """Deterministic starting point for the 'מה קרה השבוע' slide — 3 cards the
    editor is expected to rewrite/extend (3-4 supported) in deck_content.json.

    Descriptive only. The deck reports the week to the newsroom; it does not tell
    it where to put its effort next week, so no card ends in advice ("שם כדאי
    למקד", "שווה בדיקה"). The editorial call belongs to the desk, not to a
    number that happened to move."""
    have = [p for p in platforms if p['weekly_views'] > 0]
    cards = []
    if not have:
        return [dict(icon='📊', title='שבוע ללא נתונים', number='', color='#111',
                     sentence='לא נאספו נתונים לשבוע זה.')]

    movers = [p for p in have if p.get('delta_pct') is not None]
    up = [p for p in movers if p['delta_pct'] > 0]
    if up:
        best = max(up, key=lambda p: p['delta_pct'])
        d = build_delta(best['delta_pct'])
        cards.append(dict(icon='🚀', number=d['str_signed'], color=GREEN,
                          title=f"{best['name']} בצמיחה החדה ביותר השבוע",
                          sentence=f"{best['name']} עלה ב{d['str_abs']} בצפיות מול השבוע שעבר."))
    else:
        top = max(have, key=lambda p: p['weekly_views'])
        cards.append(dict(icon='🏆', number=fmt_num(top['weekly_views']), color='#111',
                          title=f"{top['name']} מוביל את הצפיות השבוע",
                          sentence=f"{top['name']} ריכז את מרב הצפיות מכל הפלטפורמות."))

    best_item, best_p = None, None
    for p in have:
        if p['top'] and (best_item is None or p['top'][0]['views'] > best_item['views']):
            best_item, best_p = p['top'][0], p
    if best_item:
        cards.append(dict(icon='⚡', number=fmt_num(best_item['views']),
                          color=PLATFORMS[best_p['key']]['colors']['accent'],
                          title=clean_title(best_item['title'], 70),
                          sentence=f"הפריט הכי נצפה השבוע · {best_p['name']}"))

    down = [p for p in movers if p['delta_pct'] < 0]
    if down:
        worst = min(down, key=lambda p: p['delta_pct'])
        d = build_delta(worst['delta_pct'])
        cards.append(dict(icon='📉', number=d['str_signed'], color=RED,
                          title=f"{worst['name']} עם הירידה החדה ביותר",
                          sentence=f"{worst['name']} ירד ב{d['str_abs']} בצפיות מול השבוע שעבר."))
    else:
        engs = [(p, max((i['engagement'] for i in p['top']), default=0)) for p in have]
        engs = [e for e in engs if e[1] > 0]
        if engs:
            p, e = max(engs, key=lambda t: t[1])
            cards.append(dict(icon='💬', number=f"{e:.1f}%",
                              color=PLATFORMS[p['key']]['colors']['accent'],
                              title=f"{p['name']} עם המעורבות החזקה ביותר",
                              sentence='שיא המעורבות על פריט בודד מבין הפלטפורמות.'))
    return cards[:4]


# ---------------------------------------------------------------- extract

def build_deck_content(gc, thumbs_enabled, use_gemini=False):
    window = compute_window()
    fb_token = os.environ.get('FACEBOOK_TOKEN', '')
    tikhub_token = os.environ.get('TIKHUB_TOKEN', '')
    rmap = load_reporters_map()
    pmap = load_programs_map()

    plats = []
    for key in PLATFORM_ORDER:
        df = load_sheet(gc, PLATFORMS[key]['sheet']) if gc else None
        plats.append(extract_platform(key, df, window, thumbs_enabled,
                                      fb_token, tikhub_token, rmap, pmap))

    follows = followers_map(gc)
    for p in plats:
        p['followers'] = follows.get(p['key'])

    return assemble_content(plats, window, use_gemini)


def assemble_content(plats, window, use_gemini=False):
    apply_reporter_overrides(dict(platforms=plats))   # before reporters[] is built
    total_this = sum(p['weekly_views'] for p in plats)
    hero_delta = None
    # hero delta from the per-platform deltas' implied prior totals
    prior = 0.0
    known = False
    for p in plats:
        if p.get('delta_pct') is not None and p['delta_pct'] != -100:
            prior += p['weekly_views'] / (1 + p['delta_pct'] / 100.0)
            known = True
        else:
            prior += p['weekly_views']
    if known and prior > 0:
        hero_delta = round((total_this - prior) / prior * 100, 1)

    reporters, seen = [], set()
    for p in sorted(plats, key=lambda x: -x['weekly_views']):
        for it in p['top']:
            nm = (it.get('reporter') or '').strip()
            if nm and not nm.startswith('@') and nm not in seen:
                seen.add(nm)
                reporters.append(nm)

    content = {
        "_readme": ("קובץ עריכה. כל הטקסטים והמספרים כאן ניתנים לשינוי ידני ואז "
                    "מריצים --render בלבד. thumb = שם קובץ בתוך out/thumbs/. "
                    "fun_fact.chosen = label של אחד מ-candidates."),
        "window": {"start": window['this'][0], "end": window['this'][1],
                   "range_str": fmt_date_range(window['d1'], window['d2']),
                   "range_short": f"{window['d1']:%d/%m}–{window['d2']:%d/%m}"},
        "hero": {"total_views": int(total_this), "delta_pct": hero_delta},
        "platforms": plats,
        "learnings": default_learnings(plats),
        "story_of_the_week": None,
        "closing": {"reporters": reporters[:12],
                    "note": "מבוסס על דשבורד הסושיאל של כאן"},
    }
    if use_gemini:
        try:
            gemini_polish_content(content)
        except Exception as e:
            print(f"   ⚠️ Gemini polish failed, keeping computed text: {e}")
    return content


def report_reporters(content):
    """Print a reporter report at the end of --extract, then strip the internal
    fields. Unresolved items show the LAST 80 chars of the caption, because that
    is where Kan puts the credit - so it is obvious which posts are genuinely
    uncredited and which just need a new reporters_map entry."""
    items = [(pl['key'], i + 1, it) for pl in content['platforms']
             for i, it in enumerate(pl.get('top', []))]
    total = len(items)
    resolved = [t for t in items if (t[2].get('reporter') or '').strip()]
    by_src = {}
    for _, _, it in resolved:
        k = it.get('reporter_source') or '?'
        by_src[k] = by_src.get(k, 0) + 1

    bar = "-" * 62
    print("")
    print(bar)
    print("  REPORTER REPORT - %d/%d credited" % (len(resolved), total))
    if by_src:
        print("  by: " + " | ".join("%s %d" % (k, v)
                                    for k, v in sorted(by_src.items(), key=lambda x: -x[1])))

    low = [t for t in resolved
           if t[2].get('reporter_source') in ('trailing-latin', 'trailing-hebrew')]
    if low:
        print("")
        print("  ~ WORTH A GLANCE (%d) - guessed from a bare trailing name:" % len(low))
        for k, n, it in low:
            others = it.get('_others') or []
            extra = ("   (also seen: %s)" % ", ".join(others)) if others else ""
            print("    %-10s #%-3d %s%s" % (k, n, it['reporter'], extra))

    # Items with no credit ANYWHERE in the text. Most are genuinely uncredited
    # (agency copy, quotes, YouTube titles) rather than a parsing miss, so the
    # report stops guessing and hands over a paste-ready block for
    # reporters_overrides.json instead - headline + link per item, which is what
    # an editor actually needs to name the reporter.
    missing = [t for t in items
               if not (t[2].get('reporter') or '').strip() and not t[2].get('_override')]
    if missing:
        print("")
        print("  x NO CREDIT IN THE TEXT (%d) - fill these in reporters_overrides.json:" % len(missing))
        for k, n, it in missing:
            print("    %-10s #%-3d %s" % (k, n, clean_title(it.get('title', ''), 60)))
        sugg = reporter_suggestions(items)
        if sugg:
            print("     (%d of them have a same-story candidate to check — see the file)"
                  % len(sugg))
        print("")
        print("  -> paste-ready block written to %s" % TODO_PATH)
        write_reporters_todo(missing, sugg)
    print("")
    report_long_headlines(content)
    print("")
    report_programs(content)
    print(bar)
    print("")

    for _, _, it in items:          # internal only - never written to the JSON
        it.pop('_others', None)
        it.pop('_raw', None)
        it.pop('_override', None)
        it.pop('_prog_src', None)
        it.pop('_tags', None)


# Hebrew function words plus the brand's own name — present in every caption, so
# they say nothing about which story an item covers.
_MATCH_STOP = set((
    "של את על עם אל גם כבר לא כן זה זו הוא היא הם הן אחרי לפני בתוך מתוך בגלל "
    "כדי לאחר בעקבות עוד ועוד היה היו יש אין כל כמו רק אבל או אז מה מי כי אך אף "
    "אחד אחת שני שתי בין ללא כאן חדשות אשר כאשר כפי לפי אולם ואילו כמה"
).split())


def _story_tokens(item):
    """Content words of an item, spelling-normalised. Hebrew captions mix full
    and defective spelling for the same word (הייתה / היתה), which would
    otherwise stop the same story matching across two platforms."""
    text = _strip_bidi((item.get('caption') or item.get('title') or ''))
    text = re.sub(r"[^\w֐-׿ ]", " ", text)
    return {w.replace('יי', 'י').replace('וו', 'ו')
            for w in text.split() if len(w) > 2 and w not in _MATCH_STOP}


def reporter_suggestions(items, floor=0.35):
    """For each uncredited item, credited items covering what looks like the SAME
    story — Kan runs one report across all five platforms and often credits it on
    only some of them.

    Deliberately NOT auto-applied. On a real week this matched 4 of 11 uncredited
    items, and one of the four was wrong: two unrelated stories about Israeli
    teenagers abroad shared enough words to score 50%. Word overlap cannot tell
    "same story" from "same subject", so the deck offers the candidate with the
    matched headline next to it and lets a human see the mismatch in one glance.
    Returns {(platform, rank): [(score, platform, rank, item), ...]}.
    """
    cache = {(k, n): _story_tokens(it) for k, n, it in items}
    out = {}
    for k, n, it in items:
        if (it.get('reporter') or '').strip() or it.get('_override'):
            continue
        mine = cache[(k, n)]
        if not mine:
            continue
        hits = []
        for k2, n2, other in items:
            if (k2, n2) == (k, n) or not (other.get('reporter') or '').strip():
                continue
            theirs = cache[(k2, n2)]
            if not theirs:
                continue
            score = len(mine & theirs) / min(len(mine), len(theirs))
            if score >= floor:
                hits.append((score, k2, n2, other))
        if hits:
            hits.sort(reverse=True, key=lambda h: h[0])
            out[(k, n)] = hits[:3]
    return out


def report_long_headlines(content):
    """Headlines the cap had to elide. Nothing was thrown away — the text is
    still in `caption` — but these are the ones where a human should decide which
    half matters, so the editorial pass gets a list instead of hunting for them."""
    cut = [(pl['key'], n, it) for pl in content['platforms']
           for n, it in enumerate(pl.get('top', []), start=1)
           if (it.get('title') or '').endswith('…')]
    if not cut:
        return
    print("  HEADLINES CUT (%d) - rewrite these in deck_content.json if the "
          "important half is missing:" % len(cut))
    for k, n, it in cut:
        print("    %-10s #%-3d %s" % (k, n, it['title']))


def report_programs(content):
    """Programme-tag coverage, plus the hashtags the map does not know yet. A
    topic tag (#מונטנגרו) is noise here; a programme tag is one line to add."""
    items = [(pl['key'], it) for pl in content['platforms']
             for it in pl.get('top', [])]
    named = [it for _, it in items if (it.get('program') or '').strip()]
    by_src, seen_tags = {}, {}
    for _, it in items:
        if it.get('program'):
            k = it.get('_prog_src') or '?'
            by_src[k] = by_src.get(k, 0) + 1
        for t in (it.get('_tags') or []):
            seen_tags[t] = seen_tags.get(t, 0) + 1

    print("  PROGRAMME REPORT - %d/%d tagged" % (len(named), len(items)))
    if by_src:
        print("  by: " + " | ".join("%s %d" % kv for kv in
                                    sorted(by_src.items(), key=lambda x: -x[1])))
    if seen_tags:
        print("  unmapped hashtags (add the real programmes to programs_map.json):")
        for t, n in sorted(seen_tags.items(), key=lambda x: -x[1])[:15]:
            print("    #%-28s x%d" % (t, n))


def write_reporters_todo(missing, suggestions=None):
    """A file the editor can work through: headline + link per uncredited item,
    then the exact JSON lines to paste into reporters_overrides.json. Leaving a
    value as "" is a valid answer - it records "no reporter" so the item never
    comes back on next week's list."""
    suggestions = suggestions or {}
    lines = ["רשימת השלמה — פריטים שאין בטקסט שלהם קרדיט לכתב/ת.",
             "מלאו שם מול כל שורה בבלוק ה-JSON שבסוף, והעתיקו אותו אל",
             "weekly_deck/reporters_overrides.json. ערך ריק (\"\") = אין כתב/ת, וזה בסדר.",
             "",
             "שורות שמתחילות ב-\"אולי\" הן ניחוש: פריט אחר מאותו שבוע שנראה כאילו",
             "הוא מכסה את אותו סיפור ויש בו קרדיט. הכותרת שלו מוצגת כדי שתוכלו",
             "לפסול ניחוש שגוי במבט אחד. שום ניחוש לא מוחל אוטומטית.",
             ""]
    for k, n, it in missing:
        lines.append("[%s #%d] %s" % (k, n, clean_title(it.get('title', ''), 90)))
        for score, k2, n2, other in suggestions.get((k, n), []):
            lines.append("   אולי %s  (%d%% חפיפה עם %s #%d: %s)"
                         % (other['reporter'], round(score * 100), k2, n2,
                            clean_title(other.get('title', ''), 55)))
        # a saved deck_content.json has no _url (it is stripped as internal), so
        # --reporters-todo rebuilds what it can from the id alone. Instagram's
        # media_id is not a shortcode and cannot become a link — the headline has
        # to carry it there.
        url = it.get('url') or (_ITEM_URL_TPL[k].format(id=it['id'])
                                 if k in _ITEM_URL_TPL and it.get('id') else '')
        if url:
            lines.append("   " + url)
        lines.append("")
    lines.append("--- JSON ---")
    entries = ['  "%s:%s": ""' % (k, it.get('id', '')) for k, _, it in missing]
    lines.append("{\n" + ",\n".join(entries) + "\n}")
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(TODO_PATH, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines) + "\n")


def save_content(content):
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(CONTENT_PATH, 'w', encoding='utf-8') as f:
        json.dump(content, f, ensure_ascii=False, indent=2)
    size = os.path.getsize(CONTENT_PATH)
    print(f"   ✅ wrote {CONTENT_PATH} ({size:,} bytes)")
    if size > 55_000:
        print(f"   ⚠️ deck_content.json is larger than the 55KB target ({size:,})")
    return CONTENT_PATH


def load_content():
    if not os.path.exists(CONTENT_PATH):
        raise SystemExit(f"❌ {CONTENT_PATH} not found — run with --extract first.")
    with open(CONTENT_PATH, encoding='utf-8') as f:
        return json.load(f)


# ---------------------------------------------------------------- optional Gemini

def gemini_polish_content(content):
    """OPT-IN ONLY (--gemini). Rewrites learning prose; never touches numbers."""
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key:
        print("   (no GEMINI_API_KEY — skipping polish)")
        return
    from google import genai
    client = genai.Client(api_key=api_key)
    seeds = [{"i": i, "title": c['title'], "sentence": c['sentence'], "number": c.get('number', '')}
             for i, c in enumerate(content['learnings'])]
    prompt = ("אתה עורך של כאן חדשות. שכתב כותרת ומשפט לכל תובנה, בעברית, קצר וענייני. "
              "אל תשנה ואל תמציא מספרים. כתוב אך ורק מה קרה — בלי המלצות, בלי "
              "'כדאי', בלי 'שווה לבדוק', ובלי להשוות פורמטים (רילז/שורטס/תמונות). "
              "החזר JSON: רשימה של {i, title, sentence}.\n\n"
              + json.dumps(seeds, ensure_ascii=False))
    text = None
    for model in ["gemini-3.5-flash", "gemini-2.5-pro"]:
        try:
            r = client.models.generate_content(model=model, contents=prompt)
            text = (r.text or "").strip()
            if text:
                break
        except Exception as e:
            print(f"   Gemini {model} failed: {e}")
    if not text:
        return
    if text.startswith("```"):
        text = text.strip("`")
        i, j = text.find('['), text.rfind(']')
        text = text[i:j + 1] if i != -1 and j > i else text
    try:
        arr = json.loads(text)
    except Exception:
        return
    for o in arr:
        if isinstance(o, dict) and isinstance(o.get('i'), int) and 0 <= o['i'] < len(content['learnings']):
            card = content['learnings'][o['i']]
            card['title'] = clean_title(o.get('title', card['title']), 70)
            card['sentence'] = clean_title(o.get('sentence', card['sentence']), 160)


# ---------------------------------------------------------------- render context

def font_faces():
    """@font-face list for the weights present on disk. The licensed Light(300)
    weight is absent, so we map 300 -> Regular(400) EXPLICITLY."""
    weights = [('Light', 300), ('Regular', 400), ('Semibold', 600),
               ('Bold', 700), ('Black', 900)]
    faces, present = [], {}
    for label, w in weights:
        path = os.path.join(FONTS_DIR, f"SimplerPro_HLAR-{label}.otf")
        if os.path.exists(path):
            faces.append(dict(weight=w, uri=_file_uri(path)))
            present[w] = _file_uri(path)
    if 300 not in present and 400 in present:
        faces.append(dict(weight=300, uri=present[400]))
    return faces


def _file_uri(path):
    from pathlib import Path
    return Path(path).resolve().as_uri()


def load_mark():
    """Kan square mark as a reusable path (no defs/id/style -> safe to inline
    several times). Orange for the light slides."""
    p = os.path.join(ASSETS_DIR, "kan-news-mark-orange.svg")
    if not os.path.exists(p):
        return None
    try:
        svg = open(p, encoding='utf-8').read()
        vb = re.search(r'viewBox="([^"]+)"', svg)
        d = re.search(r'\sd="([^"]+)"', svg)
        if not (vb and d):
            return None
        return dict(viewbox=vb.group(1), d=d.group(1), fill="#f30")
    except Exception as e:
        print(f"   ⚠️ could not load brand mark: {e}")
        return None


def logo_data_uri():
    for name in ("kan-news-full-black-a.png", "kan-news-full-black.png"):
        p = os.path.join(ASSETS_DIR, name)
        if os.path.exists(p):
            with open(p, 'rb') as f:
                return "data:image/png;base64," + base64.b64encode(f.read()).decode()
    return None


# Short Hebrew labels for the secondary candidate list in the fun-fact panel.
# Keyed on the candidate `label` in deck_content.json, so the JSON needs no
# re-extract when this list changes.
# Format-share facts the deck no longer makes. Dropped at RENDER as well as at
# extract, so a deck_content.json produced before this rule can be re-rendered
# without one slipping back in.
RETIRED_CAND_LABELS = {'shorts_share', 'reels_share'}

CAND_LABEL_HE = {
    'reply_share': 'נתח תגובות',
    'whatsapp': 'שיתופים לוואטסאפ',
    'likes_total': 'לייקים',
    'comments_total': 'תגובות',
    'comments': 'תגובות',
    'shares': 'שיתופים',
    'saves': 'שמירות',
    'reach': 'חשיפה',
    'retweets': 'ריטוויטים',
    'quotes': 'ציטוטים',
    'overperformer': 'המוביל מול פוסט רגיל',
    'eng_leader': 'שיא מעורבות',
}


def _cand_label(c):
    lbl = CAND_LABEL_HE.get(c.get('label'))
    if lbl:
        return lbl
    return " ".join(str(c.get('text', '')).split()[:3]) or str(c.get('label', ''))


# What can be unusual about an item, in priority order when several qualify.
# Phrased for a newsroom: "פי 3.4 מהרגיל", never "מהחציון".
ANOMALY_SPECS = (
    ('shares', '🔁', 'שיתופים'),
    ('comments', '💬', 'תגובות'),
    ('likes', '❤️', 'לייקים'),
    ('engagement', '⚡', 'מעורבות'),
)
ANOMALY_MIN = 3.0   # at least 3x a normal post's rate that week
# A badge's job is to point the eye at a row. Past ~4 per slide it stops
# pointing and becomes a second engagement column, so the deck shows only
# the strongest few - a display cap, not a change to what qualifies.
ANOMALY_MAX_PER_SLIDE = 4


def _typical_rates(platform):
    """This week's typical interactions-per-view. Prefers median_rates stored at
    extract (computed over the whole week); falls back to the items present so
    an older deck_content.json still renders."""
    med = dict(platform.get('median_rates') or {})
    items = platform.get('top', []) or []
    for key in ('likes', 'comments', 'shares', 'engagement'):
        if float(med.get(key) or 0) > 0:
            continue
        vals = []
        for it in items:
            views = float(it.get('views') or 0)
            if key == 'engagement':
                x = float(it.get('engagement') or 0)
            elif views > 0:
                x = float(it.get(key) or 0) / views
            else:
                continue
            if x > 0:
                vals.append(x)
        vals.sort()
        n = len(vals)
        med[key] = (vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2) if n else 0.0
    return med


def _anomaly_of(item, med_rates):
    """The single most unusual thing about this item, or None for most rows.
    Compares RATES, so a post that is merely big does not flag - only one that
    got shared/discussed/liked far beyond what its own reach explains."""
    views = float(item.get('views') or 0)
    best = None
    for key, icon, label in ANOMALY_SPECS:
        typical = float(med_rates.get(key) or 0)
        if typical <= 0:
            continue
        if key == 'engagement':
            value = float(item.get('engagement') or 0)
        elif views > 0:
            value = float(item.get(key) or 0) / views
        else:
            continue
        if value <= 0:
            continue
        ratio = value / typical
        if ratio >= ANOMALY_MIN and (best is None or ratio > best['ratio']):
            # "פי 5.7", not "×5.7" — reads as Hebrew inside a Hebrew badge and
            # sidesteps the leading-symbol bidi trap entirely.
            best = dict(ratio=ratio, icon=icon, label=label, mult="פי %.1f" % ratio)
    return best


def _cap_anomalies(rows, platform_key):
    """Keep only the strongest few badges on a slide. Says so out loud when it
    hides one, so the editor knows the deck is holding something back."""
    flagged = [r for r in rows if r['anomaly']]
    if len(flagged) <= ANOMALY_MAX_PER_SLIDE:
        return
    flagged.sort(key=lambda r: r['anomaly']['ratio'], reverse=True)
    for r in flagged[ANOMALY_MAX_PER_SLIDE:]:
        r['anomaly'] = None
    print(f"   {platform_key}: {len(flagged)} qualified, showing top {ANOMALY_MAX_PER_SLIDE}")


def _row_program(item, pmap):
    """The programme for a row. An item extracted since programmes existed already
    carries the answer — computed over the FULL caption — so an empty value there
    means "no programme tag", not "not looked at", and is trusted. Only content
    from before the feature falls back to the stored 400-char caption."""
    if 'program' in item:
        return (item.get('program') or '').strip()
    return resolve_program(item.get('caption') or item.get('title') or '', pmap)[0]


def _row_url(key, item):
    """Where a row points. Prefers the permalink stored at extract; falls back to
    the id, so a deck_content.json produced before links existed still gets them
    for the four platforms whose ids build a URL (Instagram's media_id is not a
    shortcode, so those rows stay unlinked rather than pointing somewhere wrong)."""
    url = (item.get('url') or '').strip()
    if url.startswith('http'):
        return url
    tpl, iid = _ITEM_URL_TPL.get(key), (item.get('id') or '').strip()
    return tpl.format(id=iid) if (tpl and iid) else ''


def _display_title(item):
    """headline_of already drops the credit an item's reporter came from, but it
    runs at extract. Repeating it here means a credit added later — a new
    reporters_map line, an override — also stops showing its raw @handle in the
    headline, without paying for a re-extract."""
    title = _strip_bidi(item.get('title', '') or '').strip()
    if (item.get('reporter') or '').strip():
        title = re.sub(r"\s*@[A-Za-z0-9._]{2,30}[.,;:!?]*\s*$", "", title).rstrip()
    return title


def content_to_context(content, thumbstyle='portrait'):
    """deck_content.json -> template context. Pure/deterministic: no network,
    no sheets, no Gemini. Images are inlined from the thumbs cache here."""
    window = content['window']
    pmap = load_programs_map()
    plats_raw = {p['key']: p for p in content['platforms']}
    ordered = sorted(content['platforms'], key=lambda p: (-p.get('weekly_views', 0),
                                                          PLATFORM_ORDER.index(p['key'])))
    medals = ['🥇', '🥈', '🥉']
    platforms = []
    for i, p in enumerate(ordered, start=1):
        key = p['key']
        meta = PLATFORMS[key]
        dl = build_delta(p.get('delta_pct'))
        rows = []
        meds = _typical_rates(p)
        for n, it in enumerate(p.get('top', []), start=1):
            rows.append(dict(rank=n, title=_display_title(it), reporter=it.get('reporter', '') or '',
                             program=_row_program(it, pmap),
                             prog_stated=(it.get('_prog_src') == 'override'),
                             url=_row_url(key, it),
                             views_fmt=fmt_num(it.get('views', 0)),
                             eng_str=f"{float(it.get('engagement', 0) or 0):.1f}%",
                             highlight=(n <= 3), anomaly=_anomaly_of(it, meds),
                             thumb=thumb_data_uri(it.get('thumb'))))
        _cap_anomalies(rows, p.get('key', ''))
        # An almost-empty column is worse than no column: it reads as missing
        # data on every row instead of as extra information on a few — so the
        # programme column waits until a slide has a few. But the threshold is
        # there for sparse AUTO-detection; a programme an editor typed in by hand
        # is a deliberate statement and is never hidden by it.
        show_program = (sum(1 for r in rows if r['program']) >= 2
                        or any(r['prog_stated'] for r in rows))

        top3 = []
        for n, it in enumerate(p.get('top', [])[:3]):
            top3.append(dict(medal=medals[n], title=clean_title(_display_title(it), 110),
                             views_fmt=fmt_num(it.get('views', 0)),
                             reporter=it.get('reporter', '') or '',
                             url=_row_url(key, it),
                             thumb=thumb_data_uri(it.get('thumb'))))

        ff = p.get('fun_fact') or {}
        cands = [c for c in (ff.get('candidates') or [])
                 if c.get('label') not in RETIRED_CAND_LABELS]
        chosen = next((c for c in cands if c.get('label') == ff.get('chosen')), None) or (cands[0] if cands else None)
        # the runners-up fill the panel with real per-platform numbers instead
        # of whitespace; the chosen fact stays the hero
        more = [dict(label=_cand_label(c), value=c.get('value', ''), suffix=c.get('suffix', ''))
                for c in cands if c is not chosen][:3]

        platforms.append(dict(
            key=key, name=p.get('name', meta['name']), colors=meta['colors'],
            icon=icon_svg(key, 56), icon_big=icon_svg(key, 120),
            rank_label=f"פלטפורמה {i} מתוך {len(ordered)} · לפי צפיות",
            weekly_views_fmt=fmt_num(p.get('weekly_views', 0)),
            delta_str=dl['str_signed'], delta_color=dl['color'], has_delta=dl['has_delta'],
            has_data=bool(p.get('top')),
            text_cards=(key in NO_THUMBS),
            top3=top3, top10=rows, show_program=show_program,
            has_links=any(r['url'] for r in rows),
            has_anomaly=any(r['anomaly'] for r in rows),
            fun_fact=(dict(value=chosen['value'], suffix=chosen.get('suffix', ''),
                           text=chosen.get('text', '')) if chosen else None),
            fun_fact_more=more))

    hero = content.get('hero', {})
    hd = build_delta(hero.get('delta_pct'))
    tmain, tsuf = split_suffix(fmt_num(hero.get('total_views', 0)))

    max_views = max((p.get('weekly_views', 0) for p in ordered), default=0)
    overview_rows = []
    for p in ordered:
        meta = PLATFORMS[p['key']]
        dl = build_delta(p.get('delta_pct'))
        overview_rows.append(dict(
            name=p.get('name', meta['name']), icon=icon_svg(p['key'], 42),
            followers_fmt=fmt_num(p['followers']) if p.get('followers') else "",
            bar_color=meta['colors']['bar'],
            bar_pct=int(round(p.get('weekly_views', 0) / max_views * 100)) if max_views > 0 else 0,
            views_fmt=fmt_num(p.get('weekly_views', 0)),
            has_delta=dl['has_delta'], delta_arrow=dl['arrow'],
            delta_str=dl['str_abs'], delta_color=dl['color']))

    learnings = []
    for c in content.get('learnings', []):
        num_main, num_suf = split_suffix(c.get('number', ''))
        learnings.append(dict(icon=c.get('icon', ''), title=c.get('title', ''),
                              sentence=c.get('sentence', ''),
                              number=c.get('number', ''), number_main=num_main,
                              number_suffix=num_suf, color=c.get('color', '#111')))

    story = content.get('story_of_the_week')
    if story:
        story = dict(title=story.get('title', ''), sentence=story.get('sentence', ''),
                     platforms=[dict(name=s.get('name', ''), views_fmt=fmt_num(s.get('views', 0)))
                                for s in story.get('platforms', [])])

    return dict(
        font_faces=font_faces(), logo_black=logo_data_uri(), mark=load_mark(),
        thumbstyle=thumbstyle,
        # the legend must never drift from the constant it explains
        anomaly_min=("%g" % ANOMALY_MIN),
        week=dict(range_str=window.get('range_str', ''), range_short=window.get('range_short', '')),
        hero=dict(total_fmt=fmt_num(hero.get('total_views', 0)), total_main=tmain,
                  total_suffix=tsuf, has_delta=hd['has_delta'], delta_str=hd['str_signed'],
                  delta_arrow=hd['arrow'], delta_color=hd['color']),
        overview_rows=overview_rows, platforms=platforms,
        learnings=learnings, story=story,
        reporters=content.get('closing', {}).get('reporters', []),
        closing_note=content.get('closing', {}).get('note', ''),
    )


# ---------------------------------------------------------------- mock

def build_mock_content():
    """Realistic hardcoded data through the real extract path (no creds/net)."""
    import pandas as pd
    window = compute_window()
    d1 = window['d1']
    days = [(d1 + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]

    # Real Kan captions are long and credit the reporter at the END, well past
    # the old 100-char cut. The mock mirrors that shape so --mock exercises the
    # headline extractor and the full-caption reporter search for real.
    tail = ("הכתבה המלאה עם כל הפרטים, העדויות מהשטח והתגובות שהתקבלו הבוקר "
            "ממשרד הממשלתי הרלוונטי, לצד ניתוח של מה צפוי לקרות הלאה")
    # includes the Facebook shapes: a bare trailing name after a link (Latin,
    # one name and two names) and a bare Hebrew name after a separator
    # every credit shape we handle, including the ones that must NOT resolve
    credits = ["(יותם ווקס)",                                    # parens
               "איציק זוארץ, כתב כאן11 בדרום 📸: מרכז רפואי יוספטל",  # role suffix
               "כתב: רועי קייס",                                  # credit prefix
               "📸: Ebrahim Noroozi",                             # photo credit only -> none
               "| https://bit.ly/4vYM1NT Vered Pelman Haim Goldich",  # two Latin names
               "״ Itay Blumental 📸: אבי דישי",                   # Latin byline from a handle
               "@haimgoldich",                                    # handle
               "https://bit.ly/3kQz9Lm Dana Levi 📸: אבי דישי",    # name then photo credit
               "",                                                # genuinely uncredited
               "| רובי המרשלג"]                                   # known Hebrew name

    # Kan tags the source programme at the very end of the caption, past the
    # credit. Only some items carry one — which is exactly the case the
    # conditional programme column has to handle, so the mock mirrors it.
    programs = ["#כאןבשלוש", "", "#בחציהיום", "", "", "#כאןבשש", "", "", "#מונטנגרו", ""]

    def rows(specs):
        out = []
        for i, (title, views, likes, comments, shares, eng, extra) in enumerate(specs):
            caption = (f"{title}. {tail} {credits[i % len(credits)]} "
                       f"{programs[i % len(programs)]}").strip()
            r = dict(title=caption, caption=caption, text=caption, views=views, likes=likes,
                     comments=comments, shares=shares, engagement_rate=eng,
                     date=days[i % 7], published_at=days[i % 7] + "T10:00:00Z")
            r.update(extra)
            out.append(r)
        return pd.DataFrame(out)

    yt = rows([
        ("תיעוד: רגע פגיעת הרקטה בעוטף עזה", 1_400_000, 42000, 3100, 900, 9.1, dict(video_id="dQw4w9WgXcQ", video_type="Regular")),
        ("ראיון בלעדי עם ראש הממשלה על ההסכם", 980_000, 21000, 2400, 600, 7.4, dict(video_id="9bZkp7q19f0", video_type="Regular")),
        ("כך נראתה ההצפה בצפון מהאוויר", 720_000, 15000, 900, 400, 6.0, dict(video_id="kJQP7kiw5Fk", video_type="Shorts")),
        ("מבזק: החלטת בג\"ץ בעניין הגיוס", 610_000, 18000, 2600, 800, 8.2, dict(video_id="a", video_type="Regular")),
        ("הפגנת ענק בכיכר — תיעוד מרחפן", 540_000, 9000, 500, 300, 7.1, dict(video_id="b", video_type="Shorts")),
        ("פאנל אולפן: לאן הולך המשק", 430_000, 4000, 300, 120, 4.4, dict(video_id="c", video_type="Regular")),
        ("דיווח מהשטח: שריפה בהרי ירושלים", 390_000, 6000, 400, 200, 5.8, dict(video_id="d", video_type="Shorts")),
        ("הטור השבועי של הפרשן הצבאי", 310_000, 3000, 250, 90, 3.9, dict(video_id="e", video_type="Regular")),
        ("כתבת תחקיר: מאחורי הקלעים של העסקה", 275_000, 5000, 350, 150, 6.2, dict(video_id="f", video_type="Regular")),
        ("מזג האוויר: גל החום נמשך", 240_000, 1500, 120, 40, 2.7, dict(video_id="g", video_type="Shorts")),
    ])
    tk = rows([
        ("הרגע שהחייל חזר הביתה בהפתעה", 2_100_000, 190000, 4200, 22000, 14.2, dict(video_id="t1", whatsapp_shares=15000, saves=8000, type="Video")),
        ("כתב שטח מסביר ב-40 שניות", 1_300_000, 90000, 2100, 11000, 11.8, dict(video_id="t2", whatsapp_shares=7000, saves=4000, type="Video")),
        ("מאחורי הקלעים של אולפן החדשות", 890_000, 60000, 1500, 6000, 10.4, dict(video_id="t3", whatsapp_shares=3500, saves=2500, type="Video")),
        ("שאלנו עוברי אורח על יוקר המחיה", 640_000, 40000, 900, 4000, 9.1, dict(video_id="t4", whatsapp_shares=2200, saves=1500, type="Video")),
        ("טרנד: העיתונאים עונים לתגובות", 520_000, 55000, 3000, 5200, 12.6, dict(video_id="t5", whatsapp_shares=3100, saves=1800, type="Video")),
        ("כך מזייפים סרטון — מדריך קצר", 430_000, 30000, 700, 2800, 8.3, dict(video_id="t6", whatsapp_shares=1600, saves=1200, type="Video")),
        ("שלוש דקות, כל מה שפספסת היום", 360_000, 24000, 600, 2100, 7.7, dict(video_id="t7", whatsapp_shares=1200, saves=900, type="Video")),
        ("ראיון רחוב על ההסכם המדיני", 300_000, 18000, 500, 1600, 6.9, dict(video_id="t8", whatsapp_shares=900, saves=700, type="Video")),
        ("המזג אוויר בסטייל של טיקטוק", 245_000, 12000, 300, 1100, 5.4, dict(video_id="t9", whatsapp_shares=600, saves=500, type="Video")),
        ("הצצה לחדר הבקרה בשידור חי", 190_000, 9000, 250, 800, 6.1, dict(video_id="t10", whatsapp_shares=450, saves=350, type="Video")),
    ])
    ig = rows([
        ("רילס: תיעוד ההצפה בצפון", 1_100_000, 88000, 1200, 9000, 12.9, dict(media_id="i1", type="Reel", saved=14000, reach=1_300_000)),
        ("קרוסלה: חמש נקודות על ההסכם", 720_000, 44000, 800, 3200, 9.6, dict(media_id="i2", type="Carousel", saved=9000, reach=820_000)),
        ("הסטורי שהפך לרילס הכי נצפה", 560_000, 36000, 600, 2600, 8.8, dict(media_id="i3", type="Reel", saved=7000, reach=650_000)),
        ("רגע מרגש בכנסת — רילס", 480_000, 28000, 500, 2100, 7.4, dict(media_id="i4", type="Reel", saved=5200, reach=560_000)),
        ("אינפוגרפיקה: המספרים של השבוע", 390_000, 20000, 400, 1500, 6.2, dict(media_id="i5", type="Photo", saved=3800, reach=430_000)),
        ("ראיון בזק עם הפרשן הכלכלי", 330_000, 16000, 350, 1200, 5.9, dict(media_id="i6", type="Reel", saved=2900, reach=380_000)),
        ("מאחורי הכתבה: צילום מהרחפן", 280_000, 14000, 300, 1000, 7.1, dict(media_id="i7", type="Reel", saved=2400, reach=320_000)),
        ("שאלות ותשובות עם הכתבת", 240_000, 15000, 900, 1400, 8.0, dict(media_id="i8", type="Photo", saved=2100, reach=280_000)),
        ("רילס מזג האוויר של סוף השבוע", 205_000, 9000, 200, 700, 4.8, dict(media_id="i9", type="Reel", saved=1500, reach=240_000)),
        ("גלריית התמונות של השבוע", 170_000, 7000, 150, 500, 3.6, dict(media_id="i10", type="Carousel", saved=1100, reach=200_000)),
    ])
    fb = rows([
        ("שידור חי: מסיבת העיתונאים המלאה", 640_000, 12000, 3000, 2400, 5.1, dict(post_id="f1", type="Videos", reach=1_200_000)),
        ("הכתבה שעוררה את מרב התגובות", 410_000, 9000, 4200, 1800, 6.8, dict(post_id="f2", type="Links", reach=780_000)),
        ("וידאו: סיכום היום ב-3 דקות", 320_000, 7000, 900, 1400, 4.2, dict(post_id="f3", type="Videos", reach=600_000)),
        ("פוסט דעה של הפרשן המדיני", 280_000, 8000, 2100, 1600, 7.9, dict(post_id="f4", type="Links", reach=520_000)),
        ("גלריה: התמונות שסיכמו את השבוע", 240_000, 5000, 600, 700, 3.4, dict(post_id="f5", type="Images", reach=430_000)),
        ("מבזק: עדכון מחדר החדשות", 210_000, 6000, 800, 900, 5.6, dict(post_id="f6", type="Links", reach=390_000)),
        ("ראיון מלא עם שר האוצר", 185_000, 3500, 500, 400, 4.0, dict(post_id="f7", type="Videos", reach=340_000)),
        ("סקר: מה חושב הציבור", 160_000, 7000, 1900, 1300, 8.3, dict(post_id="f8", type="Images", reach=300_000)),
        ("תזכורת: שידור מיוחד הערב", 135_000, 2000, 300, 200, 2.9, dict(post_id="f9", type="Links", reach=250_000)),
        ("מזג האוויר לסוף השבוע", 110_000, 1500, 200, 120, 2.1, dict(post_id="f10", type="Images", reach=210_000)),
    ])
    x = rows([
        ("הציוץ שסיכם את מסיבת העיתונאים: \"אין הסכם בלי שחרור כל החטופים\"", 380_000, 4200, 300, 1600, 4.4, dict(tweet_id="x1", retweets=1600, replies=300, quotes=200, type="Text")),
        ("שרשור: כל מה שקרה היום בכנסת — 12 הצבעות, קואליציה מתפצלת", 240_000, 3000, 400, 1100, 5.7, dict(tweet_id="x2", retweets=1100, replies=400, quotes=150, type="Text")),
        ("עדכון בזק מהשטח בזמן אמת: כוחות ההצלה בדרך לאזור", 190_000, 2200, 250, 800, 3.9, dict(tweet_id="x3", retweets=800, replies=250, quotes=90, type="Text")),
        ("ציטוט היום מהמליאה", 150_000, 2600, 600, 900, 6.2, dict(tweet_id="x4", retweets=900, replies=600, quotes=120, type="Text")),
        ("מבזק: תוצאות ההצבעה", 130_000, 1800, 200, 600, 4.8, dict(tweet_id="x5", retweets=600, replies=200, quotes=70, type="Text")),
        ("שרשור נתונים על יוקר המחיה", 110_000, 1200, 150, 400, 3.3, dict(tweet_id="x6", retweets=400, replies=150, quotes=50, type="Text")),
        ("הפרשן מגיב בזמן אמת", 95_000, 1400, 350, 500, 5.1, dict(tweet_id="x7", retweets=500, replies=350, quotes=60, type="Text")),
        ("תמונת השבוע עם הקשר", 80_000, 900, 120, 300, 2.7, dict(tweet_id="x8", retweets=300, replies=120, quotes=40, type="Photo")),
        ("עדכון תחבורה ומזג אוויר", 68_000, 600, 80, 180, 1.9, dict(tweet_id="x9", retweets=180, replies=80, quotes=20, type="Text")),
        ("סיכום היום בשלושה ציוצים", 55_000, 800, 200, 250, 3.4, dict(tweet_id="x10", retweets=250, replies=200, quotes=30, type="Text")),
    ])
    sheets = dict(youtube=yt, tiktok=tk, instagram=ig, facebook=fb, x=x)
    rmap = load_reporters_map()
    pmap = load_programs_map()

    plats = [extract_platform(k, sheets[k], window, False, '', '', rmap, pmap)
             for k in PLATFORM_ORDER]

    # synthesize prior-week deltas + followers (real runs read these from sheets)
    prev = dict(youtube=22.0, tiktok=52.0, instagram=8.0, facebook=-5.0, x=12.0)
    follows = dict(youtube=412000, tiktok=286000, instagram=531000, facebook=1_200_000, x=348000)
    for p in plats:
        p['delta_pct'] = prev.get(p['key'])
        p['followers'] = follows.get(p['key'])

    content = assemble_content(plats, window, use_gemini=False)
    # mock thumbnails so the thumb styles can be judged without network
    _write_mock_thumbs(content)
    return content


def _write_mock_thumbs(content):
    """Write small placeholder JPEG/PNG files so --mock exercises the real
    thumbs-cache path. Mixed portrait/landscape to test both thumb styles."""
    try:
        from PIL import Image, ImageDraw
    except Exception:
        print("   (Pillow unavailable — mock runs without thumbnails)")
        return
    os.makedirs(THUMBS_DIR, exist_ok=True)
    palette = {'youtube': (225, 25, 0), 'tiktok': (180, 83, 9),
               'instagram': (228, 64, 95), 'facebook': (24, 119, 242), 'x': (17, 17, 17)}
    for p in content['platforms']:
        if p['key'] in NO_THUMBS:
            continue
        for n, it in enumerate(p['top']):
            # alternate portrait (9:16) and landscape (16:9) to mimic real mixes
            size = (450, 800) if n % 2 == 0 else (800, 450)
            img = Image.new('RGB', size, palette.get(p['key'], (120, 120, 120)))
            d = ImageDraw.Draw(img)
            d.rectangle([12, 12, size[0] - 12, size[1] - 12], outline=(255, 255, 255), width=6)
            d.text((28, 28), f"{p['name']}\n#{n+1}\n{size[0]}x{size[1]}", fill=(255, 255, 255))
            fname = f"{p['key']}_{re.sub(r'[^A-Za-z0-9_-]', '_', it['id'])[:60]}.jpg"
            img.save(os.path.join(THUMBS_DIR, fname), format='JPEG', quality=80)
            it['thumb'] = fname


# ---------------------------------------------------------------- render

def render(context):
    from jinja2 import Environment, FileSystemLoader, select_autoescape
    from markupsafe import Markup
    env = Environment(loader=FileSystemLoader(HERE),
                      autoescape=select_autoescape(['html', 'xml']))
    for c in context['platforms']:
        c['icon'] = Markup(c['icon'])
        c['icon_big'] = Markup(c['icon_big'])
    for r in context['overview_rows']:
        r['icon'] = Markup(r['icon'])
    html = env.get_template(TEMPLATE).render(**context)
    os.makedirs(OUT_DIR, exist_ok=True)
    html_path = os.path.join(OUT_DIR, "deck.html")
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"   ✅ wrote {html_path} ({len(html):,} bytes)")
    return html_path


def to_pdf(html_path, qa_all=False):
    from playwright.sync_api import sync_playwright
    pdf_path = os.path.join(OUT_DIR, "deck.pdf")
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page(viewport={'width': 1920, 'height': 1080})
        page.goto(_file_uri(html_path), wait_until='networkidle')
        try:
            page.evaluate("document.fonts.ready")
        except Exception:
            pass
        page.wait_for_timeout(600)
        page.pdf(path=pdf_path, width='1920px', height='1080px',
                 print_background=True, scale=1)
        sections = page.query_selector_all('section.slide')
        n = len(sections) if qa_all else min(2, len(sections))
        for i in range(n):
            sections[i].screenshot(path=os.path.join(OUT_DIR, f"slide_{i+1}.png"))
        browser.close()
    print(f"   ✅ wrote {pdf_path}")
    return pdf_path


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description="Weekly social summary deck generator")
    ap.add_argument('--extract', action='store_true', help="fetch data + thumbnails, write deck_content.json")
    ap.add_argument('--render', action='store_true', help="render deck_content.json to HTML/PDF (offline)")
    ap.add_argument('--mock', action='store_true', help="mock data (no creds/network)")
    ap.add_argument('--thumbstyle', choices=['portrait', 'blur'], default='portrait',
                    help="how highlight thumbnails are framed (default: portrait)")
    ap.add_argument('--no-thumbs', action='store_true', help="skip thumbnail downloads")
    ap.add_argument('--gemini', action='store_true', help="opt in to Gemini prose polish during extract")
    ap.add_argument('--qa-all', action='store_true', help="screenshot every slide, not just the first two")
    ap.add_argument('--reporters-todo', action='store_true',
                    help="rebuild out/reporters_todo.txt from deck_content.json and exit (offline)")
    ap.add_argument('--retitle', action='store_true',
                    help="re-derive every headline in deck_content.json from its caption and exit "
                         "(offline; OVERWRITES hand-edited titles)")
    args = ap.parse_args()

    if args.retitle:
        content = load_content()
        apply_reporter_overrides(content)
        changed = 0
        for p in content.get('platforms', []):
            for it in p.get('top', []):
                new = headline_of(it.get('caption') or it.get('title', ''),
                                  it.get('reporter') or '')
                if new and new != it.get('title'):
                    it['title'] = new
                    changed += 1
        print(f"   ✏️  {changed} headlines re-derived")
        save_content(content)
        return

    if args.reporters_todo:
        content = load_content()
        apply_reporter_overrides(content)
        report_reporters(content)
        return

    do_extract = args.extract or not args.render
    do_render = args.render or not args.extract

    print(f"\n{'='*60}\n📊 Weekly Deck Generator — {datetime.now(IL_TZ):%Y-%m-%d %H:%M}\n{'='*60}\n")

    if do_extract:
        if args.mock:
            print("🧪 MOCK extract — no creds, no network.")
            content = build_mock_content()
        else:
            print("📥 Extracting from Google Sheets...")
            content = build_deck_content(get_client(), thumbs_enabled=not args.no_thumbs,
                                         use_gemini=args.gemini)
        report_reporters(content)
        save_content(content)
        print("   ✏️  edit weekly_deck/out/deck_content.json, then re-run with --render")

    if do_render:
        print(f"🎨 Rendering (thumbstyle={args.thumbstyle})...")
        content = load_content()
        filled, vetoed = apply_reporter_overrides(content)
        if filled or vetoed:
            print(f"   ✍️  reporters_overrides: {filled} credited, {vetoed} marked uncredited")
        context = content_to_context(content, thumbstyle=args.thumbstyle)
        html_path = render(context)
        print("🖨️  Rendering PDF (Playwright)...")
        pdf = to_pdf(html_path, qa_all=args.qa_all)
        print(f"\n✅ Done. Open {pdf} to review the deck.")


if __name__ == "__main__":
    main()
