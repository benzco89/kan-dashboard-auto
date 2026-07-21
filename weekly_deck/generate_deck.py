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


def clean_title(s, cap=100):
    s = str(s or "").replace("\n", " ").replace("\r", " ").strip()
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


def _looks_like_name(s):
    s = (s or "").strip()
    if not s or any(ch.isdigit() for ch in s):
        return False
    if 'צילום' in s or ':' in s or '@' in s:
        return False
    return bool(_NAME_RE.match(s))


def reporter_fallback(title):
    """Deterministic reporter credit: trailing '(שם כתב)' that looks like a
    person, else a bare @handle. '' when nothing credible. Never invents."""
    title = str(title or "")
    m = re.search(r"\(([^)]{2,30})\)\s*$", title)
    if m and _looks_like_name(m.group(1)):
        return m.group(1).strip()
    h = re.search(r"@([A-Za-z0-9_]{2,30})", title)
    if h:
        return "@" + h.group(1)
    return ""


def load_reporters_map():
    """{"@handle": "שם בעברית"} — repo-tracked, hand-maintained."""
    try:
        with open(REPORTERS_MAP_PATH, encoding='utf-8') as f:
            data = json.load(f)
        return {str(k): str(v) for k, v in data.items()
                if v and not str(k).startswith('_')}
    except Exception:
        return {}


def resolve_reporter(title, rmap):
    """Extract, then map a known @handle to a Hebrew name. An unmapped handle is
    left visible as-is so it can be spotted and added to reporters_map.json."""
    rep = reporter_fallback(title)
    if rep.startswith('@'):
        return rmap.get(rep, rep)
    return rep


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


def fun_fact_candidates(key, df):
    """4-6 deterministic candidate facts per platform. candidates[0] is the
    default `chosen`; the editor can switch `chosen` or rewrite any text."""
    c = []
    try:
        v = to_num(df['views'])
        vsum = float(v.sum())
        n = len(df)
        eng = engagement_series(key, df)
        med = float(v.median()) if n else 0.0

        if key == 'youtube':
            if 'video_type' in df.columns and vsum > 0:
                sv = float(to_num(df[df['video_type'] == 'Shorts']['views']).sum())
                c.append(_cand('shorts_share', int(round(sv / vsum * 100)), '%', 'מהצפיות ביוטיוב הגיעו מ‑Shorts'))
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
            if 'type' in df.columns and vsum > 0:
                rv = float(to_num(df[df['type'].astype(str).str.contains('Reel', case=False, na=False)]['views']).sum())
                c.append(_cand('reels_share', int(round(rv / vsum * 100)), '%', 'מהצפיות באינסטגרם הגיעו מ‑Reels'))
            if 'saved' in df.columns:
                c.append(_cand('saves', fmt_num(to_num(df['saved']).sum()), '', 'שמירות על תכני אינסטגרם השבוע'))
            if 'reach' in df.columns:
                c.append(_cand('reach', fmt_num(to_num(df['reach']).sum()), '', 'חשיפה (reach) לתכני אינסטגרם השבוע'))
        elif key == 'facebook':
            if 'reach' in df.columns:
                c.append(_cand('reach', fmt_num(to_num(df['reach']).sum()), '', 'סך החשיפה (reach) של פוסטי פייסבוק השבוע'))
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
            c.append(_cand('overperformer', f"{r:.1f}", '×', f"הפריט המוביל השבוע עשה פי {r:.1f} מחציון הצפיות בפלטפורמה"))
        if len(eng) and float(eng.max()) > 0:
            e = float(eng.max())
            c.append(_cand('eng_leader', f"{e:.1f}", '%', f"שיא מעורבות של {e:.1f}% על פריט בודד השבוע"))
    except Exception as ex:
        print(f"      candidates failed for {key}: {ex}")
    return c


def extract_platform(key, df_all, window, thumbs_enabled, fb_token, tikhub_token, rmap):
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
        title = clean_title(r.get(tc, ''))
        items.append(dict(id=str(r.get(idc, '')), title=title,
                          reporter=resolve_reporter(title, rmap),
                          views=int(r['views']), engagement=round(float(r['_eng']), 1),
                          thumb=None))

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
    """Deterministic starting point for the 'מה למדנו' slide — 3 cards the
    editor is expected to rewrite/extend (3-4 supported) in deck_content.json."""
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
                          sentence=f"{best['name']} עלה ב{d['str_abs']} בצפיות מול השבוע שעבר — שם כדאי למקד את המאמץ."))
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
                          title=f"{worst['name']} במגמת ירידה",
                          sentence='הפלטפורמה היחידה עם צפיות יורדות — שווה בדיקה.'))
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

    plats = []
    for key in PLATFORM_ORDER:
        df = load_sheet(gc, PLATFORMS[key]['sheet']) if gc else None
        plats.append(extract_platform(key, df, window, thumbs_enabled,
                                      fb_token, tikhub_token, rmap))

    follows = followers_map(gc)
    for p in plats:
        p['followers'] = follows.get(p['key'])

    return assemble_content(plats, window, use_gemini)


def assemble_content(plats, window, use_gemini=False):
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


def save_content(content):
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(CONTENT_PATH, 'w', encoding='utf-8') as f:
        json.dump(content, f, ensure_ascii=False, indent=2)
    size = os.path.getsize(CONTENT_PATH)
    print(f"   ✅ wrote {CONTENT_PATH} ({size:,} bytes)")
    if size > 40_000:
        print(f"   ⚠️ deck_content.json is larger than the 40KB target ({size:,})")
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
              "אל תשנה ואל תמציא מספרים. החזר JSON: רשימה של {i, title, sentence}.\n\n"
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


def _mark_anomalies(rows):
    """Ranks 4+ whose engagement clearly beats the week's median."""
    if len(rows) < 4:
        return
    engs = sorted(r['eng'] for r in rows)
    n = len(engs)
    med = engs[n // 2] if n % 2 else (engs[n // 2 - 1] + engs[n // 2]) / 2
    for i, r in enumerate(rows):
        r['anomaly'] = bool(i >= 3 and med > 0 and r['eng'] >= 1.4 * med)


def content_to_context(content, thumbstyle='portrait'):
    """deck_content.json -> template context. Pure/deterministic: no network,
    no sheets, no Gemini. Images are inlined from the thumbs cache here."""
    window = content['window']
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
        for n, it in enumerate(p.get('top', []), start=1):
            rows.append(dict(rank=n, title=it.get('title', ''), reporter=it.get('reporter', '') or '',
                             views_fmt=fmt_num(it.get('views', 0)),
                             eng=float(it.get('engagement', 0) or 0),
                             eng_str=f"{float(it.get('engagement', 0) or 0):.1f}%",
                             highlight=(n <= 3), anomaly=False,
                             thumb=thumb_data_uri(it.get('thumb'))))
        _mark_anomalies(rows)

        top3 = []
        for n, it in enumerate(p.get('top', [])[:3]):
            top3.append(dict(medal=medals[n], title=clean_title(it.get('title', ''), 110),
                             views_fmt=fmt_num(it.get('views', 0)),
                             reporter=it.get('reporter', '') or '',
                             thumb=thumb_data_uri(it.get('thumb'))))

        ff = p.get('fun_fact') or {}
        cands = ff.get('candidates') or []
        chosen = next((c for c in cands if c.get('label') == ff.get('chosen')), None) or (cands[0] if cands else None)

        platforms.append(dict(
            key=key, name=p.get('name', meta['name']), colors=meta['colors'],
            icon=icon_svg(key, 56), icon_big=icon_svg(key, 120),
            rank_label=f"פלטפורמה {i} מתוך {len(ordered)} · לפי צפיות",
            weekly_views_fmt=fmt_num(p.get('weekly_views', 0)),
            delta_str=dl['str_signed'], delta_color=dl['color'], has_delta=dl['has_delta'],
            has_data=bool(p.get('top')),
            text_cards=(key in NO_THUMBS),
            top3=top3, top10=rows,
            has_anomaly=any(r['anomaly'] for r in rows),
            fun_fact=(dict(value=chosen['value'], suffix=chosen.get('suffix', ''),
                           text=chosen.get('text', '')) if chosen else None)))

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

    def rows(specs):
        out = []
        for i, (title, views, likes, comments, shares, eng, extra) in enumerate(specs):
            r = dict(title=title, caption=title, text=title, views=views, likes=likes,
                     comments=comments, shares=shares, engagement_rate=eng,
                     date=days[i % 7], published_at=days[i % 7] + "T10:00:00Z")
            r.update(extra)
            out.append(r)
        return pd.DataFrame(out)

    yt = rows([
        ("תיעוד: רגע פגיעת הרקטה בעוטף עזה (יואב לימור)", 1_400_000, 42000, 3100, 900, 9.1, dict(video_id="dQw4w9WgXcQ", video_type="Regular")),
        ("ראיון בלעדי עם ראש הממשלה על ההסכם", 980_000, 21000, 2400, 600, 7.4, dict(video_id="9bZkp7q19f0", video_type="Regular")),
        ("כך נראתה ההצפה בצפון מהאוויר (צילום: מוטי מילרוד)", 720_000, 15000, 900, 400, 6.0, dict(video_id="kJQP7kiw5Fk", video_type="Shorts")),
        ("מבזק: החלטת בג\"ץ בעניין הגיוס", 610_000, 18000, 2600, 800, 8.2, dict(video_id="a", video_type="Regular")),
        ("הפגנת ענק בכיכר — תיעוד מרחפן @haimgoldich", 540_000, 9000, 500, 300, 7.1, dict(video_id="b", video_type="Shorts")),
        ("פאנל אולפן: לאן הולך המשק", 430_000, 4000, 300, 120, 4.4, dict(video_id="c", video_type="Regular")),
        ("דיווח מהשטח: שריפה בהרי ירושלים", 390_000, 6000, 400, 200, 5.8, dict(video_id="d", video_type="Shorts")),
        ("הטור השבועי של הפרשן הצבאי (רון בן ישי)", 310_000, 3000, 250, 90, 3.9, dict(video_id="e", video_type="Regular")),
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
        ("רילס: תיעוד ההצפה בצפון (נעה לנדאו)", 1_100_000, 88000, 1200, 9000, 12.9, dict(media_id="i1", type="Reel", saved=14000, reach=1_300_000)),
        ("קרוסלה: חמש נקודות על ההסכם (שירית אביטן)", 720_000, 44000, 800, 3200, 9.6, dict(media_id="i2", type="Carousel", saved=9000, reach=820_000)),
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

    plats = [extract_platform(k, sheets[k], window, False, '', '', rmap) for k in PLATFORM_ORDER]

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
    args = ap.parse_args()

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
        save_content(content)
        print("   ✏️  edit weekly_deck/out/deck_content.json, then re-run with --render")

    if do_render:
        print(f"🎨 Rendering (thumbstyle={args.thumbstyle})...")
        content = load_content()
        context = content_to_context(content, thumbstyle=args.thumbstyle)
        html_path = render(context)
        print("🖨️  Rendering PDF (Playwright)...")
        pdf = to_pdf(html_path, qa_all=args.qa_all)
        print(f"\n✅ Done. Open {pdf} to review the deck.")


if __name__ == "__main__":
    main()
