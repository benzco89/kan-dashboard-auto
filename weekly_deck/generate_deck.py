"""
Weekly Deck Generator - סיכום סושיאל שבועי כמצגת 16:9 ל-PDF ולטלגרם.

רץ כל יום ראשון: אוסף את נתוני 5 הפלטפורמות (יוטיוב / טיקטוק / אינסטגרם /
פייסבוק / X) מגוגל שיטס לחלון של 7 הימים האחרונים, בונה קונטקסט נקי, מרנדר
תבנית Jinja2 ל-HTML שטוח (עמוד לכל שקופית, 1920x1080), וממיר ל-PDF עם
Playwright. עם --send שולח את ה-PDF לערוץ הטלגרם.

    python weekly_deck/generate_deck.py            # ייצור מנתונים אמיתיים (creds מ-env)
    python weekly_deck/generate_deck.py --mock     # נתוני דמה, בלי creds/רשת

הפלט: weekly_deck/out/deck.html + deck.pdf (+ צילומי QA). אין שליחה לטלגרם —
ההפצה תיקבע בהמשך אחרי סקירת המצגת. אינו כותב לשום גיליון ואינו נוגע בקולקטורים.
התבנית והעיצוב קבועים (weekly_deck/design/weekly-social-light.dc.html); הקובץ
הזה מזין אותם בנתונים בלבד.
"""

import os
import sys
import io
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


# Per-platform metadata. `order` is only the tie-break; slides are sorted by
# real weekly views. Column names track the sheet schemas.
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


def _download(url, headers=None, timeout=15):
    import requests
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.ok and r.content and r.headers.get('content-type', '').startswith('image'):
            ct = r.headers.get('content-type', 'image/jpeg').split(';')[0]
            if len(r.content) <= 4_000_000:
                content, ct = _to_renderable(r.content, ct)
                if content:
                    return "data:%s;base64,%s" % (ct, base64.b64encode(content).decode())
    except Exception as e:
        print(f"      thumb download failed ({url[:60]}...): {e}")
    return None


def thumb_youtube(vid):
    return _download(f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg")


def thumb_fb(pid, token):
    if not token:
        return None
    try:
        from utils import http_get_json
        data = http_get_json(f"https://graph.facebook.com/{FB_API_VERSION}/{pid}",
                             params={'fields': 'full_picture', 'access_token': token},
                             timeout=15, max_retries=2)
        url = data.get('full_picture')
        return _download(url) if url else None
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
        return _download(url) if url else None
    except Exception as e:
        print(f"      ig thumb failed: {e}")
        return None


TIKTOK_SEC_UID = ("MS4wLjABAAAA3p5tyX2Z3cacCWU34-nHbK-dpVBO5Y6"
                  "IGvTj9xufL60rC6ItchtdzkEe-0frXJZX")


def tiktok_cover_map(wanted_ids, token, max_pages=5):
    """Map aweme_id -> cover data URI for the wanted top-3 ids (TikHub)."""
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
                    du = _download(url)
                    if du:
                        covers[aid] = du
                wanted.discard(aid)
        if not payload.get("has_more"):
            break
        cursor = payload.get("max_cursor", 0)
    return covers


def _x_media_url(tweet):
    """First renderable media preview URL on a tweet (fields vary by provider)."""
    for m in (tweet.get("media") or []):
        for k in ("media_url_https", "preview_image_url", "thumbnail_url", "media_url", "url"):
            u = m.get(k)
            if isinstance(u, str) and u.startswith("http"):
                return u
    return None


def x_thumb_map(wanted_ids, token, max_pages=15):
    """Map tweet_id -> media data URI via GetXAPI (same contract as
    twitter_collector.get_tweets: tweets[] + has_more + next_cursor)."""
    import re as _re
    covers = {}
    if not token or not wanted_ids:
        return covers
    try:
        from utils import http_get_json
    except Exception:
        return covers
    wanted = set(str(x) for x in wanted_ids)
    headers = {"Authorization": f"Bearer {token}"}
    cursor = None
    for _ in range(max_pages):
        if not wanted:
            break
        params = {"userName": "kann_news"}
        if cursor:
            params["cursor"] = cursor
        try:
            data = http_get_json("https://api.getxapi.com/twitter/user/tweets",
                                 headers=headers, params=params, timeout=15, max_retries=2)
        except Exception as e:
            print(f"      x thumb fetch failed: {e}")
            break
        tweets = data.get("tweets")
        if not isinstance(tweets, list) or not tweets:
            break
        for t in tweets:
            tid = ""
            for k in ("id", "tweetId", "id_str", "rest_id"):
                if t.get(k):
                    tid = str(t[k])
                    break
            if not tid:
                m = _re.search(r"/status/(\d+)", t.get("url", ""))
                tid = m.group(1) if m else ""
            if tid and tid in wanted:
                url = _x_media_url(t)
                if url:
                    du = _download(url)
                    if du:
                        covers[tid] = du
                wanted.discard(tid)
        if not data.get("has_more") or not data.get("next_cursor"):
            break
        cursor = data["next_cursor"]
    return covers


# ---------------------------------------------------------------- Gemini helpers

def _gemini_text(prompt):
    """One Gemini text call, model fallback list. None on any failure."""
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key:
        return None
    try:
        from google import genai
    except Exception as e:
        print(f"   google-genai unavailable: {e}")
        return None
    client = genai.Client(api_key=api_key)
    for model in ["gemini-3.5-flash", "gemini-2.5-pro"]:
        try:
            resp = client.models.generate_content(model=model, contents=prompt)
            t = (resp.text or "").strip()
            if t:
                return t
        except Exception as e:
            print(f"   Gemini {model} failed: {e}")
    return None


def _parse_json(text):
    """Parse a JSON array/object, tolerating ``` fences and surrounding prose."""
    if not text:
        return None
    t = text.strip()
    if t.startswith("```"):
        t = t.strip("`")
        nl = t.find("\n")
        if nl != -1 and t[:nl].strip().lower() in ("json", ""):
            t = t[nl + 1:]
    try:
        return json.loads(t)
    except Exception:
        pass
    for op, cl in (("[", "]"), ("{", "}")):
        i, j = t.find(op), t.rfind(cl)
        if i != -1 and j > i:
            try:
                return json.loads(t[i:j + 1])
            except Exception:
                continue
    return None


# ---------------------------------------------------------------- reporters

import re as _re_mod

# 2-3 Hebrew words, apostrophes/geresh/hyphen allowed inside a word.
_NAME_RE = _re_mod.compile(r"^[֐-׿]+(?:[ ׳״'\"׳״\-][֐-׿]+){1,2}$")


def _looks_like_name(s):
    s = (s or "").strip()
    if not s or any(ch.isdigit() for ch in s):
        return False
    if 'צילום' in s or ':' in s or '@' in s:
        return False
    return bool(_NAME_RE.match(s))


def reporter_fallback(title):
    """Deterministic reporter credit: trailing '(שם כתב)' that looks like a
    person, else a bare @handle. '' when nothing credible."""
    title = str(title or "")
    m = _re_mod.search(r"\(([^)]{2,30})\)\s*$", title)
    if m and _looks_like_name(m.group(1)):
        return m.group(1).strip()
    h = _re_mod.search(r"@([A-Za-z0-9_]{2,30})", title)
    if h:
        return "@" + h.group(1)
    return ""


def _reporters_gemini(items):
    """items: list of (key, row). Returns {(key,id): reporter|None} or None."""
    payload = [{"platform": key, "id": r['_id'], "title": r['title']} for key, r in items]
    prompt = (
        "אתה מזהה את שם הכתב/ת שזוכה בקרדיט בכל כותרת של כאן חדשות.\n"
        "כללים: החזר שם אדם רק אם הוא מיוחס במפורש (בסוגריים בסוף, @שם משתמש, או שם מפורש). "
        "\"צילום: X\" הוא צלם ולא כתב — החזר null אלא אם מצוין גם כתב. "
        "לעולם אל תמציא שם. כשלא בטוח — null.\n"
        "החזר JSON תקין בלבד: רשימה של אובייקטים {platform, id, reporter}, reporter הוא מחרוזת או null.\n\n"
        + json.dumps(payload, ensure_ascii=False)
    )
    arr = _parse_json(_gemini_text(prompt))
    if not isinstance(arr, list):
        return None
    out = {}
    for o in arr:
        if isinstance(o, dict) and 'platform' in o and 'id' in o:
            rep = o.get('reporter')
            if isinstance(rep, str) and rep.strip() and rep.strip().lower() not in ('null', 'none'):
                out[(str(o['platform']), str(o['id']))] = rep.strip()
            else:
                out[(str(o['platform']), str(o['id']))] = None
    return out


def assign_reporters(plat_ctx, use_gemini):
    """Fill row['reporter'] (and the matching top-3 card) for every top-10 item.
    Gemini batch (one call) primary; deterministic regex fallback on failure."""
    items = [(c['key'], r) for c in plat_ctx for r in c.get('top10', [])]
    if not items:
        return "none"
    mode = "fallback"
    result = _reporters_gemini(items) if use_gemini else None
    if result is not None:
        mode = "gemini"
        for key, r in items:
            r['reporter'] = result.get((key, str(r['_id']))) or ""
    else:
        for key, r in items:
            r['reporter'] = reporter_fallback(r['title'])
    for c in plat_ctx:
        by_id = {str(r['_id']): r['reporter'] for r in c.get('top10', [])}
        for card in c.get('top3', []):
            card['reporter'] = by_id.get(str(card['_id']), "")
    return mode


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


def mark_anomalies(rows):
    """Flag ranks 4+ whose engagement clearly beats the week's median — a row
    that punches above its view rank. Calibrated floor, not per-row noise."""
    if len(rows) < 4:
        return
    engs = sorted(r['eng'] for r in rows)
    n = len(engs)
    med = engs[n // 2] if n % 2 else (engs[n // 2 - 1] + engs[n // 2]) / 2
    for i, r in enumerate(rows):
        r['anomaly'] = bool(i >= 3 and med > 0 and r['eng'] >= 1.4 * med)


def _cand(label, value, suffix, text):
    return dict(label=label, value=str(value), suffix=suffix, text=text)


def fun_fact_candidates(key, df):
    """4-6 deterministic candidate facts per platform (value copied verbatim by
    the LLM curator). candidates[0] is the platform's default (== the old fixed
    fun-fact) so a failed/invalid curation degrades to prior behavior."""
    c = []
    try:
        v = to_num(df['views'])
        vsum = float(v.sum())
        n = len(df)
        eng = engagement_series(key, df)
        med = float(v.median()) if n else 0.0

        # --- platform-specific PRIMARY (candidates[0] = default) ---
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

        # --- generic extras (give the curator week-to-week variety) ---
        if n >= 3 and med > 0 and float(v.max()) / med >= 1.5:
            r = float(v.max()) / med
            c.append(_cand('overperformer', f"{r:.1f}", '×', f"הפריט המוביל השבוע עשה פי {r:.1f} מחציון הצפיות בפלטפורמה"))
        if len(eng) and float(eng.max()) > 0:
            e = float(eng.max())
            c.append(_cand('eng_leader', f"{e:.1f}", '%', f"שיא מעורבות של {e:.1f}% על פריט בודד השבוע"))
    except Exception as ex:
        print(f"      candidates failed for {key}: {ex}")
    return c


def curate_fun_facts(plat_ctx, use_gemini):
    """One Gemini call picks the most newsworthy candidate per platform and
    phrases it; the headline number is validated against the candidate's own
    values (LLM never does arithmetic). Invalid/failed -> keep the default."""
    if not use_gemini:
        return
    payload = []
    for c in plat_ctx:
        cands = c.get('fun_fact_candidates') or []
        if cands:
            payload.append({"platform": c['key'],
                            "candidates": [{"label": x['label'], "number": x['value'], "suffix": x['suffix'], "hint": x['text']} for x in cands]})
    if not payload:
        return
    prompt = (
        "אתה עורך של כאן חדשות. לכל פלטפורמה קיבלת מספר עובדות-מועמדות עם מספר קבוע.\n"
        "בחר לכל פלטפורמה את העובדה הכי מעניינת/עיתונאית, ונסח משפט אחד קצר בעברית סביבה.\n"
        "העתק את המספר מילה-במילה מה-number של המועמד שבחרת. אל תמציא מספרים ואל תשנה ספרות.\n"
        "החזר JSON תקין בלבד: רשימה של {platform, chosen_label, headline_number, sentence}.\n\n"
        + json.dumps(payload, ensure_ascii=False)
    )
    arr = _parse_json(_gemini_text(prompt))
    if not isinstance(arr, list):
        return
    chosen = {str(o.get('platform')): o for o in arr if isinstance(o, dict)}
    for c in plat_ctx:
        cands = c.get('fun_fact_candidates') or []
        o = chosen.get(c['key'])
        if not cands or not o:
            continue
        num = str(o.get('headline_number', '')).strip()
        lbl = o.get('chosen_label')
        match = next((x for x in cands if x['label'] == lbl and x['value'] == num), None) \
            or next((x for x in cands if x['value'] == num), None)
        sent = o.get('sentence')
        if match and isinstance(sent, str) and sent.strip():
            c['fun_fact'] = dict(value=match['value'], suffix=match['suffix'], text=clean_title(sent, 120))


def process_platform(key, df_all, window, thumbs_enabled, fb_token, tikhub_token, getxapi_token=''):
    """Return (ctx, metrics) for one platform. Filters to this/last week."""
    import pandas as pd
    meta = PLATFORMS[key]
    dc, tc, idc = meta['date_col'], meta['title_col'], meta['id_col']
    (ws, we), (lws, lwe) = window['this'], window['last']

    ctx = dict(key=key, name=meta['name'], colors=meta['colors'],
               icon=icon_svg(key, 56), icon_big=icon_svg(key, 120),
               has_data=False, has_anomaly=False, top3=[], top10=[], fun_fact=None,
               weekly_views_fmt="0")

    metrics = dict(key=key, name=meta['name'], accent=meta['colors']['accent'],
                   this_views=0, last_views=0, delta=None, top_item=None, med_eng=0.0)

    if df_all is None or df_all.empty or dc not in df_all.columns or 'views' not in df_all.columns:
        ctx['delta'] = build_delta(None)
        return ctx, metrics

    d = df_all.copy()
    d['_date'] = d[dc].astype(str).str.slice(0, 10)
    d['views'] = to_num(d['views'])

    this_df = d[(d['_date'] >= ws) & (d['_date'] <= we)]
    last_df = d[(d['_date'] >= lws) & (d['_date'] <= lwe)]

    this_views = float(this_df['views'].sum())
    last_views = float(last_df['views'].sum())
    metrics['this_views'] = this_views
    metrics['last_views'] = last_views

    delta_pct = ((this_views - last_views) / last_views * 100) if last_views > 0 else None
    metrics['delta'] = delta_pct
    dl = build_delta(delta_pct)
    ctx.update(delta_str=dl['str_signed'], delta_arrow=dl['arrow'],
               delta_color=dl['color'], has_delta=dl['has_delta'])
    ctx['weekly_views_fmt'] = fmt_num(this_views)

    if this_df.empty:
        return ctx, metrics

    ctx['has_data'] = True
    this_df = this_df.copy()
    this_df['_eng'] = engagement_series(key, this_df)
    this_df = this_df.sort_values('views', ascending=False)

    rows = []
    for i, (_, r) in enumerate(this_df.head(10).iterrows(), start=1):
        rows.append(dict(rank=i, title=clean_title(r.get(tc, '')),
                         reporter="",  # filled later by assign_reporters()
                         views=float(r['views']), views_fmt=fmt_num(r['views']),
                         eng=float(r['_eng']), eng_str=f"{float(r['_eng']):.1f}%",
                         highlight=(i <= 3), anomaly=False, thumb=None,
                         _id=str(r.get(idc, ''))))
    mark_anomalies(rows)
    ctx['top10'] = rows
    ctx['has_anomaly'] = any(x['anomaly'] for x in rows)

    med = sorted(x['eng'] for x in rows)
    metrics['med_eng'] = med[len(med) // 2] if med else 0.0

    # top-3 highlight cards
    medals = ['🥇', '🥈', '🥉']
    top3 = []
    for i, r in enumerate(rows[:3]):
        top3.append(dict(medal=medals[i], title=clean_title(r['title'], 90),
                         views_fmt=r['views_fmt'], reporter=r['reporter'],
                         thumb=None, _id=r['_id']))
    ctx['top3'] = top3

    # biggest single item (for the learnings slide)
    if rows:
        top = rows[0]
        metrics['top_item'] = dict(title=top['title'], views=top['views'],
                                   views_fmt=top['views_fmt'], accent=meta['colors']['accent'],
                                   name=meta['name'])

    cands = fun_fact_candidates(key, this_df)
    ctx['fun_fact_candidates'] = cands
    ctx['fun_fact'] = dict(value=cands[0]['value'], suffix=cands[0]['suffix'],
                           text=cands[0]['text']) if cands else None

    # thumbnails for ALL top-10 rows (+ the top-3 cards), per platform
    if thumbs_enabled:
        ids = [r['_id'] for r in rows]
        tmap = {}
        if key == 'youtube':
            tmap = {i: thumb_youtube(i) for i in ids}
        elif key == 'facebook':
            tmap = {i: thumb_fb(i, fb_token) for i in ids}
        elif key == 'instagram':
            tmap = {i: thumb_ig(i, fb_token) for i in ids}
        elif key == 'tiktok':
            tmap = tiktok_cover_map(ids, tikhub_token, max_pages=8)
        elif key == 'x':
            tmap = x_thumb_map(ids, getxapi_token, max_pages=15)
        for r in rows:
            r['thumb'] = tmap.get(r['_id'])
        for c in top3:
            c['thumb'] = tmap.get(c['_id'])

    return ctx, metrics


# ---------------------------------------------------------------- followers

def followers_map(gc):
    """Latest follower count per platform key from the wide followers sheet."""
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

def build_learnings(metrics_list, daily_insights, use_gemini):
    """1 primary + 2 secondary insights. Stats are always real/computed;
    Gemini only rewrites the Hebrew prose (never invents numbers)."""
    have = [m for m in metrics_list if m['this_views'] > 0]

    # seed 1: biggest positive mover (fallback: top platform by views)
    movers = [m for m in have if m['delta'] is not None]
    primary_m = None
    if movers:
        primary_m = max(movers, key=lambda m: m['delta'])
        if primary_m['delta'] <= 0:
            primary_m = None
    if primary_m is None and have:
        primary_m = max(have, key=lambda m: m['this_views'])

    if primary_m and primary_m.get('delta') is not None and primary_m['delta'] > 0:
        d = build_delta(primary_m['delta'])
        primary = dict(emoji='🚀', stat=d['str_signed'], stat_color=GREEN,
                       headline=f"{primary_m['name']} בצמיחה החדה ביותר השבוע",
                       body=f"{primary_m['name']} עלה ב{d['str_abs']} בצפיות מול השבוע שעבר — שם כדאי למקד את המאמץ בשבוע הבא.")
    elif primary_m:
        primary = dict(emoji='🏆', stat=fmt_num(primary_m['this_views']), stat_color='#111',
                       headline=f"{primary_m['name']} מוביל את הצפיות השבוע",
                       body=f"{primary_m['name']} ריכז את מרב הצפיות מכל הפלטפורמות השבוע.")
    else:
        primary = dict(emoji='📊', stat='', stat_color='#111',
                       headline='שבוע ללא נתונים', body='לא נאספו נתונים לשבוע זה.')

    secondary = []
    # seed 2: biggest single item of the week
    tops = [m['top_item'] for m in have if m.get('top_item')]
    if tops:
        big = max(tops, key=lambda t: t['views'])
        secondary.append(dict(emoji='⚡', stat=big['views_fmt'], stat_color=big['accent'],
                              headline=clean_title(big['title'], 70),
                              sub=f"הפריט הכי נצפה השבוע · {big['name']}"))

    # seed 3: a declining platform, else the best-engagement platform
    declining = [m for m in have if m['delta'] is not None and m['delta'] < 0]
    if declining:
        worst = min(declining, key=lambda m: m['delta'])
        d = build_delta(worst['delta'])
        secondary.append(dict(emoji='📉', stat=d['str_signed'], stat_color=RED,
                              headline=f"{worst['name']} במגמת ירידה",
                              sub="הפלטפורמה היחידה עם צפיות יורדות — שווה בדיקה"))
    else:
        eng_ranked = [m for m in have if m.get('med_eng', 0) > 0]
        if eng_ranked:
            best = max(eng_ranked, key=lambda m: m['med_eng'])
            secondary.append(dict(emoji='💬', stat=f"{best['med_eng']:.1f}%", stat_color=best['accent'],
                                  headline=f"{best['name']} עם המעורבות החזקה ביותר",
                                  sub="חציון המעורבות הגבוה ביותר מבין הפלטפורמות"))

    while len(secondary) < 2:
        secondary.append(dict(emoji='📈', stat='', stat_color='#111',
                              headline='עוד שבוע של סושיאל', sub=''))
    secondary = secondary[:2]

    if use_gemini:
        try:
            polished = _gemini_polish(primary, secondary, daily_insights, metrics_list)
            if polished:
                primary, secondary = polished
        except Exception as e:
            print(f"   ⚠️ Gemini polish failed, using computed insights: {e}")

    # split the primary stat into number + suffix for the big type treatment
    pm, ps = split_suffix(primary.get('stat', '') or '')
    primary['stat_main'], primary['stat_suffix'] = pm, ps
    return dict(primary=primary, secondary=secondary)


def _gemini_polish(primary, secondary, daily_insights, metrics_list):
    """Ask Gemini to rewrite headline/body text only. Stats stay fixed."""
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key:
        return None
    from google import genai
    client = genai.Client(api_key=api_key)

    seeds = [
        {"slot": "primary", "stat": primary['stat'], "headline": primary['headline'], "text": primary['body']},
        {"slot": "secondary1", "stat": secondary[0]['stat'], "headline": secondary[0]['headline'], "text": secondary[0]['sub']},
        {"slot": "secondary2", "stat": secondary[1]['stat'], "headline": secondary[1]['headline'], "text": secondary[1]['sub']},
    ]
    numbers = "; ".join(f"{m['name']}: {fmt_num(m['this_views'])} צפיות" for m in metrics_list if m['this_views'] > 0)
    insights_txt = "\n".join(f"- {v}" for _, v in daily_insights[:14]) if daily_insights else "אין"

    prompt = f"""אתה עורך תוכן של כאן חדשות. קיבלת 3 תובנות שבועיות עם מספרים קבועים.
שכתב אך ורק את הכותרת (headline, עד 6 מילים) והטקסט (text, משפט אחד) של כל תובנה — בעברית, ענייני, מבוסס נתונים.
אל תמציא מספרים חדשים ואל תשנה את שדה ה-stat. החזר JSON תקין בלבד: מערך של 3 אובייקטים עם השדות slot, headline, text.

מספרי השבוע: {numbers}

תובנות יומיות מהשבוע:
{insights_txt}

התובנות לשכתוב:
{json.dumps(seeds, ensure_ascii=False)}
"""
    text = ""
    for model in ["gemini-3.5-flash", "gemini-2.5-pro"]:
        try:
            resp = client.models.generate_content(model=model, contents=prompt)
            text = (resp.text or "").strip()
            if text:
                break
        except Exception as e:
            print(f"   Gemini model {model} failed: {e}")
    if not text:
        return None
    if text.startswith("```"):
        text = text.strip("`")
        text = text[text.find("["):text.rfind("]") + 1]
    arr = json.loads(text)
    by_slot = {o.get('slot'): o for o in arr}
    if 'primary' in by_slot:
        primary['headline'] = clean_title(by_slot['primary'].get('headline', primary['headline']), 60)
        primary['body'] = clean_title(by_slot['primary'].get('text', primary['body']), 200)
    for i, slot in enumerate(('secondary1', 'secondary2')):
        if slot in by_slot:
            secondary[i]['headline'] = clean_title(by_slot[slot].get('headline', secondary[i]['headline']), 60)
            secondary[i]['sub'] = clean_title(by_slot[slot].get('text', secondary[i]['sub']), 140)
    return primary, secondary


# ---------------------------------------------------------------- assembly

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


def font_faces():
    """@font-face list for the weights present on disk. The licensed Light(300)
    weight is absent (not on the VPS either), so we map 300 -> Regular(400)
    EXPLICITLY: an @font-face for weight 300 pointing at the Regular OTF, rather
    than relying on the browser's implicit nearest-weight fallback. 300 is not
    used in the slide markup, so this is purely defensive."""
    weights = [('Light', 300), ('Regular', 400), ('Semibold', 600),
               ('Bold', 700), ('Black', 900)]
    faces, present = [], {}
    for label, w in weights:
        path = os.path.join(FONTS_DIR, f"SimplerPro_HLAR-{label}.otf")
        if os.path.exists(path):
            faces.append(dict(weight=w, uri=_file_uri(path)))
            present[w] = _file_uri(path)
    if 300 not in present and 400 in present:  # explicit Light -> Regular
        faces.append(dict(weight=300, uri=present[400]))
    return faces


def _file_uri(path):
    from pathlib import Path
    return Path(path).resolve().as_uri()


def load_mark():
    """Inline the Kan square mark as a reusable path (no <defs>/id/<style>, so
    embedding it on several slides can't collide). Orange (#f30) for the light
    slides. Returns {viewbox, d, fill} or None -> template uses the typographic
    fallback."""
    import re
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
    # A full pre-composed lockup PNG, if one is ever dropped in, wins over the
    # SVG-mark + wordmark lockup. None is the normal case today.
    for name in ("kan-news-full-black-a.png", "kan-news-full-black.png"):
        p = os.path.join(ASSETS_DIR, name)
        if os.path.exists(p):
            with open(p, 'rb') as f:
                return "data:image/png;base64," + base64.b64encode(f.read()).decode()
    return None


def build_context(gc, use_gemini, thumbs_enabled):
    window = compute_window()
    fb_token = os.environ.get('FACEBOOK_TOKEN', '')
    tikhub_token = os.environ.get('TIKHUB_TOKEN', '')
    getxapi_token = os.environ.get('GETXAPI_KEY', '')

    plat_ctx, metrics = [], []
    for key in PLATFORM_ORDER:
        df = load_sheet(gc, PLATFORMS[key]['sheet']) if gc else None
        c, m = process_platform(key, df, window, thumbs_enabled, fb_token,
                                tikhub_token, getxapi_token)
        plat_ctx.append(c)
        metrics.append(m)

    mode = assign_reporters(plat_ctx, use_gemini)
    print(f"   reporters: {mode}")
    curate_fun_facts(plat_ctx, use_gemini)

    follows = followers_map(gc)
    daily_insights = _daily_insights(gc, window)
    return _assemble(plat_ctx, metrics, follows, daily_insights, window, use_gemini)


def _daily_insights(gc, window):
    if gc is None:
        return []
    import pandas as pd
    df = load_sheet(gc, "תובנות יומיות")
    if df.empty or 'date' not in df.columns:
        return []
    ws, we = window['this']
    df = df[(df['date'].astype(str) >= ws) & (df['date'].astype(str) <= we)].sort_values('date')
    return [(r['date'], r.get('insights', '')) for _, r in df.iterrows() if str(r.get('insights', '')).strip()]


def _assemble(plat_ctx, metrics, follows, daily_insights, window, use_gemini):
    by_key = {c['key']: c for c in plat_ctx}
    m_by_key = {m['key']: m for m in metrics}

    # dynamic order: by real weekly views, empty platforms last
    ordered = sorted(PLATFORM_ORDER, key=lambda k: (-m_by_key[k]['this_views'], PLATFORM_ORDER.index(k)))
    platforms = []
    for i, key in enumerate(ordered, start=1):
        c = by_key[key]
        c['rank_label'] = f"פלטפורמה {i} מתוך 5 · לפי צפיות"
        platforms.append(c)

    total_this = sum(m['this_views'] for m in metrics)
    total_last = sum(m['last_views'] for m in metrics)
    hero_delta = build_delta(((total_this - total_last) / total_last * 100) if total_last > 0 else None)
    tmain, tsuf = split_suffix(fmt_num(total_this))

    max_views = max((m_by_key[k]['this_views'] for k in ordered), default=0)
    overview_rows = []
    for key in ordered:
        c, m = by_key[key], m_by_key[key]
        dl = build_delta(m['delta'])
        overview_rows.append(dict(
            name=c['name'], icon=icon_svg(key, 42),
            followers_fmt=fmt_num(follows[key]) if key in follows else "",
            bar_color=c['colors']['bar'],
            bar_pct=int(round(m['this_views'] / max_views * 100)) if max_views > 0 else 0,
            views_fmt=fmt_num(m['this_views']),
            has_delta=dl['has_delta'], delta_arrow=dl['arrow'],
            delta_str=dl['str_abs'], delta_color=dl['color']))

    learnings = build_learnings(metrics, daily_insights, use_gemini)

    # closing credits: unique reporter names (skip @handles), in rank order
    reporters, seen = [], set()
    for key in ordered:
        for r in by_key[key].get('top10', []):
            nm = (r.get('reporter') or '').strip()
            if nm and not nm.startswith('@') and nm not in seen:
                seen.add(nm)
                reporters.append(nm)
    reporters = reporters[:12]

    return dict(
        font_faces=font_faces(),
        logo_black=logo_data_uri(),
        mark=load_mark(),
        week=dict(range_str=fmt_date_range(window['d1'], window['d2']),
                  range_short=f"{window['d1']:%d/%m}–{window['d2']:%d/%m}"),
        hero=dict(total_fmt=fmt_num(total_this), total_main=tmain, total_suffix=tsuf,
                  has_delta=hero_delta['has_delta'], delta_str=hero_delta['str_signed'],
                  delta_arrow=hero_delta['arrow'], delta_color=hero_delta['color']),
        overview_rows=overview_rows,
        platforms=platforms,
        learnings=learnings,
        reporters=reporters,  # extracted credits; empty -> closing block omitted
    )


# ---------------------------------------------------------------- mock

def build_mock_context():
    """Realistic hardcoded data run through the real pipeline (no creds/net)."""
    import pandas as pd
    window = compute_window()
    ws, we = window['this']
    d1, d2 = window['d1'], window['d2']
    # spread dates across the window
    days = [(d1 + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]

    def rows(specs, extra_cols):
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
    ], None)

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
    ], None)

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
    ], None)

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
    ], None)

    x = rows([
        ("הציוץ שסיכם את מסיבת העיתונאים", 380_000, 4200, 300, 1600, 4.4, dict(tweet_id="x1", retweets=1600, replies=300, quotes=200, type="Text")),
        ("שרשור: כל מה שקרה היום בכנסת", 240_000, 3000, 400, 1100, 5.7, dict(tweet_id="x2", retweets=1100, replies=400, quotes=150, type="Text")),
        ("עדכון בזק מהשטח בזמן אמת", 190_000, 2200, 250, 800, 3.9, dict(tweet_id="x3", retweets=800, replies=250, quotes=90, type="Text")),
        ("ציטוט היום מהמליאה", 150_000, 2600, 600, 900, 6.2, dict(tweet_id="x4", retweets=900, replies=600, quotes=120, type="Text")),
        ("מבזק: תוצאות ההצבעה", 130_000, 1800, 200, 600, 4.8, dict(tweet_id="x5", retweets=600, replies=200, quotes=70, type="Text")),
        ("שרשור נתונים על יוקר המחיה", 110_000, 1200, 150, 400, 3.3, dict(tweet_id="x6", retweets=400, replies=150, quotes=50, type="Text")),
        ("הפרשן מגיב בזמן אמת", 95_000, 1400, 350, 500, 5.1, dict(tweet_id="x7", retweets=500, replies=350, quotes=60, type="Text")),
        ("תמונת השבוע עם הקשר", 80_000, 900, 120, 300, 2.7, dict(tweet_id="x8", retweets=300, replies=120, quotes=40, type="Photo")),
        ("עדכון תחבורה ומזג אוויר", 68_000, 600, 80, 180, 1.9, dict(tweet_id="x9", retweets=180, replies=80, quotes=20, type="Text")),
        ("סיכום היום בשלושה ציוצים", 55_000, 800, 200, 250, 3.4, dict(tweet_id="x10", retweets=250, replies=200, quotes=30, type="Text")),
    ], None)

    sheets = dict(youtube=yt, tiktok=tk, instagram=ig, facebook=fb, x=x)

    # a tiny inline SVG "thumbnail" for the #1 card of each platform, to
    # exercise the <img> branch alongside the placeholder branch.
    def mock_thumb(label, color):
        svg = (f"<svg xmlns='http://www.w3.org/2000/svg' width='320' height='172'>"
               f"<rect width='320' height='172' fill='{color}'/>"
               f"<text x='160' y='96' font-size='22' fill='white' text-anchor='middle' "
               f"font-family='sans-serif'>{label}</text></svg>")
        return "data:image/svg+xml;base64," + base64.b64encode(svg.encode()).decode()

    plat_ctx, metrics = [], []
    for key in PLATFORM_ORDER:
        c, m = process_platform(key, sheets[key], window, False, '', '')
        # inject a mock thumb on every row + card so the table-row-thumb UI can
        # be judged visually (real runs download these per platform)
        accent = PLATFORMS[key]['colors']['accent']
        for r in c.get('top10', []):
            r['thumb'] = mock_thumb(c['name'], accent)
        for card in c.get('top3', []):
            card['thumb'] = mock_thumb(c['name'], accent)
        plat_ctx.append(c)
        metrics.append(m)

    # bump last-week to make deltas non-trivial (real mode gets this from the
    # sheet's prior-window rows; here we synthesize it and sync the slide header)
    prev = dict(youtube=0.82, tiktok=0.66, instagram=0.93, facebook=1.05, x=0.89)
    ctx_by_key = {c['key']: c for c in plat_ctx}
    for m in metrics:
        m['last_views'] = m['this_views'] * prev.get(m['key'], 0.9)
        m['delta'] = ((m['this_views'] - m['last_views']) / m['last_views'] * 100) if m['last_views'] else None
        dl = build_delta(m['delta'])
        c = ctx_by_key[m['key']]
        c.update(delta_str=dl['str_signed'], delta_arrow=dl['arrow'],
                 delta_color=dl['color'], has_delta=dl['has_delta'])

    # reporter extraction (fallback/regex path — no creds in mock) + fun-fact
    # defaults (curation needs Gemini, off in mock)
    mode = assign_reporters(plat_ctx, use_gemini=False)
    print(f"   reporters (mock): {mode}")
    curate_fun_facts(plat_ctx, use_gemini=False)

    follows = dict(youtube=412000, tiktok=286000, instagram=531000, facebook=1_200_000, x=348000)
    daily = [(days[i], f"תובנה יומית לדוגמה מספר {i+1}") for i in range(3)]
    return _assemble(plat_ctx, metrics, follows, daily, window, use_gemini=False)


# ---------------------------------------------------------------- render

def render(context):
    from jinja2 import Environment, FileSystemLoader, select_autoescape
    env = Environment(loader=FileSystemLoader(HERE),
                      autoescape=select_autoescape(['html', 'xml']))
    # icons/svg are trusted markup we build ourselves -> mark safe
    from markupsafe import Markup
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


def to_pdf(html_path):
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
        # QA screenshots of the first two slides
        sections = page.query_selector_all('section.slide')
        for i in range(min(2, len(sections))):
            sections[i].screenshot(path=os.path.join(OUT_DIR, f"slide_{i+1}.png"))
        browser.close()
    print(f"   ✅ wrote {pdf_path}")
    return pdf_path


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description="Weekly social summary deck generator")
    ap.add_argument('--mock', action='store_true', help="render with mock data (no creds/network)")
    ap.add_argument('--no-thumbs', action='store_true', help="skip thumbnail downloads")
    ap.add_argument('--no-gemini', action='store_true', help="skip Gemini, use computed insights")
    args = ap.parse_args()

    print(f"\n{'='*60}\n📊 Weekly Deck Generator — {datetime.now(IL_TZ):%Y-%m-%d %H:%M}\n{'='*60}\n")

    if args.mock:
        print("🧪 MOCK mode — no creds, no network.")
        context = build_mock_context()
    else:
        print("📥 Loading data from Google Sheets...")
        gc = get_client()
        context = build_context(gc, use_gemini=not args.no_gemini,
                                thumbs_enabled=not args.no_thumbs)

    print("🎨 Rendering template...")
    html_path = render(context)
    print("🖨️  Rendering PDF (Playwright)...")
    pdf_path = to_pdf(html_path)

    print(f"\n✅ Done. Open {pdf_path} to review the filled deck.")


if __name__ == "__main__":
    main()
