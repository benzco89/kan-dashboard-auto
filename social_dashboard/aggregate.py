"""
Aggregation layer: turns parsed sheet rows into the exact shapes each
dashboard page needs. All numbers are computed here (server is the source of
truth); the browser only formats and draws.

Date model mirrors the collectors: data is pulled ~08:30 daily, so "today" has
no data yet. The latest data day is yesterday. A range of N days means the
window [today-N, yesterday]; the previous comparison window is the N days
before that.
"""

from datetime import date, datetime, timedelta

try:
    from zoneinfo import ZoneInfo
    _TZ = ZoneInfo("Asia/Jerusalem")
except Exception:  # pragma: no cover
    _TZ = None


# ---------- small helpers ----------

def _num(x):
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(",", "")
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _int(x):
    return int(round(_num(x)))


def _parse_date(s):
    if not s:
        return None
    s = str(s).strip()[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def israel_today():
    now = datetime.now(_TZ) if _TZ else datetime.now()
    return now.date()


def _window(days):
    today = israel_today()
    end = today - timedelta(days=1)            # yesterday = latest data
    start = today - timedelta(days=days)
    prev_end = start - timedelta(days=1)
    prev_start = today - timedelta(days=2 * days)
    return start, end, prev_start, prev_end


def _in_range(d, start, end):
    return d is not None and start <= d <= end


def _filter(items, date_key, start, end):
    out = []
    for it in items:
        d = _parse_date(it.get(date_key))
        if _in_range(d, start, end):
            out.append(it)
    return out


def _daily(items, date_key, fields, start, end):
    """Continuous per-day series over [start, end] (zero-filled)."""
    buckets = {}
    for it in items:
        d = _parse_date(it.get(date_key))
        if not _in_range(d, start, end):
            continue
        key = d.isoformat()
        b = buckets.setdefault(key, {f: 0.0 for f in fields})
        for f in fields:
            b[f] += _num(it.get(f))
    dates, series = [], {f: [] for f in fields}
    cur = start
    while cur <= end:
        key = cur.isoformat()
        dates.append(key)
        b = buckets.get(key)
        for f in fields:
            series[f].append(round(b[f]) if b else 0)
        cur += timedelta(days=1)
    return dates, series


def _delta_pair(items, date_key, field, days):
    start, end, p_start, p_end = _window(days)
    cur = sum(_num(it.get(field)) for it in _filter(items, date_key, start, end))
    prev = sum(_num(it.get(field)) for it in _filter(items, date_key, p_start, p_end))
    return {"value": round(cur), "prev": round(prev)}


# ---------- type normalization ----------

def _yt_type(v):
    s = (v or "").strip().lower()
    return "Shorts" if "short" in s else "Regular"


def _fb_type(v):
    s = (v or "").strip().lower()
    if "reel" in s:
        return "Reels"
    if "video" in s:
        return "Videos"
    if "image" in s or "photo" in s:
        return "Images"
    if "link" in s:
        return "Links"
    return "Videos"


def _ig_type(v):
    s = (v or "").strip().lower()
    if "reel" in s:
        return "Reel"
    if "carousel" in s or "album" in s:
        return "Carousel"
    if "photo" in s or "image" in s:
        return "Photo"
    return "Reel"


# ---------- followers ----------

def _followers_latest(rows):
    return rows[-1] if rows else {}


def _follower_metric(rows, value_key, change_key):
    """Latest known value + change over the last 7 days.

    Robust to the occasional blank cell: the value is the most recent
    non-empty entry, and the weekly change sums the daily-change column the
    collector maintains (rather than diffing two rows that might be blank).
    """
    if not rows:
        return {"value": 0, "weekly_change": 0}
    value = 0
    for r in reversed(rows):
        v = _int(r.get(value_key))
        if v:
            value = v
            break
    weekly = sum(_int(r.get(change_key)) for r in rows[-7:])
    return {"value": value, "weekly_change": weekly}


def _last_data_date(data):
    rows = data.get("followers", [])
    if rows:
        d = _parse_date(rows[-1].get("date"))
        if d:
            return d.isoformat()
    return (israel_today() - timedelta(days=1)).isoformat()


# ---------- page builders ----------

def build_overview(data, days):
    start, end, p_start, p_end = _window(days)
    yt, fb, ig = data["youtube"], data["facebook"], data["instagram"]
    foll = data["followers"]

    yt_p = _filter(yt, "published_at", start, end)
    fb_p = _filter(fb, "date", start, end)
    ig_p = _filter(ig, "date", start, end)
    yt_pp = _filter(yt, "published_at", p_start, p_end)
    fb_pp = _filter(fb, "date", p_start, p_end)
    ig_pp = _filter(ig, "date", p_start, p_end)

    def _sum(items, f):
        return sum(_num(it.get(f)) for it in items)

    views = _sum(yt_p, "views") + _sum(fb_p, "views") + _sum(ig_p, "views")
    prev_views = _sum(yt_pp, "views") + _sum(fb_pp, "views") + _sum(ig_pp, "views")
    reach = _sum(fb_p, "reach") + _sum(ig_p, "reach")
    prev_reach = _sum(fb_pp, "reach") + _sum(ig_pp, "reach")
    inter = (_sum(yt_p, "likes") + _sum(yt_p, "comments")
             + _sum(fb_p, "likes") + _sum(fb_p, "comments") + _sum(fb_p, "shares")
             + _sum(ig_p, "likes") + _sum(ig_p, "comments") + _sum(ig_p, "saved") + _sum(ig_p, "shares"))
    prev_inter = (_sum(yt_pp, "likes") + _sum(yt_pp, "comments")
                  + _sum(fb_pp, "likes") + _sum(fb_pp, "comments") + _sum(fb_pp, "shares")
                  + _sum(ig_pp, "likes") + _sum(ig_pp, "comments") + _sum(ig_pp, "saved") + _sum(ig_pp, "shares"))
    content = len(yt_p) + len(fb_p) + len(ig_p)
    prev_content = len(yt_pp) + len(fb_pp) + len(ig_pp)

    # daily views per platform
    yt_dates, yt_s = _daily(yt, "published_at", ["views"], start, end)
    _, fb_s = _daily(fb, "date", ["views"], start, end)
    _, ig_s = _daily(ig, "date", ["views"], start, end)

    # per-platform sparkline = last 14 days of daily views
    spark_start = end - timedelta(days=13)
    _, yt_sp = _daily(yt, "published_at", ["views"], spark_start, end)
    _, fb_sp = _daily(fb, "date", ["views"], spark_start, end)
    _, ig_sp = _daily(ig, "date", ["views"], spark_start, end)

    foll_yt = _follower_metric(foll, "yt_subscribers", "yt_subscribers_change")
    foll_fb = _follower_metric(foll, "fb_followers", "fb_followers_change")
    foll_ig = _follower_metric(foll, "ig_followers", "ig_followers_change")
    total_val = foll_yt["value"] + foll_fb["value"] + foll_ig["value"]
    total_week = foll_yt["weekly_change"] + foll_fb["weekly_change"] + foll_ig["weekly_change"]

    # top content across platforms in period
    top = []
    for it in yt_p:
        top.append({"platform": "youtube", "title": it.get("title", ""), "views": _int(it.get("views")),
                    "type": "Short" if _yt_type(it.get("video_type")) == "Shorts" else "Video",
                    "url": it.get("video_url", "")})
    for it in fb_p:
        top.append({"platform": "facebook", "title": it.get("title", ""), "views": _int(it.get("views")),
                    "type": _fb_type(it.get("type")), "url": it.get("permalink", "")})
    for it in ig_p:
        top.append({"platform": "instagram", "title": it.get("caption", ""), "views": _int(it.get("views")),
                    "type": _ig_type(it.get("type")), "url": it.get("permalink", "")})
    top.sort(key=lambda x: x["views"], reverse=True)
    top = top[:8]

    # latest AI insight
    insight = {"date": "", "paragraphs": []}
    ins_rows = [r for r in data["insights"] if (r.get("insights") or "").strip()]
    if ins_rows:
        last = ins_rows[-1]
        text = last.get("insights", "")
        paras = [p.strip() for p in text.split("\n") if p.strip()]
        d = _parse_date(last.get("date"))
        insight = {"date": d.isoformat() if d else last.get("date", ""), "paragraphs": paras}

    avg_eng = (inter / views * 100) if views else 0

    return {
        "range": days,
        "last_date": _last_data_date(data),
        "followers": {
            "youtube": foll_yt, "facebook": foll_fb, "instagram": foll_ig,
            "total": {"value": total_val, "weekly_change": total_week},
        },
        "kpis": {
            "views": {"value": round(views), "prev": round(prev_views)},
            "reach": {"value": round(reach), "prev": round(prev_reach)},
            "interactions": {"value": round(inter), "prev": round(prev_inter)},
            "content": {"value": content, "prev": prev_content},
        },
        "chart": {"dates": yt_dates, "youtube": yt_s["views"], "facebook": fb_s["views"], "instagram": ig_s["views"]},
        "platforms": {
            "youtube": {"followers": foll_yt["value"], "views": round(_sum(yt_p, "views")), "spark": yt_sp["views"]},
            "facebook": {"followers": foll_fb["value"], "views": round(_sum(fb_p, "views")), "spark": fb_sp["views"]},
            "instagram": {"followers": foll_ig["value"], "views": round(_sum(ig_p, "views")), "spark": ig_sp["views"]},
        },
        "top_content": top,
        "insight": insight,
        "quick": {
            "avg_engagement": round(avg_eng, 2),
            "total_reach": round(reach),
            "yt_videos": len(yt_p),
            "fb_ig_posts": len(fb_p) + len(ig_p),
        },
    }


def build_youtube(data, days):
    start, end, p_start, p_end = _window(days)
    yt = data["youtube"]
    foll = data["followers"]
    cur = _filter(yt, "published_at", start, end)

    dates, series = _daily(yt, "published_at", ["views"], start, end)

    shorts = [v for v in cur if _yt_type(v.get("video_type")) == "Shorts"]
    regular = [v for v in cur if _yt_type(v.get("video_type")) == "Regular"]
    shorts_views = sum(_num(v.get("views")) for v in shorts)
    regular_views = sum(_num(v.get("views")) for v in regular)

    like_rates = [_num(v.get("like_rate")) for v in cur if _num(v.get("views")) > 0]
    avg_like = (sum(like_rates) / len(like_rates)) if like_rates else 0

    videos = []
    for v in cur:
        views = _int(v.get("views"))
        videos.append({
            "title": v.get("title", ""),
            "date": (_parse_date(v.get("published_at")) or "").__str__() if v.get("published_at") else "",
            "type": "Short" if _yt_type(v.get("video_type")) == "Shorts" else "Video",
            "views": views,
            "likes": _int(v.get("likes")),
            "comments": _int(v.get("comments")),
            "like_rate": round(_num(v.get("like_rate")), 2),
            "url": v.get("video_url", ""),
        })
    videos.sort(key=lambda x: x["views"], reverse=True)

    return {
        "range": days,
        "last_date": _last_data_date(data),
        "subscribers": _follower_metric(foll, "yt_subscribers", "yt_subscribers_change"),
        "kpis": {
            "views": _delta_pair(yt, "published_at", "views", days),
            "likes": _delta_pair(yt, "published_at", "likes", days),
            "comments": _delta_pair(yt, "published_at", "comments", days),
        },
        "chart": {"dates": dates, "views": series["views"]},
        "donut": {"shorts_views": round(shorts_views), "regular_views": round(regular_views)},
        "summary": {
            "total": len(cur), "shorts": len(shorts), "regular": len(regular),
            "avg_like_rate": round(avg_like, 2),
        },
        "videos": videos,
    }


def build_facebook(data, days):
    start, end, p_start, p_end = _window(days)
    fb = data["facebook"]
    foll = data["followers"]
    cur = _filter(fb, "date", start, end)

    dates, series = _daily(fb, "date", ["views", "reach"], start, end)

    type_order = ["Reels", "Videos", "Images", "Links"]
    types = {t: {"reach": 0.0, "count": 0} for t in type_order}
    for p in cur:
        t = _fb_type(p.get("type"))
        types[t]["reach"] += _num(p.get("reach"))
        types[t]["count"] += 1

    total_eng = sum(_num(p.get("total_engagement")) for p in cur)
    total_reach = sum(_num(p.get("reach")) for p in cur)
    avg_eng = (total_eng / total_reach * 100) if total_reach else 0

    posts = []
    for p in cur:
        reach = _num(p.get("reach"))
        likes = _int(p.get("likes"))
        shares = _int(p.get("shares"))
        posts.append({
            "title": p.get("title", ""),
            "date": (_parse_date(p.get("date")) or "").__str__() if p.get("date") else "",
            "type": _fb_type(p.get("type")),
            "views": _int(p.get("views")),
            "reach": _int(reach),
            "likes": likes,
            "shares": shares,
            "engagement": round((likes + shares) / reach * 100, 1) if reach else 0,
            "url": p.get("permalink", ""),
        })
    posts.sort(key=lambda x: x["views"], reverse=True)

    return {
        "range": days,
        "last_date": _last_data_date(data),
        "followers": _follower_metric(foll, "fb_followers", "fb_followers_change"),
        "kpis": {
            "views": _delta_pair(fb, "date", "views", days),
            "reach": _delta_pair(fb, "date", "reach", days),
            "shares": _delta_pair(fb, "date", "shares", days),
        },
        "chart": {"dates": dates, "views": series["views"], "reach": series["reach"]},
        "types": [{"name": t, "reach": round(types[t]["reach"]), "count": types[t]["count"]} for t in type_order],
        "summary": {
            "total": len(cur),
            "reels": types["Reels"]["count"],
            "videos": types["Videos"]["count"],
            "images": types["Images"]["count"],
            "avg_engagement": round(avg_eng, 2),
        },
        "posts": posts,
    }


def build_instagram(data, days):
    start, end, p_start, p_end = _window(days)
    ig = data["instagram"]
    foll = data["followers"]
    cur = _filter(ig, "date", start, end)

    dates, series = _daily(ig, "date", ["views", "reach"], start, end)

    reel = [p for p in cur if _ig_type(p.get("type")) == "Reel"]
    photo = [p for p in cur if _ig_type(p.get("type")) == "Photo"]
    carousel = [p for p in cur if _ig_type(p.get("type")) == "Carousel"]

    # saves vs shares: last 14 days
    bar_start = end - timedelta(days=13)
    bar_dates, bar_series = _daily(ig, "date", ["saved", "shares"], bar_start, end)

    total_inter = sum(_num(p.get("total_interactions")) for p in cur)
    total_reach = sum(_num(p.get("reach")) for p in cur)
    avg_eng = (total_inter / total_reach * 100) if total_reach else 0

    posts = []
    for p in cur:
        reach = _num(p.get("reach"))
        saved = _int(p.get("saved"))
        shares = _int(p.get("shares"))
        posts.append({
            "title": p.get("caption", ""),
            "date": (_parse_date(p.get("date")) or "").__str__() if p.get("date") else "",
            "type": _ig_type(p.get("type")),
            "views": _int(p.get("views")),
            "reach": _int(reach),
            "saved": saved,
            "shares": shares,
            "engagement": round((saved + shares) / reach * 100, 1) if reach else 0,
            "url": p.get("permalink", ""),
        })
    posts.sort(key=lambda x: x["views"], reverse=True)

    return {
        "range": days,
        "last_date": _last_data_date(data),
        "followers": _follower_metric(foll, "ig_followers", "ig_followers_change"),
        "kpis": {
            "views": _delta_pair(ig, "date", "views", days),
            "saved": _delta_pair(ig, "date", "saved", days),
            "shares": _delta_pair(ig, "date", "shares", days),
        },
        "chart": {"dates": dates, "views": series["views"], "reach": series["reach"]},
        "donut": {"reel": len(reel), "photo": len(photo), "carousel": len(carousel)},
        "bars": {"dates": bar_dates, "saved": bar_series["saved"], "shares": bar_series["shares"]},
        "summary": {
            "total": len(cur),
            "reels": len(reel),
            "photos": len(photo),
            "carousels": len(carousel),
            "avg_engagement": round(avg_eng, 2),
        },
        "posts": posts,
    }
