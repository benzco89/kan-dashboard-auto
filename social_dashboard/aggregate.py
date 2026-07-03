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


def _tw_type(v):
    s = (v or "").strip().lower()
    if "video" in s:
        return "Video"
    if "photo" in s or "image" in s:
        return "Photo"
    return "Text"


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
    base = value - weekly
    growth_pct = round(weekly / base * 100, 2) if base > 0 else 0.0
    return {"value": value, "weekly_change": weekly, "growth_pct": growth_pct}


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
    yt, fb, ig, tw = data["youtube"], data["facebook"], data["instagram"], data["twitter"]
    foll = data["followers"]

    yt_p = _filter(yt, "published_at", start, end)
    fb_p = _filter(fb, "date", start, end)
    ig_p = _filter(ig, "date", start, end)
    tw_p = _filter(tw, "date", start, end)
    yt_pp = _filter(yt, "published_at", p_start, p_end)
    fb_pp = _filter(fb, "date", p_start, p_end)
    ig_pp = _filter(ig, "date", p_start, p_end)
    tw_pp = _filter(tw, "date", p_start, p_end)

    def _sum(items, f):
        return sum(_num(it.get(f)) for it in items)

    views = _sum(yt_p, "views") + _sum(fb_p, "views") + _sum(ig_p, "views") + _sum(tw_p, "views")
    prev_views = _sum(yt_pp, "views") + _sum(fb_pp, "views") + _sum(ig_pp, "views") + _sum(tw_pp, "views")
    reach = _sum(fb_p, "reach") + _sum(ig_p, "reach")  # Twitter has no reach metric
    prev_reach = _sum(fb_pp, "reach") + _sum(ig_pp, "reach")
    inter = (_sum(yt_p, "likes") + _sum(yt_p, "comments")
             + _sum(fb_p, "likes") + _sum(fb_p, "comments") + _sum(fb_p, "shares")
             + _sum(ig_p, "likes") + _sum(ig_p, "comments") + _sum(ig_p, "saved") + _sum(ig_p, "shares")
             + _sum(tw_p, "likes") + _sum(tw_p, "replies") + _sum(tw_p, "retweets") + _sum(tw_p, "quotes"))
    prev_inter = (_sum(yt_pp, "likes") + _sum(yt_pp, "comments")
                  + _sum(fb_pp, "likes") + _sum(fb_pp, "comments") + _sum(fb_pp, "shares")
                  + _sum(ig_pp, "likes") + _sum(ig_pp, "comments") + _sum(ig_pp, "saved") + _sum(ig_pp, "shares")
                  + _sum(tw_pp, "likes") + _sum(tw_pp, "replies") + _sum(tw_pp, "retweets") + _sum(tw_pp, "quotes"))
    content = len(yt_p) + len(fb_p) + len(ig_p) + len(tw_p)
    prev_content = len(yt_pp) + len(fb_pp) + len(ig_pp) + len(tw_pp)

    # daily views per platform
    yt_dates, yt_s = _daily(yt, "published_at", ["views"], start, end)
    _, fb_s = _daily(fb, "date", ["views"], start, end)
    _, ig_s = _daily(ig, "date", ["views"], start, end)
    _, tw_s = _daily(tw, "date", ["views"], start, end)

    # per-platform sparkline = last 14 days of daily views
    spark_start = end - timedelta(days=13)
    _, yt_sp = _daily(yt, "published_at", ["views"], spark_start, end)
    _, fb_sp = _daily(fb, "date", ["views"], spark_start, end)
    _, ig_sp = _daily(ig, "date", ["views"], spark_start, end)
    _, tw_sp = _daily(tw, "date", ["views"], spark_start, end)

    foll_yt = _follower_metric(foll, "yt_subscribers", "yt_subscribers_change")
    foll_fb = _follower_metric(foll, "fb_followers", "fb_followers_change")
    foll_ig = _follower_metric(foll, "ig_followers", "ig_followers_change")
    foll_tw = _follower_metric(foll, "tw_followers", "tw_followers_change")
    total_val = foll_yt["value"] + foll_fb["value"] + foll_ig["value"] + foll_tw["value"]
    total_week = foll_yt["weekly_change"] + foll_fb["weekly_change"] + foll_ig["weekly_change"] + foll_tw["weekly_change"]
    total_base = total_val - total_week
    total_growth = round(total_week / total_base * 100, 2) if total_base > 0 else 0.0

    # top content across platforms in period
    top = []
    for it in yt_p:
        top.append({"platform": "youtube", "title": it.get("title", ""), "views": _int(it.get("views")),
                    "type": "Short" if _yt_type(it.get("video_type")) == "Shorts" else "Video",
                    "url": it.get("video_url", ""),
                    "date": (_parse_date(it.get("published_at")) or "").__str__() if it.get("published_at") else ""})
    for it in fb_p:
        top.append({"platform": "facebook", "title": it.get("title", ""), "views": _int(it.get("views")),
                    "type": _fb_type(it.get("type")), "url": it.get("permalink", ""),
                    "date": (_parse_date(it.get("date")) or "").__str__() if it.get("date") else ""})
    for it in ig_p:
        top.append({"platform": "instagram", "title": it.get("caption", ""), "views": _int(it.get("views")),
                    "type": _ig_type(it.get("type")), "url": it.get("permalink", ""),
                    "date": (_parse_date(it.get("date")) or "").__str__() if it.get("date") else ""})
    for it in tw_p:
        top.append({"platform": "twitter", "title": it.get("text", ""), "views": _int(it.get("views")),
                    "type": _tw_type(it.get("type")), "url": it.get("permalink", ""),
                    "date": (_parse_date(it.get("date")) or "").__str__() if it.get("date") else ""})
    top.sort(key=lambda x: x["views"], reverse=True)
    top_content = top[:8]
    # "what didn't land": the lowest-viewed *measured* items in the period
    # (views>0 so we don't surface posts that simply haven't accrued yet).
    bottom_content = sorted([x for x in top if x["views"] > 0], key=lambda x: x["views"])[:8]

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
            "youtube": foll_yt, "facebook": foll_fb, "instagram": foll_ig, "twitter": foll_tw,
            "total": {"value": total_val, "weekly_change": total_week, "growth_pct": total_growth},
        },
        "kpis": {
            "views": {"value": round(views), "prev": round(prev_views)},
            "reach": {"value": round(reach), "prev": round(prev_reach)},
            "interactions": {"value": round(inter), "prev": round(prev_inter)},
            "content": {"value": content, "prev": prev_content},
        },
        "chart": {"dates": yt_dates, "youtube": yt_s["views"], "facebook": fb_s["views"], "instagram": ig_s["views"], "twitter": tw_s["views"]},
        "platforms": {
            "youtube": {"followers": foll_yt["value"], "views": round(_sum(yt_p, "views")), "spark": yt_sp["views"]},
            "facebook": {"followers": foll_fb["value"], "views": round(_sum(fb_p, "views")), "spark": fb_sp["views"]},
            "instagram": {"followers": foll_ig["value"], "views": round(_sum(ig_p, "views")), "spark": ig_sp["views"]},
            "twitter": {"followers": foll_tw["value"], "views": round(_sum(tw_p, "views")), "spark": tw_sp["views"]},
        },
        "top_content": top_content,
        "bottom_content": bottom_content,
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

    # engagement rate - one definition across all platforms: interactions / views
    total_views = sum(_num(v.get("views")) for v in cur)
    total_inter = sum(_num(v.get("likes")) + _num(v.get("comments")) for v in cur)
    avg_eng = (total_inter / total_views * 100) if total_views else 0

    videos = []
    for v in cur:
        views = _int(v.get("views"))
        likes = _int(v.get("likes"))
        comments = _int(v.get("comments"))
        videos.append({
            "title": v.get("title", ""),
            "date": (_parse_date(v.get("published_at")) or "").__str__() if v.get("published_at") else "",
            "type": "Short" if _yt_type(v.get("video_type")) == "Shorts" else "Video",
            "views": views,
            "likes": likes,
            "comments": comments,
            "like_rate": round(_num(v.get("like_rate")), 2),
            "engagement": round((likes + comments) / views * 100, 1) if views else 0,
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
            "avg_engagement": round(avg_eng, 2),
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

    # engagement rate - one definition across all platforms: interactions / views
    # (interactions = likes + comments + shares; clicks excluded so FB is
    # comparable to the other platforms)
    total_views = sum(_num(p.get("views")) for p in cur)
    total_reach = sum(_num(p.get("reach")) for p in cur)
    total_shares = sum(_num(p.get("shares")) for p in cur)
    total_inter = sum(_num(p.get("likes")) + _num(p.get("comments")) + _num(p.get("shares")) for p in cur)
    avg_eng = (total_inter / total_views * 100) if total_views else 0
    virality = (total_shares / total_views * 100) if total_views else 0
    # reach comes only from Meta insights and has broken before (v25); when the
    # whole column is zero the page should say so instead of drawing zeros
    reach_ok = (not cur) or any(_num(p.get("reach")) > 0 for p in cur)

    posts = []
    for p in cur:
        views = _num(p.get("views"))
        likes = _int(p.get("likes"))
        comments = _int(p.get("comments"))
        shares = _int(p.get("shares"))
        posts.append({
            "title": p.get("title", ""),
            "date": (_parse_date(p.get("date")) or "").__str__() if p.get("date") else "",
            "type": _fb_type(p.get("type")),
            "views": _int(views),
            "reach": _int(p.get("reach")),
            "likes": likes,
            "shares": shares,
            "engagement": round((likes + comments + shares) / views * 100, 1) if views else 0,
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
        "reach_ok": reach_ok,
        "types": [{"name": t, "reach": round(types[t]["reach"]), "count": types[t]["count"]} for t in type_order],
        "summary": {
            "total": len(cur),
            "reels": types["Reels"]["count"],
            "videos": types["Videos"]["count"],
            "images": types["Images"]["count"],
            "virality": round(virality, 2),
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

    # engagement rate - one definition across all platforms: interactions / views
    # (interactions = likes + comments + saves + shares, same set the overview sums)
    total_views = sum(_num(p.get("views")) for p in cur)
    total_shares = sum(_num(p.get("shares")) for p in cur)
    total_saved = sum(_num(p.get("saved")) for p in cur)
    total_inter = sum(_num(p.get("likes")) + _num(p.get("comments")) + _num(p.get("saved")) + _num(p.get("shares")) for p in cur)
    avg_eng = (total_inter / total_views * 100) if total_views else 0
    virality = (total_shares / total_views * 100) if total_views else 0
    save_rate = (total_saved / total_views * 100) if total_views else 0
    reach_ok = (not cur) or any(_num(p.get("reach")) > 0 for p in cur)

    # v25 reel metrics (fill in as the collector refreshes its 7-day window)
    skip_rates = [_num(p.get("skip_rate")) for p in cur if _num(p.get("skip_rate")) > 0]
    avg_skip = (sum(skip_rates) / len(skip_rates)) if skip_rates else 0
    total_fb_views = sum(_num(p.get("fb_views")) for p in cur)
    fb_share = (total_fb_views / (total_views + total_fb_views) * 100) if (total_views + total_fb_views) else 0

    posts = []
    for p in cur:
        views = _num(p.get("views"))
        likes = _int(p.get("likes"))
        comments = _int(p.get("comments"))
        saved = _int(p.get("saved"))
        shares = _int(p.get("shares"))
        posts.append({
            "title": p.get("caption", ""),
            "date": (_parse_date(p.get("date")) or "").__str__() if p.get("date") else "",
            "type": _ig_type(p.get("type")),
            "views": _int(views),
            "reach": _int(p.get("reach")),
            "saved": saved,
            "shares": shares,
            "skip_rate": round(_num(p.get("skip_rate")), 1),
            "engagement": round((likes + comments + saved + shares) / views * 100, 1) if views else 0,
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
        "reach_ok": reach_ok,
        "donut": {"reel": len(reel), "photo": len(photo), "carousel": len(carousel)},
        "bars": {"dates": bar_dates, "saved": bar_series["saved"], "shares": bar_series["shares"]},
        "summary": {
            "total": len(cur),
            "reels": len(reel),
            "photos": len(photo),
            "carousels": len(carousel),
            "virality": round(virality, 2),
            "save_rate": round(save_rate, 2),
            "avg_engagement": round(avg_eng, 2),
            "avg_skip_rate": round(avg_skip, 1),
            "fb_views_share": round(fb_share, 1),
        },
        "posts": posts,
    }


def build_twitter(data, days):
    start, end, p_start, p_end = _window(days)
    tw = data["twitter"]
    foll = data["followers"]
    cur = _filter(tw, "date", start, end)

    dates, series = _daily(tw, "date", ["views"], start, end)

    video = [p for p in cur if _tw_type(p.get("type")) == "Video"]
    photo = [p for p in cur if _tw_type(p.get("type")) == "Photo"]
    text = [p for p in cur if _tw_type(p.get("type")) == "Text"]

    # likes vs retweets: last 14 days
    bar_start = end - timedelta(days=13)
    bar_dates, bar_series = _daily(tw, "date", ["likes", "retweets"], bar_start, end)

    total_views = sum(_num(p.get("views")) for p in cur)
    total_eng = sum(_num(p.get("likes")) + _num(p.get("retweets")) + _num(p.get("replies")) + _num(p.get("quotes")) for p in cur)
    total_rt = sum(_num(p.get("retweets")) for p in cur)
    avg_eng = (total_eng / total_views * 100) if total_views else 0
    virality = (total_rt / total_views * 100) if total_views else 0

    posts = []
    for p in cur:
        views = _num(p.get("views"))
        likes = _int(p.get("likes"))
        rts = _int(p.get("retweets"))
        replies = _int(p.get("replies"))
        quotes = _int(p.get("quotes"))
        eng = ((likes + rts + replies + quotes) / views * 100) if views else 0
        posts.append({
            "title": p.get("text", ""),
            "date": (_parse_date(p.get("date")) or "").__str__() if p.get("date") else "",
            "type": _tw_type(p.get("type")),
            "views": _int(views),
            "likes": likes,
            "retweets": rts,
            "replies": replies,
            "engagement": round(eng, 1),
            "url": p.get("permalink", ""),
        })
    posts.sort(key=lambda x: x["views"], reverse=True)

    return {
        "range": days,
        "last_date": _last_data_date(data),
        "followers": _follower_metric(foll, "tw_followers", "tw_followers_change"),
        "kpis": {
            "views": _delta_pair(tw, "date", "views", days),
            "likes": _delta_pair(tw, "date", "likes", days),
            "retweets": _delta_pair(tw, "date", "retweets", days),
        },
        "chart": {"dates": dates, "views": series["views"]},
        "donut": {"video": len(video), "photo": len(photo), "text": len(text)},
        "bars": {"dates": bar_dates, "likes": bar_series["likes"], "retweets": bar_series["retweets"]},
        "summary": {
            "total": len(cur),
            "videos": len(video),
            "photos": len(photo),
            "texts": len(text),
            "virality": round(virality, 2),
            "avg_engagement": round(avg_eng, 2),
        },
        "posts": posts,
    }
