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


def _fmt(n):
    """Compact K/M formatting for numbers embedded in alert notes."""
    n = int(round(_num(n)))
    if abs(n) >= 1_000_000:
        return ("%.1f" % (n / 1_000_000)).rstrip("0").rstrip(".") + "M"
    if abs(n) >= 1000:
        return ("%.1f" % (n / 1000)).rstrip("0").rstrip(".") + "K"
    return str(n)


def _signed(n):
    n = int(round(_num(n)))
    return ("+" if n >= 0 else "-") + _fmt(abs(n))


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


# ---------- alerts / anomaly detection ----------
#
# Turns the per-post tables into a ranked feed of "things worth noticing":
# content that beat or missed the platform's own recent baseline, unusual
# spread, weak hooks, and follower spikes/drops. Baselines are robust
# (median-based) so a single viral hit doesn't move the bar, and every alert
# carries the number + the baseline it fired against so it stays explainable.
# Thresholds live here as constants so they're easy to tune once we see real
# output.

_MIN_BASELINE = 5           # need at least this many posts to trust a median
CAP_PER_KIND_PLATFORM = 4   # keep only the N strongest alerts per (kind, platform)

# Thresholds calibrated against the live sheet's own distributions (2026-07,
# 30–90d windows). The platforms differ structurally in variance, so the hit
# bar is per-platform (a flat cross-platform ratio flags ~top 4% on IG but
# ~top 15% on YouTube). Each value targets roughly the platform's p95 = the
# genuine standouts. Re-run analyze_thresholds.py to recalibrate if the account
# profile shifts. See _median() note: baselines are robust to single hits.

# viral hit — views as a multiple of the platform's median views: (hit, strong)
_HIT = {
    "youtube":   (7.0, 15.0),
    "facebook":  (4.5, 10.0),
    "instagram": (3.5, 7.0),
    "twitter":   (5.0, 11.0),
}
# viral spread — ABSOLUTE share-rate % (share-rate medians are near-zero and
# noisy, so an absolute p95 bar is far more stable than a x-median ratio): (spread, strong)
_SPREAD = {
    "facebook":  (0.18, 0.35),
    "instagram": (0.80, 1.50),
    "twitter":   (0.14, 0.31),
}
_SAVE_RATE = 0.13           # IG absolute save-rate % (p95) -> reference value
_HOOK_SKIP = 55.0           # reel skip_rate % (p90 "worst hooks")
_FLOP_ENG_RATIO = 0.4       # engagement < 0.4x median while reach is above median
_FOLLOWER_RATIO = 3.0       # |daily change| >= 3x median |change|
_FOLLOWER_FLOOR = 50        # ignore tiny follower wiggles
_SHARE_FLOOR = 15           # need real shares before calling something viral
_SAVE_FLOOR = 15


def _median(xs):
    xs = sorted(xs)
    n = len(xs)
    if not n:
        return 0.0
    m = n // 2
    return xs[m] if n % 2 else (xs[m - 1] + xs[m]) / 2.0


def _type_label(plat, raw):
    if plat == "youtube":
        return "Short" if _yt_type(raw) == "Shorts" else "Video"
    if plat == "facebook":
        return _fb_type(raw)
    if plat == "instagram":
        return _ig_type(raw)
    if plat == "twitter":
        return _tw_type(raw)
    return ""


# per-platform field map: date_key, title_key, url_key, interactions_fn, share_key, has_reach
_ALERT_PLATFORMS = {
    "youtube": ("published_at", "title", "video_url",
                lambda p: _num(p.get("likes")) + _num(p.get("comments")), None, False),
    "facebook": ("date", "title", "permalink",
                 lambda p: _num(p.get("likes")) + _num(p.get("comments")) + _num(p.get("shares")),
                 "shares", True),
    "instagram": ("date", "caption", "permalink",
                  lambda p: (_num(p.get("likes")) + _num(p.get("comments"))
                             + _num(p.get("saved")) + _num(p.get("shares"))),
                  "shares", True),
    "twitter": ("date", "text", "permalink",
                lambda p: (_num(p.get("likes")) + _num(p.get("retweets"))
                           + _num(p.get("replies")) + _num(p.get("quotes"))),
                "retweets", False),
}


def _norm_posts(items, plat, start, end):
    """Normalize a platform's in-window posts to a common shape with derived rates."""
    date_key, title_key, url_key, inter_fn, share_key, has_reach = _ALERT_PLATFORMS[plat]
    out = []
    for it in _filter(items, date_key, start, end):
        views = _num(it.get("views"))
        if views <= 0:
            continue
        d = _parse_date(it.get(date_key))
        shares = _num(it.get(share_key)) if share_key else 0.0
        saved = _num(it.get("saved"))
        raw_type = it.get("type") or it.get("video_type") or ""
        out.append({
            "platform": plat,
            "title": it.get(title_key, ""),
            "url": it.get(url_key, ""),
            "date": d.isoformat() if d else "",
            "type": _type_label(plat, raw_type),
            "raw_type": raw_type,
            "views": views,
            "reach": _num(it.get("reach")) if has_reach else 0.0,
            "shares": shares,
            "saved": saved,
            "skip": _num(it.get("skip_rate")),
            "eng": inter_fn(it) / views * 100.0,
            "share_rate": shares / views * 100.0,
            "save_rate": saved / views * 100.0,
        })
    return out


def _alert(kind, severity, p, metric_label, value, baseline, note, impact):
    return {
        "kind": kind, "severity": severity, "platform": p["platform"],
        "title": p["title"], "url": p["url"], "date": p["date"], "type": p["type"],
        "metric_label": metric_label, "value": round(value, 2), "baseline": round(baseline, 2),
        "ratio": round((value / baseline) if baseline else 0, 1),
        "note": note, "_impact": impact,
    }


def _post_alerts(posts, plat):
    """Hit / spread / flop rules. Thresholds are per-platform; impact is stored
    as "x over the firing bar" so different kinds rank comparably in the feed."""
    out = []
    if len(posts) < _MIN_BASELINE:
        return out
    med_views = _median([p["views"] for p in posts])
    med_eng = _median([p["eng"] for p in posts if p["eng"] > 0])
    med_reach = _median([p["reach"] for p in posts if p["reach"] > 0])
    med_share = _median([p["share_rate"] for p in posts if p["shares"] > 0])
    hit, strong = _HIT.get(plat, (4.0, 8.0))
    spread = _SPREAD.get(plat)

    for p in posts:
        # viral hit — views far above the platform's own median
        if med_views > 0 and p["views"] >= med_views * hit:
            r = p["views"] / med_views
            sev = "high" if r >= strong else "med"
            out.append(_alert("viral_hit", sev, p, "צפיות", p["views"], med_views,
                              "פי %.1f מחציון הצפיות בפלטפורמה" % r, r / hit))
        # viral spread — shared far more per view than typical (absolute bar)
        if spread and p["shares"] >= _SHARE_FLOOR and p["share_rate"] >= spread[0]:
            sev = "high" if p["share_rate"] >= spread[1] else "med"
            rm = (p["share_rate"] / med_share) if med_share > 0 else 0
            note = ("שותף פי %.1f מחציון הפלטפורמה (%s שיתופים)" % (rm, _fmt(p["shares"]))
                    if rm >= 2 else
                    "שיעור שיתוף %.2f%% — בעשירון העליון (%s שיתופים)" % (p["share_rate"], _fmt(p["shares"])))
            out.append(_alert("viral_spread", sev, p, "שיעור שיתוף", p["share_rate"],
                              med_share if med_share > 0 else spread[0], note, p["share_rate"] / spread[0]))
        # flop — got distribution (reach above median) but engagement collapsed
        if (med_eng > 0 and med_reach > 0 and p["reach"] >= med_reach
                and 0 < p["eng"] < med_eng * _FLOP_ENG_RATIO):
            r = med_eng / p["eng"]
            out.append(_alert("flop", "med", p, "מעורבות", p["eng"], med_eng,
                              "חשיפה מעל החציון אך מעורבות נמוכה פי %.1f מהרגיל" % r, r / (1 / _FLOP_ENG_RATIO)))
    return out


def _ig_extra_alerts(posts):
    """Instagram-only rules: high save-rate (reference value) + weak reel hook."""
    out = []
    if len(posts) < _MIN_BASELINE:
        return out
    med_save = _median([p["save_rate"] for p in posts if p["saved"] > 0])
    for p in posts:
        if p["saved"] >= _SAVE_FLOOR and p["save_rate"] >= _SAVE_RATE:
            rm = (p["save_rate"] / med_save) if med_save > 0 else 0
            note = ("נשמר פי %.1f מחציון הפלטפורמה (%s שמירות)" % (rm, _fmt(p["saved"]))
                    if rm >= 2 else
                    "שיעור שמירה %.2f%% — בעשירון העליון (%s שמירות)" % (p["save_rate"], _fmt(p["saved"])))
            out.append(_alert("high_saves", "med", p, "שיעור שמירה", p["save_rate"],
                              med_save if med_save > 0 else _SAVE_RATE, note, p["save_rate"] / _SAVE_RATE))
    reels = [p for p in posts if "reel" in str(p["raw_type"]).lower() and p["skip"] > 0]
    if len(reels) >= _MIN_BASELINE:
        med_skip = _median([p["skip"] for p in reels])
        for p in reels:
            if p["skip"] >= _HOOK_SKIP:
                out.append(_alert("weak_hook", "med", p, "Skip%", p["skip"], med_skip or _HOOK_SKIP,
                                  "%.0f%% דילגו בשניות הראשונות — מהוק החלשים בפלטפורמה" % p["skip"],
                                  p["skip"] / _HOOK_SKIP))
    return out


_FOLLOWER_KEYS = [
    ("youtube", "yt_subscribers_change"),
    ("facebook", "fb_followers_change"),
    ("instagram", "ig_followers_change"),
    ("twitter", "tw_followers_change"),
]


def _follower_alerts(foll, days):
    """Flag days whose follower change is a big outlier vs the window's own norm."""
    start, end, _p1, _p2 = _window(days)
    rows = [r for r in foll if _in_range(_parse_date(r.get("date")), start, end)]
    out = []
    for plat, ckey in _FOLLOWER_KEYS:
        changes = []
        for r in rows:
            v = r.get(ckey)
            if v is None or str(v).strip() == "":
                continue
            changes.append((_parse_date(r.get("date")), _num(v)))
        vals = [abs(c) for _d, c in changes if c != 0]
        if len(vals) < _MIN_BASELINE:
            continue
        med = _median(vals)
        if med <= 0:
            continue
        for d, c in changes:
            if abs(c) >= med * _FOLLOWER_RATIO and abs(c) >= _FOLLOWER_FLOOR:
                spike = c > 0
                r = abs(c) / med
                out.append({
                    "kind": "follower_spike" if spike else "follower_drop",
                    "severity": "high" if r >= _FOLLOWER_RATIO * 1.6 else "med",
                    "platform": plat,
                    "title": "זינוק בעוקבים" if spike else "צניחה בעוקבים",
                    "url": "", "date": d.isoformat() if d else "",
                    "type": "עוקבים", "metric_label": "שינוי יומי",
                    "value": round(c), "baseline": round(med), "ratio": round(r, 1),
                    "note": "%s עוקבים ביום — פי %.1f מהתנודה היומית הרגילה" % (_signed(c), r),
                    "_impact": r / _FOLLOWER_RATIO,
                })
    return out


def build_alerts(data, days):
    start, end, _p1, _p2 = _window(days)
    alerts = []
    for plat in _ALERT_PLATFORMS:
        posts = _norm_posts(data[plat], plat, start, end)
        alerts.extend(_post_alerts(posts, plat))
        if plat == "instagram":
            alerts.extend(_ig_extra_alerts(posts))
    alerts.extend(_follower_alerts(data["followers"], days))

    # cap each (kind, platform) to its strongest N by impact, so no single
    # category — nor a high-volume platform like Twitter's firehose — floods the
    # feed. An alerts page is only useful if it surfaces the few standouts.
    by_kp = {}
    for a in alerts:
        by_kp.setdefault((a["kind"], a["platform"]), []).append(a)
    alerts = []
    for items in by_kp.values():
        items.sort(key=lambda a: -a.get("_impact", 0))
        alerts.extend(items[:CAP_PER_KIND_PLATFORM])

    sev_rank = {"high": 0, "med": 1}
    alerts.sort(key=lambda a: (sev_rank.get(a["severity"], 2), -a.get("_impact", 0)))
    for a in alerts:
        a.pop("_impact", None)

    def _count(kind):
        return sum(1 for a in alerts if a["kind"] == kind)

    summary = {
        "total": len(alerts),
        "high": sum(1 for a in alerts if a["severity"] == "high"),
        "hits": _count("viral_hit"),
        "spread": _count("viral_spread"),
        "saves": _count("high_saves"),
        "flops": _count("flop"),
        "hooks": _count("weak_hook"),
        "followers": _count("follower_spike") + _count("follower_drop"),
    }
    return {
        "range": days,
        "last_date": _last_data_date(data),
        "alerts": alerts,
        "summary": summary,
    }
