"""
Aggregation layer: turns parsed sheet rows into the exact shapes each
dashboard page needs. All numbers are computed here (server is the source of
truth); the browser only formats and draws.

Date model mirrors the collectors: data is pulled ~08:30 daily, so "today" has
no data yet. The latest data day is yesterday. A range of N days means the
window [today-N, yesterday]; the previous comparison window is the N days
before that.
"""

import re
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

    yt_analyses = _comment_analyses(data, "youtube")

    videos = []
    for v in cur:
        views = _int(v.get("views"))
        likes = _int(v.get("likes"))
        comments = _int(v.get("comments"))
        videos.append({
            "analysis": yt_analyses.get(str(v.get("video_id", "")).strip()),
            "title": v.get("title", ""),
            "date": (_parse_date(v.get("published_at")) or "").__str__() if v.get("published_at") else "",
            "type": "Short" if _yt_type(v.get("video_type")) == "Shorts" else "Video",
            "views": views,
            "likes": likes,
            "comments": comments,
            "like_rate": round(_num(v.get("like_rate")), 2),
            "engagement": round((likes + comments) / views * 100, 1) if views else 0,
            "url": v.get("video_url", ""),
            # extra depth for the drill-down card
            "comment_rate": round(_num(v.get("comment_rate")), 2),
            "duration": v.get("duration_formatted", "") or "",
            "views_delta": _int(v.get("views_delta")),
            "thumb": v.get("thumbnail_url", "") or "",
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


def _comment_analyses(data, platform):
    """Gemini comment analyses keyed by post/media id, filtered per platform.
    Legacy rows (written before the platform column existed) are Instagram."""
    out = {}
    for a in data.get("comment_analysis", []):
        mid = str(a.get("media_id", "")).strip()
        plat = str(a.get("platform", "")).strip() or "instagram"
        if not mid or plat != platform:
            continue
        out[mid] = {
            "summary": a.get("summary", ""),
            "why": a.get("why_it_worked", ""),
            "themes": [t.strip() for t in str(a.get("themes", "")).split(";") if t.strip()],
            "top_comments": [t.strip() for t in str(a.get("top_comments", "")).split("|") if t.strip()],
            "pos": _int(a.get("sentiment_positive")),
            "neg": _int(a.get("sentiment_negative")),
            "neu": _int(a.get("sentiment_neutral")),
            "critique": _int(a.get("coverage_criticism")),
            "controversy": str(a.get("controversy", "")).strip() == "כן",
            "n": _int(a.get("comments_pulled")),
        }
    return out


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

    analyses = _comment_analyses(data, "facebook")

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
            # extra depth for the drill-down card. `likes` is TOTAL reactions;
            # the breakdown columns are the non-like reactions (love/haha/...),
            # so thumbs-up = likes - sum(breakdown). Breakdown is only collected
            # since 2026-07-05, so the modal hides it when the sum is zero.
            "comments": comments,
            "love": _int(p.get("love")),
            "haha": _int(p.get("haha")),
            "wow": _int(p.get("wow")),
            "sad": _int(p.get("sad")),
            "angry": _int(p.get("angry")),
            "clicks": _int(p.get("clicks")),
            "avg_watch": round(_num(p.get("avg_watch_sec")), 1),
            "total_watch_min": _int(p.get("total_watch_min")),
            "share_rate": round(shares / views * 100, 2) if views else 0,
            "analysis": analyses.get(str(p.get("post_id", "")).strip()),
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

    analyses = _comment_analyses(data, "instagram")

    posts = []
    for p in cur:
        views = _num(p.get("views"))
        likes = _int(p.get("likes"))
        comments = _int(p.get("comments"))
        saved = _int(p.get("saved"))
        shares = _int(p.get("shares"))
        fb_v = _num(p.get("fb_views"))
        a = analyses.get(str(p.get("media_id", "")).strip())
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
            # extra depth for the drill-down card (collected but not shown in the table)
            "likes": likes,
            "comments": comments,
            "reposts": _int(p.get("reposts")),
            "total_interactions": _int(p.get("total_interactions")) or (likes + comments + saved + shares),
            "avg_watch": round(_num(p.get("avg_watch_sec")), 1),
            "fb_views": _int(fb_v),
            "total_views": _int(p.get("total_views")),
            "save_rate": round(saved / views * 100, 2) if views else 0,
            "share_rate": round(shares / views * 100, 2) if views else 0,
            "fb_share": round(fb_v / (views + fb_v) * 100, 1) if (views + fb_v) else 0,
            # conversion - the API exposes these for feed posts only, not reels
            "profile_visits": _int(p.get("profile_visits")),
            "follows": _int(p.get("follows")),
            "analysis": a,
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
        "stories": build_stories(data, days),
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
            # extra depth for the drill-down card
            "quotes": quotes,
            "bookmarks": _int(p.get("bookmarks")),
            "total_engagement": _int(p.get("total_engagement")),
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


def build_stories(data, days):
    """Instagram Stories page. Stories are captured once daily (they live 24h),
    so this covers stories that were live at collection time — a partial but
    honest window. Stories expose conversion/navigation metrics that feed posts
    do not: profile_visits, follows, and the tap/swipe/exit breakdown."""
    start, end, p_start, p_end = _window(days)
    st = data.get("stories", [])
    cur = _filter(st, "date", start, end)

    dates, series = _daily(st, "date", ["views", "reach"], start, end)

    def _sum(items, f):
        return sum(_num(it.get(f)) for it in items)

    n = len(cur)
    total_views = _sum(cur, "views")
    total_reach = _sum(cur, "reach")
    total_pv = _sum(cur, "profile_visits")

    nav = {k: round(_sum(cur, k)) for k in ("taps_forward", "taps_back", "swipes_forward", "exits")}
    exit_rates = [_num(s.get("exit_rate")) for s in cur if _num(s.get("views")) > 0]
    avg_exit = (sum(exit_rates) / len(exit_rates)) if exit_rates else 0

    stories = []
    for s in cur:
        views = _num(s.get("views"))
        stories.append({
            "date": (_parse_date(s.get("date")) or "").__str__() if s.get("date") else "",
            "time": s.get("time", ""),
            "type": "Video" if "video" in str(s.get("type", "")).lower() else "Image",
            "views": _int(views),
            "reach": _int(s.get("reach")),
            "profile_visits": _int(s.get("profile_visits")),
            "follows": _int(s.get("follows")),
            "replies": _int(s.get("replies")),
            "shares": _int(s.get("shares")),
            "total_interactions": _int(s.get("total_interactions")),
            "exit_rate": round(_num(s.get("exit_rate")), 1),
            "taps_forward": _int(s.get("taps_forward")),
            "taps_back": _int(s.get("taps_back")),
            "swipes_forward": _int(s.get("swipes_forward")),
            "exits": _int(s.get("exits")),
            "pv_rate": round(views and _num(s.get("profile_visits")) / views * 100, 2),
            "url": s.get("permalink", ""),
        })
    stories.sort(key=lambda x: x["views"], reverse=True)

    return {
        "range": days,
        "last_date": _last_data_date(data),
        "kpis": {
            "stories": n,
            "views": round(total_views),
            "reach": round(total_reach),
            "profile_visits": round(total_pv),
        },
        "chart": {"dates": dates, "views": series["views"], "reach": series["reach"]},
        "nav": nav,
        "summary": {
            "avg_exit_rate": round(avg_exit, 1),
            "profile_visits": round(total_pv),
            "follows": round(_sum(cur, "follows")),
            "replies": round(_sum(cur, "replies")),
            "shares": round(_sum(cur, "shares")),
            "avg_views": round(total_views / n) if n else 0,
            "avg_reach": round(total_reach / n) if n else 0,
        },
        "stories": stories,
    }


# ---------- cross-platform viral matching ----------
#
# Kan's desk crossposts the same story with near-identical copy, so token
# containment on the caption text (plus date proximity) is enough to cluster
# "the same story" across platforms - no AI call, deterministic, per-request.

_VIRAL_STOP = set("""של את על עם לא זה זו זאת הוא היא הם הן אני אתם אנחנו יש אין
גם רק כל כי מה מי איך למה בין אחרי לפני נגד מול אבל או עוד כבר היום אמש מחר
כאן חדשות בעקבות במהלך בזמן כדי לפי אצל בגלל האם כמה שני שתי כמו יותר פחות
אשר כאשר היה היו תהיה הזה הזאת האלה עצמו שלו שלה שלהם ידי לאחר עקב""".split())

_MATCH_WINDOW_DAYS = 2      # אותו סיפור חוצה פלטפורמות בתוך יום-יומיים
# 0.5 יצר אשכול-ענק של 54 פוסטים סביב טראמפ/איראן (שרשור טרנזיטיבי של נושא
# שלם); ב-0.6 האשכול הגדול ביותר הוא 5 פוסטים והסיפורים נשארים מוצלבים נכון.
_MATCH_CONTAINMENT = 0.6    # חפיפת מילים ביחס לכיתוב הקצר מבין השניים
_MIN_TOKENS = 4             # כיתוב קצר מדי לא ניתן לשיוך אמין


def _clean_caption(raw):
    """כיתוב גולמי -> כותרת: בלי סימני bidi/עיצוב וטאבים, רווחים מכווצים."""
    text = re.sub(r"[‎‏‪-‮⁦-⁩﻿]", "", str(raw))
    return re.sub(r"\s+", " ", text).strip()


def _viral_tokens(text, limit=40):
    text = re.sub(r"[^0-9א-תa-zA-Z\s]", " ", str(text))
    out = []
    for w in text.split():
        if len(w) >= 3 and w not in _VIRAL_STOP and not w.isdigit():
            out.append(w)
            if len(out) >= limit:
                break
    return frozenset(out)


def _viral_collect(data, start, end):
    """All posts in range, normalized to one shape, with token sets."""
    analyses = {p: _comment_analyses(data, p)
                for p in ("instagram", "facebook", "youtube")}
    specs = [
        ("instagram", data["instagram"], "media_id", "caption", "date", "permalink",
         lambda p: _num(p.get("likes")) + _num(p.get("comments")) + _num(p.get("saved")) + _num(p.get("shares"))),
        ("facebook", data["facebook"], "post_id", "title", "date", "permalink",
         lambda p: _num(p.get("likes")) + _num(p.get("comments")) + _num(p.get("shares"))),
        ("youtube", data["youtube"], "video_id", "title", "published_at", "video_url",
         lambda p: _num(p.get("likes")) + _num(p.get("comments"))),
        ("twitter", data.get("twitter", []), "tweet_id", "text", "date", "permalink",
         lambda p: _num(p.get("likes")) + _num(p.get("retweets")) + _num(p.get("replies")) + _num(p.get("quotes"))),
    ]
    items = []
    for plat, rows, id_col, cap_col, date_col, url_col, inter_fn in specs:
        for p in rows:
            d = _parse_date(p.get(date_col))
            if not d or not (start <= d <= end):
                continue
            toks = _viral_tokens(p.get(cap_col, ""))
            if len(toks) < _MIN_TOKENS:
                continue
            views = _num(p.get("views"))
            inter = inter_fn(p)
            pid = str(p.get(id_col, "")).strip()
            items.append({
                "platform": plat,
                "title": _clean_caption(p.get(cap_col, ""))[:200],
                "d": d,
                "date": str(d),
                "views": _int(views),
                "comments": _int(p.get("comments")),
                "shares": _int(p.get("shares") if plat != "twitter" else p.get("retweets")),
                "eng": round(inter / views * 100, 1) if views else 0,
                "url": p.get(url_col, ""),
                "analysis": analyses.get(plat, {}).get(pid),
                "toks": toks,
            })
    return items


def build_viral(data, days):
    start, end, _p1, _p2 = _window(days)
    items = _viral_collect(data, start, end)

    # union-find over similar pairs, comparing only nearby dates
    parent = list(range(len(items)))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    by_date = {}
    for i, it in enumerate(items):
        by_date.setdefault(it["d"], []).append(i)
    for d, idxs in by_date.items():
        cand = []
        for back in range(_MATCH_WINDOW_DAYS + 1):
            cand.extend(by_date.get(d - timedelta(days=back), []))
        for i in idxs:
            ti = items[i]["toks"]
            for j in cand:
                if j == i:
                    continue
                tj = items[j]["toks"]
                inter = len(ti & tj)
                if inter and inter / min(len(ti), len(tj)) >= _MATCH_CONTAINMENT:
                    ri, rj = find(i), find(j)
                    if ri != rj:
                        parent[ri] = rj

    clusters = {}
    for i in range(len(items)):
        clusters.setdefault(find(i), []).append(i)

    stories = []
    for members in clusters.values():
        plats = {items[i]["platform"] for i in members}
        if len(plats) < 2:
            continue
        posts = sorted((items[i] for i in members), key=lambda x: -x["views"])
        stories.append({
            "title": posts[0]["title"],
            "date": str(min(p["d"] for p in posts)),
            "platforms": sorted(plats),
            "total_views": sum(p["views"] for p in posts),
            "n_posts": len(posts),
            "has_analysis": any(p["analysis"] for p in posts),
            "posts": [{k: p[k] for k in
                       ("platform", "title", "date", "views", "comments",
                        "shares", "eng", "url", "analysis")} for p in posts],
        })
    stories.sort(key=lambda s: -s["total_views"])
    stories = stories[:20]

    return {
        "range": days,
        "last_date": _last_data_date(data),
        "summary": {
            "stories": len(stories),
            "total_views": round(sum(s["total_views"] for s in stories)),
            "max_platforms": max((len(s["platforms"]) for s in stories), default=0),
            "with_analysis": sum(1 for s in stories if s["has_analysis"]),
        },
        "stories": stories,
    }


# ---------- competitors (IG business discovery snapshots) ----------

def build_competitors(data, days):
    start, end, _p1, _p2 = _window(days)
    rows = data.get("competitors", [])
    ig = data["instagram"]

    by_user = {}
    for r in rows:
        d = _parse_date(r.get("date"))
        u = str(r.get("username", "")).strip()
        if d and u:
            by_user.setdefault(u, []).append((d, r))

    # per-post feed (kept 14 days by the collector); grouped per account
    posts_by_user = {}
    for p in data.get("competitor_posts", []):
        d = _parse_date(p.get("date"))
        u = str(p.get("username", "")).strip()
        if not d or not u or not (start <= d <= end):
            continue
        likes, comments = _int(p.get("likes")), _int(p.get("comments"))
        posts_by_user.setdefault(u, []).append({
            "date": str(d), "time": p.get("time", ""),
            "type": p.get("type", ""),
            "caption": _clean_caption(p.get("caption", ""))[:200],
            "likes": likes, "comments": comments, "eng": likes + comments,
            "url": p.get("permalink", ""),
        })
    for items in posts_by_user.values():
        items.sort(key=lambda x: -x["eng"])

    competitors = []
    for username, entries in by_user.items():
        entries.sort(key=lambda e: e[0])
        # בניגוד לפוסטים, צילומי המצב של הבוקר מתוארכים היום - אין לחתוך אותם
        in_range = [e for e in entries if e[0] >= start]
        latest = entries[-1][1]
        first_in = in_range[0][1] if in_range else latest
        competitors.append({
            "username": username,
            "name": latest.get("name", ""),
            "followers": _int(latest.get("followers")),
            "change_1d": _int(latest.get("followers_change")),
            "change_range": _int(latest.get("followers")) - _int(first_in.get("followers")),
            "posts_24h": _int(latest.get("posts_24h")),
            "avg_likes": _int(latest.get("avg_likes_recent")),
            "avg_comments": _int(latest.get("avg_comments_recent")),
            "eng_per_1k": round(_num(latest.get("eng_per_1k")), 2),
            "spark": [_int(r.get("followers")) for _d, r in in_range] or [_int(latest.get("followers"))],
            "top": {
                "caption": latest.get("top_caption", ""),
                "likes": _int(latest.get("top_likes")),
                "comments": _int(latest.get("top_comments")),
                "url": latest.get("top_url", ""),
            },
            "posts": posts_by_user.get(username, [])[:15],
            "is_kan": False,
        })

    # שורת ייחוס של כאן, מהנתונים המלאים שלנו (discovery לא צריך את עצמנו)
    kan_f = _follower_metric(data["followers"], "ig_followers", "ig_followers_change")
    yesterday = end
    own_yesterday = [p for p in ig if _parse_date(p.get("date")) == yesterday]
    own_recent = sorted((p for p in ig if _parse_date(p.get("date"))),
                        key=lambda p: str(p.get("date")), reverse=True)[:10]
    own_avg_likes = round(sum(_num(p.get("likes")) for p in own_recent) / len(own_recent)) if own_recent else 0
    own_avg_comments = round(sum(_num(p.get("comments")) for p in own_recent) / len(own_recent)) if own_recent else 0
    kan_followers = _int(kan_f.get("value"))
    competitors.append({
        "username": "kan_news",
        "name": "כאן חדשות",
        "followers": kan_followers,
        "change_1d": _int(data["followers"][-1].get("ig_followers_change")) if data["followers"] else 0,
        "change_range": _int(kan_f.get("weekly_change") or 0),
        "posts_24h": len(own_yesterday),
        "avg_likes": own_avg_likes,
        "avg_comments": own_avg_comments,
        "eng_per_1k": round((own_avg_likes + own_avg_comments) / kan_followers * 1000, 2) if kan_followers else 0,
        "spark": [_int(r.get("ig_followers")) for r in data["followers"]
                  if (_parse_date(r.get("date")) or start) >= start and _int(r.get("ig_followers"))],
        "top": {},
        "posts": sorted(({
            "date": str(_parse_date(p.get("date")) or ""), "time": p.get("time", ""),
            "type": p.get("type", ""),
            "caption": _clean_caption(p.get("caption", ""))[:200],
            "likes": _int(p.get("likes")), "comments": _int(p.get("comments")),
            "eng": _int(p.get("likes")) + _int(p.get("comments")),
            "url": p.get("permalink", ""),
        } for p in ig if (_parse_date(p.get("date")) or start) >= start and _parse_date(p.get("date"))),
            key=lambda x: -x["eng"])[:15],
        "is_kan": True,
    })

    competitors.sort(key=lambda c: -c["followers"])
    kan_rank = next((i + 1 for i, c in enumerate(competitors) if c["is_kan"]), 0)
    growers = [c for c in competitors if not c["is_kan"]]
    fastest = max(growers, key=lambda c: c["change_range"], default=None)

    # הזירה: הפוסטים החזקים של כל החשבונות (כולל כאן) ב-48 השעות האחרונות
    arena_cutoff = end - timedelta(days=1)
    arena = []
    for c in competitors:
        for p in c["posts"]:
            pdate = _parse_date(p["date"])
            if pdate and pdate >= arena_cutoff:
                arena.append(dict(p, name=c["name"], username=c["username"], is_kan=c["is_kan"]))
    arena.sort(key=lambda x: -x["eng"])

    return {
        "range": days,
        "last_date": _last_data_date(data),
        "summary": {
            "tracked": len(growers),
            "kan_rank": kan_rank,
            "leader": growers[0]["name"] if growers else "",
            "fastest": {"name": fastest["name"], "change": fastest["change_range"]} if fastest else None,
        },
        "arena": arena[:12],
        "competitors": competitors,
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
    # intraday hot-sniffer alert log (hot_alerts tab; may not exist yet)
    hot_history = []
    start_str = (_window(days)[0]).strftime("%Y-%m-%d")
    for h in data.get("hot_alerts", []):
        alerted = str(h.get("alerted_at", "")).strip()
        if not alerted or alerted[:10] < start_str:
            continue
        hot_history.append({
            "alerted_at": alerted,
            "platform": str(h.get("platform", "")).strip(),
            "triggers": [t.strip() for t in str(h.get("triggers", "")).split(";") if t.strip()],
            "url": str(h.get("permalink", "")).strip(),
        })
    hot_history.sort(key=lambda h: h["alerted_at"], reverse=True)

    return {
        "range": days,
        "last_date": _last_data_date(data),
        "alerts": alerts,
        "summary": summary,
        "hot_history": hot_history[:20],
    }
