"""
Facebook Graph API metric probe — READ ONLY. Does NOT write to Google Sheets.

Purpose: empirically confirm which post-insight metrics actually return data
for the Kan News Page on the current API version, after Meta removed
`post_impressions_unique` (and other legacy metrics) for all versions on
2026-06-15 and consolidated around the unified "views" / media-view metrics.

Run it from the `Test Facebook Probe` GitHub Action (it has FACEBOOK_TOKEN as a
secret). It prints, per sample post, which candidate metric returned a value vs
an error — so we can lock the exact field names before patching the collector.

Env: FACEBOOK_TOKEN (required), FACEBOOK_PAGE_ID (optional, defaults to Kan).
"""

import os
import json
import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

TOKEN = os.environ.get("FACEBOOK_TOKEN")
PAGE_ID = os.environ.get("FACEBOOK_PAGE_ID", "220634478361516")
API_VERSION = os.environ.get("FB_API_VERSION", "v25.0")
BASE = f"https://graph.facebook.com/{API_VERSION}"

# Candidate post-level insight metrics to test (new + legacy, individually).
POST_METRICS = [
    "post_total_media_view_unique",  # NEW: unique viewers ~ reach replacement
    "post_media_view",               # NEW: media views (paid/organic breakdown)
    "post_impressions_unique",       # LEGACY reach (expected: removed 2026-06-15)
    "post_impressions",              # LEGACY impressions
    "post_clicks",                   # still used by collector
    "post_activity",                 # legacy engagement
    "post_video_views",              # legacy post-level video views
]
# Candidate video-OBJECT insight metrics ({video-id}/video_insights).
VIDEO_METRICS = [
    "blue_reels_play_count",
    "total_video_views",
    "total_video_30s_views",
    "total_video_avg_time_watched",
    "total_video_view_total_time",
]


def get(url, params):
    try:
        r = requests.get(url, params=params, timeout=30)
        return r.json()
    except Exception as e:
        return {"error": {"message": f"request failed: {e}"}}


def probe_metric(obj_id, metric, endpoint="insights"):
    """Call one metric alone so a single invalid name can't fail the batch."""
    params = {"access_token": TOKEN, "metric": metric}
    if endpoint == "insights":
        params["period"] = "lifetime"
    res = get(f"{BASE}/{obj_id}/{endpoint}", params)
    if "error" in res:
        err = res["error"]
        return f"ERROR ({err.get('code')}): {err.get('message', '')[:120]}"
    data = res.get("data", [])
    if not data:
        return "(no data)"
    vals = data[0].get("values", [])
    v = vals[0].get("value") if vals else None
    return f"OK -> {json.dumps(v, ensure_ascii=False)}"


def detect_type(post):
    permalink = post.get("permalink_url", "") or ""
    if "/reel/" in permalink:
        return "Reel"
    if "/videos/" in permalink:
        return "Video"
    atts = post.get("attachments", {}).get("data", [])
    if atts:
        t = atts[0].get("type", "")
        if t in ("video_inline", "video_direct", "video_autoplay", "video"):
            return "Video"
        if t in ("photo", "cover_photo", "album"):
            return "Photo"
        if t in ("share", "link"):
            return "Link"
    return "Status"


def video_id_of(post):
    try:
        return post["attachments"]["data"][0]["target"]["id"]
    except Exception:
        return None


EXTRA_VIDEO_METRICS = [
    "blue_reels_play_count",
    "post_video_avg_time_watched",
    "post_video_view_time",
    "total_video_view_total_time",
    "total_video_avg_time_watched",
    "total_video_view_time",
    "total_video_complete_views",
    "fb_reels_total_plays",
    "fb_reels_replay_count",
]


def main():
    if not TOKEN:
        print("MISSING FACEBOOK_TOKEN"); return
    print(f"=== FB metric probe · API {API_VERSION} · page {PAGE_ID} ===\n")

    feed = get(f"{BASE}/{PAGE_ID}/feed", {
        "access_token": TOKEN, "limit": 25,
        "fields": "id,created_time,permalink_url,attachments",
    })
    if "error" in feed:
        print("FEED ERROR:", feed["error"].get("message")); return
    posts = feed.get("data", [])
    print(f"Fetched {len(posts)} recent posts.\n")

    # TABLE: reach/views across post age — settles whether photo 0s are recency or real.
    print("--- per post: type | created | reach(post_total_media_view_unique) | views(post_media_view) | clicks ---")
    for p in posts:
        t = detect_type(p)
        created = (p.get("created_time", "") or "")[:16]
        reach = probe_metric(p["id"], "post_total_media_view_unique")
        views = probe_metric(p["id"], "post_media_view")
        clicks = probe_metric(p["id"], "post_clicks")
        print(f"  {t:6s} {created}  reach={reach:14s}  views={views:14s}  clicks={clicks}")
    print()

    # Watch-time exploration on the first reel/video.
    for p in posts:
        t = detect_type(p)
        if t in ("Reel", "Video"):
            vid = video_id_of(p)
            print(f"--- watch-time candidates · {t} · post {p['id']} · video {vid} ---")
            if vid:
                for m in EXTRA_VIDEO_METRICS:
                    print(f"  video_insights/{m:30s} {probe_metric(vid, m, endpoint='video_insights')}")
            break
    print()

    print("=== done (read-only; nothing written) ===")


if __name__ == "__main__":
    main()
