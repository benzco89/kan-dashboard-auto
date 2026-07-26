"""
Facebook video-retention probe — READ ONLY. Does NOT write to Google Sheets.

Why: `views_30s` and the `completion_rate` derived from it are 0 on every row
the collector has written for months. Measured over the full sheet (4,416 rows,
2026-07-26): 56 rows ever carried a value, all of them Reels, all between
2025-11-17 and 2025-12-06. Then it stopped. Two facts frame it —

  * the Kan News page publishes only **Reels (2,392) and Photos (2,020)**;
    there is no regular-video post at all, and `total_video_30s_views` is
    documented as a regular-video metric,
  * yet it *did* return for Reels for three weeks, so this is a Meta change,
    not a shape the page never had.

`avg_watch_sec` is unaffected: 2,385 of 2,392 Reels have it.

So the question is not "is total_video_30s_views broken" but "what does v25
expose TODAY that says how much of a reel people watched". This probe answers
it by ASKING rather than guessing: `/{video-id}/video_insights` with **no
metric parameter** returns every metric available for that object. Guessing
names only tells you about the names you thought of.

    gh workflow run probe_fb_video.yml      # FACEBOOK_TOKEN lives in CI

Env: FACEBOOK_TOKEN (required), FACEBOOK_PAGE_ID (optional, defaults to Kan),
     FB_API_VERSION (optional), PROBE_POSTS (optional, default 6).
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
SAMPLE = int(os.environ.get("PROBE_POSTS", "6"))
BASE = f"https://graph.facebook.com/{API_VERSION}"

# Named candidates, tried one at a time so a single invalid name cannot fail the
# batch. The enumeration above is the real answer; this list is here to confirm
# the specific shapes we would want if they exist.
CANDIDATES = [
    "total_video_30s_views",          # what the collector uses today
    "total_video_complete_views",     # watched to the end
    "post_video_complete_views_organic",
    "post_video_retention_graph",     # per-decile retention curve
    "post_video_avg_time_watched",    # works today -> the control
    "post_video_view_time",           # works today -> the control
    "blue_reels_play_count",          # works today -> the control
    "post_video_followers",
    "post_video_social_actions",
]


def get(url, params):
    try:
        return requests.get(url, params=params, timeout=30).json()
    except Exception as e:
        return {"error": {"message": f"request failed: {e}"}}


def recent_reels(limit):
    """The page's most recent posts that have a video object behind them."""
    res = get(f"{BASE}/{PAGE_ID}/posts", {
        "access_token": TOKEN,
        "fields": "id,created_time,attachments{media_type,target}",
        "limit": max(limit * 4, 25),
    })
    if "error" in res:
        print(f"  cannot list posts: {res['error'].get('message', '')[:160]}")
        return []
    out = []
    for p in res.get("data", []):
        try:
            att = p["attachments"]["data"][0]
            vid = att["target"]["id"]
            media = att.get("media_type", "?")
        except Exception:
            continue
        if vid and vid != p["id"]:
            out.append((p["id"], vid, media, p.get("created_time", "")[:10]))
        if len(out) >= limit:
            break
    return out


def all_metrics(video_id):
    """Every metric video_insights will hand over for this object."""
    res = get(f"{BASE}/{video_id}/video_insights", {"access_token": TOKEN})
    if "error" in res:
        return None, res["error"].get("message", "")[:160]
    out = {}
    for row in res.get("data", []):
        vals = row.get("values") or []
        out[row.get("name", "?")] = vals[0].get("value") if vals else None
    return out, None


def probe_one(video_id, metric):
    res = get(f"{BASE}/{video_id}/video_insights", {"access_token": TOKEN, "metric": metric})
    if "error" in res:
        e = res["error"]
        return f"ERROR {e.get('code')}: {e.get('message', '')[:90]}"
    data = res.get("data") or []
    if not data:
        return "(no data)"
    vals = data[0].get("values") or []
    v = vals[0].get("value") if vals else None
    return f"OK -> {json.dumps(v, ensure_ascii=False)[:120]}"


def main():
    if not TOKEN:
        raise SystemExit("FACEBOOK_TOKEN is missing")
    print("=" * 66)
    print(f"Facebook video-retention probe · {API_VERSION} · page {PAGE_ID}")
    print("=" * 66)

    posts = recent_reels(SAMPLE)
    if not posts:
        raise SystemExit("no posts with a video object were returned")

    seen_names = {}
    for post_id, video_id, media, day in posts:
        print(f"\n--- {day} · {media} · post {post_id} · video {video_id}")
        avail, err = all_metrics(video_id)
        if err:
            print(f"    video_insights enumeration failed: {err}")
        else:
            print(f"    {len(avail)} metrics available:")
            for k in sorted(avail):
                v = json.dumps(avail[k], ensure_ascii=False)
                print(f"      {k:<42} {v[:70]}")
                seen_names[k] = seen_names.get(k, 0) + 1

    print("\n" + "=" * 66)
    print("NAMED CANDIDATES on the newest video object")
    print("=" * 66)
    newest = posts[0][1]
    for m in CANDIDATES:
        print(f"  {m:<42} {probe_one(newest, m)}")

    print("\n" + "=" * 66)
    print(f"AVAILABLE ON ALL {len(posts)} SAMPLED OBJECTS")
    print("=" * 66)
    for k, c in sorted(seen_names.items(), key=lambda kv: (-kv[1], kv[0])):
        mark = "*" if c == len(posts) else " "
        print(f"  {mark} {k:<42} {c}/{len(posts)}")
    print("\n* = present on every sampled object. Anything naming completion,")
    print("  retention or a play-percentage is the replacement we are after.")


if __name__ == "__main__":
    main()
