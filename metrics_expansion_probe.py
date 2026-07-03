"""
Metrics-expansion probe - READ ONLY. Does NOT write to Google Sheets.

Verifies, against the live Kan accounts, everything the metrics-expansion
packages need before touching the production collectors:

  1. FB reactions breakdown per post (love/haha/wow/sad/angry via
     reactions.type(X).summary aliases) - package 2.
  2. IG Stories: /stories listing + which per-story insight metrics the API
     accepts - package 3.
  3. Re-confirm the v25 reel metrics (reels_skip_rate, crossposted_views,
     facebook_views, reposts) on the CURRENT top reel - package 1 safety.

Run via the "Probe: metrics expansion" GitHub Action (FACEBOOK_TOKEN secret).
"""

import os
import sys
import json
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

TOKEN = os.environ.get("FACEBOOK_TOKEN")
PAGE_ID = os.environ.get("FACEBOOK_PAGE_ID") or "220634478361516"  # or-fallback: the GitHub secret is EMPTY, and workflows set empty env vars
API_VERSION = os.environ.get("PROBE_API_VERSION", "v25.0")
BASE = f"https://graph.facebook.com/{API_VERSION}"

REACTION_TYPES = ["LIKE", "LOVE", "WOW", "HAHA", "SAD", "ANGRY"]

# candidate per-story insight metrics (probed one-by-one; API errors tell us
# the valid enum when a name is wrong)
STORY_METRIC_CANDIDATES = [
    "views", "reach", "impressions", "replies", "shares",
    "total_interactions", "profile_visits", "follows",
]


def get(url, params):
    try:
        return requests.get(url, params=params, timeout=30).json()
    except Exception as e:
        return {"error": {"message": f"request failed: {e}"}}


def probe_insight(obj_id, metric, breakdown=None):
    params = {"access_token": TOKEN, "metric": metric}
    if breakdown:
        params["breakdown"] = breakdown
        params["metric_type"] = "total_value"
    res = get(f"{BASE}/{obj_id}/insights", params)
    if "error" in res:
        err = res["error"]
        return f"ERROR ({err.get('code')}): {err.get('message', '')[:150]}"
    data = res.get("data", [])
    if not data:
        return "(no data)"
    item = data[0]
    tv = item.get("total_value")
    if tv and "breakdowns" in tv:
        out = {}
        for b in tv["breakdowns"]:
            for r in b.get("results", []):
                out["/".join(r["dimension_values"])] = r["value"]
        return f"OK -> total={tv.get('value')}  {json.dumps(out, ensure_ascii=False)}"
    vals = item.get("values", [])
    v = vals[0].get("value") if vals else (tv.get("value") if tv else None)
    return f"OK -> {json.dumps(v, ensure_ascii=False)}"


def ig_account_id():
    res = get(f"{BASE}/me", {"access_token": TOKEN, "fields": "instagram_business_account"})
    return (res.get("instagram_business_account") or {}).get("id")


def probe_fb_reactions():
    print("=" * 60)
    print("1) FB reactions breakdown (package 2)")
    print("=" * 60)
    feed = get(f"{BASE}/{PAGE_ID}/feed", {"access_token": TOKEN, "limit": 3,
                                          "fields": "id,message,created_time"})
    posts = feed.get("data", [])
    if not posts:
        print("  FEED ERROR:", json.dumps(feed.get("error", {}))[:200])
        return
    fields = ",".join(
        f"reactions.type({t}).limit(0).summary(total_count).as(r_{t.lower()})"
        for t in REACTION_TYPES
    ) + ",reactions.limit(0).summary(total_count).as(r_total)"
    for post in posts[:2]:
        print(f"\n  post {post['id']}  ({(post.get('message') or '')[:50]}...)")
        res = get(f"{BASE}/{post['id']}", {"access_token": TOKEN, "fields": fields})
        if "error" in res:
            print("    ERROR:", res["error"].get("message", "")[:200])
            continue
        total = (res.get("r_total") or {}).get("summary", {}).get("total_count")
        parts = {t.lower(): (res.get(f"r_{t.lower()}") or {}).get("summary", {}).get("total_count", 0)
                 for t in REACTION_TYPES}
        print(f"    total={total}  breakdown={json.dumps(parts)}  sum(parts)={sum(parts.values())}")


def probe_ig_stories():
    print()
    print("=" * 60)
    print("2) IG Stories (package 3)")
    print("=" * 60)
    ig = ig_account_id()
    if not ig:
        print("  no IG business account via this token")
        return
    res = get(f"{BASE}/{ig}/stories", {"access_token": TOKEN,
                                       "fields": "id,media_type,timestamp,permalink"})
    if "error" in res:
        print("  STORIES ERROR:", res["error"].get("message", "")[:200])
        return
    stories = res.get("data", [])
    print(f"  live stories right now: {len(stories)}")
    for s in stories[:3]:
        print(f"    {s.get('id')}  type={s.get('media_type')}  ts={s.get('timestamp')}")
    if not stories:
        print("  (no live stories to probe insights on - rerun when a story is up)")
        return
    sid = stories[0]["id"]
    print(f"\n  insight metrics on story {sid}:")
    for m in STORY_METRIC_CANDIDATES:
        print(f"    {m:20s} {probe_insight(sid, m)}")
    print(f"    {'navigation':20s} {probe_insight(sid, 'navigation', breakdown='story_navigation_action_type')}")


def probe_ig_reel_v25():
    print()
    print("=" * 60)
    print("3) v25 reel metrics re-check on current top reel (package 1)")
    print("=" * 60)
    ig = ig_account_id()
    if not ig:
        print("  no IG business account via this token")
        return
    media = get(f"{BASE}/{ig}/media", {"access_token": TOKEN, "limit": 25,
                                       "fields": "id,media_type,media_product_type,timestamp"})
    reel = next((m for m in media.get("data", [])
                 if (m.get("media_product_type") == "REELS" or m.get("media_type") == "VIDEO")), None)
    if not reel:
        print("  no recent reel found")
        return
    print(f"  reel {reel['id']}  ts={reel.get('timestamp')}")
    for m in ("reels_skip_rate", "crossposted_views", "facebook_views", "reposts",
              "views", "reach"):
        print(f"    {m:20s} {probe_insight(reel['id'], m)}")


def main():
    if not TOKEN:
        print("MISSING FACEBOOK_TOKEN")
        sys.exit(1)
    print(f"=== metrics-expansion probe · API {API_VERSION} · read-only ===\n")
    probe_fb_reactions()
    probe_ig_stories()
    probe_ig_reel_v25()
    print("\n=== done (read-only; nothing written) ===")


if __name__ == "__main__":
    main()
