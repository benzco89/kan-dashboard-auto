"""
Instagram Graph API insight probe — READ ONLY. Does NOT write to Google Sheets.

For ONE high-performing media item it confirms — empirically, against the live
account — which deep metrics the current API version actually returns, so we can
decide what to add to instagram_collector.py before touching production.

VERIFIED FINDINGS (v24.0, Kan IG account, 2026-06-30):
  ✅ per-post available:
       views, reach, likes, comments, saved, shares, total_interactions
       ig_reels_avg_watch_time        (ms)  — already collected
       ig_reels_video_view_total_time (ms)  — TOTAL watch time; NOT yet collected
  ❌ per-post NOT available (API rejects on this account/version):
       reach/views breakdown by follow_type or surface_type
            -> "(#100) Incompatible breakdowns for metric" — for BOTH reels & feed.
       profile_activity / profile_visits / follows
            -> "does not support ... for this media product type" (reels).
       clips_replays_count / ig_reels_aggregated_all_plays_count -> not valid metrics.
  ✅ ACCOUNT-level only: reach broken down by follow_type (FOLLOWER vs NON_FOLLOWER).
       The "how far beyond our followers" question is answerable for the account as
       a whole, NOT per individual post.

Run via the "Test Instagram Probe" GitHub Action (FACEBOOK_TOKEN secret) or locally
with FACEBOOK_TOKEN in a .env file.

Env: FACEBOOK_TOKEN (required). IG_MEDIA_ID (optional; defaults to the top reel in
the current window). IG_API_VERSION (optional, defaults v24.0).
"""

import os
import sys
import json
import time
import requests

# Windows console may be cp1255 and choke on emoji/RTL — force UTF-8 output.
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
# ברירת מחדל: הריל המוצלח ביותר בחלון הנוכחי (504K views / 349K reach / 7,755 shares)
MEDIA_ID = os.environ.get("IG_MEDIA_ID", "17881474323459278")
API_VERSION = os.environ.get("IG_API_VERSION", "v25.0")
BASE = f"https://graph.facebook.com/{API_VERSION}"

BASE_METRICS = ["views", "reach", "total_interactions", "likes", "comments", "saved", "shares"]


def get(url, params):
    try:
        return requests.get(url, params=params, timeout=30).json()
    except Exception as e:
        return {"error": {"message": f"request failed: {e}"}}


def probe(obj_id, metric, breakdown=None, period=None, since=None, until=None):
    """Call one metric alone so a single invalid name can't fail the batch."""
    params = {"access_token": TOKEN, "metric": metric}
    if breakdown:
        params["breakdown"] = breakdown
        params["metric_type"] = "total_value"  # breakdowns require the total_value form
    if period:
        params["period"] = period
    if since:
        params["since"], params["until"] = since, until
    res = get(f"{BASE}/{obj_id}/insights", params)
    if "error" in res:
        err = res["error"]
        return f"ERROR ({err.get('code')}): {err.get('message', '')[:130]}"
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


def main():
    if not TOKEN:
        print("MISSING FACEBOOK_TOKEN (GitHub secret or local .env)")
        return

    print(f"=== IG insight probe · API {API_VERSION} · media {MEDIA_ID} ===\n")

    meta = get(f"{BASE}/{MEDIA_ID}", {
        "access_token": TOKEN,
        "fields": "media_product_type,media_type,permalink,caption",
    })
    if "error" in meta:
        print("MEDIA ERROR:", meta["error"].get("message"))
        return
    print(f"type={meta.get('media_product_type') or meta.get('media_type')}  "
          f"permalink={meta.get('permalink')}")
    print(f"caption={(meta.get('caption') or '')[:70]}...\n")

    print("--- base metrics (already collected) ---")
    for m in BASE_METRICS:
        print(f"  {m:22s} {probe(MEDIA_ID, m)}")

    print("\n--- כמה זמן ראו? (watch-time) ---")
    print(f"  ig_reels_avg_watch_time         {probe(MEDIA_ID, 'ig_reels_avg_watch_time')}")
    print(f"  ig_reels_video_view_total_time  {probe(MEDIA_ID, 'ig_reels_video_view_total_time')}")

    # v25 candidates: rejected on v24, plus the Dec-2025 "Reels Skip Rate" + repost.
    # On a bad name the API replies with the valid-metric enum -> we learn the real name.
    print("\n--- v25 candidates (replays / skip-rate / reposts) ---")
    for m in ("clips_replays_count", "ig_reels_aggregated_all_plays_count",
              "reels_skip_rate", "ig_reels_skip_rate", "skip_rate",
              "reposts", "repost_count"):
        print(f"  {m:36s} {probe(MEDIA_ID, m)}")

    # 🥇 per-post follower split — rejected on v24; retry on v25 (the dream answer).
    print("\n--- 🥇 פיצול עוקבים פר-פוסט (retry on v25) ---")
    print(f"  reach[follow_type]    {probe(MEDIA_ID, 'reach', breakdown='follow_type')}")
    print(f"  views[follow_type]    {probe(MEDIA_ID, 'views', breakdown='follow_type')}")
    print(f"  views[surface_type]   {probe(MEDIA_ID, 'views', breakdown='surface_type')}")

    # 🥈 Dec-2025 metrics — crossposted split.
    print("\n--- 🥈 crossposted split ---")
    for m in ("crossposted_views", "facebook_views"):
        print(f"  {m:24s} {probe(MEDIA_ID, m)}")

    # 🥉 conversion metrics — rejected for reels on v24; retry on v25.
    print("\n--- 🥉 conversion (retry on v25) ---")
    print(f"  profile_activity[action_type]  {probe(MEDIA_ID, 'profile_activity', breakdown='action_type')}")
    for m in ("profile_visits", "follows"):
        print(f"  {m:24s} {probe(MEDIA_ID, m)}")

    print("\n--- כמה יצאנו מעבר לעוקבים? (ACCOUNT-level baseline) ---")
    ig = get(f"{BASE}/me", {"access_token": TOKEN, "fields": "instagram_business_account"}) \
        .get("instagram_business_account", {}).get("id")
    if ig:
        now = int(time.time())
        print(f"  reach[follow_type] last 30d  "
              f"{probe(ig, 'reach', breakdown='follow_type', period='day', since=now - 30 * 86400, until=now)}")

    print("\n=== done (read-only; nothing written) ===")


if __name__ == "__main__":
    main()
