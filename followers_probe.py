"""
Page / IG-account insight probe — READ ONLY. Does NOT write to Google Sheets.

Why: four columns in מעקב עוקבים have been empty since the sheet began —
fb_daily_reach, fb_daily_engagements, ig_daily_reach, ig_daily_impressions,
0 real values in 230 rows from 2025-12-07 to today. So have fb_fan_adds and
fb_fan_removes.

That last pair is the tell. `page_fan_adds` is a perfectly good metric; it is
empty because `get_facebook_daily_insights` asks for five metrics in ONE call
and `page_impressions_unique` — removed by Meta on 2026-06-15 — fails the whole
request. One dead name took four live ones with it, silently, for eight months.
The post collector already learned this: its `_insight` fetches one metric at a
time "so one bad name can't fail the batch". The followers tracker never did.

So this probe asks for each metric ON ITS OWN, and asks Meta to enumerate what
it will accept by requesting a deliberately invalid name — the same trick that
answered the Instagram retention question.

    gh workflow run probe_followers.yml

Env: FACEBOOK_TOKEN (required), FACEBOOK_PAGE_ID (optional), FB_API_VERSION.
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
PAGE_ID = os.environ.get("FACEBOOK_PAGE_ID") or "220634478361516"
API_VERSION = os.environ.get("FB_API_VERSION") or "v25.0"
BASE = f"https://graph.facebook.com/{API_VERSION}"

NONSENSE = "kan_probe_not_a_metric"

# What the tracker asks for today, plus the shapes that would replace the dead
# ones. Order matters only for reading the output.
PAGE_METRICS = [
    "page_fan_adds",              # requested today — expected to work alone
    "page_fan_removes",           # requested today — expected to work alone
    "page_impressions_unique",    # requested today — removed 2026-06-15
    "page_post_engagements",      # requested today
    "page_video_views",           # requested today
    "page_follows",               # used elsewhere in the tracker, works
    "page_daily_follows",
    "page_daily_follows_unique",
    "page_total_media_view_unique",
    "page_views_total",
]

IG_METRICS = [
    "reach",                      # requested today
    "impressions",                # requested today
    "views",
    "follower_count",
    "profile_views",
    "accounts_engaged",
    "total_interactions",
    "website_clicks",
    "profile_links_taps",
]


def get(url, params):
    try:
        return requests.get(url, params=params, timeout=30).json()
    except Exception as e:
        return {"error": {"message": f"request failed: {e}"}}


def probe(obj_id, metric, extra=None):
    params = {"access_token": TOKEN, "metric": metric, "period": "day",
              "date_preset": "yesterday"}
    params.update(extra or {})
    res = get(f"{BASE}/{obj_id}/insights", params)
    if "error" in res:
        return None, res["error"].get("message", "")
    data = res.get("data") or []
    if not data:
        return "(no data)", None
    vals = data[0].get("values") or []
    v = vals[0].get("value") if vals else None
    return json.dumps(v, ensure_ascii=False)[:90], None


def ig_account():
    res = get(f"{BASE}/me", {"access_token": TOKEN,
                             "fields": "id,name,instagram_business_account"})
    return (res.get("instagram_business_account") or {}).get("id")


def section(title, obj_id, metrics, extra=None):
    print("\n" + "=" * 68)
    print(title)
    print("=" * 68)
    _, err = probe(obj_id, NONSENSE, extra)
    if err:
        print("  Meta will accept:\n    " + err[:1400])
    print()
    for m in metrics:
        val, err = probe(obj_id, m, extra)
        if err:
            print(f"  {m:<32} ERROR: {err[:70]}")
        else:
            print(f"  {m:<32} OK -> {val}")


def main():
    if not TOKEN:
        raise SystemExit("FACEBOOK_TOKEN is missing")
    print("=" * 68)
    print(f"followers_tracker probe · {API_VERSION} · page {PAGE_ID}")
    print("=" * 68)
    print("Each metric is requested ON ITS OWN. The tracker batches five into one")
    print("call, so a single removed name empties all five — which is what has")
    print("been happening since the sheet began.")

    section("PAGE INSIGHTS", PAGE_ID, PAGE_METRICS)

    ig = ig_account()
    if ig:
        section(f"INSTAGRAM ACCOUNT INSIGHTS ({ig})", ig, IG_METRICS,
                extra={"metric_type": "total_value"})
    else:
        print("\n  could not resolve the Instagram account")


if __name__ == "__main__":
    main()
