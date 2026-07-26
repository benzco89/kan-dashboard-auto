"""
Instagram reel-retention probe — READ ONLY. Does NOT write to Google Sheets.

The question: Facebook's video_insights turned out to expose a full drop-off
curve for reels (`post_video_retention_graph`) plus replay counts. Does the
Instagram Graph API expose anything equivalent for the same kind of content?

An earlier probe (2026-06-30, **v24**) recorded `clips_replays_count` and
`ig_reels_aggregated_all_plays_count` as "not valid metrics" — but that was
before the Facebook side gained its reel metrics, and the IG collector is still
pinned to v24. This asks again, on v25.

Instagram's insights endpoint requires an explicit `metric`, so it cannot be
enumerated the way video_insights can. The next best thing is to ask for a
deliberately invalid metric: Meta's error message for an unknown IG metric
lists the values it WILL accept, which is an enumeration by another name.

    gh workflow run probe_ig_retention.yml

Env: FACEBOOK_TOKEN (required), IG_API_VERSION (optional, default v25.0),
     PROBE_MEDIA (optional, how many recent items to sample, default 4).
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
API_VERSION = os.environ.get("IG_API_VERSION") or "v25.0"
SAMPLE = int(os.environ.get("PROBE_MEDIA") or "4")
BASE = f"https://graph.facebook.com/{API_VERSION}"

# Anything that could carry retention, replays or a completion share.
CANDIDATES = [
    "ig_reels_avg_watch_time",              # collected today -> the control
    "ig_reels_video_view_total_time",       # exists, not collected
    "clips_replays_count",
    "ig_reels_aggregated_all_plays_count",
    "video_retention_graph",
    "ig_reels_retention_graph",
    "post_video_retention_graph",
    "video_view_completion_rate",
    "ig_reels_video_completion_rate",
    "plays",
    "replays",
    "total_interactions",                   # collected today -> the control
    # from the enumeration the API itself returned — these EXIST and we do not
    # collect them. link_clicks in particular is the only referral metric
    # Instagram offers, and "how many people did social send to kan.org.il" has
    # been an open gap in the whole product.
    "link_clicks",
    "navigation",
    "profile_activity",
    "reels_skip_rate",                      # collected today as skip_rate
    "reposts",
    "crossposted_views",
]

# The name has to be invalid but harmless; the error lists what IS valid.
NONSENSE = "kan_probe_not_a_metric"


def get(url, params):
    try:
        return requests.get(url, params=params, timeout=30).json()
    except Exception as e:
        return {"error": {"message": f"request failed: {e}"}}


def ig_user_id():
    """The token is a PAGE token, so /me IS the page — /me/accounts does not
    exist for it. Same resolution the collector uses."""
    res = get(f"{BASE}/me", {"access_token": TOKEN,
                             "fields": "id,name,instagram_business_account"})
    acc = (res.get("instagram_business_account") or {}).get("id")
    if acc:
        print(f"  IG business account {acc} (page '{res.get('name', '')}')")
        return acc
    print(f"  could not resolve the IG account: {json.dumps(res, ensure_ascii=False)[:200]}")
    return None


def recent_media(ig_id, limit):
    res = get(f"{BASE}/{ig_id}/media", {
        "access_token": TOKEN,
        "fields": "id,media_type,media_product_type,timestamp,caption",
        "limit": limit,
    })
    if "error" in res:
        print(f"  cannot list media: {res['error'].get('message', '')[:160]}")
        return []
    return res.get("data", [])


def probe_one(media_id, metric):
    res = get(f"{BASE}/{media_id}/insights", {"access_token": TOKEN, "metric": metric})
    if "error" in res:
        return None, res["error"].get("message", "")
    data = res.get("data") or []
    if not data:
        return "(no data)", None
    vals = data[0].get("values") or []
    return json.dumps(vals[0].get("value") if vals else None, ensure_ascii=False)[:110], None


def main():
    if not TOKEN:
        raise SystemExit("FACEBOOK_TOKEN is missing")
    print("=" * 66)
    print(f"Instagram reel-retention probe · {API_VERSION}")
    print("=" * 66)

    ig_id = ig_user_id()
    if not ig_id:
        raise SystemExit(1)

    media = recent_media(ig_id, SAMPLE)
    reels = [m for m in media if m.get("media_product_type") == "REELS"] or media
    if not reels:
        raise SystemExit("no media returned")

    first = reels[0]
    print(f"\n--- enumerating via an invalid metric on {first['id']} "
          f"({first.get('media_product_type')})")
    _, err = probe_one(first["id"], NONSENSE)
    print("    " + (err or "(no error — the nonsense metric was accepted?!)")[:1500])

    for m in reels[:SAMPLE]:
        cap = (m.get("caption") or "").replace("\n", " ")[:40]
        print(f"\n--- {m.get('timestamp', '')[:10]} · {m.get('media_product_type')} · {m['id']}")
        print(f"    {cap}")
        for metric in CANDIDATES:
            val, err = probe_one(m["id"], metric)
            if err:
                print(f"      {metric:<38} ERROR: {err[:80]}")
            else:
                print(f"      {metric:<38} OK -> {val}")

    print("\n" + "=" * 66)
    print("A retention curve or a replay count here would mean Instagram can")
    print("answer 'how much of the reel did they watch', the way Facebook now can.")


if __name__ == "__main__":
    main()
