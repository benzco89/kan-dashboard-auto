"""
Audience demographics probe - READ ONLY. Does NOT write to Google Sheets.

בודק אילו נתוני דמוגרפיה ברמת החשבון הטוקן הקיים באמת מחזיר, לפני שבונים
עמוד/סקשן דמוגרפיה (יש mockup ב-design/unified_audience_demographics):
  - אינסטגרם: follower_demographics עם breakdown של age/gender/city/country
  - פייסבוק: page_fans_* (חלק הוצאו משימוש בגרסאות האחרונות - לכן בודקים)

Env: FACEBOOK_TOKEN (required). FACEBOOK_PAGE_ID (fallback לעמוד כאן).
"""

import os
import sys
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

TOKEN = os.environ.get("FACEBOOK_TOKEN")
PAGE_ID = os.environ.get("FACEBOOK_PAGE_ID") or "220634478361516"
BASE = "https://graph.facebook.com/v25.0"


def get(url, params):
    try:
        return requests.get(url, params=params, timeout=30).json()
    except Exception as e:
        return {"error": {"message": f"request failed: {e}"}}


def show(label, res):
    if "error" in res:
        print(f"  ❌ {label}: {res['error'].get('message', '')[:110]}")
        return
    data = res.get("data", [])
    if not data:
        print(f"  ⚠️ {label}: empty")
        return
    print(f"  ✅ {label}:")
    for item in data:
        tv = item.get("total_value", {})
        breakdowns = tv.get("breakdowns", [])
        if breakdowns:
            results = breakdowns[0].get("results", [])
            results.sort(key=lambda r: -(r.get("value") or 0))
            for r in results[:8]:
                dims = "/".join(r.get("dimension_values", []))
                print(f"      {dims:25s} {r.get('value', 0):,}")
        else:
            vals = item.get("values", [])
            v = vals[0].get("value") if vals else tv.get("value")
            print(f"      value: {str(v)[:200]}")


def main():
    if not TOKEN:
        print("MISSING FACEBOOK_TOKEN")
        sys.exit(1)

    res = get(f"{BASE}/me", {"access_token": TOKEN, "fields": "instagram_business_account"})
    ig_id = (res.get("instagram_business_account") or {}).get("id")

    print("=== INSTAGRAM account-level demographics ===")
    for breakdown in ("age", "gender", "city", "country"):
        res = get(f"{BASE}/{ig_id}/insights", {
            "access_token": TOKEN,
            "metric": "follower_demographics",
            "period": "lifetime",
            "metric_type": "total_value",
            "breakdown": breakdown,
        })
        show(f"follower_demographics[{breakdown}]", res)

    print("\n=== INSTAGRAM engaged-audience demographics ===")
    res = get(f"{BASE}/{ig_id}/insights", {
        "access_token": TOKEN,
        "metric": "engaged_audience_demographics",
        "period": "lifetime",
        "metric_type": "total_value",
        "breakdown": "age",
    })
    show("engaged_audience_demographics[age]", res)

    print("\n=== FACEBOOK page demographics ===")
    for metric in ("page_fans_gender_age", "page_fans_city", "page_fans_country",
                   "page_impressions_by_age_gender_unique"):
        res = get(f"{BASE}/{PAGE_ID}/insights", {
            "access_token": TOKEN, "metric": metric, "period": "lifetime",
        })
        show(metric, res)

    print("\n=== done (read-only) ===")


if __name__ == "__main__":
    main()
