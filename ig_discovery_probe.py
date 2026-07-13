"""
IG account-access probe - READ ONLY. Does NOT write to Google Sheets.

בודק מה אפשר למשוך על חשבון אינסטגרם נוסף (ברירת מחדל: kan_reshetb) עם
הטוקן הקיים, בשתי רמות:
  1. גישה מלאה - אילו עמודי פייסבוק (וחשבונות ה-IG המקושרים) הטוקן בכלל
     רואה. אם היעד שם - יש לנו insights מלאים (views/reach/עוקבים).
  2. Business Discovery - נתונים ציבוריים של כל חשבון עסקי/יוצר אחר:
     עוקבים, כמות פוסטים, ולכל פוסט לייקים/תגובות. בלי views/reach/סטוריז.

Env: FACEBOOK_TOKEN (required). IG_TARGET_USERNAME (default kan_reshetb).
"""

import os
import sys
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

TOKEN = os.environ.get("FACEBOOK_TOKEN")
TARGET = os.environ.get("IG_TARGET_USERNAME", "kan_reshetb")
BASE = "https://graph.facebook.com/v25.0"


def get(url, params=None):
    try:
        return requests.get(url, params=params, timeout=30).json()
    except Exception as e:
        return {"error": {"message": f"request failed: {e}"}}


def main():
    if not TOKEN:
        print("MISSING FACEBOOK_TOKEN")
        sys.exit(1)

    print("=== 1. pages/IG accounts this token can access directly ===")
    res = get(f"{BASE}/me/accounts", {
        "access_token": TOKEN,
        "fields": "name,id,instagram_business_account{username,followers_count}",
        "limit": 50,
    })
    if "error" in res:
        print("ACCOUNTS ERROR:", res["error"].get("message"))
    else:
        for p in res.get("data", []):
            ig = p.get("instagram_business_account") or {}
            print(f"  page: {p.get('name')} ({p.get('id')})"
                  + (f" -> IG @{ig.get('username')} ({ig.get('followers_count', '?')} followers)" if ig else " -> no IG linked"))

    own = get(f"{BASE}/me", {"access_token": TOKEN, "fields": "instagram_business_account"})
    own_ig = (own.get("instagram_business_account") or {}).get("id")
    if not own_ig:
        print("could not resolve own IG id - business discovery needs it")
        sys.exit(1)

    print(f"\n=== 2. business discovery for @{TARGET} ===")
    res = get(f"{BASE}/{own_ig}", {
        "access_token": TOKEN,
        "fields": f"business_discovery.username({TARGET})"
                  "{username,name,followers_count,media_count,"
                  "media.limit(5){caption,media_type,like_count,comments_count,timestamp,permalink}}",
    })
    if "error" in res:
        print("DISCOVERY ERROR:", res["error"].get("message"))
        sys.exit(1)

    bd = res.get("business_discovery", {})
    print(f"  @{bd.get('username')} · {bd.get('name')}")
    print(f"  followers: {bd.get('followers_count', 0):,} · total media: {bd.get('media_count', 0):,}")
    print("  --- 5 recent posts ---")
    for m in bd.get("media", {}).get("data", []):
        cap = (m.get("caption") or "").replace("\n", " ")[:70]
        print(f"  [{m.get('timestamp', '')[:10]}] {m.get('media_type', '')} · "
              f"{m.get('like_count', 0):,} likes · {m.get('comments_count', 0):,} comments · {cap}")

    print("\n✅ business discovery works - public metrics available for this account")


if __name__ == "__main__":
    main()
