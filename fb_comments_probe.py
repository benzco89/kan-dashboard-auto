"""
Facebook comment-text probe - READ ONLY. Does NOT write to Google Sheets.

Before extending comment_analyzer.py to Facebook we must verify the page
token can actually read comment TEXT on page posts (needs
pages_read_user_content; the IG probe proved the IG side, this proves FB).

Pulls the page's recent posts, picks the most-commented one, fetches its
comment thread and prints counts + top comments so we can eyeball quality.

Env: FACEBOOK_TOKEN (required). FACEBOOK_PAGE_ID (optional - the GitHub
     secret is empty, so fall back to the known Kan News page id).
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


def get(url, params=None):
    try:
        return requests.get(url, params=params, timeout=30).json()
    except Exception as e:
        return {"error": {"message": f"request failed: {e}"}}


def main():
    if not TOKEN:
        print("MISSING FACEBOOK_TOKEN")
        sys.exit(1)

    res = get(f"{BASE}/{PAGE_ID}/published_posts", {
        "access_token": TOKEN,
        "fields": "id,message,created_time,comments.summary(true).limit(0)",
        "limit": 25,
    })
    if "error" in res:
        print("POSTS ERROR:", res["error"].get("message"))
        sys.exit(1)

    posts = res.get("data", [])
    posts.sort(key=lambda p: p.get("comments", {}).get("summary", {}).get("total_count", 0),
               reverse=True)
    if not posts:
        print("no posts returned")
        sys.exit(1)

    top = posts[0]
    total = top.get("comments", {}).get("summary", {}).get("total_count", 0)
    print(f"=== most-commented recent post: {top['id']} ({total} comments) ===")
    print(f"posted: {top.get('created_time', '')[:16]}")
    print(f"message: {(top.get('message') or '')[:200]}\n")

    res = get(f"{BASE}/{top['id']}/comments", {
        "access_token": TOKEN,
        "fields": "message,like_count,comment_count,created_time",
        "limit": 50,
        "summary": "true",
    })
    if "error" in res:
        print("COMMENTS ERROR:", res["error"].get("message"))
        sys.exit(1)

    comments = res.get("data", [])
    with_text = [c for c in comments if (c.get("message") or "").strip()]
    print(f"pulled {len(comments)} comments, {len(with_text)} with TEXT")
    print(f"API summary total_count: {res.get('summary', {}).get('total_count')}")
    print(f"has next page: {'next' in res.get('paging', {})}\n")

    top_liked = sorted(with_text, key=lambda c: c.get("like_count", 0), reverse=True)[:5]
    print("--- top 5 comments by likes ---")
    for c in top_liked:
        print(f"  [{c.get('like_count', 0):>4}] {(c.get('message') or '')[:100]}")

    if not with_text:
        print("\n❌ comment TEXT is NOT readable with this token - FB extension blocked")
        sys.exit(1)
    print("\n✅ comment TEXT readable - FB extension is feasible")


if __name__ == "__main__":
    main()
