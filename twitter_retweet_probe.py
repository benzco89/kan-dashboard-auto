"""
בדיקה: מה בדיוק מגיע מ-GetXAPI על ריטווט.

בגיליון כל ריטווט מופיע עם 970-1,450 צפיות ו-0 לייקים, בלי קשר לציוץ המקורי
(גרובייס: 1,104 אצלנו מול 32,613 ב-X). ציוצים של כאן עצמה תואמים ל-X בפחות
מ-1%. הבדיקה מדפיסה את כל השדות המספריים של עטיפת הריטווט ושל הציוץ המקורי
שבתוכה, כדי לראות מאיפה מגיע המספר ואיפה נמצא המספר האמיתי.

קריאה בלבד: לא נוגע בגוגל שיטס. הפלט הגולמי נשמר ב-twitter_retweet_probe/ ל-artifact.
"""
import json
import os
import sys

from twitter_collector import API_BASE, USERNAME, _headers, get_api_key, http_get_json, tweet_id_of

OUT_DIR = "twitter_retweet_probe"
MAX_PAGES = 25
WANT_RTS = 3


def is_retweet(t):
    return (t.get("text") or "").startswith("RT @")


def numeric_fields(obj, path="", depth=0):
    """כל שדה מספרי (או מחרוזת-מספר) בעץ, עם הנתיב שלו. עוצר בעומק 4."""
    out = []
    if depth > 4:
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}" if path else k
            if isinstance(v, bool):
                continue
            if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit() and len(v) < 12):
                out.append((p, v))
            elif isinstance(v, (dict, list)):
                out.extend(numeric_fields(v, p, depth + 1))
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:3]):
            out.extend(numeric_fields(v, f"{path}[{i}]", depth + 1))
    return out


def nested_tweets(obj, path="", depth=0):
    """נתיבים של אובייקטים מקוננים שנראים כמו ציוץ (יש להם text ו-viewCount/likeCount)."""
    found = []
    if depth > 4 or not isinstance(obj, dict):
        return found
    for k, v in obj.items():
        p = f"{path}.{k}" if path else k
        if isinstance(v, dict):
            if "text" in v and ("viewCount" in v or "likeCount" in v):
                found.append((p, v))
            found.extend(nested_tweets(v, p, depth + 1))
    return found


def show(label, t):
    print(f"\n{'=' * 70}\n{label}: id={tweet_id_of(t)} createdAt={t.get('createdAt')}")
    print(f"text: {(t.get('text') or '')[:90]!r}")
    print(f"url:  {t.get('url')}")
    print(f"top-level keys: {sorted(t.keys())}")
    print("numeric fields (whole object):")
    for p, v in numeric_fields(t):
        print(f"   {p} = {v}")
    nested = nested_tweets(t)
    print(f"nested tweet-like objects: {[p for p, _ in nested] or 'NONE'}")
    for p, v in nested:
        author = v.get("author") or {}
        print(f"   -> {p}: id={tweet_id_of(v)} author=@{author.get('userName') or author.get('screen_name')} "
              f"viewCount={v.get('viewCount')} likeCount={v.get('likeCount')} "
              f"retweetCount={v.get('retweetCount')} text={(v.get('text') or '')[:50]!r}")


def main():
    if not get_api_key():
        sys.exit("❌ missing GETXAPI_KEY")
    os.makedirs(OUT_DIR, exist_ok=True)

    rts, own, cursor = [], [], None
    for page in range(1, MAX_PAGES + 1):
        params = {"userName": USERNAME}
        if cursor:
            params["cursor"] = cursor
        data = http_get_json(f"{API_BASE}/twitter/user/tweets", headers=_headers(), params=params)
        tweets = data.get("tweets") or []
        if page == 1:
            print(f"page-level keys: {sorted(data.keys())}")
            with open(os.path.join(OUT_DIR, "page1_raw.json"), "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=1)
        for t in tweets:
            (rts if is_retweet(t) else own).append(t)
        print(f"page {page}: {len(tweets)} tweets, retweets so far: {len(rts)}")
        if len(rts) >= WANT_RTS or not data.get("has_more") or not data.get("next_cursor"):
            break
        cursor = data["next_cursor"]

    with open(os.path.join(OUT_DIR, "retweets_raw.json"), "w", encoding="utf-8") as f:
        json.dump(rts, f, ensure_ascii=False, indent=1)
    if own:
        with open(os.path.join(OUT_DIR, "own_raw.json"), "w", encoding="utf-8") as f:
            json.dump(own[:3], f, ensure_ascii=False, indent=1)

    if own:
        show("OWN TWEET (for comparison)", own[0])
    for i, t in enumerate(rts[:WANT_RTS], 1):
        show(f"RETWEET {i}", t)
    if not rts:
        print("\n⚠️ no retweet found in the pages walked")


if __name__ == "__main__":
    main()
