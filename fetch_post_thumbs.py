# -*- coding: utf-8 -*-
"""מוריד תמונות של פוסטים בפייסבוק ובאינסטגרם דרך Graph API, לדוחות.

רץ ב-GitHub Actions (fetch_post_thumbs.yml) כדי להשתמש ב-FACEBOOK_TOKEN במקום להוציא אותו מהסודות.
קלט: קובץ JSON עם רשימת {key, platform, id}; פלט: תיקייה עם <key>.jpg ו-index.json.
קריאה בלבד: לא כותב לגיליונות ולא לטלגרם.

python fetch_post_thumbs.py analysis/elections_2026/thumbs/wanted.json thumbs_out
"""
import json, os, sys, requests

TOKEN = os.environ["FACEBOOK_TOKEN"]
G = "https://graph.facebook.com/v25.0"
wanted_path, out_dir = sys.argv[1], sys.argv[2]
os.makedirs(out_dir, exist_ok=True)
wanted = json.load(open(wanted_path, encoding="utf-8"))
index = {}
for w in wanted:
    key, plat, pid = w["key"], w["platform"], w.get("id")
    if not pid:
        print(key, "no id"); continue
    try:
        if plat == "facebook":
            j = requests.get(f"{G}/{pid}", params={"fields": "full_picture", "access_token": TOKEN}, timeout=60).json()
            url = j.get("full_picture")
        else:
            j = requests.get(f"{G}/{pid}", params={"fields": "media_type,media_url,thumbnail_url", "access_token": TOKEN}, timeout=60).json()
            url = j.get("thumbnail_url") or j.get("media_url")
        if "error" in j:
            print(key, "api error:", j["error"].get("message", "")[:100]); continue
        if not url:
            print(key, "no picture field"); continue
        img = requests.get(url, timeout=60); img.raise_for_status()
        fn = os.path.join(out_dir, key + ".jpg")
        open(fn, "wb").write(img.content)
        index[key] = {"file": key + ".jpg", "bytes": len(img.content)}
        print(key, "ok", len(img.content))
    except Exception as e:
        print(key, "fail", str(e)[:120])
json.dump(index, open(os.path.join(out_dir, "index.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=1)
print("saved", len(index), "of", len(wanted))
