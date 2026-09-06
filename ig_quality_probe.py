# -*- coding: utf-8 -*-
"""Can a Kan reel be had at 1080 rather than the 720 the archive is storing?

READ ONLY. Writes to no sheet, uploads nothing.

Measured 2026-09-06: every Instagram file the archive holds is ~716x1266 or
720x1280, while TikTok's copy of the same item is 1080x1910. And the Facebook
video node for those same vertical reels reports
`format: 130x230, 480x848, 720x1273, 1084x1916` yet hands `source` a 716x1266
file — for landscape items the largest format and the source agree, for
vertical ones they do not. So a ~1080 rendition appears to exist on Meta's
side and is not the one being served.

The archive is forward-only: a lower-quality copy stored today cannot be
upgraded tomorrow. So the question is worth one probe before more days pass.

Three routes, none of them assumed:
  1. **Every field Graph will give on the IG media object.** `media_url` is
     the one the archiver uses; ask for the rest by name and see what returns.
  2. **The Facebook video node for the same reel.** Cross-posted vertical
     reels exist there too, with their own `source` - possibly a different
     rendition from Instagram's.
  3. **`thumbnail_url` dimensions**, as a cheap witness to what resolution
     Meta considers native for the item.

Whatever downloads, ffprobe measures. Nothing is inferred from a field name.

    gh workflow run probe_ig_quality.yml     # FACEBOOK_TOKEN lives in CI

Env: FACEBOOK_TOKEN (required), FACEBOOK_PAGE_ID (optional),
     FB_API_VERSION (optional), PROBE_POSTS (optional, default 3).
"""

import json
import os
import re
import shutil
import subprocess
import tempfile

import requests

TOKEN = os.environ.get("FACEBOOK_TOKEN")
PAGE_ID = os.environ.get("FACEBOOK_PAGE_ID") or "220634478361516"
API_VERSION = os.environ.get("FB_API_VERSION") or "v25.0"
SAMPLE = int(os.environ.get("PROBE_POSTS") or 3)
BASE = f"https://graph.facebook.com/{API_VERSION}"

_URL_RE = re.compile(r"https?://\S+")

# Asked one at a time: an unknown field in a combined request fails the whole
# call, which would read as "none of these exist".
IG_FIELDS = [
    "media_url", "thumbnail_url", "media_type", "media_product_type",
    "permalink", "caption", "timestamp", "shortcode", "is_shared_to_feed",
    "video_url", "media_url_hd", "hd_url", "source", "format", "images",
    "video_versions", "original_media_url",
]


def scrub(t):
    return _URL_RE.sub("<url>", str(t))


def get(url, params):
    try:
        return requests.get(url, params=params, timeout=30).json()
    except Exception as e:
        return {"error": {"message": f"request failed: {scrub(e)}"}}


def ig_account():
    res = get(f"{BASE}/me", {"access_token": TOKEN,
                             "fields": "instagram_business_account"})
    return (res.get("instagram_business_account") or {}).get("id")


def recent_reels(ig_id, limit):
    res = get(f"{BASE}/{ig_id}/media", {
        "access_token": TOKEN,
        "fields": "id,caption,timestamp,media_type,media_product_type",
        "limit": 25,
    })
    return [m for m in res.get("data", [])
            if m.get("media_type") == "VIDEO"][:limit]


def measure(path):
    if not shutil.which("ffprobe"):
        return None
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries", "stream=width,height",
         "-show_entries", "format=duration,bit_rate", "-of", "json", path],
        capture_output=True, text=True)
    d = json.loads(out.stdout or "{}")
    s = (d.get("streams") or [{}])[0]
    f = d.get("format") or {}
    return (f"{s.get('width')}x{s.get('height')} "
            f"{float(f.get('duration') or 0):.0f}s "
            f"{int(f.get('bit_rate') or 0) // 1000}kbps")


def fetch_and_measure(url, dest, label):
    try:
        with requests.get(url, stream=True, timeout=180) as r:
            r.raise_for_status()
            n = 0
            with open(dest, "wb") as fh:
                for c in r.iter_content(1 << 16):
                    fh.write(c)
                    n += len(c)
    except Exception as e:
        print(f"      {label}: download failed {scrub(e)[:70]}")
        return
    print(f"      {label}: {n / 1e6:.1f}MB  {measure(dest)}")


def main():
    if not TOKEN:
        raise SystemExit("FACEBOOK_TOKEN is missing")
    print("=" * 70)
    print("Instagram quality probe · READ ONLY · is 1080 reachable?")
    print("=" * 70)

    ig_id = ig_account()
    if not ig_id:
        raise SystemExit("no instagram_business_account on this token")
    reels = recent_reels(ig_id, SAMPLE)
    tmp = tempfile.mkdtemp()

    print("\n--- route 1: every field Graph will admit on IG media ---")
    for m in reels[:1]:
        print(f"media {m['id']}  {(m.get('caption') or '')[:44]}")
        for f in IG_FIELDS:
            res = get(f"{BASE}/{m['id']}", {"access_token": TOKEN,
                                            "fields": f})
            if "error" in res:
                msg = res["error"].get("message", "")
                mark = "—" if "nonexisting field" in msg else "!"
                print(f"   {mark} {f:22s} {scrub(msg)[:56]}")
            else:
                val = {k: v for k, v in res.items() if k != "id"}
                print(f"   ✓ {f:22s} {scrub(json.dumps(val, ensure_ascii=False))[:56]}")

    print("\n--- route 3: what media_url actually delivers ---")
    for m in reels:
        res = get(f"{BASE}/{m['id']}", {"access_token": TOKEN,
                                        "fields": "media_url,thumbnail_url"})
        url = res.get("media_url")
        print(f"\n   {m['id']}  {(m.get('caption') or '')[:44]}")
        if url:
            fetch_and_measure(url, os.path.join(tmp, f"ig_{m['id']}.mp4"),
                              "instagram media_url")

    print("\n--- route 2: the Facebook video node for the same reels ---")
    res = get(f"{BASE}/{PAGE_ID}/posts", {
        "access_token": TOKEN,
        "fields": "id,permalink_url,message,attachments{target}",
        "limit": 30})
    for p in res.get("data", [])[:12]:
        if "/reel/" not in (p.get("permalink_url") or ""):
            continue
        try:
            vid = p["attachments"]["data"][0]["target"]["id"]
        except Exception:
            continue
        meta = get(f"{BASE}/{vid}", {"access_token": TOKEN,
                                     "fields": "source,length,format"})
        fmt = meta.get("format") or []
        shapes = ", ".join(f"{f.get('width')}x{f.get('height')}" for f in fmt)
        if not shapes or "191" not in shapes:      # vertical only
            continue
        print(f"\n   video {vid}  {(p.get('message') or '')[:44]}")
        print(f"      format says: {shapes}")
        if meta.get("source"):
            fetch_and_measure(meta["source"],
                              os.path.join(tmp, f"fb_{vid}.mp4"),
                              "facebook source")
        break


if __name__ == "__main__":
    main()
