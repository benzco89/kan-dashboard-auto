# -*- coding: utf-8 -*-
"""Facebook reel probe - READ ONLY. Does NOT write to Sheets or to Drive.

Two questions, both of which the archive design would otherwise answer by
assumption:

  1. **Is the file downloadable at all?** The archiver reaches Instagram through
     `media_url` and TikTok through `play_addr`. Facebook's equivalent is the
     `source` field on the video object behind the post. Whether our page token
     is allowed to read it has never been checked.
  2. **What shape is the file?** The page publishes 149 reels and zero
     `/videos/` posts in a fortnight, and 49 of those reels run over three
     minutes - one of them 28. A reel is nominally a vertical product, but a
     landscape segment letterboxed into a vertical frame would look identical
     in every column we store. Only the pixels settle it.

So: resolve `source`, download the bytes, and measure with ffprobe (present on
the GitHub runner). Nothing is uploaded and nothing is written; the files land
in the runner's temp directory and die with it.

    gh workflow run probe_fb_reel.yml       # FACEBOOK_TOKEN lives in CI

Env: FACEBOOK_TOKEN (required), FACEBOOK_PAGE_ID (optional, defaults to Kan),
     FB_API_VERSION (optional), PROBE_POSTS (optional, default 3).
"""

import json
import os
import re
import subprocess
import tempfile

import requests

TOKEN = os.environ.get("FACEBOOK_TOKEN")
PAGE_ID = os.environ.get("FACEBOOK_PAGE_ID") or "220634478361516"
API_VERSION = os.environ.get("FB_API_VERSION") or "v25.0"
SAMPLE = int(os.environ.get("PROBE_POSTS") or 3)
BASE = f"https://graph.facebook.com/{API_VERSION}"

# The signed CDN URL is exactly the kind of value media_archiver refuses to log
# (see _safe_exc_str): it is short-lived, and it grants the bytes to whoever
# holds it. This probe prints shapes, never addresses.
_URL_RE = re.compile(r"https?://\S+")


def scrub(text):
    return _URL_RE.sub("<url>", str(text))


def get(url, params):
    try:
        return requests.get(url, params=params, timeout=30).json()
    except Exception as e:
        return {"error": {"message": f"request failed: {scrub(e)}"}}


def recent_reels(limit):
    """Posts whose permalink says /reel/, newest first, with their video id."""
    res = get(f"{BASE}/{PAGE_ID}/posts", {
        "access_token": TOKEN,
        "fields": "id,created_time,permalink_url,message,"
                  "attachments{media_type,target}",
        "limit": max(limit * 6, 30),
    })
    if "error" in res:
        raise SystemExit(f"cannot list posts: "
                         f"{scrub(res['error'].get('message', ''))[:200]}")
    out = []
    for p in res.get("data", []):
        if "/reel/" not in (p.get("permalink_url") or ""):
            continue
        try:
            vid = p["attachments"]["data"][0]["target"]["id"]
        except Exception:
            continue
        if not vid or vid == p["id"]:
            continue
        out.append({"post": p["id"], "video": vid,
                    "day": (p.get("created_time") or "")[:10],
                    "text": (p.get("message") or "").replace("\n", " ")[:60]})
        if len(out) >= limit:
            break
    return out


def video_fields(video_id):
    """`source` is the download; `format` is what Meta says the shape is."""
    return get(f"{BASE}/{video_id}", {
        "access_token": TOKEN,
        "fields": "source,length,format,title,description",
    })


def download(url, dest):
    with requests.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        n = 0
        with open(dest, "wb") as fh:
            for chunk in r.iter_content(1 << 16):
                fh.write(chunk)
                n += len(chunk)
    return n


def measure(path):
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries", "stream=width,height,codec_name",
         "-show_entries", "format=duration,bit_rate", "-of", "json", path],
        capture_output=True, text=True)
    d = json.loads(out.stdout or "{}")
    s = (d.get("streams") or [{}])[0]
    f = d.get("format") or {}
    return {"w": s.get("width"), "h": s.get("height"),
            "codec": s.get("codec_name"),
            "sec": float(f.get("duration") or 0),
            "kbps": int(f.get("bit_rate") or 0) // 1000}


def main():
    if not TOKEN:
        raise SystemExit("FACEBOOK_TOKEN is missing")
    print("=" * 66)
    print(f"Facebook reel shape probe · {API_VERSION} · page {PAGE_ID}")
    print("READ ONLY — no sheet write, no Drive upload")
    print("=" * 66)

    reels = recent_reels(SAMPLE)
    if not reels:
        raise SystemExit("no /reel/ posts returned")

    tmp = tempfile.mkdtemp()
    for r in reels:
        print(f"\n--- {r['day']} · post {r['post']} · video {r['video']}")
        print(f"    {r['text']}")
        meta = video_fields(r["video"])
        if "error" in meta:
            print(f"    ❌ fields: "
                  f"{scrub(meta['error'].get('message', ''))[:160]}")
            continue
        src = meta.get("source")
        print(f"    source present: {bool(src)}   "
              f"length: {meta.get('length')}s")
        fmt = meta.get("format") or []
        if fmt:
            shapes = ", ".join(f"{f.get('width')}x{f.get('height')}"
                               for f in fmt)
            print(f"    format renditions: {shapes}")
        if not src:
            print("    ⚠️ no source — the bytes are not reachable this way")
            continue
        dest = os.path.join(tmp, f"{r['video']}.mp4")
        try:
            n = download(src, dest)
        except Exception as e:
            print(f"    ❌ download: {scrub(e)[:160]}")
            continue
        m = measure(dest)
        ratio = (m["w"] / m["h"]) if m["w"] and m["h"] else 0
        shape = ("vertical" if ratio < 0.95 else
                 "square" if ratio < 1.05 else "LANDSCAPE")
        print(f"    ✅ {n / 1e6:.1f}MB  {m['w']}x{m['h']}  {m['codec']}  "
              f"{m['sec']:.0f}s  {m['kbps']}kbps")
        print(f"    → {shape} (ratio {ratio:.2f})")


if __name__ == "__main__":
    main()
