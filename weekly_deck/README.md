# Weekly Deck Generator

Fills the approved 16:9 design (`design/weekly-social-light.dc.html`) with real
weekly data and renders a self-contained **HTML + PDF** summary of the last 7
days across YouTube / TikTok / Instagram / Facebook / X.

Delivery is intentionally **not** wired up yet — the generator only produces
files under `out/`. Distribution is decided after the deck is reviewed.

## Run

```bash
pip install -r requirements.txt                 # repo root deps
pip install -r weekly_deck/requirements.txt     # jinja2 + playwright
playwright install chromium

# realistic mock data — no creds, no network (main QA gate):
python weekly_deck/generate_deck.py --mock

# real data (needs GCP_SERVICE_ACCOUNT, FACEBOOK_TOKEN, TIKHUB_TOKEN, GEMINI_API_KEY):
python weekly_deck/generate_deck.py
```

Outputs: `out/deck.html`, `out/deck.pdf`, and `out/slide_1.png` / `out/slide_2.png`
QA screenshots. Flags: `--no-thumbs` (skip thumbnail downloads), `--no-gemini`
(use the deterministic computed insights instead of Gemini).

## How the design maps to data

- **Cover** — hero = sum of weekly views across platforms, delta vs the prior 7 days.
- **Overview** — platforms ranked by weekly views; bar width = share of the top platform.
- **Per-platform slides** (dynamic order by views) — top-3 highlight cards with
  thumbnails, a top-10 table, `חריג` badges on rows whose engagement beats the
  week's median for their view rank, and a computed `נתון מעניין` per platform
  (YT: Shorts view share · TikTok: WhatsApp share of shares · IG: Reels view
  share · FB: total reach · X: reply share of engagement).
- **מה למדנו** — 3 insights; stats are always computed, Gemini only rewrites the
  Hebrew prose (deterministic fallback on any Gemini failure).
- **Closing** — sign-off + week dates.

Platforms with no rows in the window keep their slide but show an honest empty
state (no fabricated numbers).

## Fonts (licensed — never committed)

The SimplerPro OTFs are licensed and are **gitignored** (`design/fonts/`). CI
fetches them from the VPS over SSH using a restricted deploy key
(`FONTS_SSH_KEY` repo secret) whose authorized_keys entry is forced to a single
command that only streams a tar of the fonts dir (host key pinned in the
workflow). The **Light (300)** weight is absent on the VPS; the template only
emits `@font-face` for weights present on disk, so the browser maps 300 to the
nearest available weight (300 is not used in the slide markup).

## Known gaps / fallbacks

- **Logo PNGs** (`design/assets/kan-news-full-black-a.png`, ...) were to be
  pulled from the Claude Design project, but the DesignSync tool was
  unavailable. The template falls back to a clean typographic `כאן חדשות`
  wordmark. Drop the real PNG into `design/assets/` and it is picked up
  automatically (inlined as a data URI).
- **Reporter / כתב-ת column** — the sheets carry no author field, so that column
  renders empty (kept for layout parity; no invented names).
- **X thumbnails** — no cheap image source, so X highlight cards use the styled
  placeholder.
