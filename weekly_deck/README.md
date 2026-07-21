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

## Window

The report covers the **last complete Israeli week (Sunday→Saturday, Asia/
Jerusalem)**. Run any day and it uses the most recently finished Sun–Sat; the
prior Sun–Sat is the baseline for week-over-week deltas. Running Tue 2026-07-21
⇒ 2026-07-12..2026-07-18 (prior 2026-07-05..2026-07-11).

## How the design maps to data

- **Cover** — hero = sum of weekly views across platforms, delta vs the prior week.
- **Overview** — platforms ranked by weekly views; bar width = share of the top platform.
- **Per-platform slides** (dynamic order by views) — top-3 highlight cards + a
  top-10 table (each row can carry a small leading thumbnail), `חריג` badges on
  rows whose engagement beats the week's median for their view rank, and an
  LLM-curated `נתון מעניין` (see below).
- **מה למדנו** — 3 insights; stats are always computed, Gemini only rewrites the
  Hebrew prose (deterministic fallback on any Gemini failure).
- **Closing** — sign-off, week dates, and the week's credited reporters.

Platforms with no rows in the window keep their slide but show an honest empty
state (no fabricated numbers).

## Reporter credits (כתב/ת column)

The sheets carry no author field, so reporters are extracted from captions: one
Gemini batch call over all top-10 items across the 5 platforms returns a
credited person per item (a `צילום:` photographer is NOT a reporter → null;
never invented). On Gemini failure a deterministic fallback runs: a trailing
`(שם כתב)` that looks like a 2–3-word Hebrew name, or a bare `@handle`.
Extracted names (not @handles) also fill the closing credits block.

## Weekly-varying "נתון מעניין"

Each platform computes 4–6 candidate facts deterministically (value + label +
default sentence). One Gemini call picks the most newsworthy candidate per
platform and phrases it, but must copy the headline number **verbatim** from the
chosen candidate — validated in code (the number must equal one of that
candidate's values), else it falls back to the platform default (candidate[0],
== the previous fixed fact). The model never touches arithmetic, so the deck
varies week to week with the numbers still trustworthy.

## Thumbnails

Top-10 rows on all 5 platforms: YouTube (`i.ytimg.com`, free), Facebook/
Instagram (Graph v24), TikTok (TikHub covers, heic→jpeg converted), **X
(GetXAPI** `/twitter/user/tweets`, `GETXAPI_KEY`). Each downloaded to a data URI
(heic-safe), graceful placeholder on failure. ~50 fetches/run.

## Fonts (licensed — never committed)

The SimplerPro OTFs are licensed and are **gitignored** (`design/fonts/`). CI
fetches them from the VPS over SSH using a restricted deploy key
(`FONTS_SSH_KEY` repo secret) whose authorized_keys entry is forced to a single
command that only streams a tar of the fonts dir (host key pinned in the
workflow). The **Light (300)** weight is absent on the VPS; `font_faces()` emits
an explicit `@font-face` mapping weight 300 → the Regular(400) OTF (300 is not
used in the slide markup anyway).

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
