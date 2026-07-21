# Weekly Deck Generator

Fills the approved 16:9 design (`design/weekly-social-light.dc.html`) with real
weekly data and renders a self-contained **HTML + PDF** summary of the last 7
days across YouTube / TikTok / Instagram / Facebook / X.

Delivery is intentionally **not** wired up yet — the generator only produces
files under `out/`. Distribution is decided after the deck is reviewed.

## The extract → edit → render loop

The run is split so the **editorial layer is human-authored**. Nothing calls an
LLM by default: every string and number in the deck comes from a text file you
can fix by hand.

```bash
pip install -r requirements.txt                 # repo root deps
pip install -r weekly_deck/requirements.txt     # jinja2 + playwright + pillow-heif
playwright install chromium

# 1. EXTRACT — sheets + thumbnails + all the arithmetic
python weekly_deck/generate_deck.py --extract
#    -> out/deck_content.json   (the editable file, ~20KB, Hebrew readable)
#    -> out/thumbs/<platform>_<id>.jpg

# 2. EDIT  out/deck_content.json  by hand — titles, reporters, learnings,
#    fun_fact.chosen, story_of_the_week, closing credits. It is plain UTF-8
#    JSON with no base64 in it.

# 3. RENDER — offline, instant, deterministic. Re-run this after every edit.
python weekly_deck/generate_deck.py --render
```

`--extract` and `--render` with no flag at all = both, in order. `--mock` runs
the whole loop on hardcoded data with no creds and no network (the QA gate).

Outputs: `out/deck.html`, `out/deck.pdf`, `out/slide_1.png` / `slide_2.png`
(`--qa-all` screenshots every slide).

Flags: `--thumbstyle portrait|blur` (see below) · `--no-thumbs` (skip downloads)
· `--gemini` (opt in to an LLM rewrite of the learnings prose during extract —
**off by default**, and it still never touches numbers).

### What lives in deck_content.json

```jsonc
{
  "window":  { "start", "end", "range_str", "range_short" },
  "hero":    { "total_views", "delta_pct" },
  "platforms": [{
     "key", "name", "weekly_views", "delta_pct", "followers",
     "fun_fact": { "chosen": "<label>", "candidates": [{ "label","value","suffix","text" }] },
     "top": [{ "id","title","reporter","views","engagement","thumb" }]   // thumb = filename in out/thumbs/, or null
  }],
  "learnings": [{ "icon","number","color","title","sentence" }],          // 3-4 cards
  "story_of_the_week": { "title","sentence","platforms":[{"name","views"}] } | null,
  "closing": { "reporters": [...], "note" }
}
```

Everything is derived at render time: platform order (by `weekly_views`), the
`חריג` badges (each metric vs a normal post that week), number formatting, and the
thumbnails (read from `out/thumbs/` and inlined as data URIs). So editing
`weekly_views` reorders the deck; editing `engagement` re-flags anomalies.

## Window

The report covers the **last complete Israeli week (Sunday→Saturday, Asia/
Jerusalem)**. Run any day and it uses the most recently finished Sun–Sat; the
prior Sun–Sat is the baseline for week-over-week deltas. Running Tue 2026-07-21
⇒ 2026-07-12..2026-07-18 (prior 2026-07-05..2026-07-11).

## How the design maps to data

- **Cover** — hero = sum of weekly views across platforms, delta vs the prior week.
- **Overview** — platforms ranked by weekly views; bar width = share of the top platform.
- **Per-platform slides** (dynamic order by views) — top-3 highlight cards + a
  top-10 table (square leading thumbnails) with its own **חריג column** saying
  *what* is unusual about an item — shares, comments, likes or engagement at
  2.5x or more of a normal post that week, shown as e.g. `🔁 שיתופים ×3.4`
  (empty on most rows) — and a `נתון מעניין` picked from the platform's
  candidate list (`fun_fact.chosen`).
- **מה למדנו** — 3–4 learning cards `{icon, number, color, title, sentence}`
  plus an optional wider **story of the week** block that names one story and
  shows its per-platform numbers. Authored in `deck_content.json`; the code only
  seeds deterministic defaults.
- **Closing** — sign-off and week dates.

Platforms with no rows in the window keep their slide but show an honest empty
state (no fabricated numbers).

## Headlines vs captions

Kan captions are long and run past the headline. `--extract` stores both: the
rendered `title` is the **headline only** — cut at the first 👇, line break or
sentence end (never at `:`, which is part of Kan headlines like
`פרסום ראשון: ...`), then capped ~80 chars on a word boundary — and `caption`
keeps the first 200 chars of the original for reference while editing.

## Reporter credits (כתב/ת column)

The sheets carry no author field, so `--extract` pre-fills `reporter`
deterministically **from the full caption** (the credit sits at the end, past
the headline): an explicit `כתב:` / `תחקיר:` credit, a parenthesised 2–3-word
Hebrew name (the last one wins), or an `@handle`. `צילום:` / `עריכה:` are
photographers and editors, never reporters, and nothing is ever invented —
unknowns stay empty for you to fill in.

`reporters_map.json` (repo-tracked, **user-editable and meant to grow** — just
add a line) maps `{"@handle": "שם בעברית"}` and is applied during extract. It
ships seeded with the confirmed Kan handles. Matching is **case-insensitive**
and works whether or not the caption wrote the leading `@` (`@ItayBlumental`,
`@itayblumental` and a bare `ItayBlumental` all resolve). Bare handles are only
matched against confirmed map entries — never guessed from arbitrary words.

An **unmapped handle is left visible as `@handle`** in deck_content.json, so it
is easy to spot and add to the map. Extracted names (not handles) fill the
closing credits block.

**Timing:** the map is read at `--extract`, not at `--render`. Adding a handle
after you have already extracted does **not** update the existing
deck_content.json — `--render` will keep showing the raw `@handle`. For the
current week, edit that `reporter` field by hand; the new map entry takes effect
from the next extract onward.

## Thumbnails

`--extract` downloads top-10 thumbnails per platform into `out/thumbs/` as
files; `--render` inlines them as data URIs. Sources: YouTube (`i.ytimg.com`,
free), Facebook / Instagram (Graph v24), TikTok (TikHub covers, heic→jpeg
converted). Failures degrade to a styled placeholder.

**X has no thumbnails by design** — the media is usually irrelevant, so the X
highlight cards are text-forward instead (the tweet is the hero, no grey box).

### `--thumbstyle`

Content is mixed *per item* (a YouTube top-3 can hold one Short and two
landscape videos), so both variants are per-item agnostic:

- **`portrait`** (default) — the highlight slot is a true 4:5 portrait
  (272×340), `object-fit:cover` anchored `center top` so faces and on-screen
  headlines survive. Cards are narrower, so the slide is rebalanced: cards sit
  beside a wide `נתון מעניין` panel and the top-10 table runs full width below.
- **`blur`** — keeps the original wide 16:9 slot, but the image is `contain`ed
  at full frame over a blurred, scaled-up, darkened copy of itself. **Nothing is
  ever cropped.**

Table row thumbnails are square (1:1, 26px) in both variants — squares degrade
gracefully for any source ratio at that size.

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
