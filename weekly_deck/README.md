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
#    Uncredited items are listed in out/reporters_todo.txt — fill the names into
#    weekly_deck/reporters_overrides.json so they survive the next extract.

# 3. RENDER — offline, instant, deterministic. Re-run this after every edit.
python weekly_deck/generate_deck.py --render

# Re-running --extract KEEPS the hand-written layer (learnings, story of the
# week, closing note, chosen fun-facts) as long as the window is unchanged, and
# then checks every number in it against the fresh data.
```

`--extract` and `--render` with no flag at all = both, in order. `--mock` runs
the whole loop on hardcoded data with no creds and no network (the QA gate).

## Every run gets its own folder

`--extract` names the folder after the window it just pulled —
`out/2026-07-19_25/` — and writes `deck_content.json`, `thumbs/`, `deck.html`,
`deck.pdf`, `reporters_todo.txt` and the QA screenshots into it. `--mock` writes
to `out/mock/`. Every other flag continues **the newest week folder** unless
`--week 2026-07-12_18` says otherwise.

Weeks therefore stop overwriting each other, last week's deck stays readable
next to this one, and the QA gate can no longer flatten a real deck — which it
did twice, and each time the hand-written editorial layer had to be typed again.

Outputs: `out/<week>/deck.html`, `deck.pdf`, `slide_1.png` / `slide_2.png`
(`--qa-all` screenshots every slide).

Flags: `--thumbstyle portrait|blur` (see below) · `--no-thumbs` (skip downloads)
· `--reporters-todo` (rebuild the uncredited-items list from an existing
`deck_content.json`, offline) · `--retitle` (re-derive headlines, offline) · `--gemini` (opt in to an LLM rewrite of the
prose during extract — **off by default**, and it still never touches numbers).

### What the deck deliberately does not say

It goes to the whole newsroom, so it reports the week and stops there. No card
compares one content format against another ("X% of the views came from Reels")
and none ends in advice ("worth focusing there", "worth a look"): a news desk
covers the news it has, and next week's editorial calls should not be argued
from last week's format mix. Keep hand-written `learnings` and `fun_fact` text on
the same side of that line.

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
`חריג` badges (each metric's rate vs a normal post's rate that week), number formatting, and the
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
  top-10 table (square leading thumbnails) with **one column per interaction**
  (see *Interactions, not an engagement %* below) + its own **חריג column** saying
  *what* is unusual about an item — shares, comments, likes or engagement at
  3x or more of a normal post's **rate** that week, shown as e.g.
  `🔁 פי 3.4 שיתופים` (empty on most rows, and capped at 4 badges per slide). Rates, not absolute counts: the top-10
  are the week's biggest posts by definition, so comparing raw counts would flag
  almost every row and only restate the views column. Comparing
  interactions-per-view instead surfaces the post that punched above its own
  reach — and a `נתון מעניין` picked from the platform's
  candidate list (`fun_fact.chosen`).
- **מה קרה השבוע** — 3–4 cards `{icon, number, color, title, sentence}` plus an
  optional wider **story of the week** block. Authored in `deck_content.json`;
  the code only seeds deterministic defaults.

### The story of the week

Kan runs a story everywhere, so the week's story is the one that reached the
most platforms — and that, unlike "is this interesting", is something the data
can answer. `--extract` groups the week's items by caption word overlap
(transitively, so a first-person testimonial can reach a news summary through a
third post that sits between them) and lists the strongest clusters, ranked by
platform reach first and total views second.

It **suggests**; it never decides. Word overlap can tell that two posts share
wording, not that a desk would call them the same story.

The block records the **items** it is about:

```jsonc
"story_of_the_week": {
  "title": "…", "sentence": "…",             // yours
  "items": ["facebook:<id>", "tiktok:<id>"], // which posts it is about
  "platforms": [{"name","views"}]            // COMPUTED from those items
}
```

The figures under it are recomputed from those items on every render, so they
cannot drift away from the deck around them — which is exactly what happened
when they were typed by hand. Write the words; never type the numbers.
- **Closing** — sign-off and week dates.

Platforms with no rows in the window keep their slide but show an honest empty
state (no fabricated numbers).

## Interactions, not an engagement %

The table used to carry one `מעורבות` column. It was a lie of arithmetic: every
collector had invented its own `engagement_rate`, so five slides showed five
different measurements under one heading. Facebook divided
(clicks + like + comments + shares) by **reach**; Instagram divided
(likes + comments + saves + shares) by **reach**; TikTok and X divided by
**views**. Measured over ~50 recent items per platform, **88% of Facebook's
"engagement" was link clicks** — a published 10.6% against 1.2% of actual social
interaction. A reader comparing the Facebook slide to the Instagram slide
concluded the opposite of the truth.

So the deck shows the counts themselves, one column each, and never a percent:

| | ❤️ | 💬 | 🔁 |
|---|---|---|---|
| פייסבוק | **all six reactions** | תגובות | שיתופים |
| אינסטגרם / טיקטוק | לייקים | תגובות | שיתופים |
| X | לייקים | replies | **retweets + quotes** |
| יוטיוב | לייקים | תגובות | — column absent |

`social_dashboard/metrics.py` is the single definition, shared with the
dashboard so the two can never drift again (it lives there because the VPS
deploy rsyncs only `social_dashboard/`; `social_dashboard/test_metrics.py`
locks it against real sheet rows). The **collectors are untouched** — the sheets
keep their `engagement_rate` column, because the daily Telegram report prints it
and the alert thresholds were calibrated on it.

Facebook counts every reaction because the collector's `total_engagement` keeps
only `likes` and drops love/haha/wow/sad/angry — a median 20% of reactions and
up to 42% on one post. On a news page an angry reaction is not noise.

Saves (Instagram, TikTok) get no column: Facebook, X and YouTube have no
equivalent, and a number whose meaning changes between slides is the problem
being fixed. They appear as a `נתון מעניין` instead.

**The headers are emoji on purpose.** "שיתופים" at 20px bold needs ~75px, the
number under it needs 78px — a word header would have set the column width and
taken another 20px per column out of the כותרת column. The icons are the ones
the חריג badge already uses, and the overview slide spells them out.

**Nothing is lost by dropping the percent.** Raw counts in a table sorted by
views mostly restate the views column — which is precisely why the חריג badge
compares *rates* and not counts. The counts are the context; the badge is the
signal. It still computes an engagement dimension, now defined as the sum of the
three displayed columns over views, so no number in the deck can disagree with
another.

Cost, measured against the render rather than guessed: the כותרת column drops
from 1042px to 872px (700px where the תוכנית column shows). No row that fitted
before is clipped now — the three already-clipped X rows are clipped harder
(X's table is the narrow one, sharing its row with the `נתון מעניין` panel).

A `deck_content.json` extracted *before* this change still renders: its
Facebook ❤️ figures are like-only until the next `--extract`.

## Headlines vs captions

Kan captions are long and run past the headline. `--extract` stores both: the
rendered `title` is the **headline only** and `caption` keeps the first 400
chars of the original for reference while editing.

The headline is cut at the first 👇, line break or sentence end, then **capped**
at the width the table column can actually show — derived from the grid, not
guessed: `title_budget_px()` reproduces the column arithmetic of
`template.html.j2` (it predicts Facebook's 656px column to the pixel) and
`headline_cap()` divides it by a measured character width.

| | budget | cap |
|---|---|---|
| פייסבוק / אינסטגרם / טיקטוק | 656px | 52 chars |
| יוטיוב (two interaction columns) | 748px | 59 chars |
| X (its table shares the row with the נתון מעניין panel) | 382px | 30 chars |

The character width is the **p90** of 48 real rendered headlines (12.6px;
median 11.6, max 13.5). The median would leave the widest tenth of headlines to
be cut a second time by the browser — mid-word, which is exactly what the cap
exists to prevent — and the max would shorten every row by 8 characters to
protect 3% of them.

### The narrow table (X) buys its words back

X's table shares its row with the `נתון מעניין` panel, so it is ~320px narrower
than the rest. Two things give the tweets their text back there:

- **no תוכנית column on a narrow table.** It cost 172px — about 15 characters
  off every headline — to label the two or three rows that carry a programme.
  On X those headlines *are* the tweets. (A programme an editor typed by hand is
  still never hidden; on a narrow slide that is the one case where a headline can
  reach the browser ellipsis.)

  On a **wide** table the column is free — `headline_cap` reserves its 172px
  either way — so there one tagged row is enough to show it. Hiding it would
  throw away a programme we know and lengthen nothing.
- **22px rows instead of 25px**, which buys another ~14%.

Together: 30 characters → **49**.

Elsewhere the cap assumes the תוכנית column IS showing, since that is decided
per slide at render and the headline was cut at extract — sizing for the
narrower case can only leave room to spare, never clip.

### Cards get their own, longer headline

A highlight card is five lines tall and holds ~150 characters; a table row holds
49. They used to show the same string, so an X card displayed 30 characters of a
tweet and stopped. `--extract` now stores **`title_long`** beside `title` — the
same headline rules at the card's budget — and the cards read that.

That length is also what exposed the trailing machinery every caption ends in: a
`t.co` link, the programme hashtag, the reporter's handle. The row cut simply
never reached them. URLs are stripped anywhere in a headline and trailing
hashtags at the end of one; a trailing **handle** is removed only once its
credit is known, because with no reporter resolved that handle is the visible
hint that `reporters_map.json` is missing a line.

Measured over a real week, end to end: 4 of 50 rows still reach the browser
ellipsis, by at most 37px (≈3 characters). Before the interaction columns
existed it was 8 rows and 202px, all of them on X.

**Nothing is dropped.** An earlier version cut at the first clause boundary, on
the theory that Kan writes `<hook>: <elaboration>` and the hook is the headline.
On real captions it removed the *stronger* half more often than not —
`הטרנד החדש שכובש את חטיבות הביניים` without `בני נוער משקיעים עשרות אלפי דולרים`,
or a family's quote dropped from the story it belongs to. In Kan's style the part
before the colon is usually a **teaser** and the news sits after it, so the deck
keeps both and elides only what does not fit.

The cut is placed on the last clause boundary (comma, semicolon, dash, pipe)
inside the cap, so it never lands mid-phrase, and always ends in `…` because text
really was dropped. `--extract` then lists every elided headline so the editorial
pass can rewrite the few where the important half fell off — that judgement is
the editor's, not a regex's.

Re-derive every headline in an existing `deck_content.json` — useful when these
rules change — with `--retitle`. It is opt-in because it **overwrites
hand-edited titles**.

## Which programme a clip came from (תוכנית column)

Kan tags the source programme as a **hashtag** on the post — `#כאןבשלוש`,
`#כאןבשש`, `#בחציהיום`, `#מהדורתכאןחדשות`. The tag has no spaces and Hebrew
cannot be word-split deterministically, so `programs_map.json` (repo-tracked,
meant to grow) turns `#כאןבשלוש` into `כאן בשלוש`. Matching ignores `#` and case.

On YouTube the signal is not a hashtag at all. Kan signs its video descriptions
with a fixed template:

```
… הכתבה של איילה חסון, מתוך חדשות השבת 18.07.26
```

The trailing **broadcast date** is what makes that readable without a map — prose
does not end in `18.07.26`, so the slot before it holds a programme by
construction. Measured over a real week: 66 of 83 videos (80%) carry it, against
2 that carried a mapped hashtag. Without a date the same words are ordinary text
(`תיעוד מתוך האירוע התפרסם ברשתות`) and the name still has to be one the map
knows; `מתוך תחקירו של X` is a credit and is excluded outright.

The template is a YouTube-description convention — checked across the sheets, it
does not appear on the social platforms, which use hashtags and the radio
sign-off.

**An unmapped hashtag never becomes a programme.** Posts carry topic tags too
(`#מונטנגרו`), and guessing would put nonsense in the column — so unmapped tags
are listed at the end of `--extract` instead, where a real programme is one line
to add. Two weaker signals back the hashtag up: a programme named in quotes
(`בתוכנית "X"`), and the radio signature `כאן חדשות ברשת ב׳`, which yields the
station `רשת ב׳` when no programme tag is present.

Like the credit, the tag sits at the very END of the caption, so it is resolved
on the **full raw text** at extract, never on the stored 400-char `caption`.

The column only renders on a slide where **at least two rows have a programme**.
An almost-empty column reads as missing data on every row instead of as extra
information on a few.

## Clickable rows

Every table row and highlight card is an `<a>` when the item has a link, and
Chromium's `page.pdf()` turns those into real PDF link annotations — so the
exported deck is clickable, not just the HTML. The URL is the permalink the
collectors already store for Facebook / Instagram / X; YouTube, TikTok, Facebook
and X also rebuild one from the id alone, which is how a `deck_content.json`
extracted before links existed still gets them. **Instagram rows stay unlinked**
when there is no stored permalink: a `media_id` is not a shortcode, so there is
nothing to build a URL from and a guess would point somewhere wrong.

Since a PDF gives no visual cue that a row is a link, each platform slide carries
a `↗ לחיצה על שורה פותחת את הפריט` hint beside the חריג legend.

## Reporter credits (כתב/ת column)

The sheets carry no author field, so `--extract` pre-fills `reporter`
deterministically **from the full caption** (the credit sits at the end, past
the headline), in this order:

0. a **role phrase right after the name** — `איציק זוארץ, כתב כאן11 בדרום` —
   the strongest byline signal Kan uses, so it outranks everything below.
   `כתב` is also the *verb* "wrote", and a verb takes a direct object: the rule
   ignores `כתב את`, and rejects a candidate containing a word that never appears
   inside a name (`שלנו`, `ביום`, a day name). Without both,
   `הגיע לאולפן שלנו ביום שישי, כתב את הנבואה` put **שלנו ביום שישי** in the
   כתב/ת column of a live deck;
1. an explicit `כתב:` / `כתבת:` / `תחקיר:` credit, **or the possessive form of
   the same thing** — `כתבתו של X`, `בכתבתה של X`, `מתוך תחקירו של X`, which
   carries no colon. This is how Kan credits in YouTube descriptions and at the
   end of reels. Both are tried together, before everything below;
2. a parenthesised 2–3-word Hebrew name (the last one wins);
3. an `@handle`;
4. anyone already in `reporters_map.json`, named anywhere in the caption;
5. a **bare trailing name** after the link — Facebook's shape
   (`… | https://bit.ly/x Vered Pelman Haim Goldich`). Latin runs are accepted
   structurally and the FIRST name is taken (Kan lists reporter then
   photographer); the second is reported so you can check it;

### The full-text second pass

The daily collectors store a **truncated** copy of every caption — Facebook at
700 chars, Instagram and TikTok at 500 — because a sheet cell is not an archive.
Kan credits the reporter at the very END of the caption, so on a long post **the
credit is already gone before the deck ever reads the sheet**. Measured on a real
week: all 3 uncredited Facebook items were long-text items.

So `--extract` re-fetches the original text for the ~10 items per platform that
actually reach the deck, and re-resolves the credit, the programme and the
headline against it:

| platform | source | why |
|---|---|---|
| Facebook | Graph `?ids=…&fields=message` | sheet cuts at 700 |
| Instagram | Graph `?ids=…&fields=caption` | sheet cuts at 500 |
| YouTube | Data API `videos?part=snippet` | the sheet holds the **title**, and a title never carries a byline — the credit is in the description |
| TikTok, X | — | short enough that the 500-char store never cuts them |

One batched call per platform per week. Best-effort throughout: no token, a
failed call or a missing id just leaves that item on the sheet text it had.

A camera or clapper emoji (📸 📷 🎥 🎬) opens a **media credit that runs to the
end of the caption** — everything from the marker on is cut before any search,
so an agency photographer never becomes the reporter. A name *before* the marker
still resolves normally.

`צילום:` / `עריכה:` and friends are photographers and editors, never reporters
— a role word anywhere in a trailing run disqualifies it. **Nothing is ever
invented.** A bare *Hebrew* trailing name is only accepted when it is either
already in the map or set off by a separator/URL, because every Hebrew caption
ends in Hebrew words and accepting one blindly would fabricate a credit on
almost every post. Everything else is left empty and listed in the report below.

`reporter_source` on each item records **how** the credit was found, so guesses
(`trailing-latin` / `trailing-hebrew`) are easy to spot while authoring.

Once a credit is known, the `@handle` or `(שם)` it came from is **removed from
the headline** — it would only duplicate the כתב/ת column. With no credit the
handle stays visible: it is the hint that `reporters_map.json` needs a line.

### The extract report and the TODO list

`--extract` ends with a reporter report: the hit rate, a breakdown by source, a
"worth a glance" list of bare-trailing-name guesses (with any second name seen),
and a list of the items with **no credit anywhere in the text**.

That last group is mostly not a parsing miss — plenty of items (YouTube titles,
agency copy, quoted statements) genuinely ship without a byline, and no amount of
regex will find a name that was never written. So instead of guessing, the run
writes `out/reporters_todo.txt`: each uncredited item's headline, a link to it
where one can be derived, and a **paste-ready JSON block** to fill in.

The list also carries **same-story suggestions**: Kan runs one report across all
five platforms and often credits it on only some, so an uncredited item is
matched against the credited ones by caption word overlap. These are printed as
`אולי <name> (43% חפיפה עם …)` **next to the matched headline** and are never
applied automatically. On a real week it matched 4 of 11 uncredited items and
every match held up — including one that looked wrong at first glance, a
Montenegro item matched to a post about two teenagers who turn out to be the
teenagers attacked in Montenegro. The reason it still only suggests is what the
method cannot know: word overlap can tell that two posts share wording, not that
they are the same story, and not that whoever is credited on one wrote the
other. Showing the matched headline lets a human settle both in a glance.

### The credit approval queue

An `@handle` left in the כתב/ת column is a credit the deck could not turn into
a name. Resolving it is a factual claim about a person, so it is never guessed
— but it must not be re-asked every week either. `--extract` parks every such
handle in `reporters_map.json` under `_pending` (the loader already ignores
`_` keys), and one file then holds all three states:

| | |
|---|---|
| `_pending` | proposed, waiting for a yes/no. `suggest` may be pre-filled |
| top level | **approved** — the map itself, and what actually resolves names |
| `_rejected` | answered "no". Never proposed again, and the reason is kept |

```bash
python weekly_deck/generate_deck.py --credits                  # show the queue
python weekly_deck/generate_deck.py --credits ok @HGoldich     # take its `suggest`
python weekly_deck/generate_deck.py --credits ok "@x=שם מלא"   # or name it here
python weekly_deck/generate_deck.py --credits ok all
python weekly_deck/generate_deck.py --credits no @kann_news "חשבון, לא אדם"
```

`no` also removes an existing approval, so it is how a wrong name gets undone.
Everything is offline and needs no credentials. Because the answers live in a
repo-tracked file, an approval given once holds for every future week — the
queue only ever shows what is genuinely new.

Rebuild the uncredited-items list at any time, offline, with:

```bash
python weekly_deck/generate_deck.py --reporters-todo
```

`reporters_overrides.json` (repo-tracked) takes those answers as
`{"<platform>:<item id>": "שם"}`. It pins ONE item, where `reporters_map` teaches
a reusable rule — and an empty value (`""`) records "checked, genuinely
uncredited" so the item stops coming back.

Two shapes an editor writes naturally, both understood:

- **Two reporters** — `"רן בנימיני ומזל מועלם"`. Kan credits a pair often enough
  that the כתב/ת column is sized for it.
- **The programme, in the credit** — `"אריה גולן (מתוך הבוקר הזה)"` or
  `"… (מתוך התוכנית גליק ותמר)"`. The programme is split back out into the
  תוכנית column instead of being crammed into the name, and an editor's
  programme outranks one parsed from a hashtag. This is how a programme gets in
  when the post carried no hashtag at all. Parentheses that are not a programme
  (`"יואב לימור (כתב צבאי)"`) are left alone. Overrides are applied on **both**
`--extract` and `--render`, so a name filled in after the data was pulled lands
with a re-render alone, and a re-extract never loses it.

`reporters_map.json` (repo-tracked, **user-editable and meant to grow** — just
add a line) maps `{"@handle": "שם בעברית"}` and is applied during extract. Keys
can also be a **full Latin name** (`"Vered Pelman": "ורד פלמן"`) or a **Hebrew
name used as its own value** (`"רובי המרשלג": "רובי המרשלג"`) — the latter simply
tells the extractor that this string is a person.

Latin byline spellings are **derived automatically** from handle keys, so one
line covers both: `@ItayBlumental` also matches the byline `Itay Blumental`, and
`@moav_vardi` matches `Moav Vardi`. An all-lowercase run-together handle
(`@gilicohen10`) cannot be split deterministically and needs its byline mapped
explicitly. Explicit entries always beat derived ones. It
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

- **`portrait`** (default) — the highlight slot is portrait (240×268),
  `object-fit:cover` anchored `center top` so faces and on-screen headlines
  survive; the credit sits on its own full-width line below the headline,
  because a pair of names never fits beside the views figure. Cards are narrower, so the slide is rebalanced: cards sit
  beside a wide `נתון מעניין` panel and the top-10 table runs full width below.
- **`blur`** — keeps the original wide 16:9 slot, but the image is `contain`ed
  at full frame over a blurred, scaled-up, darkened copy of itself. **Nothing is
  ever cropped.**

Table row thumbnails are square (1:1, 34px) in both variants — squares degrade
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
- **Reporter / כתב-ת column** — the sheets carry no author field, so the credit
  is parsed out of the caption, recovered from the untruncated original, mapped
  by handle or name, and completed by hand where the text has none. See
  *Reporter credits* above. Nothing is ever invented; on a real week 46 of 50
  items resolve and the rest are recorded as genuinely uncredited.
- **X thumbnails** — X highlight cards are text-forward by design (the tweet is
  the hero), not a placeholder for a missing image.
- **Instagram links** — a `media_id` is not a shortcode, so an Instagram row is
  only clickable when the collector stored a permalink for it.
