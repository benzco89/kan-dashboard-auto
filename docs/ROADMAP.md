# Roadmap

What is open, what was decided, and what was answered so it never gets asked
again. **Update it in the same commit that changes its status** — an item that
ships gets moved to *Closed* with the answer, not deleted.

Every claim here is evidence-backed: a file:line, a commit, or a probe run.
If an item has no evidence, it is a guess and belongs in a conversation, not here.

Last reviewed: **2026-08-14**

---

## Open — dashboard

### 1. Two of the eight new metrics still have no home
`ig_reels_video_view_total_time` (item 3) and **`post_video_followers`** (FB, per
video — followers attributed to one video) are the leftovers of the sweep that
shipped on 2026-07-27. `post_video_followers` returned 0 on the sampled item;
probe a big one before believing either way.

Everything else from that sweep is live — see the Closed table.

### 2. `_replay_share` is written and never called
`aggregate.py:66` defines "replays as a share of all plays — how rewatchable the
reel was". Nothing calls it. `replays` itself reaches the frontend
(`aggregate.py:548`) and no template renders it. Either surface it on the
Facebook page beside the retention curve, or delete both and stop collecting.

### 3. Instagram has no total watch time
The probe (run `30199414956`, 2026-07-26) got a clean answer for
`ig_reels_video_view_total_time` — 30.7M and 474M ms on two reels. It is not in
`instagram_collector.py`'s metric list. Facebook has `total_watch_min`;
Instagram has only `avg_watch_sec`. One line in the collector closes the gap.

---

## Open — data

### 4. Facebook and Instagram have a tail too — and nothing collects it
Measured 2026-07-27 (`meta_tail_probe.py`, run on 148 frozen posts per platform,
sampled across age buckets, with a control group of still-refreshed posts):

| | control | 10–20d | 20–40d | 40–70d | 70–120d | 120d+ | total |
|---|---|---|---|---|---|---|---|
| Facebook | +0.1% ✅ | +0.8% | +1.3% | +2.9% | +4.6% | +2.3% | **+7.8%** (19.96M → 21.51M) |
| Instagram | +0.8% ✅ | +3.3% | +5.2% | +3.4% | +3.3% | +3.1% | see below |

Medians. Facebook's is the same order as YouTube's 7.5%, so the same fix applies:
a `views_lifetime` column, never over `views`, for the reasons in the closed row
about YouTube.

**Instagram's total cannot be computed and that is a finding of its own.** Its
per-post medians are consistently positive, but the weighted totals go *negative*
in the old buckets (−6.5% at 70–120 days, −36.5% past 120) — old rows hold a
bigger number than the API now returns, because `views` replaced `plays` /
`impressions` mid-history. The tail is real; the aggregate across that boundary
is not.

**The cost is the open question.** YouTube answers 50 videos per call; Meta wants
one call per post — 2,793 + 2,661 ≈ 5,500 a week. The Graph batch endpoint takes
50 sub-requests per HTTP call, which brings it to ~110. Worth building that way
or not at all.

### 6. The local `.env` FACEBOOK_TOKEN expired
Died 2026-07-26 08:00 PDT. The GitHub secret is fine (this morning's runs
worked), so only local probing is affected. See the meta-token renewal notes.

---

## Open — daily report

### 17. The new daily baseline needs three mornings before it says anything
`telegram_reporter.py:77` now builds the comparison from a basis the report
records itself, into a new `בסיס יומי` sheet — one row per day, written after
the baseline for that day is read, so a day can never enter its own average.
Nothing could be back-filled: `views_delta` is one column overwritten every run
and is 0 on a post's first capture, so no per-day history existed to recover.

Until three rows accumulate (first write: the run of 2026-08-15, carrying
2026-08-14) the prompt carries an explicit refusal — *"אל תקבע אם היום חזק או
חלש"* — instead of a number. **Check the report on 2026-08-17**: the comparison
block should name the days it averaged, and the strong/weak verdicts should
start splitting both ways rather than landing on "חלש" most mornings.

Two things to watch once it is warm: the 30% floor for calling a day
strong/weak (`telegram_reporter.py:444`) is a first guess, not a measured
threshold — re-tune it against the recorded spread once ~3 weeks exist; and the
sheet grows one row a day forever, which is fine but nothing prunes it.

---

## Open — alerts

### 12. Recall — how many real explosions the sniffer never sees
Counting only posts published **after the sniffer went live on 2026-07-13**
(an earlier count of "5 Instagram misses" was an artifact: most of those posts
predate the sniffer and could never have alerted):

| | posts since 13/07 | in the top 2% that never alerted |
|---|---|---|
| Instagram | 162 | 1 |
| Facebook | 288 | 2 |
| TikTok | 111 | 2 |

**Two of the five had one cause and it is now fixed** — Facebook views were
never fetched. `"איפה היית חמודי?"` (25/07) reached **1,025,485 views, p99,
2.10× the bar**, while its reactions sat at 0.79× and shares at 0.36×: views
were the only axis that crossed and the only axis not measured. Same for the
26/07 post. Fixed by `_fb_views()` — one `post_media_view` call per young post,
the same metric the collector stores, so the live value and the sheet-derived
p90 are the same unit.

The other three are not bugs: two crossed only at maturity (1.03–1.15× the bar,
so below it inside the 24h window), and one never crossed any bar at all — it is
p99 on views but its own week held bigger posts, and the rule is relative.

**Still open:** whether the 24h window should stretch for slow-burning posts.
Both late-crossers were marginal, so there is no evidence yet that it is worth
the added noise. Needs a measurement, not an opinion.

**Checked 2026-08-14 — the bar is mis-scaled in principle and it does not
matter in practice.** `get_baselines` (`hot_sniffer.py:123`) builds p90 from the
*cumulative* values of the last 7 days of posts and applies it to a post younger
than 24h, which is the same age mismatch that broke the daily report's baseline.
It predicts alerts should pile up at the old edge of the window, where a post has
finally accumulated enough to clear a bar built from matured content. They do
not: across all 125 recorded alerts, matched to their publish time, the age at
alert runs **min 1.1h, median 11.5h, max 23.7h**, with 43% firing in the first
eight hours and 49% in the back half — a flat spread, not a pile-up (Facebook
median 7.6h, TikTok 12.7h, Instagram 14.7h). A genuinely exploding post clears
1.5×p90 within hours, because p90 of a distribution full of ordinary posts is
not a high bar for one that is going viral. Consistent with the audit above,
which found only two marginal late-crossers in 561 posts. **Do not re-tune the
multiplier on this mechanism argument alone.**

The real blocker for the open question is that nothing records the near-misses:
`hot_alerts` stores only what fired (`post_id, platform, alerted_at, triggers,
permalink`), so the precision cost of *any* candidate rule — a stretched window,
an age-adjusted bar — cannot be computed after the fact. Logging every young
post the sniffer evaluates, with its age, values and ratio, would make both
recall and precision measurable offline in two or three weeks without changing
what alerts. Same lesson as the daily baseline: the basis has to be recorded,
it cannot be reconstructed.

---

## Open — weekly deck

### 7. Delivery is undecided — the deck reaches nobody
The oldest open item. It is a PDF on disk and the workflow is manual by design.
Nothing is broken; nothing is delivered either.

### 8. No reporter has ever seen it
All QA so far is technical. Showing it to one or two of them is the cheapest
thing on this list and the most likely to change the deck.

### 9. Two bases now exist — decide whether to keep both
`out/<week>/` is ours (midnight cut, cumulative YouTube views);
`out/<week>__kan/` reproduces Kan's own דוח כתבות (Sunday-morning cut, YouTube
figures from their file). Verified 19–25/07: the kan-basis deck matches their
top-10 on all five platforms, item for item.

The content barely differs — 46 of 50 items identical. **The growth numbers do:**
+9% vs +4% on the cover, TikTok −17% vs −24%. If the deck goes to the same
newsroom that already receives the דוח כתבות, two different growth figures for
one week is how people stop trusting both.

### 10. Ask the report's owners two questions
Their week cut is bracketed to **between 09:45 and 11:20 on Sunday** (items at
00:21, 00:29, 09:45 fall in their previous report; 11:20 and 16:15 in the
current one). We use 10:00 as the reconstruction. And their YouTube figures are
period-restricted where ours are cumulative — if that is YouTube Analytics, both
reports are right and answer different questions.

### 11. ~39 of 50 headlines still end in `…`
Printed every run, never worked through. Only an editor can decide which half of
a cut headline mattered.

---

## Open — data (נמצא בבניית המצגת)

### 16. The Facebook sheet under-reports views before June 2026 — the dashboard reads it
`נתוני פייסבוק` holds **zero views in 38–53% of rows from 2025-11 through
2026-05**, and the zeros stop dead in June 2026: 16% in June, **0% in July and
August**. That is the date Meta removed `post_impressions_unique` (2026-06-15)
and the collector moved to v25 — the fix worked going forward and nothing ever
repaired the rows behind it.

Measured against two independent sources on the same windows:

| window | sheet | export / API | ratio |
|---|---|---|---|
| 2026-03 | 80,955,154 | 197,163,151 | **2.44×** |
| 2026-06 | 79,871,781 | 117,716,651 | 1.47× |
| 2025-11-17→12-31 | 60,280,009 | 144,284,664 | **2.39×** |

The deck sidesteps it by reading the exports and the API backfill instead
(`build_history.py` says so in `load_facebook`). **The live dashboard does
not** — any Facebook view total it shows for before June 2026 is roughly half
the real figure, and so is anything derived from it. `backfill_zero_metrics`
in `utils.py` exists for exactly this shape of problem; whether it can be
pointed at the historical rows is the open question.

---

## Open — מצגת 2024→היום

### 15. The deck's claim, and what was deliberately left out (2026-08-23)
Two Fable reviews (story/design, numbers/claims) ran against the rendered deck,
were applied, and a second Fable pass verified the fixes — 10/10, no
regressions. What that round settled, so it is not re-argued:

- **The deck argues one thing:** *the growth came from reach, not volume.*
  Jan–Jul 2026 vs 2025, four measured networks: **+6% items, +53% views, +44%
  views per item.** It is the title of slide 2 and of the summary. Per-item is
  measured against **2025 in every network** — Jan–Jul 2024 has no valid
  Facebook views at all (0 of 3,077 posts) and one Instagram month (345 of
  2,229), so a 2024 base silently compared Sep–Dec 2024 to Jan–Jul 2026 under
  a footnote promising an equal window.
- **YouTube is the honest exception, on the Studio basis.** Views ‑14.9% on
  ‑14.7% items → per item ‑0.3%, i.e. flat. The item-cumulative measure says
  ‑10.7% and is biased against new content; do not use it for YouTube.
- **Declined, by Ben:** no benchmark-vs-competitors slide (the daily
  competitor data is Instagram-only and followers-only — no views), and no
  "what we need from you" ask slide.
- **Deferred, not rejected:** merging the four top-5 slides into one
  cross-platform "peak moments" slide, dropping the four chapter separators
  (20% of the deck, and they still repeat slide 3's numbers), and an Instagram
  audience slide to match Facebook's.
- **Manual and open:** three entries in `analysis/presentation/
  deck_overrides.json` under `_todo` — two Facebook posts in the top five whose
  text the backfill never kept (permalinks are there), and one headline the
  caption itself cut mid-sentence. Nothing there is ever guessed.
- **Known 1% seam:** per-type watch hours have no dated source, so slide 13's
  hours column runs to 10.8 while every other figure cuts at 31.7. The
  deck-level total *is* cut (34.6M, not 34.9M) because `period_totals.csv`
  covers the ten overrun days even though it cannot build the whole period.

### 13. The metric history is a wasting asset — export yearly
Meta's Business Suite export keeps per-post insights only ~23 months, measured
2026-08-11 on the exports themselves: IG `Views` is empty before Jul-2024 and
`Reach` is junk before Sep-2024 (median 3–24 on posts with 1,500+ likes), while
likes/comments/counts survive all of 2024. Meta also redefined reach/views in
Aug–Sep 2024 — in the FB 2024 export `Reach from Organic` matches `Reach` until
Aug-2024 then jumps to ~2× it with Boosted at 0, which cannot be true.

So **any views/reach chart starts Sep-2024**; counts, likes and comments can
start Jan-2024. And whatever is not exported now is gone in two years — a
yearly Business Suite export belongs in the routine. Details and the plan:
`analysis/presentation/PLAN.md`, base built by `build_history.py`.

### 14. Facebook 2025 has views and engagement, but no reach and no post text
**Narrowed 2026-08-23** — the original wording ("counts but no metrics") is out
of date. The Graph backfill closed most of it: `analysis/presentation/pulled/
fb_2025_metrics.csv` holds all **4,799** posts of 2025 with views, reactions,
comments, shares, watch minutes and permalink — one zero-view row in the whole
file, and its 840,552,763 views are exactly what the deck reports for FB 2025.
Verified against the export on 2026 post ids: median ratio 1.00 on views.

What is still missing for 2025, and only that:

- **Reach.** The API's per-post reach decays with post age (median 12,542
  against 111,098 views — ratio 0.11, where 2026 runs 0.75), so it is
  deliberately dropped in `_fb_backfill`. Only a manual Business Suite export
  (Insights → Content) can recover it.
- **Post text.** The backfill did not keep it, so two of Facebook's five
  biggest posts of the period have no headline. They are listed with their
  permalinks in `analysis/presentation/deck_overrides.json` under `_todo`;
  a human opens the link and writes the line. Nothing is ever guessed there.

The appendix coverage table says `בלי חשיפה` for FB 2025 rather than `מלא`,
which is what it actually is.

---

## Open — video archive

### 18. The archive is built; the first live run has not been made (2026-09-04)
`media_archiver.py`, `drive_store.py`, `social_dashboard/content_tags.py`
(axis 1) and `.github/workflows/media_archiver.yml`, implementing
`docs/superpowers/specs/2026-09-02-video-archive-design.md` through the plan at
`docs/superpowers/plans/2026-09-04-video-archive.md`.

Before it can be trusted, three things must happen and none of them is code:

1. **`gdrive_consent.py` must be run once**, by the account that owns the
   archive folder, and its three values stored as GitHub secrets. Until then
   every run fails at `DriveStore.from_env()`.
2. **`python media_archiver.py --since-days 2` once**, then open the Drive
   folder: confirm the files play, are not watermarked, and that their count
   matches the index. The no-watermark claim rests on `play_addr` rather than
   `download_addr` and has never been checked against our own account.
3. ~~A systemd timer on the VPS~~ — **done 2026-09-06.**
   `kan-media-archiver.timer` (00,08,10,12,14,16,18,20,22:20 Asia/Jerusalem)
   and `kan-media-reconcile.timer` (03:40) are installed and enabled; both were
   fired by hand once and dispatched HTTP 204. Deploy files in
   `social_dashboard/deploy/kan-media-*`. The mode is carried as the bare word
   `ARCHIVE_MODE=reconcile`, **not** as JSON, because systemd's `Environment=`
   parses quotes itself and `{"reconcile":"1"}` can arrive with its quotes
   eaten — a 422 nobody would read.

**Where it lives — decided 2026-09-06.** Same repo; **its own spreadsheet**
(`1mktwIgMj8HOh6n066o4rc1Cat8cxVea0DHFpfVuKTaI`, "ארכיון וידאו — אינדקס",
in the Drive archive folder, shared to the service account). The sheet was
split the same day it was first written, ahead of the trigger below, because
it costs one constant and a copy of sixteen rows now and grows more expensive
with every row. The repo is not split. The archiver reads nothing from any collector
sheet — its only state is `ארכיון וידאו` — so there is no data coupling to
protect; what it *does* share is four secrets (`FACEBOOK_TOKEN`, which dies
every ~60 days, `TIKHUB_TOKEN`, `GCP_SERVICE_ACCOUNT`, `GEMINI_API_KEY`), the
VPS-timer→`workflow_dispatch` mechanism, and three imports (`content_tags`,
`utils.http_get_json`, `drive_store` — the last has zero repo dependencies).
Splitting now would mean renewing the Meta token in two places and
transplanting sixteen commits of code that has never run once. The cost of
splitting later is the same three imports, so nothing is being locked in.
**Trigger to split the repo:** the external system defines an interface, at which point
this stops being a Kan-social job and becomes that product's supply line.

Two of the design's own numbers were wrong and are corrected in the plan's
"Amendments" section:

- **Storage is ~12GB/month, not ~1GB.** 4.6 IG reels/day and 6.7 TikTok
  videos/day, and TikTok's duration is median 84s but **mean 192s, p90 502s,
  max 1,989s** — a long tail of whole programme segments. ~147GB/year at
  ~2 Mbps. A paid Google One tier was chosen over a length cap, because the
  long items are exactly the programme segments the story rails are for.
  `storage_report()` prints the measured figure on every reconcile run, so the
  estimate is replaced by a fact after two weeks.
- **The Instagram handle corruption needs no "doubled-tail trim" — it needs
  the right extraction order,** and the order differs per marker. Handles must
  be read from the **raw** caption (stripping bidi first invents 52 corrupt
  handles out of 212; reading raw gives 165 and none, over the same 1,982
  tokens). The byline regex must **tolerate** bidi after the closing bracket
  (without it, 19 bylines are found on Instagram instead of 34 — 44% lost).
  Hashtags are indifferent. `test_content_tags.py` locks all three. `person`
  and `program` are written per row precisely so this extraction never needs
  to run again on stored data: `build_row` stores `strip_bidi(caption)`, and
  feeding that stored caption back through `extract_handles` to "re-tag" a
  row would reproduce the exact 52-handle corruption above.

A third finding from the design work still stands on its own: **a hashtag means
"programme segment" at ~99% precision but names the programme only 84% of the
time, and reaches just 9% of items.** Counting programme output by hashtag
undercounts ~3× — גליקותמר 3.4/week by tag against 5.7 by byline. Measured
again over the caption files on disk, the marker that actually carries the
coverage is the **@mention** (81% of full IG captions, 45% of TikTok) with the
trailing byline behind it (5% IG, 41% TikTok); together with hashtags they
cover 86% of Instagram and 84% of TikTok. This is why axis 1 writes `person`
as its own column and derives `program` from it: `PROGRAM_BY_PERSON` is
deliberately partial, and a reporter missing from it still gets a name.

**A root folder made by hand is invisible to the archive.** The `drive.file`
scope sees only files *this* app created — a folder made in the Drive UI, or
by any other app, does not exist as far as the archiver's token is concerned,
and its id in `GDRIVE_ROOT_FOLDER_ID` would have failed every upload with
`File not found`. Caught on 2026-09-06 before the first live run, while
checking whether the Drive MCP connector could create the folder instead (it
cannot, for the same reason). `DriveStore` now creates `ROOT_FOLDER_NAME` at
the Drive root on first use and finds it by name afterwards;
`GDRIVE_ROOT_FOLDER_ID` survives only for an id the app itself produced.
Sharing that folder outward is the human step, not creating it.

**Thinking tokens were most of the Gemini bill, and the fallback could not
turn them off.** Cost was first estimated from the length of the JSON the model
returns (~73 tokens) — which ignores thinking, billed as output. Measured
2026-09-06 by `gemini_model_probe.py` on the archive's own classify prompt:
`gemini-3.5-flash` spent **595 thinking tokens to return 73**, and the
`gemini-2.5-pro` fallback spent **855** — and Pro rejects
`thinking_budget=0` outright (400 INVALID_ARGUMENT), as does `gemini-3.6-flash`.
A silent fallback to a four-times-dearer model that cannot be quietened is not
a safe fallback; the right failure here is an item with no topic, not a
surprising invoice. The chain is now `gemini-3.8-flash` → `gemini-3.7-flash`,
both served to this key and both accepting `thinking_budget=0`, which every
call passes. Per 420 items/month: **$0.74 → $0.11**, and the Pro path that
could have cost $4.08 is gone. Do not re-add a model to `GEMINI_MODELS`
without running the probe: an id that is not served fails *every* call into
whatever is next in the chain. `test_media_archiver.py` asserts the budget is
zero and that no chain entry is a Pro model.

**Signed URLs in the download path do not belong in a log.**
The `requests` library embeds the failing URL inside its `HTTPError` message,
so `str(e)` after a failed download published the signed, short-lived
`media_url`/`play_addr` straight into the public GitHub Actions log — exactly
the value the resolve-inside-download design exists to avoid persisting.
`media_archiver._safe_exc_str` now strips URLs from exception text, and
`test_media_archiver.py` drives a real `HTTPError` through the logging path
and asserts the URL never reaches stdout. Two `str(e)` sites are deliberately
left unscrubbed — a Gemini endpoint and a Drive endpoint, both of which carry
their credentials in headers, so the worst that appears is a `googleapis.com`
host. The leak has a second mouth one layer down: `utils/api_helpers.py`'s
`retry_with_backoff` prints the raw exception on every intermediate attempt,
and `discover_instagram` passes `access_token` in `params`, so a connection
failure renders the token into the message. Actions masks registered secrets,
which covers CI but not the local `--since-days 2` run step 2 above asks for;
`main()` is wrapped so nothing reaches a console unscrubbed. Do not "simplify"
`_safe_exc_str` away.

**`${{ }}` inside a workflow's `run:` is a shell injection.** The first version
of `media_archiver.yml` built its argument list with
`if [ -n "${{ inputs.since_days }}" ]; then ARGS="$ARGS --since-days ${{ inputs.since_days }}"; fi`.
GitHub substitutes those expressions into the script **as text, before the
shell parses it**, so a dispatch value of `1; rm -rf /` runs as a second
command with the job's secrets in the environment. Escaping the quotes does
not fix it — the substitution precedes parsing, so a value containing a quote
still escapes. The fix is to bind inputs to the step's `env:` and read them as
ordinary quoted shell variables, building the command as a bash array so a
value containing a space survives as one argument. `hot_sniffer.yml` does not
have this bug only because it passes its inputs straight through `env:`; any
new workflow that interpolates into `run:` has it.

**A test you cannot make fail is not a test — and this repo has now shipped
two.** The whole-branch review found that `test_media_archiver.py`'s
retry-path scrubbing checks were green and vacuous: an earlier block replaced
`ma.download_media` with a stub and never restored it, so the retry helper was
never invoked (0 calls under `settrace`), one check asserted 99 against a stub
returning 99, and the "signed URL absent from the log" check passed against an
*empty* captured buffer. Restoring the stub exposed a second layer: the log
line truncates at `[:80]`, which cut the URL's tail in both the broken and the
fixed case, so the literal `SECRET_URL in buf` assertion could never fail
either way. Both were only found by deliberately breaking the code and
checking the test went red. **In append-only test files, a monkeypatch that is
not restored silently disarms every later block that touches the same name**;
the file now restores `download_media` explicitly before the retry block.

**`--since-days` has a ceiling of about four days, and it warns rather than
lying.** Discovery is unpaginated — `limit: 50` on Graph, `count: 30` on
TikHub — which at the measured rates above covers roughly six days of
Instagram and four and a half of TikTok. `--since-days 7` would have returned
a truncated window and reported it as complete, which is precisely the failure
this file's collector-verification rule exists to catch. Both discovery
functions now detect a full page whose oldest item is still newer than the
cutoff and print a partial-coverage warning. Going genuinely further back
needs pagination, not a bigger number.

**The Hebrew names in `HANDLE_TO_PERSON` are an attribution, not a
measurement.** The handle-to-handle pairings were derived from the caption
CSVs — normalising `[._]` and trailing digits paired 28 people across the two
platforms automatically, and the rows marked `# ידני` are the ones that
needed a human because the word order is reversed (`davidovitchsharon` /
`sharondavidovitch`), the handle is a nickname (`itsik_z` / `itsikzuarets`),
or a second surname appears (`_dorit_mizrahi` / `doritassarafmizra`). But
which *person* a handle belongs to was never cross-checked against a Kan staff
list. Worth one editorial pass before the index feeds anything reader-facing.

Known and deliberately not fixed:

- **`weekly_deck/generate_deck.py` carries its own `strip_bidi`, and the two
  have already drifted** — the deck's set has `U+061C`, `content_tags`' has
  `U+FEFF`. The deck file also still writes its bidi marks as literal
  invisible codepoints, the hazard `f424dc7` removed from the shared module.
  Consolidating means choosing which set is right, which changes deck output,
  so it is an editorial decision with its own verification rather than a
  tail-end fix.
- **`run_reconcile` writes one cell at a time** — up to ~76 `update_cell`
  calls against a 60-writes-per-minute quota, with no error handling, making
  it the one path that can fail a *run* rather than an item. Bounded today
  (`days` is hardcoded to 7 and true pairs are rare), but a `batch_update`
  removes the last run-level failure mode.
- `tag_item` takes a `platform` it does not use; `discover_instagram` requests
  `media_product_type` and never reads it — the field that would separate a
  Reel from an ordinary feed video, which the archive currently takes both of.

The work is on branch `video-archive`, 16 commits from `0a007f8`, three suites
green (`test_content_tags.py` 28, `test_drive_store.py` 12,
`test_media_archiver.py` 47). Note that commit `e3f0dfd` also carries an
unrelated, pre-existing working-tree edit to item 14 of this file that
`git add docs/ROADMAP.md` swept in.

---

## Closed — do not re-open

| Question | Answer | When |
| How far back does TikTok history actually go? | **2020-11-26 — nearly six years, not Feb-2025.** The earlier belief that "the provider stops at February 2025" was wrong: run `31498296544` ended on `stop=max_pages`, our own 400-page ceiling, having pulled 3,993 of the 4,950 the profile reported. Re-run at 700 pages (`32012205657`) ended on `stop=end_of_feed` with **6,179 items, 2020-11-26 → 2026-08-17** — more than the profile's own videoCount, which undercounts. Never infer coverage from the oldest row; read the `stop=` reason, which is why `_report` prints it. The backfill added 1,012 items / 276M views in 2024 alone and closed the last hole in the cross-network daily series: all four networks are now present from 2024-09, the "typical day" median moved 5,649,606 → 5,946,076, and every `vs_typical` multiplier on the events slide dropped ~5% to its honest value. Deck totals moved 4.80B → 5.04B views and 40,154 → 41,471 items. | 2026-08-17 |
| Can Instagram's follower history be pulled back before Aug-2025? | **No, and re-exporting makes it worse.** Meta exposes Instagram audience history through a **rolling one-year window** measured back from the export date, while Facebook gave 2024-01-01 from the same screen. `Audience_instagram.csv` was taken 2026-08-11 and therefore starts 2025-08-06 (370 days, 61,879 follows) — a fresh export today would start 2025-08-17 and **lose eleven days**. The file on disk is the maximum that will ever exist for this period; there is no probe, endpoint or export that reaches further (Graph `follower_count` caps at 30 days). The consequence is a routine, not a fix: **a yearly Audience export or the history is gone.** The per-post `Follows` column is not a substitute — it spans 24 months from 2024-09 but attributes only **65%** of account growth (39,937 vs 61,880 on the overlapping window), because a post cannot see follows that came from profile visits or search. Scaling it by that ratio was tested and **fails**: against the measured net follower change in `מעקב עוקבים`, month by month, the ratio scatters **0.497 → 1.184** (mean 0.803, sd 0.258) — in some months posts explain half the growth and in others the account grew more than posts brought at all. A factor that moves 2.4× cannot carry an 11-month backward projection. Note this is *not* because 2024-09's outlier month (20,837 follows, 8× its neighbours) is an artifact — it was checked and is real: 18,536 of it comes from three posts on 1.9 and 18.9.2024. `analysis/yearly_content/meta_insights/README.md`. | 2026-08-17 |
| Is `worksheet.update('A1', [[...]])` still valid? | **No — gspread 6 reversed it to `update(values, range_name)`,** and we pin `gspread==6.2.1`. The old call hands a range string to `values`. It never failed because it only runs when a sheet is created from nothing, which happens once in a sheet's life: `save_daily_insights_to_sheets` had been carrying the broken form since "תובנות יומיות" was created under gspread 5. Found while adding the same create-if-missing path for `בסיס יומי`. Both now go through `telegram_reporter.write_rows`, with `test_daily_baseline.py` asserting the argument order against a fake worksheet. | 2026-08-14 |
| Why did the daily report keep calling every day "חלש ביחס לממוצע השבועי"? | **Because the comparison was rigged, not because the days were weak.** The old baseline (`telegram_reporter.py:77-83`) summed the *cumulative* views of everything published in the last 7 days, divided by 7, and held yesterday's ~20-hour-old posts against it — and it anchored the window on `today` while every summary used `yesterday`, so the judged day sat inside its own average. Measured on the morning of 2026-08-14, yesterday came out at **0.60× YouTube, 0.81× Facebook, 0.77× Instagram, 0.27× TikTok**. Two independent probes proved it was maturity and not content: re-running the same computation on older anchor dates, where every post in the window has matured, scattered the ratio around 1 (medians 0.89–0.99, plenty above 1); and replaying `.verify/instagram.json.gz` (26/07) against the same posts today showed a post holds **74.5%** of its 7-day value after one day, 91.5% after two — YouTube **35.8%** after one. TikTok is worst because its tail is longest, which is exactly what the reports themselves kept saying ("הזנב הארוך מציל את היום", 05/08, 11/08, 12/08). Ground truth in `תובנות יומיות`: 31/07, 02/08, 05/08, 07/08, 09/08, 12/08, 13/08 all opened on a weak-day verdict; only 06/08 ever said the opposite. Fixed by recording the basis daily — see open item 17. `test_daily_baseline.py` holds the rules. | 2026-08-14 |
|---|---|---|
| Which networks can carry a two-year **follower** curve? | **Two: YouTube and Facebook.** YouTube is measured daily from 2024-01-01 (`youtube_studio/subscribers_daily.csv`, 613,238 → 797,657). Facebook has no historical audience size at all — Meta's exports give daily **follows** and never a stock — so its curve is derived backwards from a measured anchor by a net/gross ratio. That ratio is **not** an assumption: eight months carry both figures independently (`pulled/sheet_followers.csv` measured 1,081,105 → 1,183,166 = +102,061, against 136,017 follows in the export) and it lands on **0.750 exactly**. Instagram's same test gives a different ratio, 0.515, and its follows only begin 2025-08-06. The rest have nothing worth plotting: TikTok is measured from 2026-07-21 (three weeks), X from 2026-06-28, and the WhatsApp channel has no API. So a six-network follower trend cannot be drawn honestly — 52% of the audience is all that has history. `build_deck.py:_followers_series`. | 2026-08-13 |
| How far back can Twitter/X history be pulled for a multi-year deck? | **13 days through the timeline; much further through search — but never completely.** The 13-day finding stands and is not a collection gap: `deck_history_probe.py twitter` (run `31500547213`) paged `/twitter/user/tweets` to exhaustion and returned 710 tweets, 2026-07-29 → 2026-08-11. What it does not settle is history in general, because it was the wrong endpoint for that question. `/twitter/tweet/advanced_search` sits on X's search index instead, so `since_time:`/`until_time:` reach back as far as asked — `twitter_search_probe.py` pulled 2026-06-08 → 2026-07-21 in one run (`32359547815`) and recovered the ten days of the World Cup that predate our sheet. **Recall is ~95% per pass and does not improve much by repeating**: three independent passes scored 95.0 / 95.1 / 95.4% against the 1,600 sheet rows in the overlap, and their union only reached 96.1%. The misses skew to video (34 of 63). One day is a hard hole — 2026-07-13 lost 36 of 59 rows in all three passes — so this is index incompleteness, not flakiness a retry fixes. Two traps: `since:`/`until:` are UTC and silently shift an Israeli day, so the probe uses unix `since_time`/`until_time` anchored to Israeli midnight; and a truncated day answers `has_more=false`, identical to a day that ended honestly — 13.7 returned everything down to 15:00 and stopped. Results come newest-first, so the probe re-asks for whatever sits below the oldest tweet it got until it reaches the window start. **Consequence for the deck: unchanged.** X still cannot carry a 2024–2026 comparison, and a backfill that drops ~5% of rows, unevenly, must not be merged into `נתוני טוויטר` — it would corrupt `views_delta` and every total computed from it. Search output is for one-off questions, and stays in `analysis/`. | 2026-08-20 |
| Should the hot sniffer's thresholds be raised — it fires ~26 times a week? | **No. Raised them on 2026-07-29 and reverted the same day; do not re-open on a noise argument.** Rate is not the criterion, precision is, and precision was already total: all 63 alerts from the first 17 days were cross-referenced to the sheets and scored on where each post finished *at maturity* — median 97th percentile of its platform, 44 of 59 at p95+, all at p90+, **zero below p90**. No early spike ever decayed to ordinary, which is exactly what a too-low bar would have produced. The raise (2.0×p90, floors 1000/2000/800) silenced 47 of 63 at a median of p97: the missing 4-year-old found alive, Yair Golan's declaration, the attacked reservist, the baby saved in emergency surgery. Caveat stated honestly: alerting on a number and then measuring that number is partly circular — the non-circular finding is the zero. | 2026-07-29 |
| Facebook `views_30s` / `completion_rate` are 0 in every row — recoverable? | No. Removed in Graph v25. The reel retention curve replaces them. | 2026-07-26 |
| Does Instagram expose a retention curve, replays, plays or completion? | No. Meta rejects every variant; asked directly on v25. | 2026-07-26, run `30199414956` |
| Should the collectors' 7-day window be widened? | No. FB/IG/TikTok/X posts are finished by day four — the whole week of 12–18/07 gained 0.2–0.7% between day 4 and freeze. YouTube has a real tail and already keeps 30 days. | 2026-07-27 |
| Is 30 days enough for YouTube? | Yes, as a window — no cliff at day 30 (+0.3% median over the next fortnight). The tail is real but slow; see item 4. | 2026-07-27 |
| Which `engagement_rate` is the true one? | None — `social_dashboard/metrics.py` is the single definition. The collectors keep their old column on purpose: the daily Telegram report prints it and the alert thresholds were calibrated on it. | 2026-07-26 |
| Five daily account columns collected and displayed by nothing | Shipped. Collector day-alignment fixed (`ef2f623`) plus `page_views_total`, `accounts_engaged` and `profile_views`; a "החשבון עצמו" block on the Facebook and Instagram pages (`15b8212`), verified live. Dated by `insights_day`, rows without it skipped, line appears from the third day. | 2026-07-27 |
| Does the dashboard serve stale JS after a deploy? | No. `/static/app.js` answers `cache-control: no-cache` with an ETag, so a browser revalidates every load. A tab opened *before* a deploy keeps the old file until it reloads — that is the tab, not the server. | 2026-07-27 |
| YouTube's tail — 8.9M views the 30-day window could not see | Shipped 2026-07-27: `views_lifetime` + `lifetime_checked` fill weekly beside `views`, never over it (`youtube_lifetime.yml`). First run: 131,980,309 stored → 141,118,899 actual, **9.1M recovered**, 319 videos up more than 10%. The collector now carries over columns it does not produce, with `test_youtube_merge.py` holding that rule — without it the daily run would have wiped the column for every video under 30 days old. | 2026-07-27 |
| Cold dashboard loads (0.3–3.6s while warm ones were 190–550ms) | Fixed 2026-07-27 (`gsheets.py`, warmer thread at TTL−90s). Verified live: `/api/overview` after **11 idle minutes** now answers in **621ms**, against 3.0–5.1s before. It warms only tabs somebody asked for, so a page whose tabs are not cached yet still pays once (Instagram: 1,195ms first, 424ms after) — that is the per-page split working, not a regression. A failed read keeps the previous rows. | 2026-07-27 |
| Were the three items missing from Kan's report a data gap? | No. All three are in their *previous* report. Different week boundary, nothing lost. Always check the previous xlsx before calling something missing. | 2026-07-27 |
| A green Twitter step that collected nothing | Fixed 2026-07-28. GetXAPI answers 200 with an empty or truncated feed every few days; `twitter_collector.py` read "no more pages" as "end of feed" and exited 0 — on 28.7 that was 0 tweets, no alert, and 27.7 frozen at the 13 the previous morning caught. Only a page reaching back past the cutoff now counts as full coverage (`reached_window_start`); anything short retries 3× and then exits 1, so the best-effort alert fires. Partial data is still saved — the merge never overwrites. `test_twitter_coverage.py` replays the incident. Re-run `30354470681` recovered both days (378 tweets, `stop=cutoff`, 1848 → 1921 rows). | 2026-07-28 |
