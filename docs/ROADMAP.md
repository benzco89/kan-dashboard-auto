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

### 14. Facebook 2025–2026 has counts but no metrics
The Graph probe counted FB posts for 2025–2026, but per-post reach/engagement
for those years exists nowhere: the sheets only start 12/2025 and the rich
Business Suite export was only ever taken for 2024. Closing it is a manual
export (Insights → Content), the same one already done for Instagram. Until
then the Facebook deep-dive slide rests on 2024 plus eight months of sheet.

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
