# Roadmap

What is open, what was decided, and what was answered so it never gets asked
again. **Update it in the same commit that changes its status** — an item that
ships gets moved to *Closed* with the answer, not deleted.

Every claim here is evidence-backed: a file:line, a commit, or a probe run.
If an item has no evidence, it is a guess and belongs in a conversation, not here.

Last reviewed: **2026-07-28**

---

## Open — dashboard

### 1. Two of the eight new metrics still have no home
`ig_reels_video_view_total_time` (item 3) and **`post_video_followers`** (FB, per
video — followers attributed to one video) are the leftovers of the sweep that
shipped on 2026-07-27. `post_video_followers` returned 0 on the sampled item;
probe a big one before believing either way.

Everything else from that sweep is live — see the Closed table.

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

## Closed — do not re-open

| Question | Answer | When |
|---|---|---|
| Facebook `views_30s` / `completion_rate` are 0 in every row — recoverable? | No. Removed in Graph v25. The reel retention curve replaces them. | 2026-07-26 |
| Does Instagram expose a retention curve, replays, plays or completion? | No. Meta rejects every variant; asked directly on v25. | 2026-07-26, run `30199414956` |
| Should the collectors' 7-day window be widened? | No. FB/IG/TikTok/X posts are finished by day four — the whole week of 12–18/07 gained 0.2–0.7% between day 4 and freeze. YouTube has a real tail and already keeps 30 days. | 2026-07-27 |
| Is 30 days enough for YouTube? | Yes, as a window — no cliff at day 30 (+0.3% median over the next fortnight). The tail is real but slow; see item 4. | 2026-07-27 |
| Which `engagement_rate` is the true one? | None — `social_dashboard/metrics.py` is the single definition. The collectors keep their old column on purpose: the daily Telegram report prints it and the alert thresholds were calibrated on it. | 2026-07-26 |
| Five daily account columns collected and displayed by nothing | Shipped. Collector day-alignment fixed (`ef2f623`) plus `page_views_total`, `accounts_engaged` and `profile_views`; a "החשבון עצמו" block on the Facebook and Instagram pages (`15b8212`), verified live. Dated by `insights_day`, rows without it skipped, line appears from the third day. | 2026-07-27 |
| Does the dashboard serve stale JS after a deploy? | No. `/static/app.js` answers `cache-control: no-cache` with an ETag, so a browser revalidates every load. A tab opened *before* a deploy keeps the old file until it reloads — that is the tab, not the server. | 2026-07-27 |
| YouTube's tail — 8.9M views the 30-day window could not see | Shipped 2026-07-27: `views_lifetime` + `lifetime_checked` fill weekly beside `views`, never over it (`youtube_lifetime.yml`). First run: 131,980,309 stored → 141,118,899 actual, **9.1M recovered**, 319 videos up more than 10%. The collector now carries over columns it does not produce, with `test_youtube_merge.py` holding that rule — without it the daily run would have wiped the column for every video under 30 days old. Surfaced in the UI on 2026-07-29 — a "צפיות מצטברות" block in the video drill-down, shown only where `lifetime_checked` is set and the figure beats the windowed one, because a video the weekly run has not reached yet holds a 0 that means "not measured". | 2026-07-27, UI 2026-07-29 |
| Cold dashboard loads (0.3–3.6s while warm ones were 190–550ms) | Fixed 2026-07-27 (`gsheets.py`, warmer thread at TTL−90s). Verified live: `/api/overview` after **11 idle minutes** now answers in **621ms**, against 3.0–5.1s before. It warms only tabs somebody asked for, so a page whose tabs are not cached yet still pays once (Instagram: 1,195ms first, 424ms after) — that is the per-page split working, not a regression. A failed read keeps the previous rows. | 2026-07-27 |
| Were the three items missing from Kan's report a data gap? | No. All three are in their *previous* report. Different week boundary, nothing lost. Always check the previous xlsx before calling something missing. | 2026-07-27 |
| The dashboard cannot tell that a collector stopped writing | Fixed 2026-07-29. `_vanished_alerts` measures every row against the newest stamp in its own sheet, so a sheet nobody wrote to is consistent with itself and reads as healthy — that is why 28/07 looked fine. `freshness()` compares each platform's newest stamp to the clock instead, and a red banner sits above the numbers on all five platform pages and `/alerts`. The threshold is the last *expected* run, not a fixed day count: today is the expectation only after `DAILY_RUN_DONE_HOUR` (10:00), so a single missed run is caught the same day without every morning going red. Also found on the way: `_vanished_alerts` read a hardcoded `pulled_at`, and YouTube's column is `last_updated` — it had never raised a vanished alert at all. `social_dashboard/test_freshness.py`. | 2026-07-29 |
| `_replay_share` written and never called | Shipped `b185b74` — wired at `aggregate.py:601`, the modal had been ready to draw it since the retention work. The roadmap kept listing it open for two days; that is how a stale item sends someone to build what exists. | 2026-07-29 |
| A green Twitter step that collected nothing | Fixed 2026-07-28. GetXAPI answers 200 with an empty or truncated feed every few days; `twitter_collector.py` read "no more pages" as "end of feed" and exited 0 — on 28.7 that was 0 tweets, no alert, and 27.7 frozen at the 13 the previous morning caught. Only a page reaching back past the cutoff now counts as full coverage (`reached_window_start`); anything short retries 3× and then exits 1, so the best-effort alert fires. Partial data is still saved — the merge never overwrites. `test_twitter_coverage.py` replays the incident. Re-run `30354470681` recovered both days (378 tweets, `stop=cutoff`, 1848 → 1921 rows). | 2026-07-28 |
