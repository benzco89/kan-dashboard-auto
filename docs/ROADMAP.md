# Roadmap

What is open, what was decided, and what was answered so it never gets asked
again. **Update it in the same commit that changes its status** — an item that
ships gets moved to *Closed* with the answer, not deleted.

Every claim here is evidence-backed: a file:line, a commit, or a probe run.
If an item has no evidence, it is a guess and belongs in a conversation, not here.

Last reviewed: **2026-07-27**

---

## Open — dashboard

### 1. Five follower columns are filling up and nothing displays them
`fb_daily_reach` (`page_total_media_view_unique`), `fb_daily_engagements`
(`page_post_engagements`), `fb_daily_video_views` (`page_video_views`),
`ig_daily_reach` and `ig_daily_views` sat empty for eight months and started
holding values on 2026-07-26 (`8799c48`). They appear **zero times** in
`social_dashboard/aggregate.py` and zero times in any template.

**The row date does not describe the value in it — fix that first.** Settled by
`followers_series_probe.py` (run `30251843236`, 2026-07-27): nothing is frozen,
every metric returns 8 distinct values over 8 days. The duplicate was a race with
Meta's clock. Its day buckets close at **07:00 UTC = 10:00 Israel**, and the
tracker runs at **08:30** — an hour and a half before the boundary — so it always
receives the *previous* bucket. Yesterday's manual 17:05 run and today's 08:30
run fell inside the same window, which is why both rows hold 1,080,464 while the
same call at noon returns 1,134,262.

Instagram behaves identically: the tracker wrote `ig_daily_reach = 738,833` at
08:30 and the same call four hours later returned 446,397.

So a daily chart built on these columns today would be shifted by a day and would
occasionally repeat a point. Two fixes: move the pull past 10:00 Israel, or —
better, because it does not depend on when the job happens to run — ask with an
explicit `since/until` for the day wanted and store Meta's `end_time` beside the
value.

*This is the only open item that accumulates in real time — new data every day
that nobody reads.* Next: fix the day alignment, then decide where it belongs.

### 1b. Metrics v25 offers that we do not take
Verified live over 8 days, not from docs and not from one sample — runs
`30251843236`, `30204475427`, `30199033819`:

| metric | daily range | verdict |
|---|---|---|
| **`accounts_engaged`** (IG) | 26,359–53,554 | **take it.** Unique *accounts*, not actions — a question no sum of likes and comments can answer |
| **`profile_views`** (IG) | 2,592–**8,274** | **take it.** Peak on 24/07, the day the missing boy was the story |
| **`page_views_total`** (FB) | 14,996–**29,557** | **take it.** Peak 25/07. Instagram has per-post `profile_visits`; Facebook has nothing |
| `total_interactions` (IG) | ~49,648 | maybe — overlaps what per-post sums already give |
| `website_clicks` (IG) | 4–21 | no |
| `profile_links_taps` (IG) | 0–1 | no |
| `profile_activity` (IG, per post) | 0, 0 | no |

The three worth taking share a shape: **they spike on the days a story broke.**
That is a measure of "how many people came looking for us", and no column we
currently store is a version of it.

Also open: **`ig_reels_video_view_total_time`** (item 3) and
**`post_video_followers`** (FB, per video) — followers attributed to one video,
which returned 0 on the sampled item; probe a big one before believing either way.

**Dead in v25:** `page_fan_adds`, `page_fan_removes`, `page_impressions_unique` —
"not a valid insights metric". Already replaced by `page_daily_follows` and
`page_total_media_view_unique`.

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

### 4. YouTube's tail is invisible and it is worth ~7.5%
`youtube_tail_probe.py` (run `30245535993`, 2026-07-27) compared 2,778 videos
that froze when they left the 30-day window against their live counts:
**118.8M stored vs 127.8M actual — 8.9M views the system never sees.** 11% of
videos gained over 10% after the cut; one went +330%. The tail is slow and runs
for months, so a longer window is the wrong tool (33-45 days adds only 0.3%
median). A weekly full refresh costs **64 API calls out of a 10,000/day quota**.

**Condition, and it is not cosmetic:** it must write to a NEW column
(`views_lifetime`), never over `views`. What makes weeks comparable is that
every post is measured on the same clock; letting history accrue forever would
bias every week-over-week comparison downward, permanently and increasingly. It
would also fire `views_delta` on old rows and confuse the alerts and the sniffer,
which were calibrated on the current behaviour. Same reasoning that left
`engagement_rate` alone in the collectors.

### 5. Nobody has measured the tail on Facebook / Instagram
Their posts freeze at 7 days, so the question is unanswerable from the sheet —
exactly the situation YouTube was in before the probe. The same design works
against the Graph API on old post ids. Not built. Until it is, "a Facebook post
is finished by day four" is only true *up to day seven*.

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
| Were the three items missing from Kan's report a data gap? | No. All three are in their *previous* report. Different week boundary, nothing lost. Always check the previous xlsx before calling something missing. | 2026-07-27 |
