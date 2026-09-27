# Video archive — design

**Status:** approved in conversation 2026-09-02, not yet implemented.

Pull every video item Kan News publishes to Instagram and TikTok, store the
file in Drive, classify it on two axes, and expose the result as an index a
website feature can group into story rails.

---

## 1. What was decided, and why it is not re-litigated

Seven questions were answered before this document existed. They are recorded
here so the implementation does not reopen them.

| Question | Answer |
|---|---|
| Who consumes the archive? | The website system that shows videos to visitors, grouped as "stories". |
| Which file? | The copy published to the networks — 9:16, burned-in captions, re-encoded. Confirmed sufficient; the master from the editing system is not required. |
| Watermarks? | Not a problem. TikTok's `video.download_addr` carries the watermark; `video.play_addr` does not, and it is in the same aweme object the code already fetches. Instagram's `media_url` is clean to begin with. |
| Third-party footage (📸 credits, "שימוש לפי סעיף 27א")? | Archive everything. The publish/no-publish decision happens outside this system. |
| What defines a group? | Two axes: **program** (deterministic, from the byline) and **topic** (Gemini — one fixed category plus free tags). |
| Where do files live? | A regular Google Drive, reached with a **user OAuth refresh token**. The existing service account cannot be used — see §7. |
| How far back? | Forward-only. A `--since-days N` flag covers "go back a few days when we need to". No bulk backfill of the 6,179-item TikTok history. |

Two further decisions came out of review:

- **The daily sheets cannot be the discovery source.** The collectors write once
  a day at 08:30, so an archiver running every two hours against
  `נתוני אינסטגרם` would see nothing new between daily runs. Discovery reads the
  platform APIs directly.
- **Nothing is deduplicated at capture time.** At an intraday cadence an item
  can reach TikTok at 14:00 and Instagram at 16:00; which copy is "better"
  cannot be known when the first one arrives. Every copy is kept; a nightly
  pass links them.

## 2. Shape: this is the sniffer's sibling, not the pipeline's

`hot_sniffer.py` is already the intraday, API-reading, own-sheet-writing job
this needs to be, and the archiver follows it deliberately:

| Property | `hot_sniffer.py` | `media_archiver.py` |
|---|---|---|
| Discovery | Graph + TikHub directly (`:154`, `:273`) | same |
| Cadence | intraday, `workflow_dispatch` fired by a VPS systemd timer (`kan-hot-sniffer.timer`, see `hot_sniffer.yml:4`) — **never** GitHub `schedule:`, whose cron lags 4–6 hours | same mechanism, own timer |
| Writes | only its own `hot_alerts` state sheet | only its own `ארכיון וידאו` sheet |
| Collector sheets | never touched | never touched |

The last row is the hard rule. The collector sheets carry `views_delta`
columns computed against the previous run; a second writer corrupts them.

Two existing functions are the starting point and need one change each:

- `hot_sniffer.fetch_young_instagram()` (`:154`) requests
  `id,caption,timestamp,permalink,like_count,comments_count`. The archiver
  needs `media_url,media_type,thumbnail_url` added to that field list.
- `hot_sniffer.fetch_young_tiktok()` (`:273`) already receives the full aweme
  object `v` and reads only `v['statistics']`. The archiver reads
  `v['video']['play_addr']['url_list']` from the object already in hand.

Whether these move into a shared module or are re-implemented in the archiver
is an implementation choice; the archiver must not change the sniffer's
behaviour either way.

## 3. Pipeline

One run, six stages, per platform:

```
discover ──► filter ──► resolve ──► download ──► upload ──► classify ──► index
  (API)     (already   (fresh URL)   (bytes)     (Drive)    (2 axes)    (sheet)
            in index?)
```

**discover** — items published inside the lookback window. The archiver keeps
its **own** constant, `ARCHIVE_LOOKBACK_HOURS = 48`, deliberately wider than
the sniffer's `YOUNG_HOURS = 24` (`hot_sniffer.py:88`): the sniffer is asking
"is this exploding right now", which is a question with a short shelf life,
while the archiver only has to make sure a missed run self-heals on the next
one. `--since-days N` widens the same window for the "go back a few days" case
and changes nothing else.

**filter** — drop anything whose `(platform, id)` is already a row in the index
sheet. This is what makes the cadence free: a run that finds nothing new does
one API call per platform and exits. Only new items reach the stages below, so
Gemini cost and Drive writes scale with items published (~14/day), not with run
frequency.

**resolve** — the media URL is fetched **in the same run that downloads it**
and is never persisted. Instagram signs `media_url` for a short window and
TikTok's `play_addr` likewise; a stored URL is a dead URL. This is the reason
approach (b) — "add a `media_url` column to the collectors" — was rejected: it
would have added a production-schema risk (`verify_collector.py` exists because
an inserted column renames every value after it) in order to store a value that
expires.

**download** — straight to a temp file. TikTok's CDN wants a browser-ish
`User-Agent`; a 403 is retried once with headers, then the item is left
unarchived and retried on the next run (it is absent from the index, so it is
naturally picked up again).

**upload** — see §5.

**classify** — see §6.

**index** — one row appended per archived item.

## 4. Failure behaviour

The unit of failure is the item, never the run. One item that 403s, or whose
Gemini call fails, is logged and skipped; every other item in the run still
lands. Because the index is the only record of what has been archived, a
skipped item is retried on the next run automatically — no retry queue, no
state beyond the sheet.

The one ordering rule: **the index row is written last**, after the file is in
Drive. A crash between upload and index write costs a duplicate file on the
next run, which is recoverable. The reverse — index written, file missing —
would make the archive lie about itself, and is not recoverable without an
audit.

A run that cannot reach Drive at all exits non-zero so the workflow goes red.

## 5. Drive layout

The physical file lives in exactly one place, by date:

```
/כאן חדשות — ארכיון וידאו/
  2026/09/02/
    2026-09-02_1443_instagram_17912345678901234.mp4
```

Category and program folders hold **Drive shortcuts** to that one file:

```
/לפי תוכנית/גליקותמר/         → shortcuts
/לפי קטגוריה/בחירות/          → shortcuts
```

Why shortcuts and not copies: an item belongs to a program *and* a category
*and* a date at once, a file cannot sit in three folders, and re-classifying an
item later becomes moving a 3KB pointer rather than a 40MB video.

The one caveat, stated because it is a real limitation: shortcuts behave well
in the Drive web interface but not always in Drive for Desktop. If the archive
turns out to be consumed by syncing a folder to a local machine rather than by
the web UI or the API, this decision is worth revisiting.

Filenames carry date, time, platform and id so a file is identifiable when it
is detached from its folder.

## 6. Classification

### Axis 1 — program (deterministic)

Extends `social_dashboard/content_tags.py`, which today holds only the Reshet
Bet signature and is already shared by the dashboard and the weekly deck. The
addition is one table of entities → program, applied through four markers, in
this order:

1. **program hashtag** — `#גליקותמר`, `#כאןבשש`, `#בשכונה_שלנו`,
   `#חדשותהלילה`, `#שובר_חומות`, `#סיפור_עולמי`, `#תקופת_המנדט`.
2. **trailing byline** — a caption ending in `(שם הכתב)`.
3. **@mention** of a host or reporter.
4. nothing — the item is desk output, not a program segment.

Measured over 21.6–2.9 (515 TikTok videos, 750 Instagram posts): at least one
marker is present on **87% of TikTok items and 63% of Instagram items**, and
the recurring entities are a finite list — 61 appear three or more times on
TikTok, 53 on Instagram.

Three findings the table must be built around:

- **A hashtag means "program segment" at ~99% precision, but names the program
  only 84% of the time.** Of 97 hashtagged items, 81 carry a program tag; the
  other 16 are tagged topically (`#בשר`, `#לגו`, `#קייפופ`, `#הרזיה`) — and
  reading all 16, 15 are still studio/magazine segments, tagged for reach
  instead of by program. Only one was a desk item. So a topical hashtag is
  evidence of "segment", and the program must then come from markers 2 and 3.
- **Hashtag recall is 9%.** Tagging is a habit of specific teams — the
  Glick/Almog team tagged on 28 separate days, most programs tag occasionally.
  The hashtag is a good *filter* and a bad *census*. Counting program output by
  hashtag undercounts by ~3× (Glickotamar: 3.4 items/week tagged, 5.7 by
  byline; בשכונה שלנו: 0.9 vs 2.8).
- **Instagram @handles are corrupted and must be normalised.** TikTok carries 31
  unique handles, Instagram 66, only 9 identical. Part is genuinely different
  handles per platform (`itsikzuarets`/`itsik_z`, `ifatglick1`/`ifatglick`,
  `itayblumental1`/`itayblumental`), and part is a real defect in the caption
  Meta returns: the raw text is `‪@ifatglick‬ck‬ck⁩`, whose
  last two characters repeat around bidi marks. It produces `ifatglickckck`,
  `hadasgrinbergrgrg`, `almogtamarar`, `maya_rachlinin`, `anna.pineses`,
  `daniel.grovaisis`. **This is not a collector bug** — `instagram_collector.py:306`
  stores what Meta returns; the doubling survives from the RTL editor the
  caption was written in. Any key built on `@` needs both a doubled-tail trim
  and a per-platform alias table.

This axis is pure functions over text: no network, no AI, unit-testable, and
shared with the dashboard so the two can never disagree about what
`#גליקותמר` means.

### Axis 2 — topic (Gemini)

One call per new item, with the caption and the program (if axis 1 found one).
Precedent: `comment_analyzer.py` already runs Gemini against this
spreadsheet. Returns:

- **one category** from a fixed list;
- **free tags** — `"בחירות 2026"`, `"חטיפת יהלי"` — which is what makes
  "everything about the elections that went up today" answerable without
  anybody having predicted the topic;
- **a one-line summary** for the index.

Starting category list, to be tuned once real output exists:
חדשות שולחן · חוץ · צבא וביטחון · משפט ופלילים · כלכלה · טכנולוגיה · בריאות ·
אוכל וצרכנות · תרבות ובידור · מגזין אנושי · סאטירה.

Free tags are stored as written. Normalising them into a controlled vocabulary
is deliberately out of scope: it is the kind of thing that looks tidy and
quietly destroys the signal that made free tags worth having.

## 7. Auth

| Need | Credential | Note |
|---|---|---|
| Read sheets, write the index | existing `GCP_SERVICE_ACCOUNT` | unchanged |
| Instagram media | `FACEBOOK_TOKEN` | unchanged; dies every ~60 days |
| TikTok media | `TIKHUB_TOKEN` | unchanged |
| Gemini | `GEMINI_API_KEY` | unchanged |
| **Drive upload** | **new: `GDRIVE_CLIENT_ID`, `GDRIVE_CLIENT_SECRET`, `GDRIVE_REFRESH_TOKEN`** | see below |

The service account cannot do this. A service account has no Drive storage
quota of its own; files it creates in a normal Drive fail with
`storageQuotaExceeded`, and sharing a folder with it does not help, because
the file it creates is still owned by it. The two real options are a Shared
Drive (needs Google Workspace) or a user's own credentials. With no Workspace,
this design uses a one-time OAuth consent by the account that owns the archive
folder, storing the resulting refresh token as a secret.

Scope: `https://www.googleapis.com/auth/drive.file` — access limited to files
this application itself created. It is sufficient for creating folders,
uploading files and creating shortcuts, and it cannot touch anything else in
the user's Drive.

## 8. Index sheet — `ארכיון וידאו`

One row per archived copy. `(platform, post_id)` is the key; the run reads the
sheet once at start and holds the key set in memory.

| Column | Source |
|---|---|
| `post_id`, `platform` | discovery |
| `posted_at`, `permalink`, `caption` | discovery |
| `drive_file_id`, `drive_path`, `bytes`, `duration_sec` | upload |
| `program` | axis 1 |
| `category`, `tags`, `summary` | axis 2 |
| `credit_flag` | caption carries 📸 / "סעיף 27א" / a foreign credit — informational only, per the rights decision |
| `same_as` | filled by the reconcile pass (§9) |
| `archived_at`, `archiver_version` | run |

`caption` is stored here at full length. The collectors truncate — Instagram at
500 characters (`instagram_collector.py:306`) — which is why end-of-caption
credits go missing from the sheets today.

## 9. Nightly reconcile

A separate mode of the same script (`--reconcile`), once a day, over the last
seven days of index rows:

- pair Instagram and TikTok rows that are the same item, and write each one's
  `drive_file_id` into the other's `same_as`;
- mark which copy is preferred (Instagram, which is not re-encoded by TikTok's
  pipeline).

Matching is caption-token containment with date proximity — containment ≥ 0.5
against the shorter caption, dates within ±2 days — the mechanism
`aggregate.py:1090-1110` already uses for the viral page. **Its known failure
mode is a false negative**: TikTok captions are short teasers and Instagram
captions are full news copy, so genuinely identical items score below the
threshold — "תיעוד קשה מהשומרון" scored 0.38 against its own Instagram post.
That direction of error is the safe one here: an unlinked pair leaves two files
in the archive, while a wrong link would hide real content behind a duplicate
marker. Nothing is ever deleted by this pass.

The reconcile output is also the gap report — which items exist on one platform
and not the other — which is what motivated this project. Over 27.8–2.9 that
was 17 TikTok-only items and 10 Instagram-only reels out of 41 and 28.

## 10. Testing

Fast, no network, following `test_verify_collector.py`'s habit of replaying
real incidents:

- **byline tagger** — the real corrupted handles (`ifatglickckck`,
  `hadasgrinbergrgrg`, `almogtamarar`) must resolve to their people; the two
  spellings `#בשכונה_שלנו` / `#בשכונהשלנו` to one program; a caption ending
  `(אורלי אלקלעי, הדס גרינברג)` to both names.
- **filter idempotency** — a run over an index that already holds the item does
  zero downloads. This is the property the whole cadence rests on.
- **ordering** — a fake Drive that raises on upload must leave no index row; a
  fake sheet that raises on append must leave the file (the recoverable side).
- **reconcile** — the שומרון pair, asserted as *not* linked at threshold, so
  the known false negative is documented in a test rather than discovered later.
- **URL freshness** — resolve and download must occur in one call path; a test
  asserts no code path persists a media URL.

Live verification before it is trusted: run once with `--since-days 2`, open
the Drive folder, and confirm the files play, are not watermarked, and that
their count matches the index.

## 11. Cost

TikHub is ~$0.001/call and the archiver adds 1–2 calls per run: about **1.5
cents a day at a two-hour cadence**, under 5 cents even at 30 minutes. Graph is
free. Gemini is one flash call per new item, ~14/day. Storage is ~1GB/month at
current publishing volume.

Nothing here justifies a lower frequency. What limits usefulness below roughly
an hour is that the desk does not publish that fast.

## 12. Out of scope

- Delivery to the website. The consuming team has not specified an interface,
  so this design stops at the index sheet and the Drive folders, both readable.
  A JSON endpoint on the existing dashboard is the obvious next step when there
  is somebody to agree it with.
- Bulk historical backfill.
- Facebook, YouTube and X. Instagram and TikTok are where the gap analysis was
  done and where the story rails start.
- Any editorial gate on publication.
