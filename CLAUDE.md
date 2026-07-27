# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Social media analytics dashboard for **כאן חדשות (Kan News)** - Israel's public broadcasting corporation. The system collects data from YouTube, Facebook, and Instagram, stores it in Google Sheets, and delivers AI-powered reports via Telegram.

**Important**: The data collection pipeline (GitHub Actions) is production and should NOT be modified or run locally, as it affects daily reports. Focus only on the dashboard UI.

## Architecture

```
GitHub Actions (daily 8:30 AM Israel time)
    │
    ├── followers_tracker.py  → Track subscriber counts
    ├── youtube_collector.py  → Collect video stats (30 days)
    ├── facebook_collector.py → Collect post metrics (7 days)
    ├── instagram_collector.py → Collect reels/posts (7 days)
    │
    ▼
Google Sheets (Spreadsheet ID: 1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c)
    │
    ├── נתוני יוטיוב     (YouTube Data)
    ├── נתוני פייסבוק    (Facebook Data)
    ├── נתוני אינסטגרם   (Instagram Data)
    ├── מעקב עוקבים      (Followers Tracker)
    └── תובנות יומיות    (Daily Insights)
    │
    ▼
telegram_reporter.py / weekly_reporter.py
    │
    ▼
Telegram Reports (Gemini AI analysis)
```

## Running Scripts (Avoid in Production)

```bash
# Install dependencies
pip install -r requirements.txt

# Individual collectors (requires env vars)
python youtube_collector.py
python facebook_collector.py
python instagram_collector.py
python followers_tracker.py
python telegram_reporter.py
python weekly_reporter.py
```

**Warning**: Running collectors locally will add duplicate rows to the Google Sheets and interfere with daily reports.

## What is open

`docs/ROADMAP.md` holds the open items, the decisions already taken, and the
questions that were answered for good — each one with a file:line, a commit or a
probe run behind it. **Read it before proposing work, and update it in the same
commit that changes an item's status.** Its "Closed" table exists so a settled
question (are 7 days enough? does Instagram expose retention?) is not
re-litigated from scratch every few weeks.

## Verifying a collector change

A green collector run proves it did not crash. It does **not** prove it wrote
anything. On 2026-07-26 three separate changes each produced a successful run
that had quietly written zeros, swallowed separators, or slid 230 historical
rows one column sideways — all three were found by hand, by opening the sheet.

So: snapshot before, diff after.

```bash
python verify_collector.py snap facebook       # before
gh workflow run test_facebook.yml --ref my-branch
python verify_collector.py check facebook      # after — exit 1 if anything looks wrong
```

It reports columns that moved position (the rows are written positionally, so a
column inserted mid-header renames every value after it), columns that arrived
or went empty, fill rates that dropped, and individual values that turned blank.
`test_verify_collector.py` replays the three real incidents against it.

## Environment Variables

Required secrets (configured in GitHub Actions):
- `YOUTUBE_API_KEY` - YouTube Data API v3
- `FACEBOOK_TOKEN` - Facebook/Instagram Graph API token
- `FACEBOOK_PAGE_ID` - Kan News page ID (220634478361516)
- `GCP_SERVICE_ACCOUNT` - Google Cloud service account JSON
- `GEMINI_API_KEY` - Google Gemini API
- `TELEGRAM_TOKEN` - Telegram bot token
- `TELEGRAM_CHAT_ID` - Target Telegram chat

## Design System (Kan News Brand)

Design mockups are in `/design` folder (created with Google Stitch). Key brand elements:

**Colors**:
- Primary: `#F7381B` (Kan News Red/Orange)
- YouTube: `#FF0000`
- Facebook: `#1877F2`
- Instagram: `#E4405F`

**Typography**: Heebo font family (Hebrew-optimized)

**Layout**: RTL (right-to-left), dark/light mode support

**Framework**: Tailwind CSS with custom config:
```javascript
colors: {
    primary: "#F7381B",
    "background-light": "#F3F4F6",
    "background-dark": "#111827",
    "card-light": "#FFFFFF",
    "card-dark": "#1F2937"
}
```

## Dashboard Development Focus

The current goal is building a visual dashboard UI to replace/complement Looker. Key screens designed:
1. **Overview Dashboard** - Multi-platform metrics at a glance
2. **Platform Analytics** - Deep dives per platform
3. **Video Performance** - Detailed video analytics
4. **Cross-Platform Viral** - Trending content comparison
5. **Content Calendar** - Publishing schedule
6. **Alerts & Insights** - AI-generated notifications

Recommended: Use shadcn/ui components with the Kan News color scheme.

## Data Structure in Sheets

**YouTube columns**: video_id, published_at, title, views, views_delta, likes, comments, like_rate, video_type (Shorts/Regular)

**Facebook columns**: post_id, date, title, type (Reels/Images/Videos/Links), views, reach, likes, shares, engagement_rate

**Instagram columns**: media_id, date, caption, type (Reel/Photo/Carousel), views, reach, saved, shares, engagement_rate

**Followers columns**: date, yt_subscribers, fb_followers, ig_followers (with _change variants)

## Hebrew/RTL Considerations

- All UI text is Hebrew
- Use `dir="rtl"` on HTML root
- Sheet names are Hebrew (e.g., `נתוני יוטיוב`)
- Reports formatted for Hebrew readers
