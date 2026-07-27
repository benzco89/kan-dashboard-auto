#!/usr/bin/env python3
"""Does the daily follower series actually advance — and what else is on offer?

READ-ONLY. No writes to Google Sheets, no collector touched.

Two questions, one run:

1. **The lag.** `fb_daily_reach` and `fb_daily_engagements` held byte-identical
   values on 2026-07-26 and 2026-07-27 (1,080,464 / 304,274), and the probe of
   26/07 13:39 had already returned those same two numbers. Three fetches across
   two days, one value. followers_tracker asks with
   `period=day&date_preset=yesterday`; if Meta has not finalised yesterday by
   08:30 it may answer with the day before, and the sheet then stores the same
   figure twice. A dashboard built on that would draw a flat line and call it
   stability. So: pull the real day-by-day series with since/until and compare it
   against what date_preset=yesterday returns right now.

2. **The candidates.** The metrics the earlier probes found live but nothing
   collects — `accounts_engaged` (35K unique accounts a day), `page_views_total`,
   `profile_views` — shown as a 7-day series, which is the only way to tell a
   useful metric from one that is always zero.

    python followers_series_probe.py

Env: FACEBOOK_TOKEN, FACEBOOK_PAGE_ID.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import requests

V = "v25.0"
GRAPH = "https://graph.facebook.com/%s" % V
DAYS = 8

# what followers_tracker stores today
PAGE_LIVE = ['page_total_media_view_unique', 'page_post_engagements',
             'page_video_views', 'page_daily_follows']
# found live by probe 30204475427, collected by nothing
PAGE_CANDIDATES = ['page_views_total']
IG_LIVE = ['reach', 'views']
IG_CANDIDATES = ['accounts_engaged', 'profile_views', 'website_clicks',
                 'profile_links_taps', 'total_interactions']


def get(url, params):
    try:
        r = requests.get(url, params=params, timeout=30)
        return r.json()
    except Exception as e:
        return {'error': {'message': str(e)}}


def err(res):
    e = (res or {}).get('error')
    return e.get('message', '')[:90] if e else None


def series(obj_id, metric, token, extra=None):
    """Day-by-day values over the last DAYS days, as {date: value}."""
    until = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    since = until - timedelta(days=DAYS)
    p = {'access_token': token, 'metric': metric, 'period': 'day',
         'since': since.strftime('%Y-%m-%d'), 'until': until.strftime('%Y-%m-%d')}
    p.update(extra or {})
    res = get("%s/%s/insights" % (GRAPH, obj_id), p)
    if err(res):
        return None, err(res)
    out = {}
    for block in res.get('data', []):
        for v in block.get('values', []) or []:
            day = str(v.get('end_time', ''))[:10]
            val = v.get('value')
            if isinstance(val, dict):
                val = sum(x for x in val.values() if isinstance(x, (int, float))) or 0
            out[day] = val
    return out, None


def one_day(obj_id, metric, token, day, extra=None):
    """total_value metrics answer for a range, not per day — so ask a 1-day range."""
    p = {'access_token': token, 'metric': metric, 'period': 'day',
         'since': day, 'until': (datetime.strptime(day, '%Y-%m-%d')
                                 + timedelta(days=1)).strftime('%Y-%m-%d')}
    p.update(extra or {})
    res = get("%s/%s/insights" % (GRAPH, obj_id), p)
    if err(res):
        return None, err(res)
    for block in res.get('data', []):
        tv = block.get('total_value')
        if isinstance(tv, dict) and 'value' in tv:
            return tv['value'], None
        for v in block.get('values', []) or []:
            return v.get('value'), None
    return None, 'no value'


def yesterday_value(obj_id, metric, token, extra=None):
    """Exactly what followers_tracker asks for."""
    p = {'access_token': token, 'metric': metric, 'period': 'day',
         'date_preset': 'yesterday'}
    p.update(extra or {})
    res = get("%s/%s/insights" % (GRAPH, obj_id), p)
    if err(res):
        return None
    for block in res.get('data', []):
        tv = block.get('total_value')
        if isinstance(tv, dict) and 'value' in tv:
            return tv['value']
        for v in block.get('values', []) or []:
            val = v.get('value')
            if isinstance(val, dict):
                val = sum(x for x in val.values() if isinstance(x, (int, float))) or 0
            return val
    return None


def show(title, ser, tracker_value):
    print("\n  %s" % title)
    if ser is None:
        print("     (no series)")
        return
    days = sorted(ser)
    if not days:
        print("     (empty series)")
        return
    for d in days:
        mark = ''
        if tracker_value is not None and ser[d] == tracker_value:
            mark = '   <-- what date_preset=yesterday returns now'
        print("     %s  %14s%s" % (d, format(ser[d], ',') if isinstance(ser[d], int) else ser[d], mark))
    vals = [ser[d] for d in days]
    distinct = len(set(vals))
    print("     %d ימים, %d ערכים שונים%s" % (
        len(vals), distinct, "  ⚠️ הסדרה לא מתקדמת" if distinct <= 1 else ""))


def main():
    token = os.environ.get('FACEBOOK_TOKEN')
    page = os.environ.get('FACEBOOK_PAGE_ID')
    if not token or not page:
        raise SystemExit("❌ missing FACEBOOK_TOKEN / FACEBOOK_PAGE_ID")

    print("=" * 66)
    print("🔎 האם הסדרה היומית מתקדמת, ומה עוד יש")
    print("   רץ ב-%s UTC" % datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M'))
    print("=" * 66)

    print("\n" + "-" * 66)
    print("פייסבוק — מה שהטראקר כותב היום")
    for m in PAGE_LIVE:
        ser, e = series(page, m, token)
        tv = yesterday_value(page, m, token)
        print("\n  === %s" % m)
        print("     date_preset=yesterday -> %s" % (format(tv, ',') if isinstance(tv, int) else tv))
        if e:
            print("     series ERROR: %s" % e)
        else:
            show("day by day:", ser, tv)

    print("\n" + "-" * 66)
    print("פייסבוק — מועמדים שאיננו אוספים")
    for m in PAGE_CANDIDATES:
        ser, e = series(page, m, token)
        print("\n  === %s%s" % (m, "  ERROR: %s" % e if e else ""))
        if not e:
            show("day by day:", ser, None)

    # Instagram
    res = get("%s/%s" % (GRAPH, page), {'access_token': token,
                                        'fields': 'instagram_business_account'})
    ig = (res.get('instagram_business_account') or {}).get('id')
    if not ig:
        print("\n⚠️ no instagram_business_account on the page — skipping IG")
        return

    print("\n" + "-" * 66)
    print("אינסטגרם — חשבון (%s)" % ig)
    today = datetime.now(timezone.utc).date()
    days = [(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(DAYS, 0, -1)]
    for m in IG_LIVE + IG_CANDIDATES:
        tag = '' if m in IG_LIVE else '   (מועמד — לא נאסף)'
        print("\n  === %s%s" % (m, tag))
        tv = yesterday_value(ig, m, token, {'metric_type': 'total_value'})
        print("     date_preset=yesterday -> %s" % (format(tv, ',') if isinstance(tv, int) else tv))
        ser, bad = {}, None
        for d in days:
            v, e = one_day(ig, m, token, d, {'metric_type': 'total_value'})
            if e:
                bad = e
                break
            ser[d] = v
        if bad:
            print("     series ERROR: %s" % bad)
        else:
            show("day by day:", ser, tv)

    print("\n" + "=" * 66)
    print("קריאה: אם 'date_preset=yesterday' מצביע על היום שלפני האחרון בסדרה —")
    print("הטראקר כותב כל בוקר את הנתון של שלשום, והעמודה מפגרת יום.")


if __name__ == "__main__":
    main()
