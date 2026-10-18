"""Locks the YouTube fetch loop against uploads that have no publish date.

A private, scheduled or deleted video sits in the uploads playlist without
`contentDetails.videoPublishedAt`. On 2026-10-18 one of those reached the loop
and the whole YouTube step died on a KeyError, so that morning's sheet kept the
day before's numbers. The loop must skip such an item — and a page made only of
them must not be read as the end of the list.

    python test_youtube_fetch.py
"""

import os
import sys
from datetime import datetime, timedelta

import pytz

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('YOUTUBE_API_KEY', 'test')
import youtube_collector  # noqa: E402

PASS = FAIL = 0


def check(name, got, want):
    global PASS, FAIL
    ok = got == want
    PASS, FAIL = PASS + ok, FAIL + (not ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   got {got!r}, want {want!r}"))


def _stamp(days_ago):
    return (datetime.now(pytz.utc) - timedelta(days=days_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")


def item(vid, days_ago=None):
    details = {'videoId': vid}
    if days_ago is not None:
        details['videoPublishedAt'] = _stamp(days_ago)
    return {'contentDetails': details}


def video(vid):
    return {
        'id': vid,
        'contentDetails': {'duration': 'PT2M'},
        'statistics': {'viewCount': '100', 'likeCount': '5', 'commentCount': '1'},
        'snippet': {'publishedAt': _stamp(1), 'title': vid, 'description': '',
                    'thumbnails': {'high': {'url': 'x'}}},
    }


class _Req:
    def __init__(self, result):
        self._result = result

    def execute(self):
        return self._result


class FakeYouTube:
    def __init__(self, pages):
        self.pages = pages
        self.stats_calls = []

    def playlistItems(self):
        return self

    def videos(self):
        return _Videos(self)

    def list(self, pageToken=None, **_):
        return _Req(self.pages[pageToken])


class _Videos:
    def __init__(self, yt):
        self.yt = yt

    def list(self, id, **_):
        ids = id.split(',')
        self.yt.stats_calls.append(ids)
        return _Req({'items': [video(v) for v in ids]})


def run(pages):
    yt = FakeYouTube(pages)
    youtube_collector.get_youtube_service = lambda: yt
    youtube_collector.get_uploads_playlist_id = lambda _yt: 'UU'
    df = youtube_collector.fetch_videos()
    return (list(df['video_id']) if not df.empty else []), yt.stats_calls


print("undated item in the middle of a page")
ids, calls = run({None: {'items': [item('a', 1), item('scheduled'), item('b', 2), item('old', 40)]}})
check("dated videos kept, undated skipped", ids, ['a', 'b'])
check("undated id never sent to videos().list", calls, [['a', 'b']])

print("a page made only of undated items")
ids, _ = run({
    None: {'items': [item('private1'), item('private2')], 'nextPageToken': 'p2'},
    'p2': {'items': [item('c', 3), item('old', 40)]},
})
check("paging continues to the next page", ids, ['c'])

print("cutoff still stops the loop")
ids, calls = run({
    None: {'items': [item('d', 1), item('old', 40)], 'nextPageToken': 'p2'},
    'p2': {'items': [item('never', 1)]},
})
check("stops at the first video older than 30 days", ids, ['d'])

print(f"\n{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
