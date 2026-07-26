"""What counts as an interaction, per platform — one definition shared by
everything that DISPLAYS a number (this dashboard and the weekly deck).

Why this file exists: every collector invented its own engagement_rate.
Facebook divided (clicks + like + comments + shares) by reach, Instagram
(likes + comments + saves + shares) by reach, TikTok and X by views. Measured
over ~50 recent items per platform, **88% of Facebook's "engagement" was
clicks** — 10.6% published against 1.2% of actual social interaction. So the
platform columns could not be compared to each other, and the same post scored
differently in the deck and on the dashboard, which read the number from
different places.

The collectors are NOT changed. `engagement_rate` keeps being written to the
sheets exactly as before: the daily Telegram report prints it and the alert
thresholds were calibrated against it, so redefining the column mid-history
would make it mean two different things depending on a row's date. Everything
here happens in the presentation layer, over the raw interaction columns the
collectors already store.

Two deliberate choices:

**Facebook counts ALL reactions**, not just `likes`. The collector's
total_engagement drops love/haha/wow/sad/angry — a median 20% of reactions and
up to 42% on a single post. On a news page an angry reaction is not noise, it
is the story.

**Saves are excluded from the engagement number.** Instagram and TikTok have
them; Facebook, X and YouTube have no equivalent. A rate whose numerator
changes shape between platforms is the exact problem this file exists to fix,
so saves are reported as their own figure instead. Engagement is therefore
always the sum of the columns actually shown next to it.
"""

# Interaction families -> the sheet columns that make them up. A family is the
# SUM of its columns (X's "shares" really is retweets plus quotes), so this is
# not the same as "the first column that has data".
FIELDS = {
    'facebook': {
        'likes': ('likes', 'love', 'haha', 'wow', 'sad', 'angry'),
        'comments': ('comments',),
        'shares': ('shares',),
    },
    'instagram': {
        'likes': ('likes',),
        'comments': ('comments',),
        'shares': ('shares',),
        'saves': ('saved',),
    },
    'tiktok': {
        'likes': ('likes',),
        'comments': ('comments',),
        'shares': ('shares',),
        'saves': ('saves',),
    },
    'x': {
        'likes': ('likes',),
        'comments': ('replies',),
        'shares': ('retweets', 'quotes'),
    },
    'youtube': {
        'likes': ('likes',),
        'comments': ('comments',),
    },
}

# A platform with no spec still renders rather than crashing.
DEFAULT_FIELDS = {'likes': ('likes',), 'comments': ('comments',), 'shares': ('shares',)}

ALIASES = {'twitter': 'x', 'tweet': 'x', 'yt': 'youtube', 'fb': 'facebook', 'ig': 'instagram'}

# What goes into the engagement rate, in order. Saves are not here on purpose.
ENGAGEMENT_FIELDS = ('likes', 'comments', 'shares')

# The icons are the ones the deck's חריג badge already uses, so a reader learns
# them once. YouTube has no share count in the API at all — the column is
# absent rather than a row of zeros.
DISPLAY = {
    'facebook': (('likes', '❤️', 'ריאקציות'), ('comments', '💬', 'תגובות'), ('shares', '🔁', 'שיתופים')),
    'instagram': (('likes', '❤️', 'לייקים'), ('comments', '💬', 'תגובות'), ('shares', '🔁', 'שיתופים')),
    'tiktok': (('likes', '❤️', 'לייקים'), ('comments', '💬', 'תגובות'), ('shares', '🔁', 'שיתופים')),
    'x': (('likes', '❤️', 'לייקים'), ('comments', '💬', 'תגובות'), ('shares', '🔁', 'ריטוויטים')),
    'youtube': (('likes', '❤️', 'לייקים'), ('comments', '💬', 'תגובות')),
}


def _num(x):
    """Sheet cells arrive as strings often enough that this has to be lenient."""
    if x is None:
        return 0.0
    if isinstance(x, bool):
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(",", "")
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def resolve(platform):
    p = str(platform or '').strip().lower()
    return ALIASES.get(p, p)


def spec(platform):
    return FIELDS.get(resolve(platform), DEFAULT_FIELDS)


def columns_for(platform, field):
    """The sheet columns that make up one interaction family. Empty when the
    platform has no such family (YouTube shares, Facebook saves)."""
    return spec(platform).get(field, ())


def display_columns(platform):
    """(field, icon, label) per column to show, in display order."""
    return DISPLAY.get(resolve(platform), DISPLAY['facebook'])


def count(platform, row, field):
    """One interaction family for one row. `row` may be a dict or a Series."""
    return int(round(sum(_num(row.get(c, 0)) for c in columns_for(platform, field))))


def counts(platform, row):
    """Every family the platform has, keyed by field name."""
    return {f: count(platform, row, f) for f in spec(platform)}


def total(platform, row):
    """Social interactions: exactly the sum of the displayed columns."""
    return sum(count(platform, row, f) for f in ENGAGEMENT_FIELDS)


def rate(platform, row, basis='views'):
    """Engagement as a percentage of `basis` (views by default).

    Views, not reach: reach is missing entirely on TikTok, X and YouTube, and a
    rate that switches denominators between slides is unreadable. Within a
    platform, over time, either basis behaves the same.
    """
    den = _num(row.get(basis, 0))
    if den <= 0:
        return 0.0
    return round(total(platform, row) / den * 100, 2)
