"""Which Kan outlet an item came from, when the item itself does not say so in
a field — shared by the dashboard and the weekly deck so the two can never
disagree about what counts as radio.

**Reshet Bet.** Radio items are posted to the Kan News *Facebook page*, not to a
page of their own, and the only marker is a signature near the end of the
caption. Two shapes appear, and both have to be matched:

    כאן חדשות ברשת ב 🎙️ אסתי פרז בן עמי        the newsroom sign-off
    ... סיפר על התקיפה בתוכנית "קלמן וליברמן" בכאן רשת ב'
    "..."  🎙 כאן רשת ב'                          the station alone

Anchoring on "כאן חדשות" alone — which is what the first version did — found
134 of them and missed 30, every one of which was genuinely radio: the ones
signed `🎙 כאן רשת ב'`, `לכאן רשת ב'`, `סיפר עליו לרשת ב'`.

The apostrophe is the second anchor and it is what makes a bare match safe. A
plain "רשת ב" hits ordinary Hebrew — `מעלה לרשת בשפה הערבית`,
`הפופולאריות של החיות ברשת בשנים האחרונות`, and a chain called
`רשת בית הפנקייק`. With a geresh it does not.

Measured over the full Facebook sheet (4,416 rows, 2026-07-26): 164 matches,
a strict superset of the 134 the old pattern found, and of the 7 rows that say
"רשת ב" and are left out, all 7 are genuinely not radio. No known miss, no
known false positive.
"""

import re

# "כאן [חדשות] [ב]רשת ב"  |  "[ב]רשת ב" carrying an apostrophe
RESHET_BET_RE = re.compile(r"כאן\s+(?:חדשות\s+)?ב?רשת ב|ב?רשת ב['׳’]")

RESHET_BET_NAME = "רשת ב׳"


def is_reshet_bet(text):
    """True when a caption carries the Reshet Bet signature."""
    return bool(RESHET_BET_RE.search(str(text or "")))
