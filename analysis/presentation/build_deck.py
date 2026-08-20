#!/usr/bin/env python3
"""מחשב את כל מספרי המצגת 2024→היום ל-`deck_content.json`.

    python analysis/presentation/build_deck.py

הפרדה מכוונת בין החישוב לבין הניסוח, כמו ב-`weekly_deck`: הסקריפט מחשב מספרים
ולא כותב משפטים. הכותרות, המסקנות והמשפט של כל שקף יושבים ב-JSON תחת
`editorial`, נכתבים ביד, ו**שורדים הרצה חוזרת** — ראו `_merge_editorial`.
מצגת להנהלה נופלת או עומדת על הניסוח, וזה לא משהו שסקריפט צריך להמציא.

## היחידה שכל המצגת מדברת בה

"צפיות שנצברו לתוכן שפורסם בחודש X" — לא "צפיות בחודש X". בכל ארבע הרשתות
המדד הוא מצטבר-עד-היום ברמת הפריט, ולכן הוא עקבי בין הפלטפורמות אבל מוטה
לטובת תוכן ותיק שהספיק לצבור. ההטיה קטנה בפייסבוק/אינסטגרם/טיקטוק — פוסט גמור
תוך ארבעה ימים (ROADMAP, שורה סגורה 27.7.2026) — וגדולה יותר ביוטיוב, שיש לו
זנב אמיתי. נאמר על השקף ולא מוסתר.

## מה לא נכנס לכאן, ולמה

* **חשיפה בפייסבוק 2025** — לא נמשכה (המדד נשחק עם גיל הפוסט).
* **טוויטר לפני 6/2026** — הספק מגיע 13 יום אחורה בלבד.
* **הגיליון `נתוני פייסבוק`** — 38–53% מהשורות עם אפס צפיות לפני יוני 2026.
* **צופים קבועים ביוטיוב** — מדד שהושק ב-20.3.2025.
"""

import csv
import json
import os
import sys
from collections import defaultdict

import pandas as pd

# הקונסולה של Windows היא cp1255 ונופלת על חץ יוניקוד באמצע דוח תקין
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, '..', 'yearly_content')
PULLED = os.path.join(HERE, 'pulled')
OUT = os.path.join(HERE, 'deck_content.json')

TODAY = '2026-08-11'       # יום משיכת הקבצים. **אינו** התאריך שהמספרים מדברים בו
# הכל נחתך בסוף יולי 2026. אוגוסט הוא חודש חלקי, ובכל גרף חודשי הוא מצייר
# צניחה שהיא רק «החודש עוד לא נגמר» — הקורא רואה נפילה שלא קרתה.
CUTOFF = '2026-07-31'
CUTOFF_MONTH = '2026-07'
YTD_CUT = (7, 31)          # התקופה המקבילה: 1 בינואר עד 31 ביולי
VIEWS_FROM = '2024-09'     # מטא הגדירה מחדש צפיות/חשיפה באוג'–ספט' 2024

PLATFORMS = ['facebook', 'youtube', 'tiktok', 'instagram', 'twitter', 'whatsapp']

# עוקבים **נכון ל-CUTOFF**, כלומר 31.7.2026 — לא ליום המשיכה.
#
# קודם הם היו מ-11.8, בזמן שכל שאר הדק נחתך ב-31.7. אחד עשר יום זה לא הרבה,
# אבל זה מספיק כדי ששקף יאמר «עד יולי 2026» ויציג מספר מאוגוסט, וכדי שסך
# העוקבים בשער לא יתאים לסכום הכרטיסים בשקף הנכסים. תאריך אחד לכל המסמך.
#
# חמש מהשש נקראות משורת 2026-07-31 ב-`pulled/sheet_followers.csv`; יוטיוב
# מייצוא Studio, שנותן את המספר המדויק במקום 797,000 שה-API מעגל לשלוש
# ספרות. לוואטסאפ אין API ואין סדרה — המספר נמסר ידנית ואין לו תאריך, וזו
# הסיבה שהוא היחיד כאן שאי אפשר ליישר.
FOLLOWERS = {
    'facebook': 1180119, 'tiktok': 824000, 'youtube': 797657,
    'twitter': 372932, 'whatsapp': 310905, 'instagram': 283236,
}


def _num(s):
    return pd.to_numeric(s, errors='coerce')


def clean_title(s, limit=88):
    """כותרת לשקף: בלי סימני bidi, בגבול מילה, ובלי שובל של תיוג חתוך.

    שלוש מלכודות שחוזרות בכל פעם שטקסט מהרשתות מגיע לתצוגה עברית:
    הקאפשנים מכילים תווי כיווניות בלתי נראים (U+2066-2069, RLM/LRM) שמזיזים
    חצי שורה; חיתוך לפי מספר תווים חותך באמצע מילה; ותיוג לטיני בסוף
    (`@itayblumental`) נחתך מהצד הלא נכון ומשאיר `itayblumental@` תלוי באוויר.
    """
    s = str(s or '')
    for ch in ('⁦', '⁧', '⁨', '⁩', '‎', '‏', '‪',
               '‫', '‬', '­'):
        s = s.replace(ch, '')
    s = ' '.join(s.split())                      # טאבים וירידות שורה
    if len(s) > limit:
        cut = s[:limit]
        if ' ' in cut:
            cut = cut[:cut.rfind(' ')]
        s = cut.rstrip(' ,.;:-–—') + '…'
    # תיוג שנשאר חתוך בקצה — להוריד אותו ולא להציג חצי שם
    words = s.split(' ')
    while words and (words[-1].startswith('@') or words[-1].endswith('@')):
        words.pop()
    return ' '.join(words).rstrip(' ,.;:-–—…') or s


def _ytd(d, col='dt'):
    """אותו חלון בכל שנה, כדי ש-2026 לא תושווה לשנה שלמה."""
    m, day = YTD_CUT
    return d[(d[col].dt.month < m) | ((d[col].dt.month == m) & (d[col].dt.day <= day))]


def load_meta():
    """פייסבוק ואינסטגרם — מהבסיס החודשי ש-build_history בנה."""
    p = os.path.join(HERE, 'history_monthly.csv')
    if not os.path.exists(p):
        raise SystemExit("❌ חסר history_monthly.csv — להריץ קודם build_history.py")
    return pd.read_csv(p, encoding='utf-8-sig')


def load_video(name):
    """יוטיוב/טיקטוק — שורה לפריט, עם צפיות מצטברות."""
    d = pd.read_csv(os.path.join(PULLED, name), encoding='utf-8-sig')
    d['dt'] = pd.to_datetime(d['date'], errors='coerce')
    d = d.dropna(subset=['dt'])
    d = d[d['dt'] <= CUTOFF]
    for c in ('views', 'likes', 'comments'):
        if c in d.columns:
            d[c] = _num(d[c]).fillna(0)
    return d


def yearly_from_video(d):
    out = {}
    for y, g in d.groupby(d['dt'].dt.year):
        out[str(y)] = {'posts': len(g), 'views': int(g['views'].sum()),
                       'likes': int(g['likes'].sum())}
    ytd = {}
    for y, g in _ytd(d).groupby(d['dt'].dt.year):
        ytd[str(y)] = {'posts': len(g), 'views': int(g['views'].sum())}
    return out, ytd


def monthly_from_video(d, since='2024-09'):
    s = d[d['dt'] >= since].groupby(d['dt'].dt.to_period('M'))['views'].sum()
    return [{'month': str(m), 'views': int(v)} for m, v in s.items()]


def build():
    meta = load_meta()
    yt = load_video('youtube_history.csv')
    tt = load_video('tiktok_history.csv')

    deck = {'generated_for': TODAY, 'followers': FOLLOWERS, 'platforms': {}}

    # --- פייסבוק ואינסטגרם, מהבסיס החודשי ---
    for plat in ('facebook', 'instagram'):
        sub = meta[meta['platform'] == plat].copy()
        sub = sub[sub['month'] <= CUTOFF_MONTH]     # לפני כל חישוב, לא אחריו
        sub['year'] = sub['month'].str[:4]
        block = {'yearly': {}, 'monthly_views': []}
        for y, g in sub.groupby('year'):
            ok = g[g['views_valid']] if 'views_valid' in g else g
            block['yearly'][y] = {
                'posts': int(g['posts'].sum()),
                # הפוסטים שהצפיות באמת מתייחסות אליהם. ב-2024 הצפיות מתחילות
                # בספטמבר, ולהצמיד להן ספירה של שנה שלמה זה לומר שאלפי פוסטים
                # הפיקו את מה שכמה מאות הפיקו.
                'posts_in_views_window': int(ok['posts'].sum()),
                'views_window': (ok['month'].min() if len(ok) else None),
                'views': int(ok['views'].sum(skipna=True) or 0),
                'likes': int(g['likes'].sum(skipna=True) or 0),
                'comments': int(g['comments'].sum(skipna=True) or 0),
                'shares': int(g['shares'].sum(skipna=True) or 0),
            }
            if 'watch_hours' in g:
                block['yearly'][y]['watch_hours'] = int(g['watch_hours'].sum(skipna=True) or 0)
        for _, r in sub[sub['month'] >= VIEWS_FROM].iterrows():
            if r.get('views_valid', True) and pd.notna(r.get('views')):
                block['monthly_views'].append({'month': r['month'], 'views': int(r['views'])})
        # ינואר–יולי בכל שנה. בלי זה משווים שנה שלמה מול שבעה חודשים
        # וקוראים לזה ירידה — וגם התמהיל עצמו נמדד על חלונות שונים.
        ytd = sub[sub['month'].str[5:7] <= '07']
        block['posts_ytd'] = {y: int(g['posts'].sum())
                              for y, g in ytd.groupby('year')}
        # מדדים על **חלון מקביל** — ינואר–יולי בכל שנה. בלי זה כל השוואה של
        # 2026 לשנה קודמת משווה שבעה חודשים לשנים־עשר וקוראת לזה שינוי.
        block['metrics_ytd'] = {}
        for y, g in ytd.groupby('year'):
            ok = g[g['views_valid']] if 'views_valid' in g else g
            block['metrics_ytd'][y] = {
                'posts': int(g['posts'].sum()),
                'engagement': int(g[['likes', 'comments', 'shares']]
                                  .sum(skipna=True).sum() or 0),
                'watch_hours': int(g['watch_hours'].sum(skipna=True) or 0)
                if 'watch_hours' in g else 0,
                'views': int(ok['views'].sum(skipna=True) or 0),
                'posts_in_views_window': int(ok['posts'].sum()),
            }
            for c in ('likes', 'comments', 'shares', 'saves'):
                if c in g:
                    block['metrics_ytd'][y][c] = int(g[c].sum(skipna=True) or 0)
        # **שתי גרסאות, ובכוונה.** `_ytd` היא הבסיס היחיד שמותר להשוות עליו
        # אחוזים — 2026 נגמרת ביולי. אבל טבלה שמציגה רק אותה מקטינה את הפלט
        # של 2024 ושל 2025 בכ-43%, ומספרת להנהלה שפרסמנו פחות ממה שפרסמנו.
        # לכן הטבלאות מציגות **שנים מלאות** עם 2026 מסומנת כחלקית, והאחוזים
        # מחושבים בנפרד על `_ytd` ונאמרים במפורש.
        for key, frame in (('format_mix', ytd), ('format_mix_full', sub)):
            mix = {}
            for fmt in ('רילס', 'תמונה', 'קרוסלה', 'וידאו', 'סטטוס', 'לינק'):
                if fmt in frame.columns:
                    per_year = frame.groupby('year')[fmt].sum()
                    if per_year.sum():
                        mix[fmt] = {y: int(v) for y, v in per_year.items()}
            block[key] = mix
        deck['platforms'][plat] = block

    # --- יוטיוב וטיקטוק ---
    for plat, d in (('youtube', yt), ('tiktok', tt)):
        yearly, ytd = yearly_from_video(d)
        deck['platforms'][plat] = {
            'yearly': yearly, 'ytd': ytd,
            'monthly_views': monthly_from_video(d),
        }

    # ליוטיוב יש מקור טוב יותר מה-API: ייצוא Studio נותן **צפיות בתקופה**
    # ולא "מה שתוכן משנה X צבר עד היום". ההפרש אינו זניח — 642M מול 547M —
    # וכולו הזנב של יוטיוב, שהוא היחיד מהרשתות כאן שיש לו זנב אמיתי.
    deck['platforms']['youtube'].update(_youtube_studio())
    # Shorts מול רגיל — הפילוח היחיד שיש ביוטיוב
    # לטיקטוק יש פילוח משלו — וידאו מול קרוסלת תמונות
    tt_ytd = _ytd(tt)
    deck['platforms']['tiktok']['format_mix'] = {
        {'Video': 'וידאו', 'Photo': 'קרוסלה'}.get(t, t):
            {str(y): int(v) for y, v in g.groupby(g['dt'].dt.year).size().items()}
        for t, g in tt_ytd.groupby('type')}
    yt_ytd = _ytd(yt)
    deck['platforms']['youtube']['format_mix'] = {
        {'Regular': 'סרטון רגיל', 'Shorts': 'שורטס'}.get(t, t):
            {str(y): int(v) for y, v in g.groupby(g['dt'].dt.year).size().items()}
        for t, g in yt_ytd.groupby('type')}
    for p, d in (('youtube', yt), ('tiktok', tt)):
        for y, g in d.groupby(d['dt'].dt.year):
            blk = deck['platforms'][p]['yearly'].get(str(y))
            if blk:
                blk['posts_in_views_window'] = len(g)
        deck['platforms'][p]['posts_ytd'] = {
            str(y): len(g) for y, g in _ytd(d).groupby(d['dt'].dt.year)}
    # הצפיות וזמן הצפייה מגיעים מ-Studio ומכסים את כל התקופה. רק **ספירת
    # הפריטים** מגיעה מה-API, שחותך את פלייליסט ההעלאות ב-20,000.
    deck['platforms']['youtube']['coverage_note'] = (
        'צפיות וזמן צפייה מייצוא Studio — 953 ימים רצופים, ללא פערים. '
        'ספירת הפריטים מה-API, שחושף 20,000 סרטונים אחרונים (מ-08/2022)')
    deck['platforms']['tiktok']['coverage_note'] = (
        'נמשכו 3,993 מתוך 4,950 סרטונים; ההיסטוריה מתחילה ב-2025-02-17')

    # --- טוויטר: רק מה שהגיליון מכסה ---
    tw = pd.read_csv(os.path.join(PULLED, 'sheet_twitter.csv'), encoding='utf-8-sig')
    tw['dt'] = pd.to_datetime(tw['date'], errors='coerce')
    tw = tw.dropna(subset=['dt'])
    tw = tw[tw['dt'] <= CUTOFF]
    tw['views'] = _num(tw.get('views')).fillna(0)
    deck['platforms']['twitter'] = {
        'from': str(tw['dt'].min().date()), 'to': str(tw['dt'].max().date()),
        'posts': len(tw), 'views': int(tw['views'].sum()),
        'coverage_note': 'ה-API של הספק מגיע 13 יום אחורה בלבד — אין היסטוריה לפני 6/2026',
    }
    deck['platforms']['whatsapp'] = {
        'coverage_note': 'לערוץ אין API. גודל הקהל נמסר ידנית; אין נתוני צפיות',
    }

    # --- מנויי יוטיוב, סדרה יומית ---
    subs = pd.read_csv(os.path.join(SRC, 'youtube_studio', 'subscribers_daily.csv'))
    subs['Date'] = pd.to_datetime(subs['Date'])
    subs = subs[subs['Date'] <= CUTOFF]
    deck['_subs'] = subs
    # החודשים שבהם נוספו הכי הרבה מנויים — לסימון על העקומה. השאלה היא
    # "מתי קפצנו", לא "כמה נוספו אתמול".
    m_subs = subs.groupby(subs['Date'].dt.to_period('M'))['Subscribers'].last()
    m_gain = m_subs.diff().dropna()
    deck['youtube_subscribers'] = {
        'series': [{'month': str(m), 'subs': int(v)} for m, v in m_subs.items()],
        'start': int(subs['Subscribers'].iloc[0]), 'end': int(subs['Subscribers'].iloc[-1]),
        'by_year': {str(y): int(g['Subscribers'].iloc[-1] - g['Subscribers'].iloc[0])
                    for y, g in subs.groupby(subs['Date'].dt.year)},
        'top_months': [{'month': str(m), 'gain': int(v)}
                       for m, v in m_gain.nlargest(3).items()],
    }
    aud = pd.read_csv(os.path.join(SRC, 'youtube_studio', 'monthly_audience.csv'))
    aud['Date'] = pd.to_datetime(aud['Date'])
    aud = aud[aud['Date'] <= CUTOFF]
    deck['youtube_audience'] = {str(y): int(g['Monthly audience'].mean())
                                for y, g in aud.groupby(aud['Date'].dt.year)}

    # --- גיוס עוקבים בפייסבוק (ברוטו — ראו meta_insights/README) ---
    rows = []
    for r in csv.reader(open(os.path.join(SRC, 'meta_insights', 'Follows.csv'),
                             encoding='utf-16')):
        if len(r) == 2 and r[0] not in ('Date', '') and r[1].strip():
            try:
                rows.append((r[0][:10], float(r[1])))   # תאריך מלא: נדרש ליום
            except ValueError:
                pass
    rows = [(d0, v) for d0, v in rows if d0 <= CUTOFF]
    by_month = defaultdict(float)
    for day, v in rows:
        by_month[day[:7]] += v
    fb_monthly = [{'month': m, 'follows': int(v)} for m, v in sorted(by_month.items())]
    # עקומה מצטברת: "כמה עוקבים הצטרפו מתחילת 2024 ועד כל נקודה" — זה מה
    # שהשאלה «בכמה גדלנו» באמת מבקשת, ולא הקצב היומי.
    run = 0
    cum = []
    for m in fb_monthly:
        run += m['follows']
        cum.append({'month': m['month'], 'total': run})
    top_fb = sorted(fb_monthly, key=lambda x: -x['follows'])[:3]
    deck['facebook_follows'] = {
        'monthly': fb_monthly,
        'cumulative': cum,
        'top_months': [{'month': m['month'], 'gain': m['follows']} for m in top_fb],
        'total_gross': run,
        'net_ratio': 0.75,
        'note': ('ברוטו. מול הגיליון על 8 חודשי חפיפה: 136,017 הצטרפויות מול '
                 '102,061 גידול בפועל — אחד מכל ארבעה עוזב'),
    }

    deck['followers_series'] = _followers_series(m_subs, cum, run)
    deck['assets'] = _assets_overview(deck, run)
    deck['facebook_audience'] = _facebook_audience()

    # --- מה שאינסטגרם מלמד על גיוס עוקבים לפי פורמט ---
    deck['instagram_follows_by_format'] = ig_follows_by_format()

    # --- התוכן הגדול של כל שנה ---
    deck['top_content'] = top_content(yt, tt)
    deck['top_by_platform'] = top_by_platform(yt, tt)
    deck['cross_platform'] = cross_platform(deck['top_content'])

    # --- הימים הגדולים, ומה הם עשו לקהל ---
    fb_daily = pd.DataFrame(rows, columns=['date', 'follows'])
    fb_daily['date'] = pd.to_datetime(fb_daily['date'])
    deck['big_days'] = big_days(yt, tt, subs, fb_daily)
    # החלונות מגיעים משכבת הניסוח, ולכן חייבים להתמזג לפני המדידה
    prev_ed = {}
    if os.path.exists(OUT):
        try:
            prev_ed = json.load(open(OUT, encoding='utf-8')).get('editorial', {})
        except (ValueError, OSError):
            prev_ed = {}
    ev_cfg = prev_ed.get('events') or EDITORIAL_SEED['events']
    deck['events'] = measure_events(yt, tt, subs, fb_daily, ev_cfg)

    # יום השיא נופל בתוך אחד האירועים המוגדרים — לקשר במקום להסביר בנפרד,
    # כדי שהשם על השקף השני יהיה בדיוק השם שמופיע בשקף האירועים.
    for day in deck.get('big_days', {}).get('days', []):
        for w in deck['events']['windows']:
            if w['from'] <= day['date'] <= w['to']:
                day['event'] = w.get('short') or w['name']
                break
    deck['_candidates'] = candidate_windows(yt, tt)
    deck.pop('_subs', None)

    # סך צפיות בחלון ינואר–יולי, לכל שנה — הבסיס לאחוזי הגידול בשקף השני.
    #
    # **רק שנים שכל הרשתות מכסות.** ליוטיוב יש ינואר–יולי 2024 ולפייסבוק
    # ולאינסטגרם אין (הגבול של מטא), אז סכימה נאיבית ייצרה "2024 = 136.5M"
    # שהוא יוטיוב לבדו, ומשם "+744%" ל-2025 — מספר שנראה מרהיב ומשווה רשת
    # אחת מול ארבע. שנה נכנסת רק אם כל רשת עם היסטוריה תרמה לה חודשים.
    have = {p: {m['month'][:4] for m in blk.get('monthly_views', [])
                if m['month'][5:7] <= '07'}
            for p, blk in deck['platforms'].items() if blk.get('monthly_views')}
    full = set.intersection(*have.values()) if have else set()
    totals = defaultdict(int)
    for plat, blk in deck['platforms'].items():
        for m in blk.get('monthly_views', []):
            if m['month'][5:7] <= '07' and m['month'][:4] in full:
                totals[m['month'][:4]] += m['views']
    deck['views_by_year_ytd'] = dict(sorted(totals.items()))
    deck['views_ytd_platforms'] = sorted(have)

    # סכומים לתקופה כולה, לשקף ההישגים. **רק 2024–2026**: ל-yearly של יוטיוב
    # יש גם 2022–2023, וסכימה נאיבית הייתה מייחסת למחלקה 8,709 סרטונים
    # שקדמו לתקופה. טיקטוק חלקי (מפברואר 2025), כלומר הספירה שמרנית.
    span = {'posts': 0, 'likes': 0, 'comments': 0, 'shares': 0, 'watch_hours': 0}
    for blk in deck['platforms'].values():
        for y, v in (blk.get('yearly') or {}).items():
            if y in ('2024', '2025', '2026'):
                for k in span:
                    span[k] += v.get(k, 0) or 0
    span['watch_hours'] += deck['platforms']['youtube'].get('watch_hours', 0)
    span['engagement'] = span['likes'] + span['comments'] + span['shares']
    # ממוצע יומי: מה שהופך «4.8 מיליארד» למשהו שאפשר לתפוס
    import datetime as _dt
    for yr in list(deck['views_by_year_ytd']):
        n = (_dt.date(int(yr), 7, 31) - _dt.date(int(yr), 1, 1)).days + 1
        span.setdefault('daily', {})[yr] = round(deck['views_by_year_ytd'][yr] / n)
    deck['period_totals'] = span

    # תיאור לכל קפיצה שמסומנת על העקומות — מאותה רשת, ולא כטענת סיבתיות
    for block, plat in ((deck['youtube_subscribers'], 'youtube'),
                        (deck['facebook_follows'], 'facebook')):
        heads = month_headlines(yt, tt, plat)
        for m in block.get('top_months', []):
            h = heads.get(m['month']) or {}
            m['headline'] = h.get('title', '')
            m['headline_platform'] = h.get('platform', '')

    return deck


def _youtube_studio():
    """צפיות, זמן צפייה ופילוח סוגי תוכן מייצוא Studio — 953 ימים, בלי פערים."""
    ys = os.path.join(SRC, 'youtube_studio')
    d = pd.read_csv(os.path.join(ys, 'daily_views.csv'))
    d['Date'] = pd.to_datetime(d['Date'])
    d = d[d['Date'] <= CUTOFF]

    tot = pd.read_csv(os.path.join(ys, 'type_totals.csv'))
    by_type = {}
    for _, r in tot.iterrows():
        name = {'Videos': 'סרטונים', 'Shorts': 'שורטס',
                'Live stream': 'שידור חי'}.get(r['Content type'])
        if not name:
            continue
        by_type[name] = {
            'views': int(r['Views']),
            'watch_hours': int(float(r['Watch time (hours)'])),
            'avg_duration': str(r['Average view duration']),
            'pct_viewed': float(r['Average percentage viewed (%)']),
        }
    total_watch = int(float(tot[tot['Content type'] == 'Total']
                            ['Watch time (hours)'].iloc[0]))

    bt = pd.read_csv(os.path.join(ys, 'daily_by_type.csv'))
    bt['Date'] = pd.to_datetime(bt['Date'])
    bt = bt[bt['Date'] <= CUTOFF]
    heb = {'Videos': 'סרטונים', 'Shorts': 'שורטס', 'Live stream': 'שידור חי'}
    mix = {}
    for t, g in bt.groupby('Content type'):
        if t in heb:
            mix[heb[t]] = {str(y): int(v) for y, v in
                           g.groupby(g['Date'].dt.year)['Views'].sum().items()}

    yearly = {}
    for y, g in d.groupby(d['Date'].dt.year):
        yearly[str(y)] = {'views': int(g['Views'].sum()), 'days': len(g)}

    # ינואר–יולי בכל שנה, מ-Studio. **זה המדד הלא-מוטה של יוטיוב:** צפיות
    # שהתרחשו בפועל, ולא "מה שתוכן מאותה שנה צבר עד היום" — לתוכן של 2024
    # היו שנתיים נוספות לצבור, ולערוץ יוטיוב יש זנב ארוך באמת. על מדד הפריט
    # 2026 יוצאת ‎-10%; על המדד הזה, ‎+3.5%. ההפרש הוא הטיית ההתבגרות.
    period_ytd = {}
    for y, g in _ytd(d, 'Date').groupby(d['Date'].dt.year):
        period_ytd[str(y)] = {'views': int(g['Views'].sum()), 'days': len(g)}
    pt = pd.read_csv(os.path.join(ys, 'period_totals.csv'))
    pt['Date'] = pd.to_datetime(pt['Date'], errors='coerce')   # שורת «Total»
    pt = pt.dropna(subset=['Date'])
    pt = pt[pt['Date'] <= CUTOFF]
    for y, g in _ytd(pt, 'Date').groupby(pt['Date'].dt.year):
        if str(y) in period_ytd:
            period_ytd[str(y)]['watch_hours'] = int(
                pd.to_numeric(g['Watch time (hours)'], errors='coerce').sum())

    return {
        'period_ytd': period_ytd,
        'yearly_period': yearly,
        'views_total': int(d['Views'].sum()),
        'watch_hours': total_watch,
        'by_type': by_type,
        'views_mix': mix,
        # **לא** מוגבל ל-VIEWS_FROM: גבול ספטמבר 2024 הוא של מטא בלבד.
        # להחיל אותו כאן היה מסתיר 153,867,031 צפיות אמיתיות של ינואר–אוגוסט.
        'monthly_views': [{'month': str(m), 'views': int(v)} for m, v in
                          d.groupby(d['Date'].dt.to_period('M'))['Views'].sum().items()
                          if str(m) <= CUTOFF_MONTH],
        'source_note': ('צפיות בתקופה מייצוא YouTube Studio — לא "מה שתוכן '
                        'השנה צבר עד היום"'),
    }


FB_ANCHOR = ('2026-07-31', 1180119)   # ערך מדוד מגיליון «מעקב עוקבים»

# מאיזה תאריך יש בכלל מדידת עוקבים לכל רשת. זה לא פירוט טכני: זה מה שקובע
# על אילו רשתות מותר לכתוב «גדלה ב-X%» ועל אילו אסור.
SINCE = {
    'facebook':  ('2024-01', None),
    'youtube':   ('2024-01', None),
    'instagram': ('2025-08', 'נמדד מאוגוסט 2025'),
    'tiktok':    (None, 'נמדד מיולי 2026'),
    'twitter':   (None, 'נמדד מיוני 2026'),
    'whatsapp':  (None, 'לערוץ אין API'),
}


HE_PLACE = {
    'Israel': 'ישראל', 'Palestine': 'פלסטין', 'United States': 'ארה"ב',
    'Nigeria': 'ניגריה', 'Philippines': 'הפיליפינים', 'France': 'צרפת',
    'India': 'הודו', 'Brazil': 'ברזיל', 'Germany': 'גרמניה',
    'United Kingdom': 'בריטניה', 'Jerusalem': 'ירושלים',
    'Tel Aviv': 'תל אביב', 'Petah Tikva': 'פתח תקווה', 'Ramat Gan': 'רמת גן',
    'Beersheba': 'באר שבע', 'Haifa': 'חיפה', 'Holon': 'חולון',
    'Rishon Le Zion': 'ראשון לציון', 'Ashdod': 'אשדוד', 'Netanya': 'נתניה',
}


def _facebook_audience():
    """דמוגרפיה של עמוד הפייסבוק, מ-`meta_insights/Audience.csv`.

    **תצלום נוכחי ולא היסטורי.** מטא אינה חושפת דמוגרפיה לאחור בשום ממשק,
    ולכן אי אפשר לומר «הקהל השתנה» — רק «כך הוא נראה היום». הבלוקים נקראים
    מהקובץ ולא מוקלדים: מספר שמוקלד ביד אינו יודע להתעדכן ואינו יודע להיבדק.
    """
    path = os.path.join(SRC, 'meta_insights', 'Audience.csv')
    if not os.path.exists(path):
        return None
    rows = list(csv.reader(open(path, encoding='utf-16')))
    at = {}
    for i, r in enumerate(rows):
        if len(r) == 1 and r[0].strip() in ('Age & gender', 'Top cities',
                                            'Top countries'):
            at[r[0].strip()] = i

    def _pairs(i):
        names, vals = rows[i + 1], rows[i + 2]
        out = []
        for n, v in zip(names, vals):
            try:
                out.append((HE_PLACE.get(n.split(',')[0].strip(), n), float(v)))
            except ValueError:
                pass
        return out

    out = {}
    if 'Age & gender' in at:
        ages, men, women = [], 0.0, 0.0
        for r in rows[at['Age & gender'] + 2:]:
            if len(r) != 3 or not r[0] or '-' not in r[0] and '+' not in r[0]:
                break
            m, w = float(r[1]), float(r[2])
            ages.append({'band': r[0], 'men': m, 'women': w})
            men, women = men + m, women + w
        out['age'] = ages
        out['men'] = round(men, 1)
        out['women'] = round(women, 1)
        # הגוש הגדול: גברים 25–44, כלומר שתי הרצועות ביחד
        core = sum(a['men'] for a in ages if a['band'] in ('25-34', '35-44'))
        out['core'] = {'label': 'גברים 25–44', 'pct': round(core, 1)}
    if 'Top cities' in at:
        out['cities'] = _pairs(at['Top cities'])
    if 'Top countries' in at:
        out['countries'] = _pairs(at['Top countries'])
    return out or None


def _assets_overview(deck, fb_gross):
    """כל שישה הנכסים בשקף אחד: כמה עוקבים היום, וכמה נוספו מאז 2024.

    המספר העדכני נלקח מ-`FOLLOWERS` לכל השש — אותו מקור שמופיע בשער
    ובכותרות שקפי הרשתות, כדי ששני מספרים לאותו נכס לא ייפרדו בין שקפים.

    הצמיחה היא הסיפור החלקי: היא נגזרת מאותה נקודת פתיחה בינואר 2024
    שקיימת רק ליוטיוב (מדוד) ולפייסבוק (נגזר ביחס 0.750 המאומת). אינסטגרם
    מקבל חלון קצר ומסומן, ולטיקטוק, ל-X ולוואטסאפ אין מה לכתוב — ולכן
    נכתבת שם הסיבה ולא מקף. שלוש רשתות בלי צמיחה זה מה שיש; להמציא להן
    נקודת פתיחה כדי שהשקף ייראה מלא זה בדיוק מה שאסור.
    """
    ratio = 0.75
    now = dict(FOLLOWERS)

    def _gross(fn):
        """סך התוספות היומיות בייצוא, **חתוך ב-CUTOFF כמו כל השאר.**

        החיתוך אינו קישוט. הצמיחה מחושבת כאן אחורה מנקודת הסוף: אם המלאי
        הוא מ-31.7 והתוספות נספרות עד 10.8, עשרת הימים העודפים נגרעים
        מנקודת הפתיחה ומנפחים את הצמיחה בלי שאיש יראה.
        """
        tot = 0
        for r in csv.reader(open(os.path.join(SRC, 'meta_insights', fn),
                                 encoding='utf-16')):
            if len(r) == 2 and r[0][:4].isdigit() and 'T' in r[0] and r[0][:10] <= CUTOFF:
                try:
                    tot += float(r[1])
                except ValueError:
                    pass
        return tot

    starts = {
        'facebook': int(round(now['facebook'] - ratio * _gross('Follows.csv'))),
        'youtube': 613238,
    }
    ig = _gross('Audience_instagram.csv')
    starts['instagram'] = int(round(now['instagram'] - 0.515 * ig))

    out = []
    for p, n in sorted(now.items(), key=lambda kv: -kv[1]):
        since, note = SINCE[p]
        row = {'platform': p, 'followers': n, 'note': note,
               'derived': p in ('facebook', 'instagram')}
        if since and p in starts:
            row.update(start=starts[p], since=since,
                       gain=n - starts[p], pct=(n / starts[p] - 1) * 100)
        out.append(row)
    full = [r for r in out if r.get('since') == '2024-01']
    return {
        'rows': out,
        'total': sum(now.values()),
        'full_gain': sum(r['gain'] for r in full),
        'full_pct': (sum(r['followers'] for r in full)
                     / sum(r['start'] for r in full) - 1) * 100,
        'full_names': [r['platform'] for r in full],
    }


def _followers_series(m_subs, fb_cum, fb_total_gross):
    """עקומת עוקבים חודשית — רק לשתי הרשתות שיש להן היסטוריה מלאה.

    יוטיוב נמדד ישירות, יום-יום, מ-1.1.2024.

    פייסבוק הוא מקרה אחר: מטא נותנת **הצטרפויות** ואף פעם לא מלאי, ולכן
    העקומה נגזרת אחורה מנקודת עוגן מדודה לפי יחס נטו/ברוטו של 0.750. היחס
    אינו הנחה — יש שמונה חודשים שבהם גם ההצטרפויות (הייצוא) וגם המלאי
    (הגיליון) קיימים, ושם הוא יצא 0.750 בדיוק: 136,017 הצטרפויות מול
    102,061 גידול בפועל. לכן העקומה מסומנת «נגזר» ולא מוצגת כמדידה.

    שאר הרשתות אינן כאן, וזה לא השמטה: לטיקטוק יש שלושה שבועות מדודים
    (מ-21.7.2026), ל-X שישה שבועות, ולערוץ הוואטסאפ אין API. אין דרך
    להראות להן מגמה דו-שנתית בלי להמציא אותה.
    """
    ratio = 0.75
    anchor_month, anchor = FB_ANCHOR[0][:7], FB_ANCHOR[1]
    cum = {c['month']: c['total'] for c in fb_cum}
    gross_to_anchor = cum.get(anchor_month, fb_total_gross)

    fb = [{'month': m, 'value': int(round(anchor - ratio * (gross_to_anchor - t)))}
          for m, t in sorted(cum.items()) if m <= anchor_month]
    yt = [{'month': str(m), 'value': int(v)} for m, v in m_subs.items()]

    out = {}
    for key, series, measured in (('youtube', yt, True), ('facebook', fb, False)):
        first, last = series[0]['value'], series[-1]['value']
        out[key] = {
            'series': series, 'start': first, 'end': last,
            'gain': last - first, 'pct': (last / first - 1) * 100,
            'measured': measured,
        }
    out['youtube']['start'] = 613238        # 1.1.2024, לפני הנקודה הראשונה
    out['facebook']['start'] = int(round(anchor - ratio * gross_to_anchor))
    for key in ('youtube', 'facebook'):
        b = out[key]
        b['gain'] = b['end'] - b['start']
        b['pct'] = (b['end'] / b['start'] - 1) * 100
    out['_note'] = ('פייסבוק נגזר מהצטרפויות לפי יחס נטו/ברוטו 0.750, '
                    'שאומת מול שמונה חודשי חפיפה מדודים')
    out['_missing'] = ['tiktok', 'twitter', 'whatsapp', 'instagram']
    return out


def ig_follows_by_format():
    """עוקבים לכל 1,000 צפיות — המספר שמראה מה באמת מגייס."""
    files = [os.path.join(SRC, 'Jan-01-2024_Dec-31-2024_4588672571364735.csv'),
             os.path.join(SRC, 'data_buisness_suit', '2025 חוץ מסטוריז.xlsx'),
             os.path.join(SRC, 'data_buisness_suit',
                          'Jan-01-2026_Aug-10-2026_1071135865260092.csv')]
    parts = []
    for f in files:
        d = pd.read_excel(f) if f.endswith('.xlsx') else pd.read_csv(f, encoding='utf-8-sig')
        d = d[d['Account username'] == 'kan_news'].copy()
        d['dt'] = pd.to_datetime(d['Publish time'], format='mixed', errors='coerce')
        for c in ('Views', 'Follows'):
            d[c] = _num(d[c])
        parts.append(d[['dt', 'Post type', 'Views', 'Follows']])
    d = pd.concat(parts, ignore_index=True)
    d = d[(d['dt'] >= VIEWS_FROM) & (d['dt'] <= CUTOFF)]  # לפני כן אינן תקפות
    d['fmt'] = d['Post type'].map({'IG reel': 'רילס', 'IG image': 'תמונה',
                                   'IG carousel': 'קרוסלה', 'IGTV': 'רילס'})
    out = {}
    for fmt, g in d.groupby('fmt'):
        v, f = g['Views'].sum(), g['Follows'].sum()
        out[fmt] = {'posts': len(g), 'views': int(v), 'follows': int(f),
                    'per_1k_views': round(f / v * 1000, 2) if v else 0}
    return out


def all_items(yt, tt):
    """כל הפריטים מכל הרשתות בטבלה אחת: תאריך, צפיות, כותרת, רשת."""
    frames = []
    ig_files = [os.path.join(SRC, 'Jan-01-2024_Dec-31-2024_4588672571364735.csv'),
                os.path.join(SRC, 'data_buisness_suit', '2025 חוץ מסטוריז.xlsx'),
                os.path.join(SRC, 'data_buisness_suit',
                             'Jan-01-2026_Aug-10-2026_1071135865260092.csv')]
    for f in ig_files:
        d = pd.read_excel(f) if f.endswith('.xlsx') else pd.read_csv(f, encoding='utf-8-sig')
        d = d[d['Account username'] == 'kan_news']
        frames.append(pd.DataFrame({
            'dt': pd.to_datetime(d['Publish time'], format='mixed', errors='coerce'),
            'views': _num(d['Views']), 'title': d['Description'].map(clean_title),
            'platform': 'instagram'}))

    fb25 = pd.read_csv(os.path.join(PULLED, 'fb_2025_metrics.csv'), encoding='utf-8-sig')
    frames.append(pd.DataFrame({
        'dt': pd.to_datetime(fb25['date'], errors='coerce'),
        'views': _num(fb25['views']), 'title': '', 'platform': 'facebook'}))
    for f in ('Jan-01-2024_Dec-31-2024_1509169891251840.csv',
              'Jan-01-2026_Aug-11-2026_1369894801784288.csv'):
        e = pd.read_csv(os.path.join(SRC, f), encoding='utf-8-sig', low_memory=False)
        e = e[e['Page ID'].astype(str) == '100064467291406']
        frames.append(pd.DataFrame({
            'dt': pd.to_datetime(e['Publish time'], format='mixed', errors='coerce'),
            'views': _num(e['Views']), 'title': e['Description'].map(clean_title),
            'platform': 'facebook'}))
    for d, name in ((yt, 'youtube'), (tt, 'tiktok')):
        frames.append(pd.DataFrame({
            'dt': d['dt'], 'views': d['views'],
            'title': d['title'].map(clean_title), 'platform': name}))

    a = pd.concat(frames, ignore_index=True).dropna(subset=['dt', 'views'])
    return a[(a['dt'] >= VIEWS_FROM) & (a['dt'] <= CUTOFF)]


def month_headlines(yt, tt, platform=None):
    """לכל חודש — הפריט הגדול ביותר שפורסם בו, ברשת המבוקשת.

    **זה תיאור, לא הסבר.** הפריט הגדול בחודש אינו בהכרח מה שגרם לאנשים
    להירשם, ולכן הוא מוצג כ"התוכן הגדול באותו חודש" ולא כסיבה. הסינון לפי
    רשת הוא המינימום: לסמן קפיצה במנויי יוטיוב ולהצמיד לה פוסט מטיקטוק היה
    שגוי פעמיים.
    """
    a = all_items(yt, tt)
    a = a[a['title'].astype(str).str.len() > 12]
    a['m'] = a['dt'].dt.to_period('M').astype(str)

    def _top(frame):
        out = {}
        for m, g in frame.groupby('m'):
            r = g.nlargest(1, 'views').iloc[0]
            out[m] = {'title': str(r['title']), 'platform': str(r['platform'])}
        return out

    everywhere = _top(a)
    if not platform:
        return everywhere
    # לפייסבוק 2025 אין טקסט פוסטים (הבקפיל מה-API לא שומר אותו), ולכן
    # נופלים לפריט הגדול בכל הרשתות — אבל **מציינים מאיזו רשת הוא**, כדי
    # שלא ייראה כאילו הוא של הרשת שהעקומה מתארת.
    own = _top(a[a['platform'] == platform])
    return {m: own.get(m) or everywhere.get(m) for m in everywhere}


def measure_events(yt, tt, subs, fb_follows, events):
    """מודד חלונות שנקבעו ביד. **התאריכים אינם מתגלים מהנתונים — הם נקבעים.**

    הניסיון הקודם איתר חלונות אוטומטית (רצפים מעל פי 2 מהחציון) והוא טעה
    בדיוק במקום שבו זה חשוב: הוא פתח את מלחמת יוני 2025 ב-10.6 כי באותו יום
    היה קליפ ויראלי על בריחה משוטר, בזמן שהמבצע התחיל ב-13.6. אלגוריתם מוצא
    שיאים; רק אדם יודע מה האירוע. לכן החלונות מגיעים מ-`editorial.events`,
    והסקריפט רק מודד אותם.

    `candidate_windows()` נשאר ככלי גילוי — להצעת חלונות שטרם נבדקו — אבל
    שום דבר במצגת לא מסתמך עליו.
    """
    a = all_items(yt, tt)
    a['d'] = a['dt'].dt.date
    day = a.groupby('d')['views'].sum().sort_index()
    med = float(day.median())
    sub_gain = subs.set_index(subs['Date'].dt.date)['Subscribers'].diff()
    fb_gain = fb_follows.set_index(fb_follows['date'].dt.date)['follows']

    out = []
    for key, meta in (events or {}).items():
        if not meta.get('show', True):
            continue
        s = pd.to_datetime(meta.get('from', key)).date()
        e = pd.to_datetime(meta.get('to', key)).date()
        win = day[(day.index >= s) & (day.index <= e)]
        if not len(win):
            continue
        days = (e - s).days + 1
        g = a[(a['d'] >= s) & (a['d'] <= e)]
        named = g[g['title'].astype(str).str.len() > 12]
        tops = (named if len(named) else g).nlargest(3, 'views')
        peak = win.idxmax()
        out.append({
            'key': key, 'name': meta.get('name', ''),
            'short': meta.get('short') or meta.get('name', ''),
            'from': str(s), 'to': str(e), 'days': days,
            'views': int(win.sum()),
            'vs_typical': round(float(win.sum()) / days / med, 1),
            'peak_date': str(peak), 'peak_x': round(float(win.max()) / med, 1),
            'yt_subs': int(sub_gain[(sub_gain.index >= s) & (sub_gain.index <= e)].sum() or 0),
            'fb_follows': int(fb_gain[(fb_gain.index >= s) & (fb_gain.index <= e)].sum() or 0),
            'headlines': [str(r['title']) for _, r in tops.iterrows()],
        })
    out.sort(key=lambda x: -x['views'])
    return {'typical_day': int(med), 'windows': out}


def candidate_windows(yt, tt, floor=40_000_000):
    """כלי גילוי בלבד: רצפים חשודים לאירוע, להצגה בפלט הריצה.

    לא נכנס למצגת — ראו ההסבר ב-`measure_events`.
    """
    a = all_items(yt, tt)
    a['d'] = a['dt'].dt.date
    day = a.groupby('d')['views'].sum().sort_index()
    med = float(day.median())
    runs, cur = [], None
    for d in day[day >= 2 * med].index:
        if cur and (d - cur[1]).days <= 4:
            cur = (cur[0], d)
        else:
            if cur:
                runs.append(cur)
            cur = (d, d)
    if cur:
        runs.append(cur)
    out = []
    for s, e in runs:
        win = day[(day.index >= s) & (day.index <= e)]
        if win.sum() < floor:
            continue
        g = a[(a['d'] >= s) & (a['d'] <= e)]
        named = g[g['title'].astype(str).str.len() > 12]
        top = (named if len(named) else g).nlargest(1, 'views')
        out.append((int(win.sum()), str(s), str(e),
                    str(top.iloc[0]['title'])[:60] if len(top) else ''))
    return sorted(out, reverse=True)


def big_days(yt, tt, subs, fb_follows, n=6):
    """הימים שבהם התוכן עשה הכי הרבה — ומה קרה בהם.

    לא מביאים ציר זמן של אירועים מבחוץ: **הנתונים מוצאים את היום והתוכן נותן
    לו שם.** הכותרת של הפריט הגדול באותו יום היא התיאור המדויק ביותר של מה
    שקרה, והיא גם מה שבאמת פורסם — בלי פרשנות שלנו.

    לצד הצפיות נמדד גם מה האירוע עשה לקהל: מנויי יוטיוב שנוספו והצטרפויות
    לפייסבוק *באותו יום*.
    """
    a = all_items(yt, tt)
    a['day'] = a['dt'].dt.date
    per_day = a.groupby('day')['views'].sum()
    typical = float(per_day.median())

    sub_gain = subs.set_index(subs['Date'].dt.date)['Subscribers'].diff()
    fb_gain = fb_follows.set_index(fb_follows['date'].dt.date)['follows']

    out = []
    for day, total in per_day.nlargest(n).items():
        g = a[a['day'] == day]
        named = g[g['title'].astype(str).str.len() > 12]
        top = (named if len(named) else g).nlargest(1, 'views').iloc[0]
        out.append({
            'date': str(day),
            'views': int(total),
            'vs_typical': round(total / typical, 1) if typical else 0,
            'headline': str(top['title']),
            'top_platform': top['platform'],
            'top_views': int(top['views']),
            'yt_subs': int(sub_gain.get(day, 0) or 0),
            'fb_follows': int(fb_gain.get(day, 0) or 0),
        })
    return {'typical_day': int(typical), 'days': out}


def top_by_platform(yt, tt, n=5):
    """הפריטים הגדולים של כל רשת בתקופה.

    **רק פריטים שיש להם טקסט.** לפייסבוק 2025 אין טקסט פוסטים כלל — הנתון
    ההוא נמשך מה-Graph API וה-backfill לא שמר אותו — ושניים מהפוסטים
    הגדולים ביותר ברשת הם משם. פוסט בלי כותרת הוא שורה ריקה בשקף, ולכן הם
    מדולגים ומספרם נאמר בהערה, במקום להציג «(הטקסט לא נשמר)» כשורה.
    """
    a = all_items(yt, tt)
    a = a[a['views'].notna() & (a['views'] > 0)]
    out = {}
    for plat, g in a.groupby('platform'):
        named = g[g['title'].astype(str).str.len() > 12]
        top = named.nlargest(n, 'views')
        skipped = int((g.nlargest(n, 'views')['title'].astype(str).str.len()
                       <= 12).sum())
        out[plat] = {
            'items': [{'date': str(r['dt'].date()), 'views': int(r['views']),
                       'title': str(r['title'])} for _, r in top.iterrows()],
            'skipped_untitled': skipped,
        }
    return out


def top_content(yt, tt):
    """הפריט הגדול של כל שנה בכל רשת. מספטמבר 2024 בלבד."""
    out = defaultdict(list)

    ig_files = [os.path.join(SRC, 'Jan-01-2024_Dec-31-2024_4588672571364735.csv'),
                os.path.join(SRC, 'data_buisness_suit', '2025 חוץ מסטוריז.xlsx'),
                os.path.join(SRC, 'data_buisness_suit',
                             'Jan-01-2026_Aug-10-2026_1071135865260092.csv')]
    parts = []
    for f in ig_files:
        d = pd.read_excel(f) if f.endswith('.xlsx') else pd.read_csv(f, encoding='utf-8-sig')
        d = d[d['Account username'] == 'kan_news'].copy()
        d['dt'] = pd.to_datetime(d['Publish time'], format='mixed', errors='coerce')
        d['views'] = _num(d['Views'])
        d['title'] = d['Description'].map(clean_title)
        parts.append(d[['dt', 'views', 'title']])
    ig = pd.concat(parts, ignore_index=True)

    # 2025 מהבקפיל — אין בו טקסט הפוסט, ולכן הכותרת נשארת ריקה ונמסרת
    # לשכבת הניסוח; ה-permalink הוא מה שמאפשר למלא אותה בלי לנחש.
    fb25 = pd.read_csv(os.path.join(PULLED, 'fb_2025_metrics.csv'), encoding='utf-8-sig')
    fb25['dt'] = pd.to_datetime(fb25['date'], errors='coerce')
    fb25['views'] = _num(fb25['views'])
    fb25['title'] = ''
    fb25['link'] = fb25['permalink']

    fb_exports = []
    for f in ('Jan-01-2024_Dec-31-2024_1509169891251840.csv',
              'Jan-01-2026_Aug-11-2026_1369894801784288.csv'):
        e = pd.read_csv(os.path.join(SRC, f), encoding='utf-8-sig', low_memory=False)
        e = e[e['Page ID'].astype(str) == '100064467291406'].copy()
        e['dt'] = pd.to_datetime(e['Publish time'], format='mixed', errors='coerce')
        e['views'] = _num(e['Views'])
        # ב-2024 ה-Title לרוב ריק והטקסט יושב ב-Description
        e['title'] = (e['Title'].fillna('').astype(str).str.strip()
                      .where(lambda s: s.str.len() > 0,
                             e['Description'].fillna('').astype(str))
                      .map(clean_title))
        e['link'] = e['Permalink']
        fb_exports.append(e[['dt', 'views', 'title', 'link']])

    fb = pd.concat([fb25[['dt', 'views', 'title', 'link']]] + fb_exports,
                   ignore_index=True)
    for d in (ig, yt, tt):
        if 'link' not in d.columns:
            d['link'] = ''

    for name, d in (('instagram', ig), ('facebook', fb),
                    ('youtube', yt[['dt', 'views', 'title', 'link']]),
                    ('tiktok', tt[['dt', 'views', 'title', 'link']])):
        d = d[(d['dt'] >= VIEWS_FROM) & (d['dt'] <= CUTOFF) & d['views'].notna()]
        for y, g in d.groupby(d['dt'].dt.year):
            r = g.nlargest(1, 'views').iloc[0]
            out[str(y)].append({'platform': name, 'views': int(r['views']),
                                'title': clean_title(r['title']),
                                'link': str(r.get('link', '') or ''),
                                'date': str(r['dt'].date())})
    out = {y: sorted(v, key=lambda x: -x['views']) for y, v in sorted(out.items())}
    return out


def _tokens(s):
    """מילים משמעותיות בלבד — מילות קישור קצרות יוצרות התאמות שווא."""
    bad = {'של', 'עם', 'את', 'על', 'לא', 'זה', 'הוא', 'היא', 'כי', 'גם'}
    return {w.strip('.,:;"\'!?()״׳-') for w in str(s).split()
            if len(w) > 2 and w not in bad}


def cross_platform(top):
    """אותו סיפור שהגיע לראש בכמה רשתות באותה שנה.

    הכותרות לא זהות בין הרשתות (כל רשת נערכת בנפרד), ולכן ההשוואה היא לפי
    חפיפת מילים ולא לפי טקסט מדויק. 0.5 — מתחת לזה נדבקים סיפורים שרק חולקים
    נושא.
    """
    res = {}
    for y, items in top.items():
        best = None
        for i, a in enumerate(items):
            for b in items[i + 1:]:
                ta, tb = _tokens(a['title']), _tokens(b['title'])
                if not ta or not tb:
                    continue
                overlap = len(ta & tb) / min(len(ta), len(tb))
                if overlap >= 0.5:
                    grp = [a, b]
                    tot = sum(x['views'] for x in grp)
                    if not best or tot > best['views']:
                        best = {'title': max(grp, key=lambda x: len(x['title']))['title'],
                                'views': tot,
                                'platforms': [x['platform'] for x in grp]}
        if best:
            res[y] = best
    return res


EDITORIAL_SEED = {
    "cover_title": "הסושיאל של כאן חדשות",
    "cover_subtitle": "ינואר 2024 – יולי 2026",
    "slides": {},
    # אירועים חדשותיים. **התאריכים כאן קובעים** — הסקריפט רק מודד אותם.
    # שמות ותאריכי המבצעים אומתו מול מקורות חיצוניים, ולא נגזרו מהכותרות:
    # "עם כלביא" 13–24.6.2025 (מלחמת 12 הימים), "שאגת הארי" מ-28.2.2026.
    # להוסיף אירוע: מפתח כלשהו עם from/to/name. `show: false` מסתיר.
    "events": {
        "am_kelavia": {"name": "מבצע «עם כלביא» — מלחמת 12 הימים",
                       "short": "עם כלביא",
                       "from": "2025-06-13", "to": "2025-06-24", "show": True},
        "hostages_end": {"name": "שחרור החטופים וסיום מלחמת חרבות ברזל",
                         "short": "שחרור החטופים",
                         "from": "2025-10-09", "to": "2025-10-20", "show": True},
        "shaagat_haari": {"name": "מבצע «שאגת הארי»",
                          "short": "שאגת הארי",
                          "from": "2026-02-28", "to": "2026-03-31", "show": True},
        "bibas": {"name": "החזרת חללי משפחת ביבס ושחרור אברה מנגיסטו",
                  "short": "ביבס ומנגיסטו",
                  "from": "2025-02-20", "to": "2025-02-27", "show": True},
    },
    "_how_to_edit": ("כל טקסט כאן נכתב ביד ושורד הרצה חוזרת של הסקריפט. "
                     "המספרים מתעדכנים מהנתונים; המשפטים לא."),
}


def _merge_editorial(deck):
    """שכבת הניסוח לא נדרסת. זה הלקח מ-weekly_deck, ששטף אותה פעמיים."""
    prev = {}
    if os.path.exists(OUT):
        try:
            prev = json.load(open(OUT, encoding='utf-8')).get('editorial', {})
        except (ValueError, OSError):
            prev = {}
    ed = dict(EDITORIAL_SEED)
    ed.update(prev)
    deck['editorial'] = ed
    return deck


def main():
    deck = _merge_editorial(build())
    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(deck, f, ensure_ascii=False, indent=2)

    print("נכתב %s" % OUT)
    print("\nעוקבים: %s בסך הכל" % format(sum(FOLLOWERS.values()), ','))
    for p, n in sorted(FOLLOWERS.items(), key=lambda x: -x[1]):
        print("   %-10s %10s" % (p, format(n, ',')))
    print("\nצפיות לפי שנה (תוכן שפורסם באותה שנה, מצטבר עד היום):")
    for p in ('facebook', 'instagram', 'youtube', 'tiktok'):
        y = deck['platforms'][p].get('yearly', {})
        parts = ["%s %s" % (k, format(v['views'], ',')) for k, v in sorted(y.items())
                 if v.get('views')]
        print("   %-10s %s" % (p, ' | '.join(parts)))
    print("\nיוטיוב מנויים: %s → %s" % (
        format(deck['youtube_subscribers']['start'], ','),
        format(deck['youtube_subscribers']['end'], ',')))
    cands = deck.pop('_candidates', [])
    if cands:
        print("\nחלונות מועמדים שאותרו בנתונים (לא נכנסים למצגת — לבדיקה בלבד):")
        for tot, s0, e0, ttl in cands[:8]:
            mark = '' if any(w['from'] <= e0 and w['to'] >= s0
                             for w in deck['events']['windows']) else '  ← לא מכוסה'
            print("   %s → %s  %14s%s" % (s0, e0, format(tot, ','), mark))
            print("      %s" % ttl)
    print("\nאירועים שנמדדו:")
    for w in deck['events']['windows']:
        print("   %-46s %s → %s  %14s  שיא x%.1f ב-%s"
              % (w['name'], w['from'], w['to'], format(w['views'], ','),
                 w['peak_x'], w['peak_date']))
    print()
    print("אינסטגרם, עוקבים ל-1000 צפיות לפי פורמט:")
    for fmt, v in sorted(deck['instagram_follows_by_format'].items(),
                         key=lambda x: -x[1]['per_1k_views']):
        print("   %-8s %5.2f  (%s פוסטים)" % (fmt, v['per_1k_views'], format(v['posts'], ',')))


if __name__ == '__main__':
    sys.exit(main())
