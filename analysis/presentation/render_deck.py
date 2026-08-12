#!/usr/bin/env python3
"""מרנדר את `deck_content.json` ל-`deck.html` — 16:9, RTL, עצמאי.

    python analysis/presentation/build_deck.py     # מספרים
    python analysis/presentation/render_deck.py    # שקפים

הגרפים נבנים כ-SVG בקוד, בלי ספריות — הדק נפתח בדפדפן בלי רשת, ואפשר להדפיס
אותו ל-PDF (Ctrl+P, רוחב מלא, ללא שוליים).

## החלטות הצבע, ולמה

הפלטות אומתו מול `scripts/validate_palette.js` ולא נבחרו בעין:

* **צבעי המותג של הרשתות נכשלים כסדרות בגרף אחד** — טיקטוק מול יוטיוב ΔE 3.2
  (דויטראנופיה) ואינסטגרם מול יוטיוב 7.8 בראייה תקינה. לכן אין כאן שום גרף
  שבו שש הרשתות הן סדרות צבעוניות באותו מרחב; ההשוואה לאורך זמן היא
  **סדרות קטנות** — גרף לכל רשת, סדרה אחת בכל אחד, זהות מהכותרת והאייקון.
* צבעי המותג כן משמשים בדירוג המאונך, שם לכל בר יש אייקון, שם וערך צמודים —
  זהות שאינה נשענת על צבע בלבד.
* **תמהיל הפורמטים** משתמש ב-`#1D4ED8 / #B45309 / #15803D`, שעובר את כל
  הבדיקות עם אזהרת CVD אחת (6.8) שהתווית הישירה על כל מקטע מכשירה.
"""

import html
import json
import os
import sys

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'deck_content.json')
OUT = os.path.join(HERE, 'deck.html')
FONTS = '../../weekly_deck/design/fonts'

ACCENT = '#FF3300'
INK = '#111111'
MUTED = '#808080'
GRID = '#e0e0e0'
SURFACE = '#f4f4f4'

BRAND = {
    'facebook': '#1877F2', 'instagram': '#E4405F', 'youtube': '#E11900',
    'tiktok': '#B45309', 'twitter': '#2F3542', 'whatsapp': '#128C7E',
}
HEB = {
    'facebook': 'פייסבוק', 'instagram': 'אינסטגרם', 'youtube': 'יוטיוב',
    'tiktok': 'טיקטוק', 'twitter': 'X', 'whatsapp': 'ערוץ וואטסאפ',
}
# פלטת התמהיל — אומתה; האזהרה שנותרה מחייבת תווית ישירה על כל מקטע.
# הצבע נקשר ל**שם** הקטגוריה ולא למיקום ברשימה: "תמונה" חייבת להיות אותו צבע
# בפייסבוק ובאינסטגרם, אחרת הצבע עוקב אחרי הסדר במקום אחרי הדבר עצמו.
MIX_BY_NAME = {
    'וידאו': '#1D4ED8', 'רילס': '#1D4ED8', 'שורטס': '#1D4ED8',
    'תמונה': '#B45309', 'סרטון רגיל': '#B45309', 'סרטונים': '#B45309',
    'קרוסלה': '#15803D', 'שידור חי': '#15803D',
    'סטטוס': '#6B7280', 'לינק': '#9CA3AF',
}
MIX = ['#1D4ED8', '#B45309', '#15803D', '#6B7280']


def mix_color(name, i=0):
    return MIX_BY_NAME.get(name, MIX[i % len(MIX)])


def num(s, cls='mono'):
    """מספרים ב-RTL: bidi מעיף את הסימן לצד הלא נכון — `+82,762` מוצג
    `82,762+`. בידוד LTR מפורש הוא התיקון היחיד שמחזיק."""
    return '<span class="%s" dir="ltr">%s</span>' % (cls, esc(s))


def signed(n):
    return num('%s%s' % ('+' if n >= 0 else '−', format(abs(int(n)), ',')))

ICONS = {
    'youtube': '<rect x="1" y="4.5" width="22" height="15" rx="4.5" fill="#FF0000"/>'
               '<path d="M10 8.5l6 3.5-6 3.5z" fill="#fff"/>',
    'facebook': '<rect width="24" height="24" rx="6" fill="#1877F2"/><path d="M15.5 20v-6.4h2.1'
                'l.32-2.5h-2.42V9.5c0-.72.2-1.2 1.24-1.2h1.32V6.06c-.64-.08-1.36-.12-2.06-.12'
                '-2.02 0-3.4 1.24-3.4 3.5v1.96H10.1v2.5h2.14V20z" fill="#fff"/>',
    'instagram': '<rect x="3" y="3" width="18" height="18" rx="5.4" fill="none" stroke="#E4405F"'
                 ' stroke-width="1.9"/><circle cx="12" cy="12" r="4" fill="none" stroke="#E4405F"'
                 ' stroke-width="1.9"/><circle cx="17.4" cy="6.6" r="1.1" fill="#E4405F"/>',
    'tiktok': '<rect width="24" height="24" rx="6" fill="#111"/><path d="M14 4c.3 1.9 1.5 3.1 3.4'
              ' 3.3v2.4c-1.1 0-2.2-.3-3.1-.9v4.7c0 2.6-2 4.5-4.5 4.5S5.3 20.1 5.3 17.6c0-2.3 1.7'
              '-4.2 4-4.4v2.5c-.9.2-1.6 1-1.6 1.9 0 1.1.9 2 2 2s2-.9 2-2V4z" fill="#FBBF24"/>',
    'twitter': '<rect width="24" height="24" rx="6" fill="#000"/><path d="M6.3 6h2.7l3.1 4.2L15.6'
               ' 6h2.1l-4.3 5.7L18 18h-2.7l-3.3-4.5L8.4 18H6.3l4.5-6z" fill="#fff"/>',
    'whatsapp': '<rect width="24" height="24" rx="6" fill="#25D366"/><path d="M12 5.5c-3.6 0-6.5'
                ' 2.9-6.5 6.5 0 1.2.3 2.3.9 3.2L5.5 18.5l3.4-.9c.9.5 1.9.8 3.1.8 3.6 0 6.5-2.9'
                ' 6.5-6.5S15.6 5.5 12 5.5zm3.7 9.1c-.2.4-.9.8-1.2.8-.3 0-.7.1-2.4-.6-2-.9-3.3'
                '-3-3.4-3.1-.1-.1-.7-.9-.7-1.8s.4-1.2.6-1.4c.2-.2.4-.2.5-.2h.4c.1 0 .3 0 .5.4'
                'l.6 1.5c.1.1 0 .3 0 .4l-.2.3-.3.3c-.1.1-.2.2-.1.4.1.2.5.9 1.1 1.4.8.7 1.4.9'
                ' 1.6 1 .2.1.3.1.4-.1l.6-.7c.1-.2.3-.1.4-.1l1.4.7c.2.1.4.2.4.3 0 .1 0 .5-.2.9z"'
                ' fill="#fff"/>',
}


def esc(s):
    return html.escape(str(s or ''))


def fmt(n):
    return format(int(n), ',')


def short(n):
    n = float(n or 0)
    if n >= 1e9:
        return '%.2fB' % (n / 1e9)
    if n >= 1e6:
        return '%.1fM' % (n / 1e6)
    if n >= 1e3:
        return '%.0fK' % (n / 1e3)
    return '%d' % n


def icon(p, size=44):
    return ('<svg width="%d" height="%d" viewBox="0 0 24 24">%s</svg>'
            % (size, size, ICONS.get(p, '')))


# ---------- גרפים ----------

def sparkline(points, color, h=250, key='views', zero_base=True, marks=None):
    """סדרה אחת, נמתחת לרוחב ההורה. אין מקרא — הכותרת מעל מזהה אותה.

    ה-SVG נשלט ב-viewBox ו-`width:100%%` ולא ברוחב קבוע: רוחב קבוע בתוך גריד
    השאיר חצי כרטיס ריק, וב-1/3 עמודה גם גלש מהשקף.
    """
    if not points:
        return ''
    W = 1000                                   # קואורדינטות; הרוחב בפועל מה-CSS
    vals = [p[key] for p in points]
    hi = max(vals) or 1
    # סדרה שלא מתקרבת לאפס (מנויים) נראית כקו ישר כשהציר מתחיל באפס. קו —
    # להבדיל מבר — מותר לו בסיס שאינו אפס, בתנאי ששני קצות הציר מסומנים,
    # וזה מה שמוחזר למטה.
    lo = 0 if zero_base else min(vals) - (hi - min(vals)) * .15
    span = (hi - lo) or 1
    pad_t, pad_b, pad_l = 18, 30, 62           # מקום לתוויות הסקאלה משמאל
    inner_h = h - pad_t - pad_b
    inner_w = W - pad_l
    step = inner_w / max(len(vals) - 1, 1)
    pts = [(pad_l + i * step, pad_t + inner_h - ((v - lo) / span) * inner_h)
           for i, v in enumerate(vals)]
    base = pad_t + inner_h
    out = ['<svg viewBox="0 0 %d %d" preserveAspectRatio="none" class="spark" '
           'style="height:%dpx" role="img">' % (W, h, h)]
    # רשת אופקית שקטה: שלושה קווים, לא יותר
    for frac in (0, .5, 1):
        y = pad_t + inner_h * frac
        out.append('<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="%s" '
                   'stroke-width="1"/>' % (pad_l, y, W, y, GRID))
    out.append('<path d="M%.1f,%.1f %s L%.1f,%.1f Z" fill="%s" opacity=".12"/>'
               % (pts[0][0], base, ' '.join('L%.1f,%.1f' % xy for xy in pts),
                  pts[-1][0], base, color))
    out.append('<polyline points="%s" fill="none" stroke="%s" stroke-width="2.5" '
               'stroke-linejoin="round" vector-effect="non-scaling-stroke"/>'
               % (' '.join('%.1f,%.1f' % xy for xy in pts), color))
    # סימוני אירועים: קו אנכי + נקודה על החודש שבו הייתה הקפיצה
    idx = {p['month']: i for i, p in enumerate(points)}
    flags = []
    for mk in (marks or []):
        i = idx.get(mk['month'])
        if i is None:
            continue
        mx, my = pts[i]
        out.append('<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="%s" '
                   'stroke-width="1.5" stroke-dasharray="4 4" opacity=".55"/>'
                   % (mx, pad_t, mx, base, ACCENT))
        out.append('<circle cx="%.1f" cy="%.1f" r="6" fill="%s" stroke="#fff" '
                   'stroke-width="2.5"/>' % (mx, my, ACCENT))
        flags.append((mx / W * 100, mk))

    x, y = pts[-1]
    out.append('<circle cx="%.1f" cy="%.1f" r="5" fill="%s" stroke="#fff" '
               'stroke-width="2"/>' % (x, y, color))
    out.append('</svg>')
    # התוויות מחוץ ל-SVG: preserveAspectRatio="none" היה מותח אותן לרוחב
    legend = ''
    if flags:
        legend = ('<div class="marks"><div class="mkh">החודשים הגדולים — '
                  'ולצדם התוכן הגדול באותו חודש</div>%s</div>') % ''.join(
            '<div class="mk"><span class="mkn">%s</span>'
            '<span class="mkm">%s</span><span class="mkt">%s%s</span></div>'
            % (num('+%s' % fmt(mk['gain'])), esc(mk['month']),
               icon(mk['headline_platform'], 18) if mk.get('headline_platform') else '',
               esc(mk.get('headline', '')))
            for _, mk in sorted(flags, key=lambda f: -f[1]['gain']))
    return ('<div class="chart"><div class="cy"><span>%s</span><span>%s</span></div>%s'
            '<div class="cx"><span>%s</span><span>%s</span></div></div>%s'
            % (short(hi), short(lo) if lo else '0', ''.join(out),
               esc(points[0]['month']), esc(points[-1]['month']), legend))


def bar_row(label_html, value, hi, color, right_text, sub='', missing=False):
    """`missing` = אין נתון. מצייר מסלול ריק ולא בר מינימלי — בר של 2%
    על פלטפורמה בלי נתונים נקרא כ«מעט צפיות» במקום «לא נמדד»."""
    if missing:
        fill = '<div class="bnone">אין נתון</div>'
    else:
        pct = max(1.2, (value / hi * 100) if hi else 0)
        fill = '<div class="bfill" style="width:%.1f%%;background:%s"></div>' % (pct, color)
    return ('<div class="brow"><div class="bl">%s</div>'
            '<div class="btrack">%s</div><div class="bv">%s</div><div class="bs">%s</div>'
            '</div>' % (label_html, fill, right_text, sub))


def stacked(series, h=48):
    """מקטעים עם רווח 2px ותווית ישירה על כל אחד — האזהרה בפלטה מחייבת את זה."""
    total = sum(v for _, v, _ in series) or 1
    out = ['<div class="stack" style="height:%dpx">' % h]
    for name, v, c in series:
        pct = v / total * 100
        out.append('<div class="seg" style="width:%.4f%%;background:%s" title="%s %s">%s</div>'
                   % (pct, c, esc(name), fmt(v),
                      '<span>%s %d%%</span>' % (esc(name), round(pct)) if pct >= 9 else ''))
    out.append('</div>')
    return ''.join(out)


# ---------- שקפים ----------

def slide(label, notes, body, bg=SURFACE):
    return ('<section data-label="%s" data-notes="%s" dir="rtl" style="background:%s">%s</section>'
            % (esc(label), esc(notes), bg, body))


def head(title, kicker='', right=''):
    return ('<div class="shead"><div class="stitle">'
            '<div class="rule"></div><div>%s<h2>%s</h2></div></div>'
            '<div class="sright">%s</div></div>'
            % ('<div class="kicker">%s</div>' % esc(kicker) if kicker else '',
               esc(title), right))


def s_cover(d):
    tot = sum(p.get('yearly', {}).get(y, {}).get('views', 0)
              for p in d['platforms'].values() if isinstance(p.get('yearly'), dict)
              for y in ('2024', '2025', '2026'))
    ed = d['editorial']
    strip = ''.join(
        '<div class="cp">%s<div><div class="cpn">%s</div><div class="cpf">%s</div></div></div>'
        % (icon(p, 40), esc(HEB[p]), num(fmt(n)))
        for p, n in sorted(d['followers'].items(), key=lambda x: -x[1]))
    body = (
        '<div class="cover">'
        '<div class="cbar"></div>'
        '<div>'
        '<div class="kicker">סיכום פעילות</div>'
        '<h1>%s</h1>'
        '<div class="csub">%s</div>'
        '</div>'
        '<div class="cstrip">%s</div>'
        '<div class="cfoot">'
        '<div><div class="clab">סך הצפיות בכל הרשתות</div>'
        '<div class="cbig">%s<span>%s</span></div></div>'
        '<div class="cright"><div class="clab">קהל עוקבים</div>'
        '<div class="cnum">%s</div></div>'
        '</div></div>'
        % (esc(ed.get('cover_title')), esc(ed.get('cover_subtitle')), strip,
           short(tot)[:-1], short(tot)[-1], num(fmt(sum(d['followers'].values())))))
    return slide('שער', 'המספר הגדול: כמה צפיות הפיקו כל הנכסים יחד, וכמה עוקבים יש.',
                 body, 'radial-gradient(120% 120% at 78% 12%,#fff 0%,#f7f7f7 55%,#ececec 100%)')


def s_assets(d):
    """דירוג הנכסים. צבעי מותג מותרים כאן — לכל בר אייקון, שם וערך צמודים."""
    rows = []
    for p in d['platforms']:
        blk = d['platforms'][p]
        # ליוטיוב הצפיות בתקופה (Studio) גוברות על הצבירה לפי שנת פרסום,
        # אחרת אותו שקף מציג שני מספרים שונים לאותה רשת
        y = (blk.get('yearly_period') or {}).get('2026') or blk.get('yearly', {}).get('2026', {})
        v = y.get('views') or blk.get('views') or 0
        rows.append((p, v, d['followers'].get(p, 0)))
    rows.sort(key=lambda r: -r[1])
    hi = max(r[1] for r in rows) or 1
    # גידול שנתי בחלון זהה (ינואר–יולי). 2024 חסרה כי מדדי מטא מתחילים
    # בספטמבר — להציג לה אחוז היה להשוות שבעה חודשים ריקים לשנה מלאה.
    ytd = d.get('views_by_year_ytd') or {}
    years = sorted(ytd)
    chips = ''
    for i, y in enumerate(years):
        delta = ''
        if i and ytd[years[i - 1]]:
            pct = (ytd[y] / ytd[years[i - 1]] - 1) * 100
            delta = ('<span class="%s">%s</span>'
                     % ('up' if pct >= 0 else 'down', num('%+.0f%%' % pct)))
        chips += ('<div class="ychip"><div class="ycy">%s</div>'
                  '<div class="ycv">%s</div>%s</div>' % (y, num(short(ytd[y])), delta))
    body = [head('הנכסים הדיגיטליים', 'שש רשתות · %s עוקבים' % fmt(sum(d['followers'].values())),
                 # X מוחרג מהסכום: שישה שבועות של מדידה בתוך סך שנתי היו
                 # מנפחים את 2026 מול 2025 בלי שקרה כלום בפועל
                 '<div class="rnum">%s</div><div class="rlab">צפיות ב-2026 עד יולי'
                 '<br><span class="tiny">בארבע הרשתות עם היסטוריה מלאה</span></div>'
                 % short(sum(v for p, v, _ in rows if p not in ('twitter', 'whatsapp')))),
            ('<div class="ychips"><div class="ycl">צפיות בכל הרשתות, ינואר–יולי '
             'בכל שנה</div>%s</div>' % chips) if chips else '']
    body.append('<div class="bhead"><div>רשת</div><div>נתח מהצפיות ב-2026</div>'
                '<div>צפיות</div><div>עוקבים</div></div>')
    body.append('<div class="blist">')
    # חלקיות הכיסוי חייבת לשבת ליד הבר. בר קצר של X נקרא כ«ביצועים חלשים»
    # בזמן שהוא בעצם «נמדדו שבעה שבועות».
    partial = {'twitter': 'נמדד מ-21.6.2026 בלבד'}
    for p, v, f in rows:
        lab = ('<div class="pl">%s<div><div class="pn">%s</div>%s</div></div>'
               % (icon(p), esc(HEB[p]),
                  '<div class="pw">%s</div>' % esc(partial[p]) if p in partial else ''))
        body.append(bar_row(lab, v, hi, BRAND[p], short(v) if v else '—',
                            num(fmt(f)) if f else '—', missing=not v))
    body.append('</div>')
    body.append('<div class="foot">ערוץ הוואטסאפ אינו חושף צפיות כלל, ו-X נמדד רק '
                'משבעה שבועות — שניהם אינם ברי-השוואה כאן. ראו שקף המדידה.</div>')
    return slide('הנכסים', 'מבט עילי. אורך הבר = נתח מהצפיות ב-2026.', ''.join(body))


def s_growth(d):
    """סדרות קטנות. שש רשתות בגרף אחד היו נכשלות בהפרדת צבע — ראו התיעוד."""
    cards = []
    for p in ('facebook', 'instagram', 'youtube', 'tiktok'):
        pts = d['platforms'][p].get('monthly_views', [])
        if not pts:
            continue
        tot = sum(x['views'] for x in pts)
        cards.append(
            '<div class="card"><div class="ch">%s<div><div class="pn">%s</div>'
            '<div class="cs">%s צפיות · %s עד %s</div></div></div>%s</div>'
            % (icon(p, 34), esc(HEB[p]), short(tot),
               esc(pts[0]['month']), esc(pts[-1]['month']),
               sparkline(pts, BRAND[p], h=210)))
    body = [head('הצמיחה', 'צפיות חודשיות, ספטמבר 2024 – יולי 2026', ''),
            '<div class="grid2">%s</div>' % ''.join(cards),
            '<div class="foot">כל גרף בקנה מידה משלו — ההשוואה היא של <b>מגמה</b>, '
            'לא של גובה; הערך המרבי מסומן על ציר ה-Y. הערכים המוחלטים בשקף הקודם. '
            'טיקטוק מתחיל בפברואר 2025, מגבלת הספק.</div>']
    return slide('הצמיחה', 'ארבע רשתות, כל אחת בקנה המידה שלה. משווים מגמה.', ''.join(body))


def s_audience(d):
    """כמה גדלנו, ומתי — לא כמה נוספו אתמול.

    שתי העקומות מצטברות בכוונה: השאלה היא «בכמה גדל הקהל בשנתיים», והקצב
    היומי עונה על שאלה אחרת. הקפיצות מסומנות על העקומה עצמה, וכל אחת נושאת
    את הכותרת של הפריט הגדול באותו חודש.
    """
    ys = d['youtube_subscribers']
    fb = d['facebook_follows']
    pts = [{'month': s['month'], 'views': s['subs']} for s in ys['series']]
    fb_pts = [{'month': c['month'], 'views': c['total']} for c in fb['cumulative']]
    gross = fb['total_gross']
    net = int(gross * fb['net_ratio'])

    body = [head('בכמה גדלנו', 'ינואר 2024 עד יולי 2026',
                 '<div class="rnum">%s</div><div class="rlab">קהל בכל הרשתות היום</div>'
                 % fmt(sum(d['followers'].values()))),
            '<div class="grid2">'
            '<div class="card"><div class="ch">%s<div><div class="pn">מנויי יוטיוב</div>'
            '<div class="cs">%s ← %s</div></div>'
            '<div class="delta">%s<span>%s</span></div></div>%s</div>'
            % (icon('youtube', 34), num(fmt(ys['start'])), num(fmt(ys['end'])),
               signed(ys['end'] - ys['start']),
               num('%.0f%%' % ((ys['end'] / ys['start'] - 1) * 100)),
               sparkline(pts, BRAND['youtube'], h=210, zero_base=False,
                         marks=ys.get('top_months'))),
            '<div class="card"><div class="ch">%s<div><div class="pn">הצטרפויות לפייסבוק</div>'
            '<div class="cs">מצטבר מינואר 2024</div></div>'
            '<div class="delta">%s<span>ברוטו</span></div></div>%s</div>'
            % (icon('facebook', 34), num('+%s' % fmt(gross)),
               sparkline(fb_pts, BRAND['facebook'], h=210,
                         marks=fb.get('top_months'))),
            '</div>',
            '<div class="panel"><div class="ptitle">מה המספרים האלה אומרים</div>'
            '<div class="grid3">'
            '<div class="fact"><b>%s</b> מנויים נוספו ליוטיוב, גידול של <b>%s</b>. '
            'זה הנתון המדויק היחיד שיש לנו לאורך כל התקופה.</div>'
            '<div class="fact"><b>%s</b> הצטרפו לעמוד הפייסבוק — אבל זה מספר '
            '<b>ברוטו</b>. לפי היחס שנמדד על שמונה חודשי חפיפה, הגידול נטו הוא '
            'כ-<b>%s</b>: אחד מכל ארבעה מצטרפים עוזב.</div>'
            '<div class="fact">לאינסטגרם, טיקטוק ו-X <b>אין היסטוריית עוקבים</b> — '
            'מטא חושפת שנה אחורה בלבד והשאר לא חושפים כלל. יש רק המספר של היום.</div>'
            '</div></div>'
            % (signed(ys['end'] - ys['start']),
               num('%.0f%%' % ((ys['end'] / ys['start'] - 1) * 100)),
               num(fmt(gross)), num('~%s' % fmt(net)))]
    # מה שמגייס: עוקבים ל-1000 צפיות
    return slide('בכמה גדלנו', 'הצמיחה המצטברת, והחודשים שבהם היא קפצה.',
                 ''.join(body))


def s_ig_conversion(d):
    """מה ממיר צופה לעוקב. שייך לשקף אינסטגרם, לא לשקף הצמיחה."""
    f = d.get('instagram_follows_by_format') or {}
    if not f:
        return ''
    order = sorted(f.items(), key=lambda x: -x[1]['per_1k_views'])
    hi = order[0][1]['per_1k_views'] or 1
    bars = ''.join(bar_row('<div class="pl"><div class="pn">%s</div></div>' % esc(k),
                           v['per_1k_views'], hi, mix_color(k, i),
                           num('%.2f' % v['per_1k_views']),
                           '%s פוסטים' % num(fmt(v['posts'])))
                   for i, (k, v) in enumerate(order))
    ratio = order[0][1]['per_1k_views'] / max(order[-1][1]['per_1k_views'], .01)
    return ('<div class="panel"><div class="ptitle">מה ממיר צופה לעוקב — '
            'עוקבים לכל 1,000 צפיות</div>%s'
            '<div class="foot">רילס ממירים פי %.0f מקרוסלה. הפער עקבי בשלוש '
            'השנים בנפרד, ולא נובע משנה חריגה אחת.</div></div>' % (bars, ratio))


def s_output(d):
    """נפח ותמהיל — הסיפור של המעבר לווידאו."""
    cards = []
    for p in ('facebook', 'instagram', 'youtube'):
        mix = d['platforms'][p].get('format_mix', {})
        if not mix:
            continue
        rows = []
        for y in ('2024', '2025', '2026'):
            seg = [(k, v.get(y, 0), mix_color(k, i))
                   for i, (k, v) in enumerate(sorted(mix.items()))]
            seg = [s for s in seg if s[1]]
            if not seg:
                continue
            seg.sort(key=lambda s: -s[1])
            rows.append('<div class="mixrow"><div class="my">%s</div>%s'
                        '<div class="mt">%s</div></div>'
                        % (y, stacked(seg), num(fmt(sum(s[1] for s in seg)))))
        # השוואה הוגנת: ינואר–יולי בכל שנה, ולא שנה שלמה מול שבעה חודשים
        ytd = d['platforms'][p].get('posts_ytd') or {}
        note = ''
        if ytd.get('2024') and ytd.get('2026'):
            a, b = ytd['2024'], ytd['2026']
            pct = (b / a - 1) * 100
            note = ('<div class="mixnote"><b>ינואר–יולי:</b> %s ← %s פריטים '
                    '<span class="%s">%s</span></div>'
                    % (num(fmt(a)), num(fmt(b)),
                       'up' if pct >= 0 else 'down',
                       num('%+.0f%%' % pct)))
        cards.append('<div class="card"><div class="ch">%s<div class="pn">%s</div></div>'
                     '<div class="mixlist">%s</div>%s</div>'
                     % (icon(p, 34), esc(HEB[p]), ''.join(rows), note))
    body = [head('מה פרסמנו', 'ינואר–יולי בכל שנה · נפח ותמהיל הפורמטים', ''),
            '<div class="grid3">%s</div>' % ''.join(cards),
            '<div class="foot">בפייסבוק <b>וידאו</b> מאחד רילס וּוידאו: מטא מפרסמת כל וידאו '
            'כרילס מאמצע 2025, וההפרדה בנתונים הגולמיים היא שינוי שלה ולא שינוי בעמוד. '
            'כל השנים נמדדות באותו חלון — ינואר עד יולי.</div>']
    return slide('מה פרסמנו', 'הנפח והתמהיל בשלוש הרשתות שיש להן פילוח פורמטים.',
                 ''.join(body))


def s_youtube_types(d):
    """הפילוח שרק יוטיוב מאפשר: איפה הצפיות ואיפה זמן הצפייה — ואלה לא אותו מקום."""
    b = d['platforms']['youtube']
    types = b.get('by_type') or {}
    if not types:
        return ''
    hi_v = max(t['views'] for t in types.values())
    hi_w = max(t['watch_hours'] for t in types.values())
    rows = ''
    for i, (name, t) in enumerate(sorted(types.items(), key=lambda x: -x[1]['views'])):
        c = mix_color(name, i)
        rows += (
            '<div class="ytr"><div class="ytn">%s<span>%s ממוצע · %.0f%% נצפו</span></div>'
            '<div class="ytbar"><div style="width:%.1f%%;background:%s"></div></div>'
            '<div class="ytv">%s</div>'
            '<div class="ytbar"><div style="width:%.1f%%;background:%s"></div></div>'
            '<div class="ytv">%s</div></div>'
            % (esc(name), esc(t['avg_duration'].lstrip('0:') or t['avg_duration']),
               t['pct_viewed'], t['views'] / hi_v * 100, c, num(short(t['views'])),
               t['watch_hours'] / hi_w * 100, c, num(short(t['watch_hours']))))
    return ('<div class="panel"><div class="ptitle">איפה הצפיות, ואיפה זמן הצפייה</div>'
            '<div class="ythead"><div></div><div>צפיות</div><div></div>'
            '<div>שעות צפייה</div><div></div></div>%s'
            '<div class="foot">שורטס מביאים <b>34%% מהצפיות</b> אבל רק <b>4%% משעות '
            'הצפייה</b>. שידור חי הוא ההפך: 1%% מהצפיות ו-7%% מהזמן. שתי מטרות שונות, '
            'ומדידה לפי צפיות בלבד מסתירה את זה.</div></div>' % rows)


def s_platform(d, p, extra_html=''):
    b = d['platforms'][p]
    y = dict(b.get('yearly', {}))
    # ליוטיוב עדיפות לצפיות בתקופה מ-Studio על פני הצבירה לפי שנת פרסום
    for k, v in (b.get('yearly_period') or {}).items():
        y.setdefault(k, {}).update({'views': v['views']})
    years = [k for k in ('2024', '2025', '2026') if k in y]
    cells = []
    for k in years:
        v = y[k]
        # ספירת הפריטים שהצפיות באמת מתייחסות אליהם, לא של השנה כולה
        n = v.get('posts_in_views_window') or v.get('posts', 0)
        win = v.get('views_window')
        tag = ''
        if k == '2026':
            tag = ' <span>עד 31.7</span>'
        elif win and not win.endswith('-01'):
            tag = ' <span>מספטמבר</span>'
        cells.append('<div class="yc"><div class="yy">%s%s</div>'
                     '<div class="yv">%s</div><div class="yl">צפיות</div>'
                     '<div class="ys">%s פריטים</div></div>'
                     % (k, tag, num(short(v.get('views', 0))), num(fmt(n))))
    pts = b.get('monthly_views', [])
    note = b.get('coverage_note', '')
    # כשיש פאנל ייעודי (יוטיוב), שורת ה-KPI מיותרת והשקף גולש בלעדיה
    extras = []
    if not extra_html:
        tot_h = b.get('watch_hours') or sum(v.get('watch_hours', 0) for v in y.values())
        if tot_h:
            extras.append('<div class="kpi"><div class="kv">%s</div>'
                          '<div class="kl">שעות צפייה</div></div>' % num(fmt(tot_h)))
        for lab, key in (('לייקים', 'likes'), ('תגובות', 'comments'), ('שיתופים', 'shares')):
            tot = sum(v.get(key, 0) for v in y.values())
            if tot:
                extras.append('<div class="kpi"><div class="kv">%s</div>'
                              '<div class="kl">%s</div></div>' % (num(short(tot)), lab))
    total_v = b.get('views_total') or sum(x['views'] for x in pts)
    span = 'צפיות 2024–2026' if b.get('views_total') else 'צפיות מספטמבר 2024'
    body = [head(HEB[p], 'עוקבים: %s' % fmt(d['followers'].get(p, 0)),
                 ('<div class="rnum">%s</div><div class="rlab">%s</div>'
                  % (short(total_v), span)) if pts else ''),
            '<div class="yrow" style="grid-template-columns:repeat(%d,1fr)">%s</div>'
            % (len(cells), ''.join(cells)),
            ('<div class="panel%s">%s</div>'
             % ('' if extra_html else ' grow',
                sparkline(pts, BRAND[p], h=200 if extra_html else 300))) if pts else '',
            ('<div class="kpirow" style="grid-template-columns:repeat(%d,1fr)">%s</div>'
             % (len(extras), ''.join(extras))) if extras else '',
            extra_html,
            ('<div class="foot">%s</div>' % esc(note)) if note else '']
    return slide(HEB[p], 'עומק לרשת %s.' % HEB[p], ''.join(body))


def s_thin(d):
    """טוויטר וּוואטסאפ על שקף אחד — לשניהם אין היסטוריה, ומתיחה של זה תשקר."""
    tw, wa = d['platforms']['twitter'], d['platforms']['whatsapp']
    body = [head('X וערוץ הוואטסאפ', 'שתי הרשתות שאין להן היסטוריה', ''),
            '<div class="grid2">'
            '<div class="card"><div class="ch">%s<div><div class="pn">X</div>'
            '<div class="cs">%s עוקבים</div></div></div>'
            '<div class="bignum mono">%s</div>'
            '<div class="note">%s ציוצים · %s עד %s</div>'
            '<div class="note warn">%s</div></div>'
            % (icon('twitter', 34), fmt(d['followers']['twitter']),
               short(tw.get('views', 0)), fmt(tw.get('posts', 0)),
               esc(tw.get('from')), esc(tw.get('to')), esc(tw.get('coverage_note'))),
            '<div class="card"><div class="ch">%s<div><div class="pn">ערוץ וואטסאפ</div>'
            '<div class="cs">הנכס הצעיר</div></div></div>'
            '<div class="bignum mono">%s</div><div class="note">עוקבים</div>'
            '<div class="note warn">%s</div></div>'
            % (icon('whatsapp', 34), fmt(d['followers']['whatsapp']),
               esc(wa.get('coverage_note'))),
            '</div>']
    # לשתי הרשתות האלה אין די נתונים למלא שקף. במקום לנפח אותן, נותנים להן
    # הקשר: כמה גדול הקהל שלהן ביחס לשאר. האפור מרמז שהשורות האחרות הן
    # רקע להשוואה ולא הנושא.
    fol = sorted(d['followers'].items(), key=lambda x: -x[1])
    hi = fol[0][1]
    bars = ''.join(
        bar_row('<div class="pl">%s<div class="pn">%s</div></div>'
                % (icon(p, 34), esc(HEB[p])),
                n, hi, BRAND[p] if p in ('twitter', 'whatsapp') else '#dcdcdc',
                num(fmt(n)), 'ללא היסטוריה' if p in ('twitter', 'whatsapp') else '')
        for p, n in fol)
    # המחרוזת הזו כן עוברת עיצוב-% (bars) — ולכן אחוז ספרותי נכתב כפול
    body.append('<div class="panel grow"><div class="ptitle">גודל הקהל בהשוואה</div>'
                '<div class="barlist">%s</div>'
                '<div class="foot">ערוץ הוואטסאפ, בלי שהושקע בו מה שהושקע ברשתות '
                'הוותיקות, כבר גדול מ-39%% ממנויי יוטיוב ומתקרב ל-X. שניהם נכסים '
                'אמיתיים שאין עליהם היסטוריה למדוד.</div></div>' % bars)
    return slide('X ווואטסאפ', 'שתי רשתות בלי היסטוריה. אומרים את זה ולא מותחים.',
                 ''.join(body))


def _he_date(iso):
    """2026-02-25 -> 25.2.26"""
    y, m, dd = iso.split('-')
    return '%d.%d.%s' % (int(dd), int(m), y[2:])


def s_events(d):
    """מה אירוע חדשותי עושה למספרים.

    החלונות אותרו מהנתונים — רצפים של ימים שחצו פי 2 מהחציון — כדי שיהיו
    **אירועים ולא פוסטים מוצלחים**: יום בודד גדול הוא סרטון שתפס, ואירוע
    חדשותי נמשך ימים. השמות מגיעים מ-`editorial.events` ונכתבים ביד, כי מה
    שקרה בעולם אינו משהו שהסקריפט יכול להסיק מכותרות.
    """
    ev = d.get('events') or {}
    wins = ev.get('windows') or []
    names = ((d.get('editorial') or {}).get('events') or {})
    # השם והתאריכים כבר נמדדו לפי שכבת הניסוח; כאן רק דריסת הכותרת אופציונלית
    shown = [(w, names.get(w.get('key'), {})) for w in wins][:5]
    if not shown:
        return ''
    hi = max(w['views'] for w, _ in shown)
    typ = ev.get('typical_day', 0)
    rows = []
    for w, meta in shown:
        gains = []
        if w['fb_follows']:
            gains.append('<span class="gpill">%s<i>עוקבי פייסבוק</i></span>'
                         % num('+%s' % fmt(w['fb_follows'])))
        if w['yt_subs']:
            gains.append('<span class="gpill">%s<i>מנויי יוטיוב</i></span>'
                         % num('+%s' % fmt(w['yt_subs'])))
        rows.append(
            '<div class="ev">'
            '<div class="evd"><div class="evn">%s</div>'
            '<div class="evr">%s – %s <span>%d ימים</span></div></div>'
            '<div class="evb"><div class="evfill" style="width:%.1f%%"></div>'
            '<div class="evtxt">%s</div></div>'
            '<div class="evv">%s<span>%s</span></div>'
            '<div class="evg">%s</div></div>'
            % (esc(w.get('name') or meta.get('name', '')),
               _he_date(w['from']), _he_date(w['to']), w['days'],
               w['views'] / hi * 100,
               esc(meta.get('headline') or (w['headlines'] or [''])[0]),
               num(short(w['views'])),
               esc('פי %.1f מיום רגיל' % w['vs_typical']), ''.join(gains)))
    body = [head('אירועים חדשותיים', 'מה קרה למספרים כשהחדשות התפוצצו',
                 '<div class="rnum">%s</div><div class="rlab">צפיות ביום רגיל (חציון)</div>'
                 % short(typ)),
            '<div class="evlist">%s</div>' % ''.join(rows),
            '<div class="foot">תאריכי המבצעים אומתו מול מקורות חיצוניים — «עם כלביא» '
            '13–24.6.2025, «שאגת הארי» מ-28.2.2026 — ולא נגזרו מהנתונים. '
            'איתור אוטומטי פתח את מלחמת יוני ב-10.6, כי באותו יום היה קליפ ויראלי '
            'על בריחה משוטר; אלגוריתם מוצא שיאים, לא אירועים.</div>']
    return slide('אירועים', 'האירועים החדשותיים הגדולים ומה הם עשו למספרים.',
                 ''.join(body))


def s_top(d):
    # כותרות שנכתבו ביד ממלאות חורים שהנתונים לא מכסים — לפייסבוק 2025 אין
    # טקסט פוסטים, כי הבקפיל מה-API לא שומר אותו.
    written = (d.get('editorial') or {}).get('slides') or {}
    blocks = []
    for y, items in sorted(d['top_content'].items()):
        rows = ''
        for i in items:
            title = i['title'] or written.get('top_%s_%s' % (y, i['platform']), '')
            rows += ('<div class="trow"><div class="tp">%s</div>'
                     '<div class="tt">%s</div><div class="tv">%s</div></div>'
                     % (icon(i['platform'], 28),
                        esc(title) or '<span class="todo">— למלא כותרת —</span>',
                        num(short(i['views']))))
        blocks.append('<div class="card"><div class="yhead">%s</div>%s</div>' % (y, rows))
    body = [head('רגעי השיא', 'הפריט הגדול של כל שנה, בכל רשת', ''),
            '<div class="grid3">%s</div>' % ''.join(blocks)]
    xp = d.get('cross_platform') or {}
    if xp:
        cards = ''.join(
            '<div class="xp"><div class="xpy">%s</div>'
            '<div class="xpi">%s</div>'
            '<div class="xpt">%s</div><div class="xpv">%s <span>צפיות יחד</span></div></div>'
            % (y, ''.join(icon(p, 26) for p in v['platforms']),
               esc(v['title']), num(short(v['views'])))
            for y, v in sorted(xp.items()))
        body.append('<div class="panel"><div class="ptitle">הסיפורים הגדולים עוברים '
                    'בין הרשתות</div><div class="xprow">%s</div>'
                    '<div class="foot">זוהה לפי חפיפת מילים בכותרות (0.5 ומעלה) — '
                    'כל רשת עורכת את הכותרת בנפרד, אז טקסט זהה לא היה תופס.</div>'
                    '</div>' % cards)
    body.append('<div class="foot">מספטמבר 2024 בלבד — לפני כן מטא לא מספקת צפיות '
                'ברמת פריט.</div>')
    return slide('רגעי שיא', 'התוכן הגדול של כל שנה.', ''.join(body))


def s_method(d):
    body = [head('הערת מדידה', 'למה הגרפים מתחילים בספטמבר 2024', ''),
            '<div class="panel wide">'
            '<p>מטא שינתה את הגדרת הצפיות והחשיפה באוגוסט–ספטמבר 2024, ומוחקת מדדים '
            'ברמת פריט אחרי כ-23 חודשים. נמדד על הייצואים עצמם:</p>'
            '<ul>'
            '<li><b>צפיות באינסטגרם</b> — התא ריק לפני יולי 2024.</li>'
            '<li><b>חשיפה באינסטגרם</b> — לפני ספטמבר 2024 חציון 3–24, על פוסטים עם '
            '1,500 לייקים ומעלה. זה לא נתון.</li>'
            '<li><b>פייסבוק</b> — «חשיפה אורגנית» שווה ל«חשיפה» עד אוגוסט 2024 ואז קופצת '
            'לפי 2 ממנה, כשהקידום אפס. אורגני לא יכול לעלות על הסך הכל.</li>'
            '<li>אותה בדיקה מול ה-API החזירה את אותם ערכים <b>ספרה בספרה</b> — כלומר '
            'הנתון עצמו אבד, לא הייצוא.</li>'
            '</ul>'
            '<p>לכן: <b>ספירות, לייקים ותגובות</b> תקפים מינואר 2024. '
            '<b>צפיות וחשיפה</b> — מספטמבר 2024. קו שנמתח על פני הגבול הזה מראה צמיחה '
            'שהיא כולה שינוי מדידה.</p>'
            '<p class="dim">היחידה בפייסבוק, אינסטגרם וטיקטוק: <b>צפיות שנצברו לתוכן '
            'שפורסם באותה תקופה</b>. פוסט שם גמור תוך ארבעה ימים, ולכן זה קרוב מאוד '
            'ל«צפיות באותה תקופה». <b>יוטיוב נמדד אחרת ובכוונה</b> — צפיות בפועל '
            'בתקופה, מייצוא Studio, כי לו יש זנב אמיתי: לפי שנת הפרסום הוא היה מסתכם '
            'ב-547M, ובפועל נצפו 642M. בכל רשת נבחר המדד המדויק יותר עבורה.</p>'
            '</div>',
            _coverage_table()]
    return slide('הערת מדידה', 'השקף שמונע את השאלה מהקהל.', ''.join(body))


COVERAGE = [
    ('פייסבוק', 'ok:מספטמבר', 'ok:מלא', 'ok:מלא',
     'ייצוא Business Suite ל-2024 ו-2026; 2025 נמשך מה-Graph API'),
    ('אינסטגרם', 'ok:מספטמבר', 'ok:מלא', 'ok:מלא', 'שלושה ייצואי Business Suite'),
    ('יוטיוב', 'ok:מלא', 'ok:מלא', 'ok:מלא',
     'ייצוא Studio — צפיות, זמן צפייה ומנויים יומיים; ספירת פריטים מה-API'),
    ('טיקטוק', 'no:אין', 'part:מפברואר', 'ok:מלא', 'TikHub; הספק נעצר בפברואר 2025'),
    ('X', 'no:אין', 'no:אין', 'part:מיוני', 'הספק מגיע 13 יום אחורה בלבד'),
    ('ערוץ וואטסאפ', 'no:אין', 'no:אין', 'part:עוקבים', 'אין API כלל; גודל הקהל ידני'),
]


def _coverage_table():
    """מה יש ומה אין, לכל רשת ולכל שנה. שקף גיבוי לשאלה «ולמה אין שם X?»"""
    rows = []
    for name, y24, y25, y26, src in COVERAGE:
        cells = ''
        for cell in (y24, y25, y26):
            kind, text = cell.split(':', 1)
            cells += '<td><span class="pill %s">%s</span></td>' % (kind, esc(text))
        rows.append('<tr><th>%s</th>%s<td class="src">%s</td></tr>'
                    % (esc(name), cells, esc(src)))
    return ('<div class="panel"><div class="ptitle">מה מכוסה, ומאיפה</div>'
            '<table class="cov"><tr><th></th><th>2024</th><th>2025</th><th>2026</th>'
            '<th>מקור</th></tr>%s</table></div>' % ''.join(rows))


CSS = """
@font-face{font-family:'SimplerPro';src:url('%(f)s/SimplerPro_HLAR-Regular.otf') format('opentype');font-weight:400}
@font-face{font-family:'SimplerPro';src:url('%(f)s/SimplerPro_HLAR-Semibold.otf') format('opentype');font-weight:600}
@font-face{font-family:'SimplerPro';src:url('%(f)s/SimplerPro_HLAR-Bold.otf') format('opentype');font-weight:700}
@font-face{font-family:'SimplerPro';src:url('%(f)s/SimplerPro_HLAR-Black.otf') format('opentype');font-weight:900}
*{box-sizing:border-box}
body{margin:0;background:#2a2a2a;font-family:'SimplerPro','Arial Hebrew','Segoe UI',sans-serif;color:%(ink)s}
section{width:1920px;height:1080px;padding:64px 84px;display:flex;flex-direction:column;
  position:relative;overflow:hidden;margin:0 auto 26px;box-shadow:0 8px 40px rgba(0,0,0,.4)}
/* כל מספר מבודד ל-LTR: ב-RTL ה-bidi מעיף סימן/אחוז לצד הלא נכון */
.mono{font-family:'SF Mono',Menlo,Consolas,monospace;font-variant-numeric:tabular-nums;
  direction:ltr;unicode-bidi:isolate;display:inline-block}
h1{margin:0;font-size:104px;font-weight:900;line-height:.98;letter-spacing:-.02em}
h2{margin:2px 0 0;font-size:56px;font-weight:900;letter-spacing:-.02em}
.kicker{font-size:19px;font-weight:700;letter-spacing:.04em;color:%(a)s}
.shead{display:flex;align-items:flex-end;justify-content:space-between;margin-bottom:26px}
.stitle{display:flex;align-items:center;gap:20px}
.rule{width:44px;height:8px;background:%(a)s}
.sright{text-align:left}
.rnum{font-family:'SF Mono',Menlo,monospace;font-size:52px;font-weight:700;line-height:1}
.rlab{font-size:16px;color:%(m)s;margin-top:2px;line-height:1.3}
.tiny{font-size:13px;color:#9a9a9a}
/* שער */
.cover{flex:1;display:flex;flex-direction:column;justify-content:space-between}
.cbar{position:absolute;top:0;right:0;width:8px;height:100%%;background:%(a)s}
.csub{margin-top:28px;font-size:32px;color:#404040}
.cfoot{display:flex;align-items:flex-end;justify-content:space-between;
  border-top:1px solid %(g)s;padding-top:44px}
.clab{font-size:23px;font-weight:600;color:%(m)s;margin-bottom:10px}
.cbig{font-family:'SF Mono',Menlo,monospace;font-size:190px;font-weight:700;line-height:.85;letter-spacing:-.03em}
.cbig span{font-size:92px;color:%(a)s}
.cright{text-align:left}
.cnum{font-size:76px;font-weight:700;line-height:1}
.cstrip{display:flex;gap:44px;align-items:center;flex-wrap:wrap}
.cp{display:flex;align-items:center;gap:13px}
.cpn{font-size:21px;font-weight:700}
.cpf{font-size:19px;color:%(m)s}
/* דירוג */
.bhead{display:grid;grid-template-columns:280px 1fr 150px 170px;gap:26px;
  padding-bottom:12px;border-bottom:1px solid %(g)s;font-size:14px;font-weight:700;
  color:%(m)s;letter-spacing:.03em}
.bhead div:last-child,.bhead div:nth-child(3){text-align:left}
.blist{flex:1;display:flex;flex-direction:column;justify-content:space-around;padding:8px 0}
.brow{display:grid;grid-template-columns:280px 1fr 150px 170px;gap:26px;align-items:center}
.barlist{display:flex;flex-direction:column;gap:18px;margin-top:6px}
.bl .pl,.pl{display:flex;align-items:center;gap:16px}
.pn{font-size:27px;font-weight:700}
.btrack{position:relative;height:40px;background:#e6e6e6;border-radius:8px;overflow:hidden}
.bfill{position:absolute;top:0;right:0;height:100%%;border-radius:8px}
.bnone{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;
  font-size:16px;color:#9a9a9a;letter-spacing:.03em}
.bv{font-size:31px;font-weight:700;text-align:left}
.bs{font-size:19px;color:%(m)s;text-align:left}
.pw{font-size:15px;color:#8a4b00;margin-top:2px}
/* כרטיסים */
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:26px;align-content:stretch}
.grid3{flex:1;display:grid;grid-template-columns:repeat(3,1fr);gap:24px;align-content:center}
.card{background:#fff;border:1px solid %(g)s;border-radius:14px;padding:24px 26px;
  display:flex;flex-direction:column;gap:12px}
.card > .chart{flex:1;display:flex;flex-direction:column;justify-content:center}
.ch{display:flex;align-items:center;gap:14px}
.cs{font-size:16px;color:%(m)s;margin-top:2px}
.panel{background:#fff;border:1px solid %(g)s;border-radius:14px;padding:24px 26px;margin-top:18px}
.panel.grow{flex:1;display:flex;flex-direction:column;justify-content:center}
.panel.grow .barlist{flex:1;justify-content:space-evenly}
/* גרף: ה-SVG נמתח, התוויות לא — לכן הן מחוצה לו */
.chart{position:relative;padding:0 0 22px 0}
.spark{display:block;width:100%%}
.cy{position:absolute;top:12px;left:0;bottom:34px;width:56px;display:flex;
  flex-direction:column;justify-content:space-between;font-size:14px;color:%(m)s;
  text-align:left;direction:ltr}
.cx{display:flex;justify-content:space-between;font-size:14px;color:%(m)s;
  margin-top:6px;padding-left:62px;direction:ltr}
.delta{margin-inline-start:auto;text-align:left}
.delta{white-space:nowrap}
.delta .mono{font-size:32px;font-weight:700;line-height:1;color:#186a2e}
.delta span{display:block;font-size:14px;color:%(m)s;margin-top:2px;font-weight:400}
.delta span .mono{font-size:14px;font-weight:400;color:%(m)s}
.marks{display:flex;flex-direction:column;gap:7px;margin-top:10px;
  border-top:1px solid %(g)s;padding-top:10px}
.mk{display:grid;grid-template-columns:96px 74px 1fr;gap:10px;align-items:baseline}
.mkn{font-size:19px;font-weight:700;color:%(a)s;text-align:left}
.mkm{font-size:15px;color:%(m)s;direction:ltr;text-align:left}
.mkt{font-size:15px;line-height:1.4;color:#444;overflow:hidden;max-height:40px;
  display:flex;align-items:flex-start;gap:7px}
.mkt svg{flex:none;margin-top:1px}
.mkh{font-size:15px;color:%(m)s;font-weight:700;margin-bottom:6px}
.fact{font-size:19px;line-height:1.5;color:#333;border-inline-start:3px solid %(g)s;
  padding-inline-start:16px}
.kpirow{display:grid;grid-template-columns:repeat(4,1fr);gap:20px;margin-top:18px}
.kpi{background:#fff;border:1px solid %(g)s;border-radius:14px;padding:18px 22px}
.kv{font-size:40px;font-weight:700;line-height:1.05}
.kl{font-size:17px;color:%(m)s;margin-top:4px}
.dimx{font-size:15px;color:%(m)s}
.panel.wide{flex:1;font-size:25px;line-height:1.55}
.panel.wide ul{margin:14px 0;padding-inline-start:26px}
.panel.wide li{margin-bottom:8px}
.ptitle{font-size:24px;font-weight:700;margin-bottom:14px}
.note{font-size:17px;color:#444;line-height:1.45}
.note.warn{color:#8a4b00;background:#fff6e8;border-radius:8px;padding:10px 12px}
.bignum{font-size:72px;font-weight:700;line-height:1}
.mini{width:100%%;font-size:17px;color:#444;border-collapse:collapse}
.mini td{padding:3px 0}
.mini td:last-child{text-align:left;font-weight:700}
.foot{margin-top:16px;font-size:18px;color:%(m)s;line-height:1.5}
/* שנים */
.yrow{display:grid;grid-template-columns:repeat(3,1fr);gap:24px;margin-bottom:4px}
.yc{background:#fff;border:1px solid %(g)s;border-radius:14px;padding:22px 26px}
.yy{font-size:22px;font-weight:700;color:%(m)s}
.yy span{font-size:14px;color:%(a)s}
.yv{font-size:60px;font-weight:700;line-height:1.05}
.yl{font-size:16px;color:%(m)s}
.ys{font-size:18px;margin-top:8px}
/* תמהיל */
.mixlist{display:flex;flex-direction:column;gap:20px}
.mixnote{font-size:19px;color:#444;border-top:1px solid %(g)s;padding-top:14px}
.mixnote .up{color:#186a2e;font-weight:700}
.mixnote .down{color:#b42318;font-weight:700}
.mixrow{display:flex;align-items:center;gap:14px}
.my{font-size:19px;font-weight:700;width:48px;color:%(m)s;flex:none}
.mt{font-size:19px;font-weight:700;width:60px;text-align:left;flex:none}
.stack{display:flex;gap:2px;border-radius:6px;overflow:hidden;flex:1;min-width:0}
.seg{display:flex;align-items:center;justify-content:center;color:#fff;font-size:14px;font-weight:700}
/* רגעי שיא */
.yhead{font-size:34px;font-weight:900;color:%(a)s;border-bottom:1px solid %(g)s;padding-bottom:8px}
.trow{display:flex;align-items:center;gap:12px;padding:10px 0;border-bottom:1px solid #f0f0f0}
.tt{flex:1;font-size:17px;line-height:1.35}
.tv{font-size:23px;font-weight:700}
.todo{color:%(a)s;font-weight:700}
/* אירועים */
.ythead{display:grid;grid-template-columns:230px 1fr 110px 1fr 110px;gap:16px;
  font-size:15px;color:%(m)s;font-weight:700;margin-bottom:10px}
.ythead div:nth-child(2),.ythead div:nth-child(4){text-align:center}
.ytr{display:grid;grid-template-columns:230px 1fr 110px 1fr 110px;gap:16px;
  align-items:center;margin-bottom:12px}
.ytn{font-size:22px;font-weight:700}
.ytn span{display:block;font-size:14px;color:%(m)s;font-weight:400;margin-top:2px}
.ytbar{height:26px;background:#ececec;border-radius:6px;overflow:hidden;
  display:flex;justify-content:flex-end}
.ytbar div{height:100%%;border-radius:6px}
.ytv{font-size:22px;font-weight:700;text-align:left}
.evlist{flex:1;display:flex;flex-direction:column;justify-content:space-evenly;padding:6px 0}
.ev{display:grid;grid-template-columns:250px 1fr 190px 330px;gap:22px;align-items:center}
.evd{text-align:right}
.evn{font-size:22px;font-weight:700;line-height:1.15}
.evr{font-size:14px;color:%(m)s;margin-top:3px;direction:ltr;text-align:right}
.evr span{display:block}
.ychips{display:flex;align-items:flex-end;gap:34px;margin:6px 0 18px;
  border-bottom:1px solid %(g)s;padding-bottom:16px}
.ycl{font-size:16px;color:%(m)s;font-weight:700;margin-inline-end:8px;max-width:190px;line-height:1.3}
.ychip{display:flex;align-items:baseline;gap:10px}
.ycy{font-size:17px;color:%(m)s;font-weight:700}
.ycv{font-size:30px;font-weight:700}
.ychip .up{font-size:19px;font-weight:700;color:#186a2e}
.ychip .down{font-size:19px;font-weight:700;color:#b42318}
.evb{position:relative;height:62px;background:#ececec;border-radius:8px;overflow:hidden;
  display:flex;align-items:center}
.evfill{position:absolute;top:0;right:0;height:100%%;background:%(a)s;opacity:.16}
.evtxt{position:relative;padding:0 18px;font-size:19px;line-height:1.3;
  max-height:56px;overflow:hidden}
.evv{text-align:left}
.evv .mono{font-size:34px;font-weight:700;line-height:1}
.evv span{display:block;font-size:15px;color:%(m)s;margin-top:3px;white-space:nowrap}
.evg{display:flex;gap:10px;justify-content:flex-start}
.gpill{background:#f0f4ff;border:1px solid #dbe4ff;border-radius:10px;padding:8px 14px;
  font-size:22px;font-weight:700;color:#1D4ED8;text-align:center}
.gpill i{display:block;font-size:13px;font-style:normal;font-weight:400;color:%(m)s;margin-top:2px}
.xprow{display:grid;grid-template-columns:repeat(3,1fr);gap:22px}
.xp{border-inline-start:3px solid %(a)s;padding-inline-start:16px}
.xpy{font-size:19px;font-weight:700;color:%(m)s}
.xpi{display:flex;gap:6px;margin:6px 0}
.xpt{font-size:18px;line-height:1.35;margin-bottom:6px}
.xpv{font-size:26px;font-weight:700}
.xpv span{font-size:15px;color:%(m)s;font-weight:400}
.dim{color:#555;font-size:22px}
.cov{width:100%%;border-collapse:collapse;font-size:19px}
.cov th,.cov td{padding:9px 12px;text-align:right;border-bottom:1px solid #f0f0f0}
.cov tr:first-child th{font-size:16px;color:%(m)s;font-weight:700}
.cov th{font-weight:700}
.cov .src{color:%(m)s;font-size:17px}
.pill{display:inline-block;padding:3px 12px;border-radius:99px;font-size:16px;font-weight:700}
.pill.ok{background:#e7f5ea;color:#186a2e}
.pill.part{background:#fff3e0;color:#8a4b00}
.pill.no{background:#f2f2f2;color:#8a8a8a}
/* מסך צר מ-1920: מכווצים את השקף כולו במקום לשבור את הפריסה. הדק בנוי
   לפיקסלים קבועים, ופריסה נוזלית הייתה מזיזה כל גרף וכל תווית.
   `zoom` ולא `transform`, כי transform לא משנה את הגובה שהאלמנט תופס
   ומשאיר רווחים ענקיים בין השקפים. הערך המדויק נקבע בסקריפט שבתחתית;
   נקודות השבירה כאן הן גיבוי למקרה שהוא לא רץ. */
section{zoom:var(--z,1)}
@media (max-width:1919px){section{zoom:var(--z,.88)}}
@media (max-width:1700px){section{zoom:var(--z,.78)}}
@media (max-width:1500px){section{zoom:var(--z,.70)}}
@media (max-width:1360px){section{zoom:var(--z,.63)}}
@media (max-width:1200px){section{zoom:var(--z,.55)}}
@media (max-width:1000px){section{zoom:var(--z,.45)}}
@media (max-width:820px){section{zoom:var(--z,.36)}}
@media print{
  body{background:#fff}
  section{margin:0;box-shadow:none;page-break-after:always;zoom:1}
}
"""


FIT_SCRIPT = """
<script>
// מתאים את השקף לרוחב החלון בדיוק. 1920 הוא הרוחב שהדק בנוי בו, ו-40
// פיקסלים נשמרים לשוליים כדי שלא ייווצר פס גלילה אופקי.
(function () {
  var W = 1920, PAD = 40;
  function fit() {
    var z = Math.min(1, (window.innerWidth - PAD) / W);
    document.documentElement.style.setProperty('--z', z);
  }
  fit();
  addEventListener('resize', fit);
  // בהדפסה חוזרים לגודל מלא, אחרת ה-PDF יוצא מוקטן
  addEventListener('beforeprint', function () {
    document.documentElement.style.setProperty('--z', 1);
  });
  addEventListener('afterprint', fit);
})();
</script>"""


def render():
    d = json.load(open(DATA, encoding='utf-8'))
    slides = [
        s_cover(d), s_assets(d), s_growth(d), s_audience(d), s_output(d),
        s_platform(d, 'facebook'), s_platform(d, 'instagram', s_ig_conversion(d)),
        s_platform(d, 'youtube', s_youtube_types(d)), s_platform(d, 'tiktok'),
        s_thin(d), s_events(d), s_top(d), s_method(d),
    ]
    slides = [s for s in slides if s]
    css = CSS % {'f': FONTS, 'a': ACCENT, 'ink': INK, 'm': MUTED, 'g': GRID}
    doc = ('<!DOCTYPE html><html lang="he" dir="rtl"><head><meta charset="utf-8">'
           '<title>הסושיאל של כאן חדשות — 2024 עד יולי 2026</title>'
           '<style>%s</style></head><body>%s%s</body></html>'
           % (css, ''.join(slides), FIT_SCRIPT))
    with open(OUT, 'w', encoding='utf-8') as f:
        f.write(doc)
    print('נכתב %s — %d שקפים, %d KB' % (OUT, len(slides), len(doc) // 1024))
    return 0


if __name__ == '__main__':
    sys.exit(render())
