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
# מה מפרסמים בכל רשת. «פריטים» היא מילה של אנליסט, לא של מסמך בעברית.
UNIT = {'facebook': 'פוסטים', 'instagram': 'פוסטים',
        'youtube': 'סרטונים', 'tiktok': 'סרטונים'}
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
    # הלוגו האמיתי: תו מוזיקלי עם הדגלון, ובשלוש השכבות של המותג —
    # ציאן ומג'נטה מוסטים מאחורי הלבן. הגרסה הקודמת הייתה צורה מומצאת בצהוב.
    'tiktok': '<rect width="24" height="24" rx="6" fill="#010101"/>'
              '<g transform="translate(3.2 3.2) scale(0.72)">'
              '<path d="M16.6 5.82A4.28 4.28 0 0 1 15.54 3h-3.09v12.4a2.59 2.59 0 0 1-2.59 2.5'
              'c-1.42 0-2.6-1.16-2.6-2.6 0-1.72 1.66-3.01 3.37-2.48V9.66c-3.45-.46-6.47 2.22'
              '-6.47 5.64 0 3.33 2.76 5.7 5.69 5.7 3.14 0 5.69-2.55 5.69-5.7V9.01a7.35 7.35 0 0 0'
              ' 4.3 1.38V7.3s-1.88.09-3.24-1.48z" fill="#25F4EE" transform="translate(-1 -1)"/>'
              '<path d="M16.6 5.82A4.28 4.28 0 0 1 15.54 3h-3.09v12.4a2.59 2.59 0 0 1-2.59 2.5'
              'c-1.42 0-2.6-1.16-2.6-2.6 0-1.72 1.66-3.01 3.37-2.48V9.66c-3.45-.46-6.47 2.22'
              '-6.47 5.64 0 3.33 2.76 5.7 5.69 5.7 3.14 0 5.69-2.55 5.69-5.7V9.01a7.35 7.35 0 0 0'
              ' 4.3 1.38V7.3s-1.88.09-3.24-1.48z" fill="#FE2C55" transform="translate(1 1)"/>'
              '<path d="M16.6 5.82A4.28 4.28 0 0 1 15.54 3h-3.09v12.4a2.59 2.59 0 0 1-2.59 2.5'
              'c-1.42 0-2.6-1.16-2.6-2.6 0-1.72 1.66-3.01 3.37-2.48V9.66c-3.45-.46-6.47 2.22'
              '-6.47 5.64 0 3.33 2.76 5.7 5.69 5.7 3.14 0 5.69-2.55 5.69-5.7V9.01a7.35 7.35 0 0 0'
              ' 4.3 1.38V7.3s-1.88.09-3.24-1.48z" fill="#fff"/></g>',
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


def heb(n, unit=''):
    """מספר במילים עבריות: 4.8 מיליארד, 29.8 מיליון.

    `4.80B` הוא סימון טכני באנגלית בתוך מסמך עברי. בכותרות ובמספרי-על
    קוראים במילים; בצירי גרפים ובתוויות צפופות נשאר הסימון הקצר, כי שם
    המקום מוגבל ומילה שלמה הופכת לרעש.
    """
    n = float(n or 0)
    if n >= 1e9:
        val, word = '%.1f' % (n / 1e9), 'מיליארד'
    elif n >= 1e6:
        val, word = '%.1f' % (n / 1e6), 'מיליון'
    else:
        val, word = format(int(n), ','), ''
    val = val.rstrip('0').rstrip('.') if '.' in val else val
    out = '%s %s' % (num(val), word) if word else num(val)
    return '%s %s' % (out, unit) if unit else out


def icon(p, size=44):
    return ('<svg width="%d" height="%d" viewBox="0 0 24 24">%s</svg>'
            % (size, size, ICONS.get(p, '')))


# ---------- גרפים ----------

def sparkline(points, color, h=250, key='views', zero_base=True, marks=None,
              legend=True):
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
    leg = ''
    if flags and legend:
        leg = ('<div class="marks"><div class="mkh">החודשים הגדולים — '
                  'ולצדם התוכן הגדול באותו חודש</div>%s</div>') % ''.join(
            '<div class="mk"><span class="mkn">%s</span>'
            '<span class="mkm">%s</span><span class="mkt">%s%s</span></div>'
            % (num('+%s' % fmt(mk['gain'])), esc(mk['month']),
               icon(mk['headline_platform'], 18) if mk.get('headline_platform') else '',
               esc(mk.get('headline', '')))
            for _, mk in sorted(flags, key=lambda f: -f[1].get('gain', 0)))
    return ('<div class="chart"><div class="cy"><span>%s</span><span>%s</span></div>%s'
            '<div class="cx"><span>%s</span><span>%s</span></div></div>%s'
            % (short(hi), short(lo) if lo else '0', ''.join(out),
               esc(points[0]['month']), esc(points[-1]['month']), leg))


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

_PAGE = {'n': 0}


def slide(label, notes, body, bg=SURFACE, section='', chrome=True):
    """שקף. `section` הוא שם הפרק שיודפס בפינה יחד עם מספר העמוד."""
    _PAGE['n'] += 1
    foot = ''
    if chrome:
        foot = ('<div class="chrome"><span>%s</span><span class="pg">%d</span></div>'
                % (esc(section), _PAGE['n']))
    return ('<section data-label="%s" data-notes="%s" dir="rtl" style="background:%s">'
            '%s%s</section>'
            % (esc(label), esc(notes), bg, body, foot))


def head(title, kicker='', right=''):
    return ('<div class="shead"><div class="stitle">'
            '<div class="rule"></div><div>%s<h2>%s</h2></div></div>'
            '<div class="sright">%s</div></div>'
            % ('<div class="kicker">%s</div>' % esc(kicker) if kicker else '',
               esc(title), right))


def total_views(d):
    """סך הצפיות — **הגדרה אחת לכל המסמך**.

    השער חישב מ-`yearly` והשקף השלישי מ-`monthly_views`, ושניהם הופיעו על
    אותו מסמך: 4.78B מול 4.80B. הסדרה החודשית היא הבסיס הנכון — היא כבר
    מסוננת לחודשים תקפים, חתוכה ביולי, וביוטיוב מגיעה מ-Studio.
    """
    return sum(sum(m['views'] for m in blk.get('monthly_views', []))
               for blk in d['platforms'].values() if blk.get('monthly_views'))


def s_cover(d):
    ed = d['editorial']
    tot = total_views(d)
    strip = ''.join(
        '<div class="cp">%s<div><div class="cpn">%s</div><div class="cpf">%s</div></div></div>'
        % (icon(p, 40), esc(HEB[p]), num(fmt(n)))
        for p, n in sorted(d['followers'].items(), key=lambda x: -x[1]))
    body = (
        '<div class="cover">'
        '<div class="cbar"></div>'
        '<div>'
        '<h1>%s</h1>'
        '<div class="csub">%s</div>'
        '</div>'
        '<div class="cstrip">%s</div>'
        '<div class="chero">'
        '<div class="cbig">%s</div>'
        '<div class="clab">צפיות בכל הרשתות</div>'
        '<div class="cfol">%s עוקבים</div>'
        '</div></div>'
        % (esc(ed.get('cover_title')), esc(ed.get('cover_subtitle')), strip,
           heb(tot), num(fmt(sum(d['followers'].values())))))
    return slide('שער', 'מספר גיבור אחד: סך הצפיות, והקהל כשורה מתחתיו.',
                 body, 'radial-gradient(120% 120% at 78% 12%,#fff 0%,#f7f7f7 55%,#ececec 100%)',
                 chrome=False)


def s_platform(d, p):
    """שקף עומק לרשת אחת. כל מה שנוגע לרשת נמצא כאן ולא מפוזר בין שקפים.

    מבנה קבוע לכל הרשתות, כדי שהעין תלמד אותו פעם אחת: שלושה כרטיסי שנים,
    גרף חודשי עם האירועים, ושתי לוחיות עומק — התמהיל, והמדד הייחודי לרשת.
    בלי הערות שוליים מפוזרות: כל הסתייגויות המדידה מרוכזות בשקף האחרון.
    """
    b = d['platforms'][p]
    y = dict(b.get('yearly', {}))
    for k, v in (b.get('yearly_period') or {}).items():
        y.setdefault(k, {}).update({'views': v['views']})
    pts = b.get('monthly_views', [])
    marks = [{'month': w['peak_date'][:7], 'label': w.get('short') or w['name']}
             for w in ((d.get('events') or {}).get('windows') or [])]

    cells = []
    for k in [k for k in ('2024', '2025', '2026') if k in y]:
        v = y[k]
        n = v.get('posts_in_views_window') or v.get('posts', 0)
        win = v.get('views_window')
        tag = ''
        if k == '2026':
            tag = ' <span>עד 31.7</span>'
        elif win and not win.endswith('-01'):
            tag = ' <span>מספטמבר</span>'
        cells.append('<div class="yc"><div class="yy">%s%s</div>'
                     '<div class="yv">%s</div><div class="yl">צפיות</div>'
                     '<div class="ys">%s %s</div></div>'
                     % (k, tag, heb(v.get('views', 0)), num(fmt(n)),
                        UNIT.get(p, 'פרסומים')))

    # כרטיסי השנים מציגים 2025 מלאה מול 2026 חלקית, ומי שקורא רק את השקף
    # הזה רואה קריסה שלא קרתה — באינסטגרם 566M מול 345M נראה כמו ‎-40%‎
    # בזמן שההשוואה ההוגנת היא ‎+8%‎. לכן החלון הזהה יושב ליד הכותרת.
    ytd = {}
    for m in pts:
        if m['month'][5:7] <= '07':
            ytd[m['month'][:4]] = ytd.get(m['month'][:4], 0) + m['views']
    yrs = sorted(ytd)
    fair = ''
    if len(yrs) >= 2 and ytd[yrs[-2]]:
        pct = (ytd[yrs[-1]] / ytd[yrs[-2]] - 1) * 100
        fair = ('<div class="fair %s">%s<span>ינואר–יולי, %s מול %s</span></div>'
                % ('up' if pct >= 0 else 'down', num('%+.0f%%' % pct), yrs[-1], yrs[-2]))
    span_txt = ('%s – %s' % (pts[0]['month'], pts[-1]['month'])) if pts else ''
    body = [head(HEB[p], 'עוקבים היום: %s' % fmt(d['followers'].get(p, 0)),
                 ('%s<div class="rnum">%s</div>'
                  '<div class="rlab">סך הצפיות<br><span class="tiny">%s</span></div>'
                  % (fair, heb(b.get('views_total') or sum(x['views'] for x in pts)),
                     esc(span_txt)))
                 if pts else ''),
            '<div class="yrow" style="grid-template-columns:repeat(%d,1fr)">%s</div>'
            % (len(cells), ''.join(cells)),
            ('<div class="panel chartpanel">%s</div>'
             % sparkline(pts, BRAND[p], h=210, marks=marks, legend=False)) if pts else '',
            _deep_row(d, p)]
    return slide(HEB[p], 'עומק לרשת %s.' % HEB[p], ''.join(body),
                 section='רשת אחר רשת')


def _deep_row(d, p):
    """שתי לוחיות עומק — או אחת ברוחב מלא, אם לרשת אין תמהיל פורמטים."""
    mix, depth = _mix_panel(d, p), _depth_panel(d, p)
    if not mix:
        return '<div class="grid1 deep">%s</div>' % depth
    return '<div class="grid2 deep">%s%s</div>' % (mix, depth)


def _mix_panel(d, p):
    """תמהיל הפורמטים של הרשת, ינואר–יולי בכל שנה."""
    mix = d['platforms'][p].get('format_mix') or {}
    if not mix:
        return ''
    rows = ''
    for yr in ('2024', '2025', '2026'):
        seg = [(k, v.get(yr, 0), mix_color(k, i))
               for i, (k, v) in enumerate(sorted(mix.items()))]
        seg = [s for s in seg if s[1]]
        if not seg:
            continue
        seg.sort(key=lambda s: -s[1])
        rows += ('<div class="mixrow"><div class="my">%s</div>%s<div class="mt">%s</div></div>'
                 % (yr, stacked(seg), num(fmt(sum(s[1] for s in seg)))))
    ytd = d['platforms'][p].get('posts_ytd') or {}
    note = ''
    if ytd.get('2024') and ytd.get('2026'):
        pct = (ytd['2026'] / ytd['2024'] - 1) * 100
        note = ('<div class="mixnote">נפח הפרסום <span class="%s">%s</span> '
                'מ-2024 ל-2026</div>'
                % ('up' if pct >= 0 else 'down', num('%+.0f%%' % pct)))
    return ('<div class="panel"><div class="ptitle">מה פרסמנו · ינואר–יולי</div>'
            '<div class="mixlist">%s</div>%s</div>' % (rows, note))


def _depth_panel(d, p):
    """המדד הייחודי לכל רשת — מה שרק היא יודעת לספר."""
    b = d['platforms'][p]

    if p == 'youtube':
        types = b.get('by_type') or {}
        hi_v = max((t['views'] for t in types.values()), default=1)
        hi_w = max((t['watch_hours'] for t in types.values()), default=1)
        rows = ''
        for i, (name, t) in enumerate(sorted(types.items(), key=lambda x: -x[1]['views'])):
            c = mix_color(name, i)
            rows += ('<div class="ytr"><div class="ytn">%s</div>'
                     '<div class="ytbar"><div style="width:%.1f%%;background:%s"></div></div>'
                     '<div class="ytv">%s</div>'
                     '<div class="ytbar"><div style="width:%.1f%%;background:%s"></div></div>'
                     '<div class="ytv">%s</div></div>'
                     % (esc(name), t['views'] / hi_v * 100, c, num(short(t['views'])),
                        t['watch_hours'] / hi_w * 100, c, num(short(t['watch_hours']))))
        return ('<div class="panel"><div class="ptitle">צפיות מול זמן צפייה</div>'
                '<div class="ythead"><div></div><div>צפיות</div><div></div>'
                '<div>שעות</div><div></div></div>%s'
                '<div class="mixnote">שורטס מביאים שליש מהצפיות ו-4%% מהזמן.</div>'
                '</div>' % rows)

    if p == 'instagram':
        f = d.get('instagram_follows_by_format') or {}
        order = sorted(f.items(), key=lambda x: -x[1]['per_1k_views'])
        hi = order[0][1]['per_1k_views'] if order else 1
        bars = ''.join(
            bar_row('<div class="pl"><div class="pn">%s</div></div>' % esc(k),
                    v['per_1k_views'], hi, mix_color(k, i),
                    num('%.2f' % v['per_1k_views']), '')
            for i, (k, v) in enumerate(order))
        return ('<div class="panel"><div class="ptitle">מה ממיר צופה לעוקב · '
                'עוקבים לכל 1,000 צפיות</div><div class="barlist">%s</div></div>'
                % bars)

    if p == 'facebook':
        fb = d.get('facebook_follows') or {}
        pts = [{'month': c['month'], 'views': c['total']} for c in fb.get('cumulative', [])]
        return ('<div class="panel"><div class="ptitle">הצטרפויות לעמוד · מצטבר</div>'
                '%s<div class="mixnote">%s הצטרפו מינואר 2024 (ברוטו)</div></div>'
                % (sparkline(pts, BRAND['facebook'], h=170, legend=False),
                   num('+%s' % fmt(fb.get('total_gross', 0)))))

    return ''


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
                 ''.join(body), section='רשת אחר רשת')


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
            '<div class="evv">%s<span>צפיות · %s</span></div>'
            '<div class="evg">%s</div></div>'
            % (esc(w.get('name') or meta.get('name', '')),
               _he_date(w['from']), _he_date(w['to']), w['days'],
               w['views'] / hi * 100,
               esc(meta.get('headline') or (w['headlines'] or [''])[0]),
               heb(w['views']),
               esc('פי %.1f מיום רגיל' % w['vs_typical']), ''.join(gains)))
    body = [head('אירועים חדשותיים', 'מה קרה למספרים כשהחדשות התפוצצו',
                 '<div class="rnum">%s</div><div class="rlab">צפיות ביום רגיל (חציון)</div>'
                 % heb(typ)),
            '<div class="evlist">%s</div>' % ''.join(rows),
            '<div class="foot">תאריכי המבצעים אומתו מול מקורות חיצוניים — «עם כלביא» '
            '13–24.6.2025, «שאגת הארי» מ-28.2.2026 — ולא נגזרו מהנתונים. '
            'איתור אוטומטי פתח את מלחמת יוני ב-10.6, כי באותו יום היה קליפ ויראלי '
            'על בריחה משוטר; אלגוריתם מוצא שיאים, לא אירועים.</div>']
    return slide('אירועים', 'האירועים החדשותיים הגדולים ומה הם עשו למספרים.',
                 ''.join(body), section='מה הניע את המספרים')


COVERAGE = [
    ('פייסבוק', 'ok:מספטמבר', 'ok:מלא', 'ok:מלא',
     'ייצוא Business Suite ל-2024 ו-2026; 2025 נמשך מה-Graph API'),
    ('אינסטגרם', 'ok:מספטמבר', 'ok:מלא', 'ok:מלא', 'שלושה ייצואי Business Suite'),
    ('יוטיוב', 'ok:מלא', 'ok:מלא', 'ok:מלא',
     'ייצוא Studio — צפיות, זמן צפייה ומנויים יומיים; ספירת הסרטונים מה-API'),
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


def s_achievements(d):
    """המספרים הגדולים על שקף אחד — מה שהמחלקה השיגה בשנתיים."""
    tot_views = total_views(d)
    ytd = d.get('views_by_year_ytd') or {}
    yrs = sorted(ytd)
    growth = ((ytd[yrs[-1]] / ytd[yrs[-2]] - 1) * 100) if len(yrs) >= 2 else 0
    yt = d['platforms']['youtube']
    ys = d['youtube_subscribers']
    fb_gross = (d.get('facebook_follows') or {}).get('total_gross', 0)
    tt_total = sum(m['views'] for m in d['platforms']['tiktok'].get('monthly_views', []))

    # שלושה מספרים ראשיים ושלושה תומכים. קודם כל השישה היו באותו גודל,
    # כך ש-4.8 מיליארד ו-337 אלף נראו שווי משקל.
    main = [
        (heb(tot_views), 'צפיות בכל הרשתות', 'ינואר 2024 – יולי 2026'),
        (num('%+.0f%%' % growth), 'גידול בצפיות', 'ינואר–יולי, 2026 מול 2025'),
        (num(fmt(sum(d['followers'].values()))), 'עוקבים', 'בשש רשתות'),
    ]
    pt = d.get('period_totals') or {}
    big_day = (d.get('big_days') or {}).get('days', [{}])[0]
    sub = [
        (heb((pt.get('daily') or {}).get('2026', 0)), 'צפיות ביום בממוצע · 2026'),
        (heb(pt.get('engagement', 0)), 'לייקים, תגובות ושיתופים'),
        (heb(pt.get('watch_hours', 0)), 'שעות צפייה ביוטיוב ובפייסבוק'),
        (heb(big_day.get('views', 0)),
         'ביום השיא · %s%s' % (_he_date(big_day.get('date', '')),
                               ' · %s' % big_day['event'] if big_day.get('event') else '')),
        (signed(ys['end'] - ys['start']), 'מנויי יוטיוב · גידול של 30%'),
        (num('+%s' % fmt(fb_gross)), 'הצטרפויות לפייסבוק · ברוטו'),
    ]
    cards = ''.join(
        '<div class="ach"><div class="achv">%s</div>'
        '<div class="achl">%s</div><div class="achs">%s</div></div>' % (v, esc(lab), esc(s2))
        for v, lab, s2 in main)
    subs = ''.join(
        '<div class="ach2"><div class="ach2v">%s</div><div class="ach2l">%s</div></div>'
        % (v, esc(lab)) for v, lab in sub)
    ed = ((d.get('editorial') or {}).get('titles') or {})
    body = [head(ed.get('achievements', 'שנתיים של צמיחה'), 'הישגי התקופה', ''),
            '<div class="achwrap"><div class="achgrid">%s</div>'
            '<div class="achrow2">%s</div></div>' % (cards, subs),
            '<div class="foot">%s</div>'
            % ('טיקטוק לבדו הביא %s צפיות — רשת שלא נמדדה אצלנו עד 2025.'
               % heb(tt_total))]
    return slide('הישגים', 'המספרים הגדולים של התקופה.', ''.join(body),
                 section='הישגי התקופה')


def s_divider(d):
    """מפריד פרק. במסמך ארוך זה מה שאומר לקורא שהוא עובר שלב."""
    items = ''.join('<div class="dvi">%s<span>%s</span></div>'
                    % (icon(p, 40), esc(HEB[p]))
                    for p in ('facebook', 'tiktok', 'instagram', 'youtube'))
    body = ('<div class="divider"><div class="kicker">החלק השני</div>'
            '<h2 class="dvh">רשת אחר רשת</h2>'
            '<div class="dvsub">כל רשת: כמה צפיות, מה פרסמנו, ומה ייחודי לה</div>'
            '<div class="dvrow">%s</div></div>' % items)
    return slide('מפריד', 'מעבר לחלק השני.', body, section='רשת אחר רשת')


def s_appendix(d):
    """כל הסתייגויות המדידה במקום אחד, בסוף — לא מפוזרות על השקפים."""
    rows = ''
    for name, y24, y25, y26, src in COVERAGE:
        cells = ''
        for cell in (y24, y25, y26):
            kind, text = cell.split(':', 1)
            cells += '<td><span class="pill %s">%s</span></td>' % (kind, esc(text))
        rows += '<tr><th>%s</th>%s<td class="src">%s</td></tr>' % (esc(name), cells, esc(src))
    body = [head('נספח', 'מאיפה המספרים, ומה הגבולות שלהם', ''),
            '<div class="grid2 deep">'
            '<div class="panel"><div class="ptitle">מה מכוסה, ומאיפה</div>'
            '<table class="cov"><tr><th></th><th>2024</th><th>2025</th><th>2026</th>'
            '<th>מקור</th></tr>%s</table></div>' % rows,
            '<div class="panel"><div class="ptitle">שלוש הערות מדידה</div>'
            '<div class="note2"><b>למה חלק מהגרפים מתחילים בספטמבר 2024.</b> '
            'מטא שינתה את הגדרת הצפיות והחשיפה באוגוסט–ספטמבר 2024, ומוחקת מדדים '
            'ברמת פוסט אחרי כ-23 חודשים. לפני ספטמבר החשיפה באינסטגרם היא חציון '
            '3–24 על פוסטים עם 1,500 לייקים — כלומר לא נתון. בדיקה מול ה-API '
            'החזירה את אותם ערכים בדיוק, כך שהנתון עצמו אבד ולא הייצוא. '
            'יוטיוב אינו כפוף לזה ומתחיל בינואר 2024.</div>'
            '<div class="note2"><b>למה הכל נחתך ביולי 2026.</b> אוגוסט הוא חודש '
            'חלקי, וכל גרף חודשי היה מצייר בסופו צניחה שהיא רק «החודש עוד לא '
            'נגמר». ההשוואות השנתיות הן ינואר–יולי בשתי השנים.</div>'
            '<div class="note2"><b>מה נמדד בכל רשת.</b> בפייסבוק, אינסטגרם '
            'וטיקטוק — צפיות שנצברו לתוכן שפורסם בתקופה; פוסט שם גמור תוך ארבעה '
            'ימים, אז זה קרוב לצפיות בתקופה. ביוטיוב — צפיות בפועל בתקופה מייצוא '
            'Studio, כי לו יש זנב אמיתי. בכל רשת נבחר המדד המדויק יותר עבורה.</div>'
            '</div></div>']
    return slide('נספח', 'מקורות והסתייגויות.', ''.join(body), section='נספח')


def _chev(pct, color, size=38):
    """צ'יברון — ^ למעלה, v למטה.

    הגרסה הקודמת הייתה קו עם ראש משולש שזוויתו השתנתה עם הקצב. היא נראתה
    כמו סקיצה ולא כמו סימן. צ'יברון הוא צורה אחת מוכרת שנקראת מיד, והמספר
    לידו נושא את העוצמה.
    """
    if pct >= 0:
        pts = 'M7,25 L19,12 L31,25'
    else:
        pts = 'M7,13 L19,26 L31,13'
    return ('<svg width="%d" height="%d" viewBox="0 0 38 38" class="chev">'
            '<path d="%s" fill="none" stroke="%s" stroke-width="5.5" '
            'stroke-linecap="round" stroke-linejoin="round"/></svg>'
            % (size, size, pts, color))


def s_assets(d):
    """הנכסים והצמיחה בשקף אחד.

    היו שני שקפים — «הנכסים» ו«הצמיחה» — ושניהם דירגו את אותן רשתות ונשאו
    את אותה רצועת 1.2←1.9. ההבדל האמיתי היחיד היה עמודת העוקבים, ולכן הם
    מאוחדים: כל רשת בשורה אחת, עם הקהל שלה, הצפיות אשתקד והשנה, והשינוי.
    """
    rows = []
    for p in d['platforms']:
        blk = d['platforms'][p]
        ytd = {}
        for m in blk.get('monthly_views', []):
            if m['month'][5:7] <= '07':
                ytd[m['month'][:4]] = ytd.get(m['month'][:4], 0) + m['views']
        yrs = sorted(ytd)
        prev = ytd[yrs[-2]] if len(yrs) >= 2 else None
        cur = ytd[yrs[-1]] if yrs else (blk.get('views') or 0)
        pct = ((cur / prev - 1) * 100) if prev else None
        rows.append((p, d['followers'].get(p, 0), prev, cur, pct,
                     blk.get('coverage_note', '')))
    rows.sort(key=lambda r: -(r[3] or 0))

    out = ''
    for p, fol, prev, cur, pct, note in rows:
        if pct is None:
            change = ('<div class="axnote">%s</div>'
                      % ('נמדד מ-21.6.2026 בלבד' if p == 'twitter'
                         else 'אין נתוני צפיות'))
            nums = ('' if p == 'whatsapp' else
                    '<div class="axv now partial">%s<span class="pw2">שישה שבועות</span></div>'
                    % heb(cur))
            out += ('<div class="axrow thin"><div class="axl">%s<div class="pn">%s</div></div>'
                    '<div class="axf">%s</div><div class="axspan">%s%s</div></div>'
                    % (icon(p, 46), esc(HEB[p]), num(fmt(fol)), nums, change))
            continue
        col = '#186a2e' if pct >= 0 else '#b42318'
        out += ('<div class="axrow"><div class="axl">%s<div class="pn">%s</div></div>'
                '<div class="axf">%s</div>'
                '<div class="axv">%s</div>'
                '<div class="axv now">%s</div>'
                '<div class="axc">%s<span style="color:%s">%s</span></div></div>'
                % (icon(p, 46), esc(HEB[p]), num(fmt(fol)), heb(prev), heb(cur),
                   _chev(pct, col), col, num('%+.0f%%' % pct)))

    ytd = d.get('views_by_year_ytd') or {}
    yrs = sorted(ytd)
    tot = ''
    if len(yrs) >= 2:
        pct = (ytd[yrs[-1]] / ytd[yrs[-2]] - 1) * 100
        tot = ('<div class="axrow total"><div class="axl">סך הכל</div>'
               '<div class="axf">%s</div><div class="axv">%s</div>'
               '<div class="axv now">%s</div>'
               '<div class="axc">%s<span>%s</span></div></div>'
               % (num(fmt(sum(d['followers'].values()))), heb(ytd[yrs[-2]]),
                  heb(ytd[yrs[-1]]), _chev(pct, ACCENT, 44),
                  num('%+.0f%%' % pct)))

    ed = ((d.get('editorial') or {}).get('titles') or {})
    body = [head(ed.get('assets', 'שש רשתות, 3.8 מיליון עוקבים'),
                 'הצפיות נמדדות בחלון ינואר–יולי בכל שנה', ''),
            '<div class="axhead"><div>רשת</div><div>עוקבים היום</div>'
            '<div>צפיות 2025</div><div>צפיות 2026</div><div>שינוי</div></div>',
            '<div class="axlist">%s%s</div>' % (out, tot)]
    return slide('הנכסים', 'הנכסים והצמיחה שלהם בשקף אחד.', ''.join(body),
                 section='הנכסים והצמיחה')


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
.clab{font-size:26px;font-weight:600;color:%(m)s;margin-top:16px}
.chero{text-align:center}
.cbig{font-family:'SF Mono',Menlo,monospace;font-size:250px;font-weight:700;line-height:.82;
  letter-spacing:-.04em}
.cbig span{font-size:118px;color:%(a)s}
.cfol{font-size:34px;color:#404040;margin-top:18px}
.cright{text-align:left}
.cnum{font-size:76px;font-weight:700;line-height:1}
.cstrip{display:flex;gap:44px;align-items:center;flex-wrap:wrap;justify-content:center}
.cp{display:flex;align-items:center;gap:13px}
.cpn{font-size:21px;font-weight:700}
.cpf{font-size:19px;color:%(m)s}
/* דירוג */
.bhead{display:grid;grid-template-columns:260px 1fr 230px 150px;gap:24px;
  padding-bottom:12px;border-bottom:1px solid %(g)s;font-size:14px;font-weight:700;
  color:%(m)s;letter-spacing:.03em}
.bhead div:last-child,.bhead div:nth-child(3){text-align:left}
.blist{flex:1;display:flex;flex-direction:column;justify-content:space-around;padding:8px 0}
.brow{display:grid;grid-template-columns:260px 1fr 230px 150px;gap:24px;align-items:center}
.barlist{display:flex;flex-direction:column;gap:18px;margin-top:6px}
.bl .pl,.pl{display:flex;align-items:center;gap:16px}
.pn{font-size:27px;font-weight:700}
.btrack{position:relative;height:40px;background:#e6e6e6;border-radius:8px;overflow:hidden}
.bfill{position:absolute;top:0;right:0;height:100%%;border-radius:8px}
.bnone{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;
  font-size:16px;color:#9a9a9a;letter-spacing:.03em}
.bv{font-size:29px;font-weight:700;text-align:left;white-space:nowrap}
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
.panel.chartpanel{flex:1;display:flex;flex-direction:column;justify-content:center;
  margin-top:16px;margin-bottom:16px}
.grid2.deep{flex:none;gap:22px}
.grid1.deep{display:grid;grid-template-columns:1fr}
.grid1.deep .panel{margin-top:0}
.grid2.deep .panel{margin-top:0;display:flex;flex-direction:column}
.topitem{margin-top:auto;padding-top:14px;border-top:1px solid %(g)s}
.tl{font-size:14px;color:%(m)s;font-weight:700}
.tx{font-size:18px;line-height:1.35;margin:5px 0 6px}
.tn{font-size:20px;font-weight:700}
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
/* כרומת מסמך */
.chrome{position:absolute;bottom:34px;left:84px;right:84px;display:flex;
  justify-content:space-between;align-items:baseline;font-size:15px;color:#a8a8a8}
.chrome .pg{font-family:'SF Mono',Menlo,monospace;font-size:17px;font-weight:700;color:#8a8a8a}
/* תוכן עניינים */
.toc{flex:1;display:flex;flex-direction:column;justify-content:center;max-width:1100px}
.tochead{margin:6px 0 40px;font-size:76px;font-weight:900;letter-spacing:-.02em}
.tocrow{display:grid;grid-template-columns:64px auto 1fr;gap:20px;align-items:baseline;
  padding:18px 0;border-bottom:1px solid %(g)s}
.tocn{font-family:'SF Mono',Menlo,monospace;font-size:26px;font-weight:700;color:%(a)s}
.toct{font-size:34px;font-weight:700}
/* הישגים */
.achwrap{flex:1;display:flex;flex-direction:column;justify-content:center;gap:26px}
.achgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:26px}
.ach{background:#fff;border:1px solid %(g)s;border-radius:14px;padding:40px}
.achv{font-size:88px;font-weight:700;line-height:1;letter-spacing:-.03em}
.achl{font-size:27px;font-weight:700;margin-top:16px}
.achs{font-size:18px;color:%(m)s;margin-top:5px}
.achrow2{display:grid;grid-template-columns:repeat(3,1fr);gap:22px}
.ach2{background:#fff;border:1px solid %(g)s;border-radius:14px;padding:22px 26px;
  display:flex;align-items:baseline;gap:14px}
.ach2v{font-size:36px;font-weight:700;line-height:1;white-space:nowrap}
.ach2l{font-size:17px;color:#444;font-weight:700;line-height:1.25}
/* צמיחה */
.axhead{display:grid;grid-template-columns:360px 300px 260px 300px 300px;gap:26px;
  font-size:15px;font-weight:700;color:%(m)s;letter-spacing:.03em;
  padding-bottom:14px;border-bottom:1px solid %(g)s}
.axhead div:nth-child(5){text-align:right}
.axlist{flex:1;display:flex;flex-direction:column;justify-content:space-evenly;padding:6px 0}
.axrow{display:grid;grid-template-columns:360px 300px 260px 300px 300px;gap:26px;
  align-items:center;padding:18px 0;border-bottom:1px solid #ededed}
.axrow.total{border-top:2px solid %(ink)s;border-bottom:none;
  padding-top:26px;margin-top:8px}
.axrow.total .axl{font-size:28px;font-weight:900}
.axl{display:flex;align-items:center;gap:18px}
.axf{font-size:27px;font-weight:700;text-align:right;color:#333}
.axv{font-size:30px;font-weight:700;color:%(m)s;text-align:right}
.axv.now{font-size:38px;color:%(ink)s}
.axv.now .pw2{display:block;font-size:14px;font-weight:400;
  color:#8a4b00;margin-top:4px}
.axc{display:flex;align-items:center;gap:12px;justify-content:flex-end;
  direction:ltr}
.axc span{font-size:42px;font-weight:900}
.axrow.total .axc span{color:%(a)s}
.chev{display:block;flex:none}
.axspan{grid-column:span 3;display:grid;grid-template-columns:260px 300px 300px;
  gap:26px;align-items:center}
.axspan .axv{grid-column:2}
.axspan .axnote{grid-column:3;justify-self:end}
.axrow.thin{color:#666}
.axnote{font-size:19px;color:#8a4b00;background:#fff6e8;border-radius:8px;padding:8px 14px}
.axrow.thin .axv.now{margin-inline-end:8px}
.ghero{display:grid;grid-template-columns:auto auto auto 1fr;gap:34px;align-items:center;
  background:#fff;border:1px solid %(g)s;border-radius:16px;padding:30px 40px;margin-bottom:26px}
.ghy{font-size:18px;color:%(m)s;font-weight:700;margin-bottom:4px}
.ghv{font-size:46px;font-weight:700;line-height:1;color:%(m)s}
.ghv.big{font-size:64px;color:%(ink)s}
.ghp{font-size:64px;font-weight:900;color:%(a)s;text-align:left}
.gharw{opacity:.9}
.grlist{flex:1;display:flex;flex-direction:column;justify-content:space-evenly}
.gr{display:grid;grid-template-columns:280px 220px 110px 240px 1fr;gap:20px;align-items:center}
.grl{display:flex;align-items:center;gap:16px}
.grv{font-size:34px;font-weight:700;color:%(m)s;text-align:left}
.grv.now{font-size:42px;color:%(ink)s;text-align:right}
.grarw{display:flex;justify-content:center}
.arw{display:block}
.grp{font-size:46px;font-weight:900;text-align:left}
/* מפריד פרק */
.divider{flex:1;display:flex;flex-direction:column;justify-content:center;gap:18px}
.dvh{margin:0;font-size:92px;font-weight:900;letter-spacing:-.02em;line-height:1}
.dvsub{font-size:28px;color:#404040}
.dvrow{display:flex;gap:52px;margin-top:36px}
.dvi{display:flex;align-items:center;gap:14px;font-size:26px;font-weight:700}
.note2{font-size:19px;line-height:1.55;color:#333;margin-bottom:18px}
.thesis{flex:1;display:flex;flex-direction:column;justify-content:space-evenly;gap:20px;padding:10px 0}
.tclaim{margin:6px 0 0;font-size:86px;font-weight:900;letter-spacing:-.02em;line-height:1}
.tsub{font-size:24px;color:%(m)s}
.tbars{display:flex;flex-direction:column;gap:20px;margin-top:6px}
.trow2{display:grid;grid-template-columns:78px 1fr 130px;gap:22px;align-items:center}
.ty{font-size:30px;font-weight:700;color:%(m)s}
.tbar{display:flex;gap:3px;height:86px;border-radius:10px;overflow:hidden}
.tseg{display:flex;align-items:center;justify-content:center;color:#fff;overflow:hidden}
.tseg span{text-align:center;font-size:19px;font-weight:700;line-height:1.2}
.tseg i{display:block;font-style:normal;font-size:26px;font-weight:900}
.tt2{font-size:32px;font-weight:700;text-align:left}
.tfacts{display:grid;grid-template-columns:repeat(3,1fr);gap:34px;
  border-top:1px solid %(g)s;padding-top:26px}
.tf{font-size:22px;line-height:1.5;color:#333}
.tf b{font-size:26px;color:%(ink)s}
.fair{font-size:26px;font-weight:700;line-height:1;margin-bottom:6px;white-space:nowrap}
.fair span{display:block;font-size:13px;color:%(m)s;font-weight:400;margin-top:2px}
.fair.up{color:#186a2e}
.fair.down{color:#b42318}
.cd{margin-inline-start:auto;text-align:left;font-size:24px;font-weight:700;white-space:nowrap}
.cd span{display:block;font-size:13px;color:%(m)s;font-weight:400;margin-top:1px}
.cd.up{color:#186a2e}
.cd.down{color:#b42318}
.elegend{display:flex;gap:26px;margin-bottom:14px;font-size:16px;color:#444}
.elg{display:flex;align-items:center;gap:8px}
.elg i{width:18px;height:0;border-top:2px dashed %(a)s;display:inline-block}
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
.ev{display:grid;grid-template-columns:250px 1fr 250px 330px;gap:22px;align-items:center}
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
.evv{text-align:left;white-space:nowrap}
.evv .mono{font-size:34px;font-weight:700;line-height:1;white-space:nowrap}
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
    # הסדר הוא הסיפור: מי אנחנו, מה המספרים, מה הניע אותם, ואז לעומק
    # בכל רשת. כל מה שנוגע לרשת אחת יושב בשקף שלה ולא חוזר במקום אחר.
    # סיכום הישגים, לא ניתוח: קודם כמה השגנו, אחר כך כמה גדלנו, ואז לעומק.
    # הרשתות לפי גודל — מובילים בגדולה ולא בוותיקה.
    slides = [
        s_cover(d),
        s_achievements(d),
        s_assets(d),
        s_events(d),
        s_divider(d),
        s_platform(d, 'facebook'),
        s_platform(d, 'tiktok'),
        s_platform(d, 'instagram'),
        s_platform(d, 'youtube'),
        s_thin(d),
        s_appendix(d),
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
