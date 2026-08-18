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
# קונבנציית הצבע לשנים — **אחת לכל הדק, בלי יוצא מן הכלל.** שנה מקבלת צבע ולא
# מיקום ברשימה, כדי ש-2026 תהיה אדומה גם כשהיא ראשונה וגם כשהיא אחרונה.
YEAR = {'2024': '#bfbfbf', '2025': '#404040', '2026': '#FF3300'}
# נתיב הסימן, מ-`weekly_deck/design/assets/kan-news-mark-white.svg` (מעוקב ב-git).
# מוטמע ולא מקושר, כדי שה-PDF לא ייפול על קובץ חיצוני שלא נמצא. ה-PNG של
# הוורדמארק המלא לא נכנס לריפו: השער כבר אומר «כאן חדשות בסושיאל» ב-104px.
MARK_PATH = ('M.89,77.51v418.03c0,42.22,34.22,76.44,76.44,76.44h418.03c42.22,0,'
             '76.44-34.22,76.44-76.44V77.51c0-42.22-34.22-76.44-76.44-76.44H77.33'
             'C35.11,1.07.89,35.3.89,77.51ZM282.6,373.8l-34.17,34.17-87.28-87.27'
             '-34.17-34.17,34.17-34.17,87.28-87.27,34.17,34.17-87.28,87.27,87.28,'
             '87.27ZM377.36,450.54h-47.38V122.51h47.38v328.03Z')


def mark(size=64, color='#ffffff'):
    return ('<svg viewBox="0 0 571.48 572.69" width="%d" height="%d" '
            'aria-label="כאן"><path fill="%s" d="%s"/></svg>'
            % (size, size, color, MARK_PATH))
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


def heb_plain(n):
    """כמו `heb()` אבל **טקסט בלבד** — לתוויות שיושבות בתוך SVG.

    `heb()` עוטף את המספר ב-`num()`, כלומר ב-`<span>`. בתוך `<text>` של SVG
    התגית נדפסת כתו-אחר-תו על השקף. זה קרה בגרפי אינסטגרם וטיקטוק.
    """
    n = float(n or 0)
    if n >= 1e9:
        v, w = '%.1f' % (n / 1e9), ' מיליארד'
    elif n >= 1e6:
        v, w = '%.1f' % (n / 1e6), ' מיליון'
    else:
        return format(int(n), ',')
    return (v.rstrip('0').rstrip('.') if '.' in v else v) + w


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

EVENT_TAG = {'החזרת חללי משפחת ביבס ושחרור אברה מנגיסטו': 'משפחת ביבס'}


def _event_tag(name):
    """שם קצר לתווית שיושבת על הגרף. «מבצע «עם כלביא» — מלחמת 12 הימים» לא
    נכנס מעל נקודה בגרף; «עם כלביא» כן, והקורא יודע על מה מדובר.

    חיתוך לפי מילים לבד לא מספיק: «החזרת חללי משפחת ביבס» נחתך ל«החזרת חללי
    משפחת», שאינו שם של שום דבר. לשמות כאלה יש מפה מפורשת."""
    if name in EVENT_TAG:
        return EVENT_TAG[name]
    if '«' in name and '»' in name:
        return name[name.index('«') + 1:name.index('»')]
    for sep in (' — ', ' - ', ','):
        if sep in name:
            name = name.split(sep)[0]
    return ' '.join(name.split()[:3])


def event_chart(points, marks, color, key='views', h=330, fmt_val=None,
                zero_base=True, axis=True):
    """סדרה חודשית עם **שמות האירועים כתובים על הגרף**, לא במקרא מתחתיו.

    התוויות מתחלפות בין שתי שורות גובה כדי שלא ידרסו זו את זו, ומחוברות
    לנקודה בקו מקווקו. ה-SVG כאן **אינו** `preserveAspectRatio="none"` —
    מתיחה לא אחידה הייתה מעוותת את הטקסט שבתוכו.
    """
    if not points:
        return ''
    W, H = 1000, h
    vals = [p[key] for p in points]
    hi = max(vals) or 1
    # סדרת מלאי (גודל קהל) לא מתקרבת לאפס, ועל ציר שמתחיל באפס היא קו ישר.
    # בסיס שאינו אפס מותר לקו — בתנאי ששני קצות הציר מסומנים, ולכן `ends`.
    lo = 0 if zero_base else min(vals) - (hi - min(vals)) * .35
    span = (hi - lo) or 1
    pad_t = 96 if marks else 18
    pad_b, pad_l = (48 if axis else 16), 8
    ih, iw = H - pad_t - pad_b, W - pad_l
    step = iw / max(len(vals) - 1, 1)
    pts = [(pad_l + i * step, pad_t + ih - ((v - lo) / span) * ih)
           for i, v in enumerate(vals)]
    base = pad_t + ih
    o = ['<svg viewBox="0 0 %d %d" class="evch" role="img">' % (W, H)]
    for frac in (0, .5, 1):
        y = pad_t + ih * frac
        o.append('<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="%s" '
                 'stroke-width="1"/>' % (pad_l, y, W, y, GRID))
    o.append('<path d="M%.1f,%.1f %s L%.1f,%.1f Z" fill="%s" opacity=".14"/>'
             % (pts[0][0], base, ' '.join('L%.1f,%.1f' % xy for xy in pts),
                pts[-1][0], base, color))
    o.append('<polyline points="%s" fill="none" stroke="%s" stroke-width="3" '
             'stroke-linejoin="round"/>'
             % (' '.join('%.1f,%.1f' % xy for xy in pts), color))

    idx = {p['month']: i for i, p in enumerate(points)}
    hit = [(idx[m['month']], m) for m in (marks or []) if m['month'] in idx]
    hit.sort()
    for n, (i, mk) in enumerate(hit):
        mx, my = pts[i]
        ly = 26 if n % 2 == 0 else 66          # שתי שורות לסירוגין
        o.append('<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="%s" '
                 'stroke-width="1.5" stroke-dasharray="4 4" opacity=".5"/>'
                 % (mx, ly + 6, mx, my - 8, ACCENT))
        o.append('<circle cx="%.1f" cy="%.1f" r="7" fill="%s" stroke="#fff" '
                 'stroke-width="3"/>' % (mx, my, ACCENT))
        # התווית מתיישרת לפי המקום בגרף כדי לא לגלוש מהקצוות
        anchor = 'start' if mx < 120 else ('end' if mx > W - 120 else 'middle')
        o.append('<text x="%.1f" y="%.1f" text-anchor="%s" fill="%s" '
                 'font-size="21" font-weight="900" direction="rtl">%s</text>'
                 % (mx, ly, anchor, ACCENT, esc(_event_tag(mk['label']))))
        if fmt_val:
            o.append('<text x="%.1f" y="%.1f" text-anchor="%s" fill="#555" '
                     'font-size="18" font-weight="700">%s</text>'
                     % (mx, ly + 22, anchor, esc(fmt_val(vals[i]))))
    # ציר זמן אמיתי: מפריד בתחילת כל שנה, ושם השנה מתחת לטווח שלה. שתי
    # תוויות בקצוות לא אומרות לקורא איפה הוא נמצא על 31 חודשים.
    spans = {} if axis else None
    for i, pt in enumerate(points if axis else []):
        spans.setdefault(pt['month'][:4], []).append(i)
    for y, ids in sorted((spans or {}).items()):
        x0, x1 = pts[ids[0]][0], pts[ids[-1]][0]
        if ids[0] > 0:
            o.append('<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="%s" '
                     'stroke-width="1"/>' % (x0, pad_t, x0, base + 8, '#c9c9c9'))
        part = ' (עד יולי)' if ids[-1] == len(points) - 1 and             points[-1]['month'][5:] != '12' and y != points[0]['month'][:4] else ''
        o.append('<text x="%.1f" y="%.1f" text-anchor="middle" fill="%s" '
                 'font-size="23" font-weight="900" direction="rtl">%s%s</text>'
                 % ((x0 + x1) / 2, base + 34, YEAR.get(y, MUTED), y, part))
    o.append('</svg>')
    return '<div class="evchw">%s</div>' % ''.join(o)


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


def head(title, kicker='', right='', plat=None):
    """הדקדוק שחוזר בכל שקף תוכן: קיקר → כותרת → קו אדום → (ואז הגוף).

    הקו יושב **מתחת** לכותרת ולא לצידה. זו לא בחירה אסתטית: לצד הכותרת הוא
    חלק ממנה ונע עם אורכה, ומתחתיה הוא אותו קו באותו מקום בכל שקף — וזה מה
    שהופך אוסף שקפים לדק אחד. `plat` מוסיף אייקון רשת לקיקר.
    """
    kick = ''
    if kicker:
        kick = ('<div class="kicker">%s<span>%s</span></div>'
                % (icon(plat, 30) if plat else '', esc(kicker)))
    return ('<div class="shead"><div class="stitle">%s<h2>%s</h2>'
            '<div class="rule"></div></div><div class="sright">%s</div></div>'
            % (kick, esc(title), right))


def foot(text):
    """ההסתייגות האפורה בתחתית. נצמדת לרצפה בכל שקף, לא לסוף התוכן."""
    return '<div class="foot">%s</div>' % text


def divider_stats(d, p):
    """שלושת מספרי-העל של מפריד רשת: קהל, צפיות, וצמיחה.

    השלישי היה קודם נפח הפרסום, והוא מדד כמה עבדנו ולא כמה הצלחנו — פתיחת
    פרק על המספר החלש בשלישייה. עכשיו הוא גידול העוקבים, למי שיש לו אחד.
    לטיקטוק אין: מלאי העוקבים שלו נמדד רק מ-21.7.2026, ולכן שם נשאר הנפח —
    ולא נקודת פתיחה מומצאת כדי שהשקף ייראה אחיד.

    הצפיות מ-`monthly_views` ולא מ-`yearly`: זו ההגדרה היחידה בדק
    (`total_views`), והיא כבר חתוכה לחודשים תקפים. ל-`yearly` של טיקטוק
    ויוטיוב יש גם שנים שקדמו לתקופה.
    """
    b = d['platforms'][p]
    views = sum(m['views'] for m in b.get('monthly_views', []))
    row = next((r for r in ((d.get('assets') or {}).get('rows') or [])
                if r['platform'] == p and 'gain' in r), None)
    out = [(num(fmt(d['followers'][p])), esc('עוקבים ביולי 2026')),
           (heb(views), esc('צפיות'))]
    if row:
        since = ('מאז ינואר 2024' if row['since'] == '2024-01'
                 else 'מ%s' % _he_month(row['since']))
        out.append((num('+%s' % fmt(row['gain'])),
                    esc('עוקבים חדשים %s · ' % since)
                    + num('%+.1f%%' % row['pct'])))
    else:
        posts = sum(v.get('posts', 0) for y, v in (b.get('yearly') or {}).items()
                    if y in ('2024', '2025', '2026'))
        out.append((num(fmt(posts)), esc(UNIT.get(p, 'פריטים') + ' פורסמו')))
    return out


def divider(plat, kicker, stats, notes=''):
    """מפריד פרק — שחור, אייקון גדול, שם הרשת, ושלושה מספרי-על.

    `stats` היא רשימת (ערך, תווית). שלושה זה המקסימום שנקרא ממרחק; אם לרשת
    אין שלושה מספרים ראויים, היא לא מקבלת מפריד אלא שורה בשקף משותף.
    """
    cells = ''.join('<div><div class="dvn">%s</div><div class="dvl">%s</div></div>'
                    % (v, l) for v, l in stats)
    return slide(HEB[plat], notes or 'מעבר לפרק %s.' % HEB[plat],
                 '<div class="dv"><div class="kicker"><span>%s</span></div>'
                 '<div class="dvh">%s<h2>%s</h2></div><div class="rule"></div>'
                 '<div class="dvs">%s</div></div>' % (esc(kicker), icon(plat, 116),
                                                      esc(HEB[plat]), cells),
                 bg='#000000', section='', chrome=False)


def total_views(d):
    """סך הצפיות — **הגדרה אחת לכל המסמך**.

    השער חישב מ-`yearly` והשקף השלישי מ-`monthly_views`, ושניהם הופיעו על
    אותו מסמך: 4.78B מול 4.80B. הסדרה החודשית היא הבסיס הנכון — היא כבר
    מסוננת לחודשים תקפים, חתוכה ביולי, וביוטיוב מגיעה מ-Studio.
    """
    return sum(sum(m['views'] for m in blk.get('monthly_views', []))
               for blk in d['platforms'].values() if blk.get('monthly_views'))


def s_cover(d):
    """שער שחור: הסימן, הכותרת, ומספר גיבור אחד.

    רצועת שש הרשתות שהייתה כאן ירדה — היא הייתה שקף «הנכסים» בזעיר אנפין,
    ואותם שישה מספרים בדיוק חוזרים שני שקפים אחר כך בגודל קריא.

    התווית של מספר הגיבור אומרת **מאיזה תאריך הצפיות נמדדות**, ולא רק «צפיות
    בכל הרשתות». התקופה בכותרת מתחילה בינואר 2024 כי הפרסומים, הלייקים
    והתגובות באמת מתחילים שם; הצפיות הן המדד היחיד שמטא הגדירה מחדש באמצע,
    והמקום להגיד את זה הוא צמוד למספר ולא בהערת שוליים בסוף.
    """
    ed = d['editorial']
    tot = total_views(d)
    body = (
        '<div class="cover">'
        '<div>%s'
        '<div class="kicker"><span>סיכום פעילות</span></div>'
        '<h1>%s</h1><div class="rule"></div>'
        '<div class="csub">%s</div></div>'
        '<div class="chero">'
        '<div class="cbig">%s</div>'
        '<div class="clab">צפיות, ו-%s עוקבים</div>'
        # המשפט לא מסתיים בספרה בכוונה: ב-RTL הנקודה שאחרי «2024» קופצת לתחילת
        # השורה הבאה כשהטקסט נשבר. סיום במילה מונע את זה בלי תווי כיוון.
        '<div class="cfol">הצפיות נמדדות מספטמבר 2024, אחרי שמטא הגדירה אותן '
        'מחדש; פרסומים, לייקים ותגובות — מינואר 2024 ואילך</div>'
        '</div></div>'
        % (mark(96), esc(ed.get('cover_title')), esc(ed.get('cover_subtitle')),
           heb(tot), num(fmt(sum(d['followers'].values())))))
    return slide('שער', 'מספר גיבור אחד: סך הצפיות, והקהל כשורה מתחתיו.',
                 body, '#000000', chrome=False)


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
            '<div class="ytr" style="grid-template-columns:repeat(%d,1fr)">%s</div>'
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
    """X וערוץ הוואטסאפ — שתי הרשתות שאין להן היסטוריה.

    השקף הזה החזיק קודם דירוג של שש הרשתות לפי גודל קהל, כלומר את שקף
    «הנכסים» בפעם השנייה, וסביבו שתי טענות שאין להן נתון: «הנכס הצעיר»
    (איננו יודעים מתי הערוץ נפתח) ו«כבר גדול מ-39% ממנויי יוטיוב» — יחס בין
    שני נכסים שאין ביניהם קשר. שניהם ירדו. מה שנשאר הוא מה שבאמת יש: כמה
    עוקבים, מה נמדד, ומאיזה תאריך.
    """
    tw, wa = d['platforms']['twitter'], d['platforms']['whatsapp']
    cards = [
        ('twitter', 'X', num(fmt(d['followers']['twitter'])), 'עוקבים',
         '%s ציוצים · %s צפיות' % (num(fmt(tw.get('posts', 0))),
                                   heb(tw.get('views', 0))),
         'נמדד %s – %s' % (_he_date(tw['from']), _he_date(tw['to']))
         if tw.get('from') else '',
         tw.get('coverage_note', '')),
        ('whatsapp', HEB['whatsapp'], num(fmt(d['followers']['whatsapp'])),
         'עוקבים', '', '', wa.get('coverage_note', '')),
    ]
    out = ''
    for plat, name, big, lab, extra, when, note in cards:
        out += ('<div class="tbox thin"><div class="tbh">%s<span>%s</span></div>'
                '<div class="tbv">%s</div><div class="tbs">%s</div>'
                '%s%s<div class="tbn">%s</div></div>'
                % (icon(plat, 34), esc(name), big, esc(lab),
                   '<div class="thx">%s</div>' % extra if extra else '',
                   '<div class="thw">%s</div>' % esc(when) if when else '',
                   esc(note)))
    body = [head('X וערוץ הוואטסאפ', 'שתי הרשתות שאין להן היסטוריה'),
            '<div class="thgrid">%s</div>' % out,
            foot('לשתי הרשתות האלה אין סדרה לאורך זמן, ולכן אין כאן גרף ואין '
                 'אחוזי שינוי. הן מופיעות בדק במספר העדכני שלהן ובתאריך שממנו '
                 'הוא נמדד — ותו לא.')]
    return slide('X ווואטסאפ', 'שתי רשתות בלי היסטוריה. אומרים את זה ולא מותחים.',
                 ''.join(body), section='רשת אחר רשת')


def _he_date(iso):
    """2026-02-25 -> 25.2.26"""
    y, m, dd = iso.split('-')
    return '%d.%d.%s' % (int(dd), int(m), y[2:])


HE_MONTHS = ('ינואר', 'פברואר', 'מרץ', 'אפריל', 'מאי', 'יוני', 'יולי',
             'אוגוסט', 'ספטמבר', 'אוקטובר', 'נובמבר', 'דצמבר')


def _he_month(ym):
    """2025-08 -> אוגוסט 2025"""
    y, m = ym.split('-')
    return '%s %s' % (HE_MONTHS[int(m) - 1], y)


COVERAGE = [
    ('פייסבוק', 'ok:מספטמבר', 'ok:מלא', 'ok:מלא',
     'ייצוא Business Suite ל-2024 ו-2026; 2025 נמשך מה-Graph API'),
    ('אינסטגרם', 'ok:מספטמבר', 'ok:מלא', 'ok:מלא', 'שלושה ייצואי Business Suite'),
    ('יוטיוב', 'ok:מלא', 'ok:מלא', 'ok:מלא',
     'ייצוא Studio — צפיות, זמן צפייה ומנויים יומיים; ספירת הסרטונים מה-API'),
    # הפיד לא נעצר בפברואר 2025 — התקרה שלנו נגמרה. ריצה 32012205657 עם 700
    # עמודים החזירה 6,179 פריטים עד 26.11.2020, ונעצרה ב-end_of_feed.
    ('טיקטוק', 'ok:מלא', 'ok:מלא', 'ok:מלא', 'TikHub — כל הפיד, מנובמבר 2020'),
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
    pt = d.get('period_totals') or {}
    tt_total = sum(m['views'] for m in d['platforms']['tiktok'].get('monthly_views', []))

    # שלושה מספרים ראשיים ושלושה תומכים. קודם כל השישה היו באותו גודל,
    # כך ש-4.8 מיליארד ו-337 אלף נראו שווי משקל.
    #
    # התקופה על כרטיס הצפיות היא **ספטמבר** 2024 ולא ינואר, כמו בשער. זה המדד
    # היחיד שמטא הגדירה מחדש באמצע, ושתי התוויות חייבות לומר אותו דבר.
    #
    # במקום «הצטרפויות לפייסבוק ברוטו» יושב כאן נפח הפרסום: הברוטו היה 337,816
    # בזמן ששקף «הנכסים» מציג 253,362 נטו לאותו נכס עצמו, שני שקפים אחריו.
    # שני מספרים לאותה עובדה על מסמך אחד זה מה שההנהלה תתפוס, ובצדק.
    main = [
        (heb(tot_views), 'צפיות בכל הרשתות', 'ספטמבר 2024 – יולי 2026'),
        (num('%+.0f%%' % growth), 'גידול בצפיות', 'ינואר–יולי, 2026 מול 2025'),
        (num(fmt(pt.get('posts', 0))), 'פריטים פורסמו', 'בארבע הרשתות שנמדדות'),
    ]
    big_day = (d.get('big_days') or {}).get('days', [{}])[0]
    sub = [
        (heb((pt.get('daily') or {}).get('2026', 0)), 'צפיות למה שפורסם ביום · 2026'),
        (heb(pt.get('engagement', 0)), 'לייקים, תגובות ושיתופים'),
        (heb(pt.get('watch_hours', 0)), 'שעות צפייה ביוטיוב ובפייסבוק'),
        (heb(big_day.get('views', 0)),
         'ביום השיא · %s%s' % (_he_date(big_day.get('date', '')),
                               ' · %s' % big_day['event'] if big_day.get('event') else '')),
        (signed(ys['end'] - ys['start']),
         esc('מנויי יוטיוב · גידול של ')
         + num('%.0f%%' % ((ys['end'] / ys['start'] - 1) * 100))),
        (num(fmt(sum(d['followers'].values()))), esc('עוקבים בשש הרשתות')),
    ]
    sub = [(v, l if '<' in l else esc(l)) for v, l in sub]
    cards = ''.join(
        '<div class="ach"><div class="achv">%s</div>'
        '<div class="achl">%s</div><div class="achs">%s</div></div>' % (v, esc(lab), esc(s2))
        for v, lab, s2 in main)
    subs = ''.join(
        '<div class="ach2"><div class="ach2v">%s</div><div class="ach2l">%s</div></div>'
        % (v, lab) for v, lab in sub)
    ed = ((d.get('editorial') or {}).get('titles') or {})
    body = [head(ed.get('achievements', 'שנתיים של צמיחה'), 'הישגי התקופה', ''),
            '<div class="achwrap tight"><div class="achgrid">%s</div>'
            '<div class="achrow2">%s</div></div>' % (cards, subs),
            foot('טיקטוק לבדה הביאה %s צפיות — הרשת השנייה בגודלה אחרי פייסבוק.'
                 % heb(tt_total))]
    return slide('הישגים', 'המספרים הגדולים של התקופה.', ''.join(body),
                 section='הישגי התקופה')


def s_published(d):
    """נפח הפרסום — **שנים מלאות**, ו-2026 מסומנת כחלקית.

    הגרסה הקודמת חתכה את כל השנים לינואר–יולי כדי שיהיו ברות-השוואה, וזה
    הוריד 6,104 פריטים מהפלט של המחלקה: 2024 הוצגה כ-8,022 במקום 14,126.
    לדק שמסכם שנתיים זו הקטנה של העבודה עצמה. לכן הטבלה מציגה מה שבאמת
    פורסם, העמודה של 2026 נושאת «עד 31.7», ו**האחוז היחיד שנאמר בשקף מחושב
    על ינואר–יולי בשתי השנים** ונאמר שם במפורש — לא נגזר מהעמודות שבטבלה.
    """
    yrs = ['2024', '2025', '2026']
    plats = ['facebook', 'instagram', 'youtube', 'tiktok']
    yearly = {p: (d['platforms'][p].get('yearly') or {}) for p in plats}
    ytd = {p: (d['platforms'][p].get('posts_ytd') or {}) for p in plats}

    def _n(p, y):
        return (yearly[p].get(y) or {}).get('posts', 0)

    tot = {y: sum(_n(p, y) for p in plats) for y in yrs}
    if not tot['2026']:
        return ''

    hd = ''.join('<div style="color:%s">%s%s</div>'
                 % (YEAR[y], y, '<em>עד 31.7</em>' if y == '2026' else '')
                 for y in yrs)
    rows = ''
    for p in sorted(plats, key=lambda x: -_n(x, '2025')):
        cells = ''.join('<div%s>%s</div>'
                        % (' class="hi"' if y == '2026' else '', num(fmt(_n(p, y))))
                        for y in yrs)
        rows += ('<div class="prow"><div class="pl">%s<span class="pn">%s</span></div>%s</div>'
                 % (icon(p, 38), esc(HEB[p]), cells))
    rows += ('<div class="prow ptot"><div class="pl"><span class="pn">סה"כ</span></div>%s</div>'
             % ''.join('<div%s>%s</div>' % (' class="hi"' if y == '2026' else '',
                                            num(fmt(tot[y]))) for y in yrs))

    # התמהיל בפאנל הצדדי נמדד על אותן שנים מלאות כמו הטבלה לידו
    fm = d['platforms']['facebook'].get('format_mix_full') or {}
    vid = fm.get('וידאו') or {}
    side = ''
    if vid.get('2026') and vid.get('2024'):
        side = ('<div class="ppanel"><div class="ptitle">הווידאו בפייסבוק חזר</div>'
                '<div class="pbig">%s</div>'
                '<div class="pnote">סרטוני פייסבוק בשבעת החודשים הראשונים של 2026 — '
                'יותר מ-%s בכל 2025 כולה, ובדרך ל-%s של 2024.</div></div>'
                % (num(fmt(vid['2026'])), num(fmt(vid.get('2025', 0))),
                   num(fmt(vid['2024']))))

    y0 = sum(ytd[p].get('2024', 0) for p in plats)
    y2 = sum(ytd[p].get('2026', 0) for p in plats)
    pace = round(tot['2026'] / 7 * 12)
    body = [head('מה פרסמנו', 'נפח הפרסום'),
            '<div class="pubwrap"><div class="ptable">'
            '<div class="prow phead"><div></div>%s</div>%s</div>%s</div>' % (hd, rows, side),
            foot('העמודות הן <b>שנים מלאות</b>; 2026 נחתכת ב-31 ביולי ולכן אינה '
                 'ברת-השוואה ישירה. על בסיס מקביל — ינואר–יולי בשתי השנים — הנפח '
                 'עלה מ-%s ל-%s, <b>%s</b>. בקצב הזה 2026 תסיים סביב %s פריטים, '
                 'יותר מכל שנה קודמת.'
                 % (num(fmt(y0)), num(fmt(y2)),
                    num('%+.0f%%' % ((y2 / y0 - 1) * 100)), num(fmt(pace))))]
    return slide('מה פרסמנו', 'נפח הפרסום בארבע הרשתות.',
                 ''.join(body), section='מבט עילי')


NET = {
    'facebook': {'title': 'הצמיחה, המעורבות והזמן',
                 'top': 'follows', 'boxes': ('engagement', 'perpost', 'watch')},
    'instagram': {'title': 'ההגעה והמעורבות',
                  'top': 'views', 'boxes': ('perpost', 'engagement', 'saves')},
    # ליוטיוב **לא** צפיות-לפריט: מדד הפריט מוטה נגד תוכן חדש, כי לתוכן של
    # 2024 היו שנתיים נוספות לצבור והזנב ביוטיוב אמיתי. `ytviews` מגיע
    # מ-Studio ומודד צפיות שהתרחשו בפועל.
    'youtube': {'title': 'המנויים והצפיות',
                'top': 'subs', 'boxes': ('ytviews',)},
    'tiktok': {'title': 'הצמיחה בצפיות',
               'top': 'views', 'boxes': ('views', 'perpost', 'likes')},
}
BOX = {
    'engagement': ('מעורבות', 'לייקים, תגובות ושיתופים'),
    'perpost': ('צפיות לפריט', 'ממוצע לפריט'),
    'watch': ('שעות צפייה', 'זמן שנצפה בפועל'),
    'saves': ('שמירות', 'פריטים שנשמרו'),
    'likes': ('לייקים', 'סך הלייקים'),
    'views': ('צפיות', 'סך הצפיות'),
    'ytviews': ('צפיות', 'צפיות שהתרחשו בפועל'),
    'ytsubs': ('מנויים חדשים', 'תוספת נטו'),
}


def s_network(d, p):
    """שקף העומק של רשת: הצמיחה למעלה, והמדדים למטה.

    הגרף העליון הוא **מה שהרשת יודעת למדוד לאורך זמן** — עוקבים חדשים
    בפייסבוק, מנויים ביוטיוב, וצפיות לחודש באינסטגרם ובטיקטוק, שבהן אין
    סדרת עוקבים היסטורית. עליו מסומנים אירועי החדשות, כהסבר לקפיצה.

    המשבצות תמיד מושוות **על חלון שווה** ל-2024. השוואה של שבעה חודשים
    לשנה שלמה מקטינה את הצמיחה, לא מגדילה אותה.
    """
    cfg = NET.get(p)
    b = d['platforms'][p]
    if not cfg:
        return ''
    marks = [{'month': w['peak_date'][:7], 'label': w.get('short') or w['name']}
             for w in ((d.get('events') or {}).get('windows') or [])]

    if cfg['top'] == 'follows':
        ser, key = (d.get('facebook_follows') or {}).get('monthly', []), 'follows'
        ttl = 'עוקבים חדשים לחודש'
    elif cfg['top'] == 'subs':
        raw = (d.get('youtube_subscribers') or {}).get('series', [])
        ser = [{'month': raw[i]['month'], 'gain': raw[i]['subs'] - raw[i - 1]['subs']}
               for i in range(1, len(raw))]
        key, ttl = 'gain', 'מנויים חדשים לחודש'
    else:
        ser, key = b.get('monthly_views', []), 'views'
        ttl = 'צפיות לחודש'
    if not ser:
        return ''
    # הציר יושב מתחת לגרף **התחתון** שקיים בפועל. כשאין גרף מלאי, הוא יושב
    # מתחת לגרף העליון — ולא מצויר כ"גרף בגובה 1 פיקסל" רק כדי לקבל ציר,
    # מה שהשאיר שרידי קו מרחפים והשמיט את תוויות השנים לגמרי.
    lvl = ((d.get('followers_series') or {}).get(p) or {})
    has_level = bool(lvl.get('series'))
    chart = event_chart(ser, marks, BRAND[p], key=key,
                        h=150 if has_level else 268, axis=not has_level,
                        fmt_val=(lambda v: heb_plain(v)) if key == 'views'
                        else (lambda v: format(int(v), ',')))

    level = ''
    if has_level:
        sr = lvl['series']
        note = ('נמדד יומית' if lvl.get('measured')
                else 'נגזר מההצטרפויות ביחס 0.750')
        level = ('<div class="nbt2">%s · %s ← %s <em>· %s</em></div>%s'
                 % ('מנויים' if p == 'youtube' else 'גודל הקהל',
                    num(fmt(sr[0]['value'])), num(fmt(sr[-1]['value'])), note,
                    event_chart(sr, None, INK, key='value', h=100,
                                zero_base=False, axis=True)))

    my = b.get('metrics_ytd') or {}
    ye = b.get('yearly') or {}
    # ליוטיוב ולטיקטוק אין `metrics_ytd` אבל יש `ytd` — ובלעדיו הצפיות של
    # 2026 (שבעה חודשים) נחלקו בשנת 2024 **המלאה** והראו «-41%» על ערוץ
    # שבפועל צומח. חלון שווה, בכל רשת, בלי יוצא מן הכלל.
    ty = b.get('ytd') or {}
    py = b.get('period_ytd') or {}
    subs_ytd = {}
    for r in (d.get('youtube_subscribers') or {}).get('series', []):
        if r['month'][5:] <= '07':
            subs_ytd.setdefault(r['month'][:4], []).append(r['subs'])

    def _val(kind, y):
        m, v, t = my.get(y) or {}, ye.get(y) or {}, ty.get(y) or {}
        if kind == 'perpost':
            src = t if t.get('views') else v
            n = (src.get('posts_in_views_window') or src.get('posts')
                 or v.get('posts_in_views_window') or v.get('posts') or 1)
            return src.get('views', 0) / n
        if kind == 'engagement':
            return m.get('engagement') or (v.get('likes', 0) + v.get('comments', 0)
                                           + v.get('shares', 0))
        if kind == 'watch':
            return m.get('watch_hours') or v.get('watch_hours', 0)
        if kind == 'ytviews':
            return (py.get(y) or {}).get('views', 0)
        if kind == 'ytsubs':
            a = subs_ytd.get(y) or []
            return (a[-1] - a[0]) if len(a) > 1 else 0
        if kind in m:
            return m[kind]
        if kind in t:
            return t[kind]
        return v.get(kind, 0)

    boxes = ''
    for kind in cfg['boxes']:
        v0, v2 = _val(kind, '2024'), _val(kind, '2026')
        if not v0 or not v2:
            continue
        pct = (v2 / v0 - 1) * 100
        col = '#186a2e' if pct >= 0 else '#b42318'
        title, sub = BOX[kind]
        big = (num(fmt(round(v2))) if kind in ('perpost', 'ytsubs') else heb(v2))
        basis = ('מול הממוצע ב-2024' if kind == 'perpost'
                 else 'מול אותם חודשים ב-2024')
        boxes += ('<div class="tbox"><div class="tbh">%s</div>'
                  '<div class="tbs">%s · 2026</div><div class="tbv">%s</div>'
                  '<div class="tbd">%s<span style="color:%s">%s</span>'
                  '<i>%s</i></div></div>'
                  % (esc(title), esc(sub), big,
                     _chev(pct, col, 30), col, num('%+.0f%%' % pct), esc(basis)))

    body = [head(cfg['title'], HEB[p], plat=p),
            '<div class="nbchart"><div class="nbt">%s · %s – %s</div>%s%s</div>'
            % (esc(ttl), esc(_he_month(ser[0]['month'])),
               esc(_he_month(ser[-1]['month'])), chart, level),
            '<div class="nbgrid">%s</div>' % boxes,
            foot('ההשוואה היא <b>על חלון שווה</b> ולא מול 2024 כשנה שלמה: 2026 '
                 'עוד לא נגמרה, וחלוקה של שבעה חודשים בשנים־עשר <b>מקטינה</b> '
                 'את הצמיחה. האירועים מסומנים בחודש השיא שלהם — הסבר לקפיצה, '
                 'לא טענה שהם לבדם יצרו אותה.')]
    return slide(HEB[p], 'עומק לרשת %s.' % HEB[p], ''.join(body), section=HEB[p])


def s_youtube(d):
    """יוטיוב: קו המנויים, ומה שבאמת ייחודי לו — צפיות מול זמן.

    השקף הזה החזיק קודם שני גרפים ששניהם לא אמרו כלום. המנויים החדשים לחודש
    נעים בין ~5,000 ל-11,000 ומציירים קו מתפתל בלי צורה, **וסימוני האירועים
    עליו ישבו על חודשים שאינם שיאים** — שחרור החטופים סומן על 6,622, פחות
    מחודשים רגילים רבים. גרף שמכריז על קפיצה שלא קרתה גרוע מאין גרף.

    מה שכן ייחודי ליוטיוב הוא הפער בין צפייה לזמן: שורטס מביאים שליש
    מהצפיות ו-4% מהזמן, ושידור חי אחוז אחד מהצפיות ו-7% מהזמן. זו הבחירה
    בין חשיפה לזמן, והיא לא קיימת בשום רשת אחרת בדק.
    """
    b = d['platforms']['youtube']
    bt = b.get('by_type') or {}
    if not bt:
        return ''
    lvl = ((d.get('followers_series') or {}).get('youtube') or {})
    chart = ''
    if lvl.get('series'):
        sr = lvl['series']
        pct = (sr[-1]['value'] / sr[0]['value'] - 1) * 100
        chart = ('<div class="nbt">מנויים · %s ← %s <em>· %s · נמדד יומית</em></div>%s'
                 % (num(fmt(sr[0]['value'])), num(fmt(sr[-1]['value'])),
                    num('%+.1f%%' % pct),
                    event_chart(sr, None, BRAND['youtube'], key='value', h=92,
                                zero_base=False, axis=True)))

    tv = sum(v['views'] for v in bt.values()) or 1
    th = sum(v['watch_hours'] for v in bt.values()) or 1
    order = sorted(bt.items(), key=lambda kv: -kv[1]['views'])
    rows = ''
    for name, v in order:
        sv, sh = v['views'] / tv * 100, v['watch_hours'] / th * 100
        # הצבע מסמן את הפער: אדום היכן שהנתח בצפיות ובזמן מתפצל
        hi = 'class="hi"' if abs(sv - sh) > 20 else ''
        rows += ('<div class="ytrow"><div class="pn">%s</div>'
                 '<div>%s</div><div %s>%s</div>'
                 '<div>%s</div><div %s>%s</div></div>'
                 % (esc(name), heb(v['views']), hi, num('%.0f%%' % sv),
                    heb(v['watch_hours']), hi, num('%.0f%%' % sh)))
    head_row = ('<div class="ytrow ytheadr"><div></div><div>צפיות</div>'
                '<div>נתח מהצפיות</div><div>שעות צפייה</div>'
                '<div>נתח מהזמן</div></div>')

    sh_v = bt.get('שורטס', {}).get('views', 0) / tv * 100
    sh_h = bt.get('שורטס', {}).get('watch_hours', 0) / th * 100
    body = [head('המנויים, והפער בין צפייה לזמן', 'יוטיוב', plat='youtube'),
            '<div class="nbchart">%s</div>' % chart,
            '<div class="ytype">%s%s</div>' % (head_row, rows),
            '<div class="ppline">שורטס מביאים <b>%s</b> מהצפיות ו-<b>%s</b> '
            'מהזמן — צפייה ממוצעת של %s לעומת %s בסרטון ו-%s בשידור חי. '
            'הבחירה ביניהם היא בחירה בין חשיפה לזמן.'
            % (num('%.0f%%' % sh_v), num('%.0f%%' % sh_h),
               num('0:28'), num('3:50'), num('22:54')),
            foot('הנתונים מייצוא YouTube Studio לכל התקופה. «אורך צפייה» הוא '
                 'הממוצע בפועל, ולא אורך הפריט.')]
    return slide('יוטיוב', 'עומק לרשת יוטיוב.', ''.join(body), section='יוטיוב')


def s_top(d, p):
    """התוכן שעבד הכי טוב ברשת — חמישה פריטים, לפי צפיות.

    רק פריטים שיש להם טקסט. לפייסבוק 2025 אין טקסט פוסטים בכלל, ולכן שניים
    מחמשת הגדולים ברשת מדולגים; מספרם נאמר בהערה ולא מוצג כשורה ריקה.
    """
    blk = ((d.get('top_by_platform') or {}).get(p) or {})
    items = blk.get('items') or []
    if not items:
        return ''
    hi = items[0]['views'] or 1
    rows = ''
    for i, it in enumerate(items, 1):
        rows += ('<div class="tcrow"><div class="tcn">%02d</div>'
                 '<div class="tct">%s<div class="tcd">%s</div></div>'
                 '<div class="tcv">%s<i>צפיות</i></div>'
                 '<div class="tcb"><i style="width:%.1f%%;background:%s"></i></div>'
                 '</div>'
                 % (i, esc(it['title']), esc(_he_date(it['date'])),
                    heb(it['views']), it['views'] / hi * 100, BRAND[p]))
    skipped = blk.get('skipped_untitled', 0)
    note = ('התוכן מדורג לפי צפיות מצטברות עד היום, ולכן פריט ותיק צבר יותר '
            'זמן מפריט חדש.')
    if skipped:
        note += (' <b>%s מחמשת הגדולים בפייסבוק אינם ברשימה</b> — הנתון שלהם '
                 'הגיע מה-Graph API, שאינו שומר את טקסט הפוסט, ופריט בלי '
                 'כותרת אינו שורה בשקף.' % num(fmt(skipped)))
    body = [head('התוכן שעבד', HEB[p], plat=p),
            '<div class="tclist">%s</div>' % rows,
            foot(note)]
    return slide('%s — תוכן' % HEB[p], 'הפריטים הגדולים ברשת %s.' % HEB[p],
                 ''.join(body), section=HEB[p])


def s_audience(d):
    """מי עוקב אחרי עמוד הפייסבוק. **תצלום נוכחי בלבד.**

    מטא אינה חושפת דמוגרפיה לאחור בשום ממשק, ולכן אין כאן שום «השתנה»,
    «גדל» או «צעיר יותר מבעבר» — רק איך הקהל נראה היום.
    """
    a = d.get('facebook_audience') or {}
    if not a.get('countries'):
        return ''
    tiles = [
        (num('%.1f%%' % a['men']), 'גברים', 'מול %s נשים' % num('%.1f%%' % a['women'])),
        (num('%.1f%%' % a['core']['pct']), a['core']['label'], 'הגוש הגדול בקהל'),
        (num('%.1f%%' % a['countries'][0][1]), a['countries'][0][0],
         'אחריה %s' % ' ו'.join('%s %s' % (n, num('%.1f%%' % v))
                                for n, v in a['countries'][1:3])),
    ]
    cells = ''.join('<div class="autile"><div class="aul">%s</div>'
                    '<div class="auv">%s</div><div class="aus">%s</div></div>'
                    % (esc(lab), v, s2) for v, lab, s2 in tiles)
    cities = ''.join('<div><b>%s</b> %s</div>' % (esc(n), num('%.1f%%' % v))
                     for n, v in a.get('cities', [])[:5])
    body = [head('מי עוקב אחרינו', 'פייסבוק', plat='facebook'),
            '<div class="augrid">%s</div>' % cells,
            '<div class="aucities">%s</div>' % cities,
            foot('תצלום נוכחי, יולי 2026. <b>מטא אינה חושפת דמוגרפיה היסטורית</b> '
                 'בשום ממשק, ולכן אי אפשר לומר כאן מה השתנה — רק איך הקהל נראה '
                 'היום. האחוזים נקראים מבלוקי Age &amp; gender, ‏Top cities '
                 'ו-Top countries בייצוא, ולא הוקלדו.')]
    return slide('קהל פייסבוק', 'פרופיל הקהל של עמוד הפייסבוק, כתצלום נוכחי.',
                 ''.join(body), section='פייסבוק')


def s_summary(d):
    """סגירה. הניסוח הוא ברירת מחדל, אבל **המספרים מחושבים** — כדי ששקף
    הסיכום לא ייפרד משאר הדק ברגע שנתון מתעדכן."""
    tot = total_views(d)
    a = d.get('assets') or {}
    pt = d.get('period_totals') or {}
    py = {p: (d['platforms'][p].get('posts_ytd') or {})
          for p in ('facebook', 'instagram', 'youtube', 'tiktok')}
    y0 = sum(v.get('2024', 0) for v in py.values())
    y2 = sum(v.get('2026', 0) for v in py.values())
    ig = d['platforms']['instagram']['yearly']

    def _pp(y):
        v = ig[y]
        return v['views'] / (v.get('posts_in_views_window') or v['posts'])

    pts = []
    if tot and pt.get('posts'):
        pts.append('%s צפיות ו-%s פריטים שפורסמו בשנתיים ושבעה חודשים.'
                   % (heb(tot), num(fmt(pt['posts']))))
    if a.get('full_gain'):
        pts.append('הקהל עומד על %s עוקבים בשש רשתות. בפייסבוק וביוטיוב — השתיים '
                   'שנמדדות במלואן מאז ינואר 2024 — נוספו %s, גידול של %s.'
                   % (num(fmt(a['total'])), num(fmt(a['full_gain'])),
                      num('%.1f%%' % a['full_pct'])))
    if y0 and y2:
        pts.append('הנפח גדל: %s פריטים בינואר–יולי 2026 מול %s ב-2024, עלייה של %s.'
                   % (num(fmt(y2)), num(fmt(y0)), num('%.0f%%' % ((y2 / y0 - 1) * 100))))
    if '2024' in ig and '2026' in ig:
        pts.append('וכל פריט מגיע רחוק יותר: פוסט אינסטגרם צובר %s צפיות מול %s '
                   'ב-2024, עלייה של %s.'
                   % (num(fmt(round(_pp('2026')))), num(fmt(round(_pp('2024')))),
                      num('%.1f%%' % ((_pp('2026') / _pp('2024') - 1) * 100))))

    rows = ''.join('<div class="sumrow"><div class="sumn">%02d</div>'
                   '<div class="sumt">%s</div></div>' % (i, t)
                   for i, t in enumerate(pts, 1))
    HEN = {2: 'שתי', 3: 'שלוש', 4: 'ארבע', 5: 'חמש', 6: 'שש'}
    body = ['<div class="sumhead"><div class="kicker"><span>סיכום</span></div>'
            '<h2>%s</h2><div class="rule"></div></div>'
            % esc('%s נקודות' % HEN.get(len(pts), len(pts))),
            '<div class="sumlist">%s</div>' % rows]
    return slide('סיכום', 'ארבע נקודות לסגירה.', ''.join(body),
                 bg='#000000', section='', chrome=False)


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
            '<div class="grid2 deep apx">'
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


def dual_line(series, marks=None, h=230):
    """שתי עקומות על ציר משותף, זמן זורם משמאל לימין.

    `series` = [(שם, צבע, [{'month','value'}])]. הציר אינו מתחיל באפס: שתי
    הסדרות נעות בטווח 0.6–1.2 מיליון, ומאפס הן היו שני קווים ישרים צמודים.
    קו — להבדיל מבר — מותר לו בסיס שאינו אפס כששני קצות הציר מסומנים, וזה
    מה שנעשה כאן. אין מילוי מתחת לקווים: שני מילויים חופפים היו הופכים את
    אזור החפיפה לצבע שלישי שאינו של אף רשת.
    """
    W, pad_t, pad_b, pad_l, pad_r = 1000, 26, 20, 68, 22
    vals = [p['value'] for _, _, pts in series for p in pts]
    # גבולות עגולים ולא «מינימום פחות 12%»: תווית ציר צריכה להיות מספר
    # שקורא יכול לקרוא, לא תוצאה של ריפוד. 613,238 → 600K, 1,180,119 → 1.2M.
    stepq = 10 ** (len(str(int(max(vals) - min(vals)))) - 1)
    lo = int(min(vals) // stepq * stepq)
    hi = int(-(-max(vals) // stepq) * stepq)
    span = (hi - lo) or 1
    inner_h, inner_w = h - pad_t - pad_b, W - pad_l - pad_r
    base = pad_t + inner_h
    months = [p['month'] for p in series[0][2]]
    step = inner_w / max(len(months) - 1, 1)

    def xy(i, v):
        return pad_l + i * step, pad_t + inner_h - ((v - lo) / span) * inner_h

    # ה-SVG משתנה בגובה יחד ברוחב (`height:auto`) ולא נעול לגובה קבוע.
    # עם גובה קבוע, ברירת המחדל `xMidYMid meet` הייתה מתאימה את הגרף לגובה
    # ומרכזת אותו — כלומר גרף צר באמצע השקף עם שוליים ריקים משני הצדדים.
    out = ['<svg viewBox="0 0 %d %d" class="dual" role="img">' % (W, h)]
    for frac in (0, .25, .5, .75, 1):
        y = pad_t + inner_h * frac
        out.append('<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="%s" '
                   'stroke-width="1"/>' % (pad_l, y, W - pad_r, y, GRID))

    for i, mk in enumerate(marks or [], 1):
        if mk['month'] not in months:
            continue
        mx = xy(months.index(mk['month']), lo)[0]
        out.append('<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="%s" '
                   'stroke-width="1.5" stroke-dasharray="5 5" opacity=".5"/>'
                   % (mx, pad_t - 12, mx, base, ACCENT))
        out.append('<circle cx="%.1f" cy="%.1f" r="11" fill="%s"/>'
                   '<text x="%.1f" y="%.1f" text-anchor="middle" fill="#fff" '
                   'font-size="14" font-weight="800">%d</text>'
                   % (mx, pad_t - 12, ACCENT, mx, pad_t - 7, i))

    for name, color, pts in series:
        xys = [xy(i, p['value']) for i, p in enumerate(pts)]
        out.append('<polyline points="%s" fill="none" stroke="%s" '
                   'stroke-width="3" stroke-linejoin="round" '
                   'stroke-linecap="round"/>'
                   % (' '.join('%.1f,%.1f' % c for c in xys), color))
        ex, ey = xys[-1]
        out.append('<circle cx="%.1f" cy="%.1f" r="5.5" fill="%s" stroke="#fff" '
                   'stroke-width="2.5"/>' % (ex, ey, color))
    out.append('</svg>')
    # שמות הרשתות במקרא ולא על קצה הקו: טקסט עברי בתוך SVG שזמנו זורם
    # שמאלה־ימינה נצמד לקו עצמו ומכסה אותו.
    key = ''.join('<div class="dk"><i style="background:%s"></i>%s</div>'
                  % (color, esc(name)) for name, color, _ in series)
    return ('<div class="dkey">%s</div><div class="chart dualwrap">'
            '<div class="cy dcy"><span>%s</span><span>%s</span></div>%s'
            '<div class="cx dcx"><span>%s</span><span>%s</span></div></div>'
            % (key, short(hi), short(lo), ''.join(out), months[0], months[-1]))


def s_assets(d):
    """מבט כללי על כל שישה הנכסים: כמה עוקבים, וכמה נוספו מאז 2024.

    השקף הזה כבר היה טבלת צפיות של ינואר–יולי מול ינואר–יולי, וכבר היה גרף
    עומק של שתי רשתות. שניהם החטיאו את התפקיד: זהו השקף שאומר «הנה מה שיש
    לנו», ולכן הוא חייב לשאת את **כל** השש, גם את אלה שאין להן מה לספר.

    ולכן הכרטיסים אחידים: אותו מבנה לשש הרשתות, והשורה התחתונה בכרטיס היא
    או צמיחה או הסיבה שאין. שלוש רשתות ללא צמיחה נראות פחות טוב מטבלה
    מלאה — אבל נקודת פתיחה מומצאת כדי למלא אותן היא בדיוק מה שאסור, וממילא
    מספיק שההנהלה תשאל «ממתי?» פעם אחת כדי שכל השקף ייפול.
    """
    a = d.get('assets') or {}
    if not a.get('rows'):
        return ''

    cards = ''
    for r in a['rows']:
        p = r['platform']
        if 'pct' in r:
            win = '' if r['since'] == '2024-01' else \
                ' <em class="fwin">מ%s</em>' % _he_month(r['since'])
            foot = ('<div class="fcg">%s<span>%s</span></div>'
                    '<div class="fcd">נוספו %s עוקבים%s</div>'
                    % (_chev(r['pct'], '#186a2e', 30),
                       num('%+.1f%%' % r['pct']),
                       num('+%s' % fmt(r['gain'])), win))
        else:
            foot = '<div class="fcnone">%s</div>' % esc(r['note'])
        cards += ('<div class="acard"><div class="fch">%s<span>%s</span>%s</div>'
                  '<div class="fcn">%s</div>%s</div>'
                  % (icon(p, 38), esc(HEB[p]),
                     '<em class="derv">נגזר</em>' if r['derived'] else '',
                     num(fmt(r['followers'])), foot))

    names = ' ו'.join('ב' + HEB[p] for p in a['full_names'])
    ed = ((d.get('editorial') or {}).get('titles') or {})
    body = [head(ed.get('assets', 'שישה נכסים, 3.8 מיליון עוקבים'),
                 'גודל הקהל היום, והצמיחה מאז 2024', ''),
            '<div class="agrid">%s</div>' % cards,
            '<div class="asum">%s — שתי הרשתות שנמדדות במלואן מאז ינואר '
            '2024 — נוספו <b>%s עוקבים</b>, גידול של <b>%s</b>.</div>'
            % (names, num(fmt(a['full_gain'])), num('%.1f%%' % a['full_pct'])),
            '<div class="foot">מטא אינה מוסרת גודל קהל היסטורי אלא הצטרפויות '
            'בלבד, ולכן פייסבוק ואינסטגרם נגזרים מהן — פייסבוק ביחס 0.750 '
            'שאומת מול שמונה חודשים שבהם שני הנתונים קיימים, אינסטגרם ביחס '
            '0.515 שנמדד באותה דרך. לשלוש הרשתות האחרות אין היסטוריית עוקבים '
            'בשום מקור, ולכן נכתבת שם תחילת המדידה במקום מספר.</div>']
    return slide('הנכסים', 'כל שישה הנכסים במבט אחד.', ''.join(body),
                 section='הנכסים והצמיחה')


CSS = """
@font-face{font-family:'SimplerPro';src:url('%(f)s/SimplerPro_HLAR-Regular.otf') format('opentype');font-weight:400}
@font-face{font-family:'SimplerPro';src:url('%(f)s/SimplerPro_HLAR-Semibold.otf') format('opentype');font-weight:600}
@font-face{font-family:'SimplerPro';src:url('%(f)s/SimplerPro_HLAR-Bold.otf') format('opentype');font-weight:700}
@font-face{font-family:'SimplerPro';src:url('%(f)s/SimplerPro_HLAR-Black.otf') format('opentype');font-weight:900}
*{box-sizing:border-box}
body{margin:0;background:#2a2a2a;font-family:'SimplerPro','Arial Hebrew','Segoe UI',sans-serif;color:%(ink)s}
section{width:1920px;height:1080px;padding:76px 96px 64px;display:flex;flex-direction:column;
  position:relative;overflow:hidden;margin:0 auto 26px;box-shadow:0 8px 40px rgba(0,0,0,.4)}
/* כל מספר מבודד ל-LTR: ב-RTL ה-bidi מעיף סימן/אחוז לצד הלא נכון */
/* המספרים נשארים בגופן המותג. `.mono` היא **בידוד כיוון ולא גופן** — היא
   קיימת כי ב-RTL ה-bidi מעיף סימן/אחוז לצד הלא נכון. משפחה מונוספייס נראתה
   כאן כמו טקסט מתוך מסמך אחר, ושברה את המערכת בכל שקף שבו יש מספר. */
.mono{font-variant-numeric:tabular-nums;direction:ltr;unicode-bidi:isolate;
  display:inline-block}
h1{margin:0;font-size:104px;font-weight:900;line-height:.98;letter-spacing:-.02em}
h2{margin:8px 0 0;font-size:64px;font-weight:900;line-height:1.05;letter-spacing:-.02em}
.kicker{display:flex;align-items:center;gap:12px;font-size:24px;font-weight:700;
  letter-spacing:.05em;color:%(a)s;line-height:1}
.kicker svg{flex:none}
.shead{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:8px}
.stitle{min-width:0}
/* הקו מתחת לכותרת, לא לצידה — אותו קו באותו מקום בכל שקף */
.rule{width:110px;height:6px;background:%(a)s;margin-top:20px}
.sright{text-align:left;flex:none;padding-inline-start:40px}
/* מפריד פרק */
.dv{flex:1;display:flex;flex-direction:column;justify-content:space-between;
  color:#fff;padding:40px 0 20px}
.dvh{display:flex;align-items:center;gap:34px;margin-top:20px}
.dvh h2{margin:0;font-size:150px;line-height:1}
.dv .rule{width:170px;height:8px;margin-top:52px}
/* המספרים נצמדים לרצפה. במרכז הם נראו כמו גוש טקסט שנפל, ולא כמו שקף */
.dvs{display:flex;gap:110px;margin-top:auto;padding-top:40px;
  border-top:1px solid #333;flex-wrap:wrap}
.dvn{font-size:76px;font-weight:900;line-height:1;white-space:nowrap}
.dvl{font-size:24px;color:#bfbfbf;margin-top:12px;max-width:420px;line-height:1.35}
.rnum{font-size:52px;font-weight:900;line-height:1}
.rlab{font-size:16px;color:%(m)s;margin-top:2px;line-height:1.3}
.tiny{font-size:13px;color:#9a9a9a}
/* שער — שחור */
.cover{flex:1;display:flex;flex-direction:column;justify-content:space-between;color:#fff}
.cover h1{margin-top:22px}
.cover .rule{width:160px;margin-top:34px}
.cover .kicker{margin-top:64px}
.csub{margin-top:34px;font-size:34px;color:#bfbfbf}
.clab{font-size:40px;font-weight:700;color:#fff;margin-top:14px}
.chero{padding-bottom:8px}
.cbig{font-size:210px;font-weight:900;line-height:.84;letter-spacing:-.04em}
.cbig span{font-size:104px;color:%(a)s}
.cfol{font-size:23px;color:#8f8f8f;margin-top:22px;max-width:1080px;line-height:1.5}
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
/* הנספח נמתח על גובה השקף; שני פאנלים שצפים בחצי העליון נראים כמו טיוטה */
.grid2.apx{flex:1;gap:34px;align-content:stretch}
.grid2.apx .panel{margin-top:26px;display:flex;flex-direction:column}
.grid2.apx .cov{margin-top:6px}
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
.chrome{position:absolute;bottom:30px;left:96px;right:96px;display:flex;
  justify-content:space-between;align-items:baseline;font-size:15px;color:#a8a8a8}
.chrome .pg{font-size:17px;font-weight:700;color:#8a8a8a}
/* תוכן עניינים */
.toc{flex:1;display:flex;flex-direction:column;justify-content:center;max-width:1100px}
.tochead{margin:6px 0 40px;font-size:76px;font-weight:900;letter-spacing:-.02em}
.tocrow{display:grid;grid-template-columns:64px auto 1fr;gap:20px;align-items:baseline;
  padding:18px 0;border-bottom:1px solid %(g)s}
.tocn{font-size:26px;font-weight:900;color:%(a)s}
.toct{font-size:34px;font-weight:700}
/* הישגים */
.achwrap{flex:1;display:flex;flex-direction:column;gap:26px;padding-top:22px}
/* השורה הראשונה נמתחת על השטח הפנוי במקום שהכול יצטופף במרכז ויישאר
   שליש שקף ריק מתחת */
.achgrid{flex:1;display:grid;grid-template-columns:repeat(3,1fr);gap:26px}
.ach{background:#fff;border:1px solid %(g)s;border-radius:14px;padding:40px;
  display:flex;flex-direction:column;justify-content:center}
.achv{font-size:80px;font-weight:900;line-height:1;letter-spacing:-.03em;white-space:nowrap}
.achl{font-size:27px;font-weight:700;margin-top:16px}
.achs{font-size:18px;color:%(m)s;margin-top:5px}
.achrow2{display:grid;grid-template-columns:repeat(3,1fr);gap:22px}
.ach2{background:#fff;border:1px solid %(g)s;border-radius:14px;padding:22px 26px;
  display:flex;align-items:baseline;gap:14px}
.ach2v{font-size:36px;font-weight:700;line-height:1;white-space:nowrap}
.ach2l{font-size:17px;color:#444;font-weight:700;line-height:1.25}
/* הנכסים — שישה כרטיסים זהים */
.agrid{display:grid;grid-template-columns:repeat(3,1fr);gap:24px;flex:1}
.acard{background:#fff;border:1px solid %(g)s;border-radius:16px;
  padding:26px 30px;display:flex;flex-direction:column;justify-content:center}
.fcnone{font-size:18px;color:%(m)s;background:#f4f4f4;border-radius:8px;
  padding:7px 12px;align-self:flex-start;margin-top:12px}
.fcd{font-size:18px;color:%(m)s;margin-top:2px}
.fwin{font-style:normal;color:#8a4b00}
.asum{font-size:24px;margin-top:22px;padding-top:18px;
  border-top:2px solid %(ink)s}
.asum b{font-weight:900}
/* הקהל לאורך הזמן */
.fgrid{display:grid;grid-template-columns:1fr 1fr;gap:26px;margin-bottom:18px}
.fcard{background:#fff;border:1px solid %(g)s;border-radius:16px;padding:20px 26px}
.fch{display:flex;align-items:center;gap:12px;font-size:23px;font-weight:800}
.derv{font-style:normal;font-size:14px;font-weight:700;color:#8a4b00;
  background:#fff6e8;border-radius:6px;padding:3px 9px}
.fcn{font-size:62px;font-weight:900;line-height:1.05;margin-top:4px}
.fcw{font-size:19px;color:%(m)s;margin-top:2px}
.fcg{display:flex;align-items:center;gap:10px;margin-top:10px}
.fcg span{font-size:34px;font-weight:900;color:#186a2e}
.fcg i{font-style:normal;font-size:18px;color:%(m)s;margin-inline-start:4px}
.dualwrap{padding-bottom:6px}
.dual{display:block;width:100%%;height:auto}
.dcy{top:11%%;bottom:13%%;width:60px}
.dcx{padding-left:68px;padding-right:22px}
.dkey{display:flex;gap:26px;justify-content:flex-start;margin-bottom:2px}
.dk{display:flex;align-items:center;gap:9px;font-size:19px;font-weight:800}
.dk i{width:26px;height:5px;border-radius:3px;display:inline-block}
/* המספור על הגרף רץ שמאלה־ימינה לפי הזמן, ולכן גם המקרא — אחרת
   «1» על הגרף ו«1» ברשימה יושבים בקצוות מנוגדים של השקף. */
.fevs{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-top:14px;
  direction:ltr}
.fev{display:flex;align-items:flex-start;gap:10px;direction:rtl}
.fevn{flex:none;width:26px;height:26px;border-radius:50%%;background:%(a)s;
  color:#fff;font-size:16px;font-weight:800;display:flex;align-items:center;
  justify-content:center;margin-top:2px}
.fevt{font-size:17px;font-weight:800;line-height:1.25}
.fevg{font-size:15px;color:%(m)s;display:flex;align-items:center;gap:5px;
  margin-top:3px;flex-wrap:wrap}
/* צמיחה */
.axhead{display:grid;grid-template-columns:420px 380px 380px 340px;gap:30px;
  font-size:15px;font-weight:700;color:%(m)s;letter-spacing:.03em;
  padding-bottom:14px;border-bottom:1px solid %(g)s}
.axhead div:nth-child(n+2){text-align:right}
.axlist{flex:1;display:flex;flex-direction:column;justify-content:space-evenly;padding:6px 0}
.axrow{display:grid;grid-template-columns:420px 380px 380px 340px;gap:30px;
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
/* השורה הזאת נושאת את האמירה, ולכן הכרטיסים לא מותחים את עצמם על השקף */
.augrid{display:grid;grid-template-columns:repeat(3,1fr);gap:80px;margin-top:56px}
.autile{border-top:1px solid %(g)s;padding-top:30px}
.aul{font-size:29px;font-weight:700;color:#404040}
.auv{font-size:104px;font-weight:900;line-height:1;margin-top:16px}
.aus{font-size:26px;color:#404040;margin-top:14px;line-height:1.4}
.aucities{display:flex;gap:64px;margin-top:auto;padding-top:34px;
  border-top:1px solid %(g)s;font-size:29px;color:#404040}
.aucities b{font-weight:900;color:%(ink)s}
.sumhead{color:#fff}
.sumlist{flex:1;display:flex;flex-direction:column;justify-content:center;color:#fff}
.sumrow{display:grid;grid-template-columns:96px 1fr;gap:34px;align-items:baseline;
  border-top:1px solid #404040;padding:30px 0}
.sumrow:last-child{border-bottom:1px solid #404040}
.sumn{font-size:44px;font-weight:900;color:%(a)s}
.sumt{font-size:34px;line-height:1.42}
/* גרף עם אירועים כתובים עליו */
.evchw{margin-top:14px}
.evch{display:block;width:100%%;height:auto}
.nbchart{margin-top:4px}
.nbt{font-size:24px;font-weight:900}
.nbt2{font-size:22px;font-weight:900;margin-top:2px}
.nbt2 em{font-style:normal;font-weight:400;font-size:20px;color:%(m)s}
.nbgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:22px;margin-top:16px}
.ytype{flex:1;margin-top:22px;font-size:28px}
.ytrow{display:grid;grid-template-columns:1.15fr 1fr .8fr 1fr .8fr;
  align-items:baseline;padding:13px 0;border-bottom:1px solid %(g)s}
.ytrow>div:not(.pn){text-align:left;font-weight:900}
.ytheadr{padding:0 0 12px;border-bottom:2px solid %(ink)s;font-size:24px;
  font-weight:700;color:%(m)s}
.ytheadr>div{text-align:left;font-weight:700}
.ytrow .hi{color:%(a)s}
.ytrow .pn{font-size:34px;font-weight:900}
.ppline{font-size:30px;line-height:1.4;color:#262626;margin-top:18px}
.ppline b{font-weight:900}
/* התוכן שעבד — שמות ייחודיים, אין להם מקבילה בקובץ */
.tclist{flex:1;display:flex;flex-direction:column;justify-content:center;gap:6px}
.tcrow{display:grid;grid-template-columns:64px 1fr 190px;gap:22px;
  align-items:center;padding:13px 0;border-bottom:1px solid %(g)s;position:relative}
.tcn{font-size:30px;font-weight:900;color:%(a)s}
.tct{font-size:27px;font-weight:700;line-height:1.28}
.tcd{font-size:21px;color:%(m)s;font-weight:400;margin-top:5px}
.tcv{font-size:34px;font-weight:900;text-align:left;line-height:1;white-space:nowrap}
.tcv i{display:block;font-style:normal;font-size:19px;font-weight:400;
  color:%(m)s;margin-top:5px}
.tcb{grid-column:1/-1;height:5px;background:#ececec;border-radius:3px;
  overflow:hidden;margin-top:8px}
.tcb i{display:block;height:100%%;border-radius:3px}
.thgrid{flex:1;display:grid;grid-template-columns:1fr 1fr;gap:40px;
  align-content:center;margin-top:20px}
.tbox.thin{padding:40px 44px}
.tbox.thin .tbh{display:flex;align-items:center;gap:16px;font-size:34px}
.tbox.thin .tbv{font-size:88px;margin-top:20px}
.thx{font-size:28px;font-weight:700;margin-top:22px}
.thw{font-size:23px;color:%(m)s;margin-top:6px}
.tbn{font-size:22px;color:#8a4b00;background:#fff6e8;border-radius:10px;
  padding:14px 16px;margin-top:26px;line-height:1.45}
.tbox{background:#fff;border:1px solid %(g)s;border-radius:16px;padding:18px 24px}
.tbh{font-size:26px;font-weight:900}
.tbs{font-size:20px;color:%(m)s;margin-top:2px}
.tbv{font-size:52px;font-weight:900;line-height:1;margin-top:14px;white-space:nowrap}
.tbd{display:flex;align-items:center;gap:10px;margin-top:8px}
.tbd span{font-size:34px;font-weight:900;color:#186a2e}
.tbd i{font-style:normal;font-size:19px;color:%(m)s;font-weight:700}
.nbh{font-size:23px;font-weight:900;color:#404040}
.nbrow{display:flex;gap:30px;margin-top:8px}
.nbv{font-size:34px;font-weight:900;line-height:1;white-space:nowrap}
.nbl{font-size:21px;color:%(m)s;font-weight:700;margin-top:6px}
.nbl em{font-style:normal;font-weight:400;display:block;font-size:17px}
.nbs{font-size:23px;color:#404040;margin-top:16px}
/* מה פרסמנו — טבלת נפח, בלי בר */
.pubwrap{flex:1;display:grid;grid-template-columns:1.7fr 1fr;gap:76px;align-items:center}
.ptable{font-size:34px}
.prow{display:grid;grid-template-columns:1.35fr 1fr 1fr 1fr;align-items:center;
  padding:24px 0;border-bottom:1px solid %(g)s}
.prow>div:not(.pl){text-align:left;font-weight:900}
.phead{padding:0 0 14px;border-bottom:2px solid %(ink)s;font-size:27px;font-weight:700}
.phead>div{text-align:left;font-weight:700}
.phead em{display:block;font-style:normal;font-size:19px;font-weight:400;
  color:%(m)s;margin-top:3px}
.ptot{border-bottom:none;border-top:2px solid %(ink)s;font-size:42px}
.prow .hi{color:%(a)s}
.ppanel{border-top:7px solid %(a)s;padding-top:28px}
.pbig{font-size:100px;font-weight:900;line-height:1;margin-top:12px}
.pnote{font-size:24px;color:#404040;line-height:1.5;margin-top:18px}
/* ההסתייגות נצמדת לרצפה בכל שקף, לא לסוף התוכן — אחרת היא קופצת בין
   שקף לשקף לפי אורך הגוף, וזה בדיוק מה שהורס את תחושת המערכת */
.foot{margin-top:auto;padding-top:22px;font-size:22px;color:%(m)s;line-height:1.5}
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
.yrow{display:grid;grid-template-columns:230px 1fr 110px 1fr 110px;gap:16px;
  align-items:center;margin-bottom:12px}
.ytn{font-size:22px;font-weight:700}
.ytn span{display:block;font-size:14px;color:%(m)s;font-weight:400;margin-top:2px}
.ytbar{height:26px;background:#ececec;border-radius:6px;overflow:hidden;
  display:flex;justify-content:flex-end}
.ytbar div{height:100%%;border-radius:6px}
.ytv{font-size:22px;font-weight:700;text-align:left}
.ychips{display:flex;align-items:flex-end;gap:34px;margin:6px 0 18px;
  border-bottom:1px solid %(g)s;padding-bottom:16px}
.ycl{font-size:16px;color:%(m)s;font-weight:700;margin-inline-end:8px;max-width:190px;line-height:1.3}
.ychip{display:flex;align-items:baseline;gap:10px}
.ycy{font-size:17px;color:%(m)s;font-weight:700}
.ycv{font-size:30px;font-weight:700}
.ychip .up{font-size:19px;font-weight:700;color:#186a2e}
.ychip .down{font-size:19px;font-weight:700;color:#b42318}
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
    # החלק הראשון — מבט עילי. החלק השני — רשת אחר רשת, וכל אחת נפתחת במפריד
    # שחור שנושא שלושה מספרי-על. המפריד הוא לא קישוט: בדק של 18 שקפים הוא מה
    # שאומר לקורא «עברת שלב», ובלעדיו ארבע הרשתות נקראות כרצף אחד ארוך.
    # הסדר בין הרשתות הוא לפי גודל הקהל, לא לפי ותק.
    ORDER = [('facebook', 'פרק ראשון'), ('instagram', 'פרק שני'),
             ('youtube', 'פרק שלישי'), ('tiktok', 'פרק רביעי')]
    slides = [
        s_cover(d),
        s_achievements(d),
        s_assets(d),
        s_published(d),
    ]
    for p, kick in ORDER:
        slides.append(divider(p, kick, divider_stats(d, p)))
        slides.append(s_youtube(d) if p == 'youtube'
                      else (s_network(d, p) or s_platform(d, p)))
        slides.append(s_top(d, p))
        if p == 'facebook':
            slides.append(s_audience(d))
    slides += [s_thin(d), s_summary(d), s_appendix(d)]
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
