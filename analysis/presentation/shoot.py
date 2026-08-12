#!/usr/bin/env python3
"""צילום כל שקף ל-PNG, כדי להסתכל על מה שנבנה. גם מייצא PDF.

    python analysis/presentation/shoot.py            # כל השקפים
    python analysis/presentation/shoot.py 1 3 7      # רק אלה
    python analysis/presentation/shoot.py --pdf      # גם PDF

הרנדר מאמת את המספרים; רק העין מאמתת את הפריסה — חפיפות תוויות, גלישה,
טקסט שנחתך. אלה לא נתפסים בשום בדיקה אוטומטית.
"""
import os
import sys

from playwright.sync_api import sync_playwright

HERE = os.path.dirname(os.path.abspath(__file__))
DECK = os.path.join(HERE, 'deck.html')
SHOTS = os.path.join(HERE, 'shots')


def main():
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    want = {int(a) for a in args} if args else None
    os.makedirs(SHOTS, exist_ok=True)

    with sync_playwright() as pw:
        b = pw.chromium.launch()
        page = b.new_page(viewport={'width': 1920, 'height': 1080})
        page.goto('file:///' + DECK.replace('\\', '/'))
        page.wait_for_timeout(1200)          # שהגופנים ייטענו

        n = page.locator('section').count()
        for i in range(n):
            if want and (i + 1) not in want:
                continue
            sec = page.locator('section').nth(i)
            label = sec.get_attribute('data-label') or str(i + 1)
            path = os.path.join(SHOTS, '%02d.png' % (i + 1))
            sec.screenshot(path=path)
            print('  %02d  %s' % (i + 1, label))

        if '--pdf' in sys.argv:
            pdf = os.path.join(HERE, 'deck.pdf')
            page.pdf(path=pdf, width='1920px', height='1080px',
                     print_background=True, margin={'top': '0', 'bottom': '0',
                                                    'left': '0', 'right': '0'})
            print('PDF: %s' % pdf)
        b.close()
    print('%d שקפים -> %s' % (n, SHOTS))


if __name__ == '__main__':
    sys.exit(main())
