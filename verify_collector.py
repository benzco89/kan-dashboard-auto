"""
Snapshot a sheet, run a collector, diff what actually changed. READ ONLY.

Why this exists: on 2026-07-26, three separate collector changes produced a
GREEN run that had quietly damaged or lost data.

  * `_insight` ran the retention curve through a helper that SUMS dicts, so two
    new columns were written as 0 on every row. The run succeeded.
  * the curve was stored comma-separated, and Google Sheets read `998,999,915`
    as a number with thousands separators and swallowed every comma. 67 rows
    lost their bucket boundaries. The run succeeded.
  * a new column was inserted in the MIDDLE of a positional header, sliding 230
    historical rows one column to the left — a June row reported a Twitter
    follower count under "website clicks". The run succeeded.

Every one was found by hand, by opening the sheet and comparing it with a
backup. This makes that the default instead of something someone remembers.

    python verify_collector.py snap facebook       # before
    gh workflow run test_facebook.yml --ref my-branch
    python verify_collector.py check facebook      # after

Exit code is 1 when anything suspicious is found, so CI can gate on it.

Env: GCP_SERVICE_ACCOUNT (or a local service-account.json).
"""

import os
import sys
import json
import gzip

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SNAP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".verify")

SHEETS = {
    "youtube": "נתוני יוטיוב",
    "facebook": "נתוני פייסבוק",
    "instagram": "נתוני אינסטגרם",
    "stories": "סטוריז אינסטגרם",
    "twitter": "נתוני טוויטר",
    "tiktok": "נתוני טיקטוק",
    "followers": "מעקב עוקבים",
    "insights": "תובנות יומיות",
    "comment_analysis": "ניתוח תגובות",
    "hot_alerts": "hot_alerts",
    "competitors": "מתחרים",
    "competitor_posts": "פוסטים מתחרים",
    "demographics": "דמוגרפיה",
}

# The first column that identifies a row, per sheet. Without it a diff can only
# compare row counts; with it, a value that changed can be traced to its row.
KEYS = {
    "youtube": "video_id", "facebook": "post_id", "instagram": "media_id",
    "twitter": "tweet_id", "tiktok": "video_id", "followers": "date",
    "stories": "media_id", "competitor_posts": "post_id",
}

EMPTY = {"", "0", "0.0", "none", "nan", "-"}


def _blank(v):
    return str(v).strip().lower() in EMPTY


def fetch(sheet_key):
    """Raw value matrix, header included — NOT parsed into dicts.

    Deliberately raw: the column-shift bug is invisible once rows are keyed by
    header name, because every value still has *a* name, just the wrong one.
    Position is the thing being checked.
    """
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    raw = os.environ.get("GCP_SERVICE_ACCOUNT")
    if raw:
        creds = service_account.Credentials.from_service_account_info(json.loads(raw), scopes=SCOPES)
    else:
        path = next((p for p in ("service-account.json",
                                 os.path.join("social_dashboard", "service-account.json"))
                     if os.path.exists(p)), None)
        if not path:
            raise SystemExit("no credentials: set GCP_SERVICE_ACCOUNT or add service-account.json")
        creds = service_account.Credentials.from_service_account_file(path, scopes=SCOPES)

    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
    resp = (svc.spreadsheets().values()
            .get(spreadsheetId=SPREADSHEET_ID, range=SHEETS[sheet_key]).execute())
    return resp.get("values", [])


def snap(sheet_key):
    os.makedirs(SNAP_DIR, exist_ok=True)
    values = fetch(sheet_key)
    path = os.path.join(SNAP_DIR, f"{sheet_key}.json.gz")
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(values, f, ensure_ascii=False)
    print(f"  snapshot: {len(values) - 1} rows x {len(values[0]) if values else 0} cols -> {path}")


def _index(values, key_col):
    header = values[0] if values else []
    try:
        ki = header.index(key_col)
    except ValueError:
        return None, header
    out = {}
    for row in values[1:]:
        if ki < len(row) and row[ki]:
            out[row[ki]] = row
    return out, header


def check(sheet_key):
    path = os.path.join(SNAP_DIR, f"{sheet_key}.json.gz")
    if not os.path.exists(path):
        raise SystemExit(f"no snapshot for {sheet_key} — run `snap {sheet_key}` first")
    with gzip.open(path, "rt", encoding="utf-8") as f:
        before = json.load(f)
    after = fetch(sheet_key)

    problems = []
    hb = before[0] if before else []
    ha = after[0] if after else []
    print(f"\n  rows {len(before) - 1} -> {len(after) - 1}"
          f"   cols {len(hb)} -> {len(ha)}\n")

    # --- 1. the header, BY POSITION -----------------------------------------
    added = [c for c in ha if c not in hb]
    removed = [c for c in hb if c not in ha]
    if added:
        print(f"  columns added:   {added}")
    if removed:
        print(f"  columns REMOVED: {removed}")
        problems.append(f"{len(removed)} column(s) removed")
    moved = [(c, hb.index(c), ha.index(c)) for c in hb if c in ha and hb.index(c) != ha.index(c)]
    if moved:
        # This is the 230-row scramble: the rows are written positionally, so a
        # column that changed position means every value after it now sits under
        # a different name.
        print(f"\n  ⚠ {len(moved)} COLUMN(S) MOVED — historical rows are positional, so their"
              f"\n    values now sit under a different header:")
        for c, i, j in moved[:8]:
            print(f"      {c:<24} position {i} -> {j}")
        problems.append(f"{len(moved)} column(s) changed position")

    # --- 2. per column: how populated, before and after ----------------------
    def populated(values, header, col):
        if col not in header:
            return None
        i = header.index(col)
        rows = values[1:]
        if not rows:
            return 0.0
        return sum(1 for r in rows if i < len(r) and not _blank(r[i])) / len(rows)

    print("\n  column fill rate (before -> after):")
    for c in ha:
        pa = populated(after, ha, c)
        pb = populated(before, hb, c) if c in hb else None
        if pb is None:
            flag = "  <-- NEW, and it arrived EMPTY" if pa == 0 else ""
            if pa == 0:
                problems.append(f"new column '{c}' is entirely empty")
            print(f"    {c:<24}    new -> {pa:5.0%}{flag}")
        elif pa == 0 and pb > 0:
            print(f"    {c:<24} {pb:5.0%} -> {pa:5.0%}   <-- WENT DEAD")
            problems.append(f"column '{c}' lost every value")
        elif pb and pa < pb * 0.9:
            print(f"    {c:<24} {pb:5.0%} -> {pa:5.0%}   <-- dropped")
            problems.append(f"column '{c}' fill rate dropped {pb:.0%} -> {pa:.0%}")

    # --- 3. individual values that went blank --------------------------------
    key = KEYS.get(sheet_key)
    if key:
        ib, _ = _index(before, key)
        ia, _ = _index(after, key)
        if ib is None or ia is None:
            print(f"\n  (no '{key}' column — skipping the per-row check)")
        else:
            lost, gone = [], []
            for k, rb in ib.items():
                ra = ia.get(k)
                if ra is None:
                    gone.append(k)
                    continue
                for c in hb:
                    if c not in ha:
                        continue
                    i, j = hb.index(c), ha.index(c)
                    vb = rb[i] if i < len(rb) else ""
                    va = ra[j] if j < len(ra) else ""
                    if not _blank(vb) and _blank(va):
                        lost.append((k, c, vb))
            print(f"\n  rows that disappeared entirely: {len(gone)}")
            if gone:
                problems.append(f"{len(gone)} row(s) disappeared")
                print(f"    e.g. {gone[:3]}")
            print(f"  values that went from a value to blank/0: {len(lost)}")
            for k, c, v in lost[:6]:
                print(f"    {str(k)[-10:]:>10}  {c:<20} was {str(v)[:28]}")
            if lost:
                problems.append(f"{len(lost)} value(s) went blank")

    print()
    if problems:
        print("  ⚠ SUSPICIOUS:")
        for p in problems:
            print(f"    - {p}")
        print("\n  A green collector run proves it did not crash. It does not prove")
        print("  it wrote anything. Check each line above before merging.")
        return 1
    print("  ✅ nothing lost, nothing moved, nothing arrived empty.")
    return 0


def main():
    args = sys.argv[1:]
    if len(args) != 2 or args[0] not in ("snap", "check") or args[1] not in SHEETS:
        print(__doc__)
        print("  sheets:", ", ".join(sorted(SHEETS)))
        raise SystemExit(2)
    cmd, sheet = args
    print(f"\n{'=' * 62}\n  {cmd} · {sheet} ({SHEETS[sheet]})\n{'=' * 62}")
    if cmd == "snap":
        snap(sheet)
        return 0
    return check(sheet)


if __name__ == "__main__":
    sys.exit(main())
