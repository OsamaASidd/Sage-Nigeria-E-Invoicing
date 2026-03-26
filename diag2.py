import pyodbc

ODBC_CONN = (
    "Driver={Pervasive ODBC Client Interface};"
    "ServerName=localhost;DBQ=PROTONSECURITYSERVIC;"
    "UID=Peachtree;PWD=cool123;"
)

AAP_RECNUM = 2312

conn = pyodbc.connect(ODBC_CONN)
cursor = conn.cursor()
print("Connected.\n")

# ── 1. What do JournalEx=3 entries look like for AAP? ────────────────────────
print("=" * 70)
print("1. JournalEx=3 entries for AAP (all time, last 10)")
print("=" * 70)
cursor.execute(
    'SELECT JrnlKey_TrxNumber, PostOrder, Module, JournalEx, '
    'TransactionDate, MainAmount, Reference, Description '
    'FROM "JrnlHdr" '
    'WHERE CustVendId=? AND JournalEx=3 '
    'ORDER BY TransactionDate DESC',
    (AAP_RECNUM,)
)
rows = cursor.fetchall()
print("  Total JEx=3 rows for AAP: %d" % len(rows))
for r in rows[:10]:
    print("  TRX=%s PO=%s Date=%s Amt=%s Ref=%r Desc=%r" % (
        r[0], r[1], str(r[4])[:10], r[5], r[6], str(r[7])[:40]))

# ── 2. ALL tables — search every single one for amount 16498133 ──────────────
print("\n" + "=" * 70)
print("2. SEARCHING ALL TABLES for amount 16,498,133.95 (SAJ/2602/0068)")
print("=" * 70)
all_tables = [t.table_name for t in cursor.tables(tableType="TABLE")]
print("  Total tables: %d" % len(all_tables))
print("  Tables: %s\n" % all_tables)

target = 16498133.95
found_in = []
for tbl in all_tables:
    try:
        cols = [c.column_name for c in cursor.columns(table=tbl)]
        # Find numeric-ish columns
        num_cols = [c for c in cols if any(k in c.upper() for k in
                    ["AMOUNT", "AMT", "TOTAL", "BALANCE", "PRICE", "COST", "VALUE"])]
        for col in num_cols[:5]:
            try:
                cursor.execute(
                    'SELECT COUNT(*) FROM "%s" WHERE "%s" > ? AND "%s" < ?' % (tbl, col, col),
                    (target - 1.0, target + 1.0)
                )
                cnt = cursor.fetchone()[0]
                if cnt > 0:
                    found_in.append((tbl, col, cnt))
                    print("  *** FOUND in %s.%s (%d rows)" % (tbl, col, cnt))
                    cursor.execute(
                        'SELECT TOP 3 * FROM "%s" WHERE "%s" > ? AND "%s" < ?' % (tbl, col, col),
                        (target - 1.0, target + 1.0)
                    )
                    sample = cursor.fetchall()
                    sample_cols = [c[0] for c in cursor.description]
                    for row in sample:
                        d = {k: v for k, v in zip(sample_cols, row)
                             if v is not None and str(v).strip() not in ("", "0")}
                        print("      %s" % d)
            except Exception:
                pass
    except Exception:
        pass

if not found_in:
    print("  Amount 16,498,133.95 NOT FOUND in any table!")

# ── 3. Also search for 16856250 ───────────────────────────────────────────────
print("\n" + "=" * 70)
print("3. SEARCHING ALL TABLES for amount 16,856,250.00 (TEST-SAJ/2603)")
print("=" * 70)
target2 = 16856250.00
found_in2 = []
for tbl in all_tables:
    try:
        cols = [c.column_name for c in cursor.columns(table=tbl)]
        num_cols = [c for c in cols if any(k in c.upper() for k in
                    ["AMOUNT", "AMT", "TOTAL", "BALANCE", "PRICE", "COST", "VALUE"])]
        for col in num_cols[:5]:
            try:
                cursor.execute(
                    'SELECT COUNT(*) FROM "%s" WHERE "%s" > ? AND "%s" < ?' % (tbl, col, col),
                    (target2 - 1.0, target2 + 1.0)
                )
                cnt = cursor.fetchone()[0]
                if cnt > 0:
                    found_in2.append((tbl, col, cnt))
                    print("  *** FOUND in %s.%s (%d rows)" % (tbl, col, cnt))
                    cursor.execute(
                        'SELECT TOP 3 * FROM "%s" WHERE "%s" > ? AND "%s" < ?' % (tbl, col, col),
                        (target2 - 1.0, target2 + 1.0)
                    )
                    sample = cursor.fetchall()
                    sample_cols = [c[0] for c in cursor.description]
                    for row in sample:
                        d = {k: v for k, v in zip(sample_cols, row)
                             if v is not None and str(v).strip() not in ("", "0")}
                        print("      %s" % d)
            except Exception:
                pass
    except Exception:
        pass

if not found_in2:
    print("  Amount 16,856,250.00 NOT FOUND in any table!")

# ── 4. Check all columns of a JEx=3 row ───────────────────────────────────────
print("\n" + "=" * 70)
print("4. FULL JrnlHdr row dump for most recent JEx=3 AAP entry (all columns)")
print("=" * 70)
cursor.execute(
    'SELECT * FROM "JrnlHdr" WHERE CustVendId=? AND JournalEx=3 '
    'ORDER BY TransactionDate DESC',
    (AAP_RECNUM,)
)
row = cursor.fetchone()
if row:
    col_names = [c[0] for c in cursor.description]
    for k, v in zip(col_names, row):
        if v is not None and str(v).strip() not in ("", "0"):
            print("  %-40s = %r" % (k, v))
else:
    print("  No JEx=3 rows found for AAP")

# ── 5. Check JrnlRow for SAJ/2602/0068 anywhere ───────────────────────────────
print("\n" + "=" * 70)
print("5. SEARCHING JrnlRow for invoice number SAJ/2602/0068")
print("=" * 70)
jrnlrow_cols = [c.column_name for c in cursor.columns(table="JrnlRow")]
print("  JrnlRow columns: %s" % jrnlrow_cols)
text_cols = [c for c in jrnlrow_cols if any(k in c.upper() for k in
             ["REF", "INV", "NUMBER", "NUM", "DESC", "MEMO", "NOTE", "COMMENT"])]
print("  Text-ish cols: %s" % text_cols)
for col in text_cols[:6]:
    try:
        cursor.execute(
            'SELECT COUNT(*) FROM "JrnlRow" WHERE "%s" LIKE ?' % col,
            ("%SAJ/2602%",)
        )
        cnt = cursor.fetchone()[0]
        if cnt > 0:
            print("  *** FOUND in JrnlRow.%s: %d rows" % (col, cnt))
            cursor.execute(
                'SELECT TOP 3 * FROM "JrnlRow" WHERE "%s" LIKE ?' % col,
                ("%SAJ/2602%",)
            )
            sample = cursor.fetchall()
            scols = [c[0] for c in cursor.description]
            for r in sample:
                d = {k: v for k, v in zip(scols, r)
                     if v is not None and str(v).strip() not in ("", "0")}
                print("      %s" % d)
        else:
            print("  JrnlRow.%s: not found" % col)
    except Exception as e:
        print("  JrnlRow.%s: error - %s" % (col, e))

conn.close()
print("\n\nDone.")