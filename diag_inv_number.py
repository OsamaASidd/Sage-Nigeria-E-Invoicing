"""
diag_inv_number.py
==================
Finds exactly where Sage stores real invoice numbers for recurring invoices
that have blank JrnlHdr.Reference.

Run with:
    python diag_inv_number.py
"""
import pyodbc

ODBC_CONN = (
    "Driver={Pervasive ODBC Client Interface};"
    "ServerName=localhost;DBQ=PROTONSECURITYSERVIC;"
    "UID=Peachtree;PWD=cool123;"
)

# The 3 recurring AAP invoices with blank Reference
TARGET_POST_ORDERS = [342375, 342376, 342377]
TARGET_TRX_NUMBER  = 339
TARGET_CUST_RECNUM = 2312  # AAP's CustomerRecordNumber

def to_str(val):
    if val is None: return ""
    return str(val).strip()

print("Connecting to Sage...")
conn = pyodbc.connect(ODBC_CONN)
cursor = conn.cursor()
print("Connected.\n")

# ── All tables ────────────────────────────────────────────────────────────────
all_tables = sorted([t.table_name for t in cursor.tables(tableType="TABLE")])
print(f"Total tables: {len(all_tables)}")
print("Tables:", all_tables)

# ── Show ALL JrnlHdr columns for TRX 339 ─────────────────────────────────────
print("\n" + "="*60)
print("JrnlHdr full row for TRX 339 (all columns)")
print("="*60)
cursor.execute('SELECT * FROM "JrnlHdr" WHERE JrnlKey_TrxNumber=339')
cols = [c[0] for c in cursor.description]
row  = cursor.fetchone()
if row:
    for col, val in zip(cols, row):
        if val is not None and to_str(val) not in ("", "0"):
            print(f"  {col:40s} = {val!r}")

# ── ALL 6 JrnlHdr rows for AAP (post orders 342374-342379) ───────────────────
print("\n" + "="*60)
print("All JrnlHdr rows for AAP (CustVendId=2312) — all columns")
print("="*60)
cursor.execute('SELECT * FROM "JrnlHdr" WHERE CustVendId=2312 ORDER BY TransactionDate DESC')
cols = [c[0] for c in cursor.description]
rows = cursor.fetchall()
print(f"Found {len(rows)} rows")
for row in rows:
    d = {c: v for c, v in zip(cols, row) if v is not None and to_str(v) not in ("","0")}
    print(f"\n  PostOrder={d.get('PostOrder')} TRX={d.get('JrnlKey_TrxNumber')} Date={str(d.get('TransactionDate',''))[:10]}")
    for col, val in d.items():
        if col not in ('PostOrder','JrnlKey_TrxNumber','TransactionDate'):
            print(f"    {col:40s} = {val!r}")

# ── Search EVERY table for PostOrder values 342374-342379 ────────────────────
print("\n" + "="*60)
print("Searching ALL tables for PostOrder values 342374–342379")
print("="*60)
po_values = list(range(342374, 342380))

for table in all_tables:
    try:
        tab_cols = [c.column_name for c in cursor.columns(table=table)]
        # Find any column that might be a PostOrder reference
        for col in tab_cols:
            col_lower = col.lower()
            if any(k in col_lower for k in ['post', 'order', 'jrnl', 'trx', 'trans']):
                try:
                    placeholders = ",".join("?" * len(po_values))
                    cursor.execute(f'SELECT * FROM "{table}" WHERE "{col}" IN ({placeholders})',
                                   po_values)
                    found = cursor.fetchall()
                    if found:
                        rcols = [c[0] for c in cursor.description]
                        print(f"\n  ✓ TABLE={table} COLUMN={col} → {len(found)} rows")
                        for r in found[:3]:
                            rd = dict(zip(rcols, r))
                            # Print non-empty values
                            relevant = {k:v for k,v in rd.items() if v is not None and to_str(v) not in ("","0")}
                            print(f"    {relevant}")
                except Exception:
                    continue
    except Exception:
        continue

# ── Search EVERY table for TRX number 339 ────────────────────────────────────
print("\n" + "="*60)
print("Searching ALL tables for TrxNumber=339")
print("="*60)
for table in all_tables:
    try:
        tab_cols = [c.column_name for c in cursor.columns(table=table)]
        for col in tab_cols:
            col_lower = col.lower()
            if any(k in col_lower for k in ['trx','trans','journal','jrnl','number','num']):
                try:
                    cursor.execute(f'SELECT * FROM "{table}" WHERE "{col}"=339')
                    found = cursor.fetchall()
                    if found:
                        rcols = [c[0] for c in cursor.description]
                        print(f"\n  ✓ TABLE={table} COLUMN={col} → {len(found)} rows")
                        for r in found[:3]:
                            rd = dict(zip(rcols, r))
                            relevant = {k:v for k,v in rd.items() if v is not None and to_str(v) not in ("","0")}
                            print(f"    {relevant}")
                except Exception:
                    continue
    except Exception:
        continue

# ── Search EVERY table for CustomerRecordNumber=2312 (AAP) ───────────────────
print("\n" + "="*60)
print("Searching ALL tables for CustomerRecordNumber=2312 (AAP)")
print("="*60)
for table in all_tables:
    try:
        tab_cols = [c.column_name for c in cursor.columns(table=table)]
        for col in tab_cols:
            col_lower = col.lower()
            if any(k in col_lower for k in ['cust','vendor','client','party','recnum','record']):
                try:
                    cursor.execute(f'SELECT * FROM "{table}" WHERE "{col}"=2312')
                    found = cursor.fetchall()
                    if found:
                        rcols = [c[0] for c in cursor.description]
                        print(f"\n  ✓ TABLE={table} COLUMN={col} → {len(found)} rows")
                        for r in found[:3]:
                            rd = dict(zip(rcols, r))
                            relevant = {k:v for k,v in rd.items() if v is not None and to_str(v) not in ("","0")}
                            print(f"    {relevant}")
                except Exception:
                    continue
    except Exception:
        continue

conn.close()
print("\n\nDone. Paste this full output.")