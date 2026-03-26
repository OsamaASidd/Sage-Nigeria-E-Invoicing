"""
diag_sync.py
============
Runs the sync logic directly and shows exactly what Sage returns
and what gets inserted/updated for each record.

Run with:
    python diag_sync.py
"""
import sqlite3, pyodbc
from datetime import datetime, date

ODBC_CONN = (
    "Driver={Pervasive ODBC Client Interface};"
    "ServerName=localhost;DBQ=PROTONSECURITYSERVIC;"
    "UID=Peachtree;PWD=cool123;"
)
DB_PATH = "einvoice.db"
DATE_FROM = "2026-01-01"
DATE_TO   = "2026-04-30"

def to_str(val):
    if val is None: return ""
    return str(val).strip()

def to_float(val):
    try: return float(val or 0)
    except: return 0.0

print(f"\n=== STEP 1: Connect to Sage ===")
try:
    sage = pyodbc.connect(ODBC_CONN)
    print("Connected OK")
except Exception as e:
    print(f"FAILED: {e}")
    exit(1)

cursor = sage.cursor()

print(f"\n=== STEP 2: Fetch JrnlHdr rows ({DATE_FROM} to {DATE_TO}) ===")
cursor.execute(
    'SELECT JrnlKey_TrxNumber, PostOrder, CustVendId, TransactionDate, MainAmount, '
    'Reference, Description, JournalEx FROM "JrnlHdr" '
    "WHERE Module='R' AND JournalEx IN (8, 9) "
    "AND TransactionDate>=? AND TransactionDate<=? ORDER BY TransactionDate DESC",
    (DATE_FROM, DATE_TO)
)
headers = cursor.fetchall()
print(f"Sage returned {len(headers)} rows")

print(f"\n=== STEP 3: Build customer map ===")
cust_map = {}
cursor.execute('SELECT CustomerRecordNumber, CustomerID, Customer_Bill_Name FROM "Customers"')
for cr in cursor.fetchall():
    rec = {"id": to_str(cr[1]), "name": to_str(cr[2])}
    if cr[0] is not None: cust_map[cr[0]] = rec
    if to_str(cr[1]): cust_map[to_str(cr[1])] = rec
print(f"Customer map built: {len(cust_map)} entries")
# Check AAP specifically
aap_by_id  = cust_map.get("AAP")
print(f"AAP lookup by 'AAP' text: {aap_by_id}")
print(f"AAP lookup by 2312 int  : {cust_map.get(2312)}")

sage.close()

print(f"\n=== STEP 4: Current DB state ===")
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
pk_info = conn.execute("PRAGMA table_info(invoices)").fetchall()
pk_col  = next((r[1] for r in pk_info if r[5] == 1), None)
print(f"PRIMARY KEY column: {pk_col}")
existing_po = {r[0]: r[1] for r in conn.execute("SELECT post_order, status FROM invoices").fetchall()}
print(f"Existing post_orders in DB: {len(existing_po)}")
aap_rows = conn.execute("SELECT post_order, trx_number, invoice_num, invoice_date, status FROM invoices WHERE customer_id='AAP'").fetchall()
print(f"AAP rows currently in DB: {len(aap_rows)}")
for r in aap_rows: print(f"  {dict(r)}")

print(f"\n=== STEP 5: Simulate sync — what would happen to each row ===")
new_count = 0
update_count = 0
skip_count = 0
problems = []

for hdr in headers:
    trx_num, post_order, cust_vendor_id, tx_date = hdr[0], hdr[1], hdr[2], hdr[3]
    main_amt, ref = to_float(hdr[4]), to_str(hdr[5])
    tx_date_str = tx_date.strftime("%Y-%m-%d") if isinstance(tx_date, (datetime, date)) else str(tx_date)[:10]
    inv_num = ref if ref else f"PO-{post_order}"
    cust = cust_map.get(cust_vendor_id) or cust_map.get(to_str(cust_vendor_id)) or {}

    if post_order in existing_po:
        status = existing_po[post_order]
        if status == "posted":
            skip_count += 1
            action = f"SKIP (already posted)"
        else:
            update_count += 1
            action = f"UPDATE (status={status})"
    else:
        new_count += 1
        action = "INSERT NEW"

    cust_id = cust.get("id", "???")
    print(f"  TRX={trx_num:6d} PO={post_order} date={tx_date_str} ref={inv_num:30s} cust_id={cust_id:8s} → {action}")

    if not cust:
        problems.append(f"  !! CustVendId={cust_vendor_id} NOT FOUND in cust_map")

print(f"\nSummary: {new_count} INSERT, {update_count} UPDATE, {skip_count} SKIP")
if problems:
    print("\nPROBLEMS FOUND:")
    for p in set(problems): print(p)

print(f"\n=== STEP 6: Actually run the INSERTs now? ===")
answer = input("Run the actual sync inserts into DB now? (yes/no): ").strip().lower()
if answer == "yes":
    now = datetime.now().isoformat()
    inserted = 0
    updated  = 0
    errors   = []
    for hdr in headers:
        trx_num, post_order, cust_vendor_id, tx_date = hdr[0], hdr[1], hdr[2], hdr[3]
        main_amt, ref, desc = to_float(hdr[4]), to_str(hdr[5]), to_str(hdr[6])
        jrnl_ex = int(hdr[7]) if hdr[7] is not None else 0
        tx_date_str = tx_date.strftime("%Y-%m-%d") if isinstance(tx_date, (datetime, date)) else str(tx_date)[:10]
        inv_num = ref if ref else f"PO-{post_order}"
        cust = cust_map.get(cust_vendor_id) or cust_map.get(to_str(cust_vendor_id)) or {}
        cust_name = cust.get("name", "") or desc or f"Unknown ({cust_vendor_id})"
        inv_type = "Credit Note" if (jrnl_ex == 9 or main_amt < 0) else "Invoice"

        try:
            if post_order in existing_po:
                if existing_po[post_order] != "posted":
                    conn.execute(
                        "UPDATE invoices SET trx_number=?,invoice_num=?,customer_name=?,customer_id=?,"
                        "invoice_date=?,amount=?,invoice_type=?,last_synced=? WHERE post_order=?",
                        (trx_num, inv_num, cust_name, cust.get("id",""),
                         tx_date_str, main_amt, inv_type, now, post_order)
                    )
                    updated += 1
            else:
                conn.execute(
                    "INSERT INTO invoices (post_order,trx_number,invoice_num,customer_name,customer_id,"
                    "customer_tin,customer_email,customer_phone,customer_address,customer_city,"
                    "invoice_date,amount,status,invoice_description,invoice_type,last_synced) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'pending',?,?,?)",
                    (post_order, trx_num, inv_num, cust_name, cust.get("id",""),
                     cust.get("tin",""), cust.get("email",""), cust.get("phone",""),
                     "", "", tx_date_str, main_amt, desc, inv_type, now)
                )
                inserted += 1
        except Exception as e:
            errors.append(f"PO={post_order} TRX={trx_num}: {e}")

    conn.commit()
    print(f"\nDone: {inserted} inserted, {updated} updated")
    if errors:
        print(f"\nERRORS ({len(errors)}):")
        for e in errors: print(f"  {e}")
    else:
        print("No errors!")

    print(f"\n=== AAP rows after sync ===")
    aap_rows2 = conn.execute("SELECT post_order, trx_number, invoice_num, invoice_date, status FROM invoices WHERE customer_id='AAP'").fetchall()
    print(f"AAP rows: {len(aap_rows2)}")
    for r in aap_rows2: print(f"  {dict(r)}")
else:
    print("Skipped. No changes made.")

conn.close()
print("\nDone.")