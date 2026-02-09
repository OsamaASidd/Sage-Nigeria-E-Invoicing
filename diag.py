"""Diagnostic 2 - find sales invoices and customer linking"""
import pyodbc
conn = pyodbc.connect(
    "Driver={Pervasive ODBC Client Interface};"
    "ServerName=localhost;DBQ=PROTONSECURITYSERVIC;"
    "UID=Peachtree;PWD=cool123;"
)
c = conn.cursor()

# 1. All distinct Module values
print("=== DISTINCT MODULE VALUES ===")
try:
    c.execute('SELECT DISTINCT Module FROM "JrnlHdr"')
    for row in c.fetchall():
        print(f"  Module = '{row[0]}'")
except:
    pass

# 2. Sample rows where Module is NOT 'G'
print("\n=== NON-G MODULE ROWS (first 15) ===")
try:
    c.execute("SELECT Module, JrnlKey_TrxNumber, CustVendId, TransactionDate, MainAmount, Reference, Description FROM \"JrnlHdr\" WHERE Module <> 'G'")
    cols = [x[0] for x in c.description]
    for row in c.fetchmany(15):
        d = dict(zip(cols, row))
        print(f"  Module={d['Module']!r} TRX={d['JrnlKey_TrxNumber']} Cust={d['CustVendId']!r} "
              f"Date={d['TransactionDate']} Amt={d['MainAmount']} Ref={d['Reference']!r} "
              f"Desc={str(d['Description'])[:50]!r}")
except Exception as e:
    print(f"  Error: {e}")

# 3. Customers table - first 5 rows to see structure
print("\n=== CUSTOMERS COLUMNS ===")
for col in c.columns(table="Customers"):
    print(f"  {col.column_name} ({col.type_name})")

print("\n=== CUSTOMERS SAMPLE (5 rows) ===")
c.execute('SELECT * FROM "Customers"')
cols = [x[0] for x in c.description]
for row in c.fetchmany(5):
    d = dict(zip(cols, row))
    d2 = {k: v for k, v in d.items() if v and str(v).strip() and str(v).strip() != '0'}
    print(f"  {d2}")

# 4. Check if CustVendId in JrnlHdr matches CustomerRecordNumber in Customers
print("\n=== CUSTOMER RECORD NUMBER LINKING ===")
try:
    c.execute("""
        SELECT TOP 5 j.CustVendId, j.Reference, j.Module, c.CustomerID
        FROM "JrnlHdr" j
        INNER JOIN "Customers" c ON j.CustVendId = c.CustomerRecordNumber
        WHERE j.Module <> 'G' AND j.CustVendId <> 0
    """)
    cols = [x[0] for x in c.description]
    for row in c.fetchall():
        d = dict(zip(cols, row))
        print(f"  CustVendId={d['CustVendId']} -> CustomerID={d['CustomerID']!r} Module={d['Module']!r} Ref={d['Reference']!r}")
except Exception as e:
    print(f"  JOIN failed: {e}")
    # Fallback: manual check
    print("  Trying manual lookup...")
    c.execute("SELECT CustVendId FROM \"JrnlHdr\" WHERE Module <> 'G' AND CustVendId <> 0")
    cust_ids = [row[0] for row in c.fetchmany(5)]
    print(f"  Sample CustVendId values: {cust_ids}")
    if cust_ids:
        for cid in cust_ids[:3]:
            try:
                c.execute(f'SELECT CustomerID, CustomerRecordNumber FROM "Customers" WHERE CustomerRecordNumber = {cid}')
                row = c.fetchone()
                if row:
                    print(f"    CustVendId {cid} -> CustomerID={row[0]!r} RecNum={row[1]}")
                else:
                    print(f"    CustVendId {cid} -> NOT FOUND in Customers")
            except Exception as e2:
                print(f"    Error: {e2}")

conn.close()
print("\nDone!")