import pyodbc

db = "PROTONSECURITYSERVIC"

conn = pyodbc.connect(
    f"Driver={{Pervasive ODBC Client Interface}};"
    f"ServerName=localhost;"
    f"DBQ={db};"
    f"UID=Peachtree;PWD=cool123;"
)

cursor = conn.cursor()
tables = [t.table_name for t in cursor.tables(tableType="TABLE")]
print(f"✅ Connected! {len(tables)} tables\n")
for t in sorted(tables):
    print(f"   📋 {t}")

# Sample customer data
for t in tables:
    if "CUSTOMER" in t.upper():
        cursor.execute(f'SELECT * FROM "{t}"')
        rows = cursor.fetchmany(3)
        cols = [c[0] for c in cursor.description]
        print(f"\n📋 {t} ({len(cols)} columns): {cols[:15]}")
        for row in rows:
            print(f"   → {dict(list(zip(cols, row))[:8])}")
        break

# Sample journal headers (invoices)
for t in tables:
    if "JRNL" in t.upper() and "HDR" in t.upper():
        cursor.execute(f'SELECT * FROM "{t}"')
        rows = cursor.fetchmany(3)
        cols = [c[0] for c in cursor.description]
        print(f"\n📋 {t} ({len(cols)} columns): {cols[:15]}")
        for row in rows:
            print(f"   → {dict(list(zip(cols, row))[:8])}")
        break

conn.close()
print("\n✅ Done!")
