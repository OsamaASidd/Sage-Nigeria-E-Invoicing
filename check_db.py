import sqlite3

conn = sqlite3.connect('einvoice.db')

print("=== AAP invoices ===")
rows = conn.execute("SELECT post_order, trx_number, invoice_num, invoice_date, status FROM invoices WHERE customer_id='AAP'").fetchall()
if rows:
    for r in rows: print(r)
else:
    print("NONE FOUND")

print()
print("=== Total invoices ===")
print(conn.execute("SELECT COUNT(*) FROM invoices").fetchone()[0])

print()
print("=== post_orders 342374 to 342380 ===")
rows2 = conn.execute("SELECT post_order, trx_number, invoice_num, invoice_date, customer_id FROM invoices WHERE post_order BETWEEN 342374 AND 342380").fetchall()
if rows2:
    for r in rows2: print(r)
else:
    print("NONE FOUND")

print()
print("=== Schema PK check ===")
for r in conn.execute("PRAGMA table_info(invoices)").fetchall():
    if r[5] == 1: print(f"PRIMARY KEY: {r[1]}")

print()
print("=== Customers named ARDOVA ===")
rows3 = conn.execute("SELECT post_order, trx_number, invoice_num, invoice_date, customer_id, customer_name FROM invoices WHERE customer_name LIKE '%ARDOVA%'").fetchall()
if rows3:
    for r in rows3: print(r)
else:
    print("NONE FOUND")

conn.close()
print("\nDone.")