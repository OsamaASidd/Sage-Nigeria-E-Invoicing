"""
Nigeria E-Invoicing Dashboard
==============================
- Page ALWAYS loads (never crashes on DB lock)
- Sage sync via AJAX only (never blocks page load)
- Single threading lock prevents concurrent DB access
- Pagination (25 per page)
- Line items fetched on-demand when posting
- FIXED: Uses PostOrder (unique) instead of JrnlKey_TrxNumber (shared across modules)
- FIXED: Per-line VAT from Sage instead of blanket 7.5%
- FIXED: cust_map keyed by BOTH CustomerRecordNumber AND CustomerID text
         so new customers whose CustVendId is a text ID are no longer missed
- NEW: Preview payload before posting
- NEW: /api/debug-sync to inspect CustVendId values live
"""

import os, io, json, sqlite3, threading, pyodbc, requests
from datetime import datetime, date
from decimal import Decimal
from flask import Flask, render_template, jsonify, send_file, request

ODBC_CONN = (
    "Driver={Pervasive ODBC Client Interface};"
    "ServerName=localhost;DBQ=PROTONSECURITYSERVIC;"
    "UID=Peachtree;PWD=cool123;"
)
API_URL = "https://preprod-ng.flick.network/v1"
API_HEADERS = {
    "Content-Type": "application/json",
    "participant-id": "019a0b76-f33e-787a-8d0f-70dc096efba6",
    "x-api-key": "4b2e92e2929ce78f586ed468ddb7d666321e6f2a4cdcf65773669bcfec967719",
}
SUPPLIER = {
    "name": "PROTON SECURITY SERVICES LIMITED",
    "address": "Lagos, Nigeria",
    "tin": "23773131-0001",
    "email": "info@protonsecurity.com",
    "telephone": "+234",
    "business_id": "019a0b76-f33e-787a-8d0f-70dc096efba6",
    "street_name": "Lagos",
    "city_name": "Lagos",
    "postal_zone": "100001",
    "country": "NG",
}

# Tax category IDs for Flick API — change these if you get "Invalid Tax category" errors
TAX_CAT_STANDARD = os.environ.get("TAX_CAT_STANDARD", "STANDARD_VAT")
TAX_CAT_EXEMPT = os.environ.get("TAX_CAT_EXEMPT", "ZERO_VAT")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "einvoice.db")
PDF_DIR = os.path.join(BASE_DIR, "invoices")
os.makedirs(PDF_DIR, exist_ok=True)
PER_PAGE = 25
app = Flask(__name__)
_db_lock = threading.Lock()

# === SQLITE ===
def _open_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn

def db_read(sql, params=()):
    with _db_lock:
        conn = _open_db()
        try: return [dict(r) for r in conn.execute(sql, params).fetchall()]
        finally: conn.close()

def db_read_one(sql, params=()):
    with _db_lock:
        conn = _open_db()
        try:
            row = conn.execute(sql, params).fetchone()
            return dict(row) if row else None
        finally: conn.close()

def db_write(sql, params=()):
    with _db_lock:
        conn = _open_db()
        try: conn.execute(sql, params); conn.commit()
        finally: conn.close()

def db_write_many(operations):
    with _db_lock:
        conn = _open_db()
        try:
            for sql, params in operations: conn.execute(sql, params)
            conn.commit()
        finally: conn.close()

def init_db():
    """
    Schema uses post_order as PRIMARY KEY (always unique in Sage).
    JrnlKey_TrxNumber is NOT unique: Sage reuses it for recurring invoices
    (same TRX number, different PostOrder each month). Using trx_number as PK
    caused recurring invoices to silently overwrite each other.
    Migration: if old trx_number-PK table exists, rename and rebuild.
    """
    with _db_lock:
        conn = _open_db()
        try:
            # Detect old schema (PK was trx_number)
            old_schema = False
            try:
                info = conn.execute("PRAGMA table_info(invoices)").fetchall()
                pk_col = next((r[1] for r in info if r[5] == 1), None)
                if pk_col == "trx_number":
                    old_schema = True
            except:
                pass

            if old_schema:
                print("[MIGRATION] Old schema detected (PK=trx_number). Migrating to PK=post_order...")
                conn.execute("ALTER TABLE invoices RENAME TO invoices_old")
                try:
                    conn.execute("ALTER TABLE invoice_lines RENAME TO invoice_lines_old")
                except:
                    pass
                conn.commit()

            conn.execute("""CREATE TABLE IF NOT EXISTS invoices (
                post_order INTEGER PRIMARY KEY,
                trx_number INTEGER,
                invoice_num TEXT, customer_name TEXT, customer_id TEXT,
                customer_tin TEXT, customer_email TEXT, customer_phone TEXT,
                customer_address TEXT, customer_city TEXT, invoice_date TEXT,
                amount REAL DEFAULT 0, vat_amount REAL DEFAULT 0,
                status TEXT DEFAULT 'pending',
                irn TEXT, qr_code TEXT, posted_at TEXT,
                error_message TEXT, api_response TEXT,
                invoice_description TEXT,
                invoice_type TEXT DEFAULT 'Invoice',
                last_synced TEXT)""")

            conn.execute("""CREATE TABLE IF NOT EXISTS invoice_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_order INTEGER,
                trx_number INTEGER,
                line_num INTEGER, item_code TEXT, description TEXT,
                quantity REAL DEFAULT 1, unit_price REAL DEFAULT 0,
                amount REAL DEFAULT 0, tax_rate REAL DEFAULT 0)""")

            conn.execute("CREATE INDEX IF NOT EXISTS idx_invoices_trx ON invoices(trx_number)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_invoices_status ON invoices(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_invoices_customer ON invoices(customer_id)")
            conn.commit()

            if old_schema:
                conn.execute("""
                    INSERT OR IGNORE INTO invoices
                        (post_order, trx_number, invoice_num, customer_name, customer_id,
                         customer_tin, customer_email, customer_phone, customer_address,
                         customer_city, invoice_date, amount, vat_amount, status,
                         irn, qr_code, posted_at, error_message, api_response,
                         invoice_description, invoice_type, last_synced)
                    SELECT
                        COALESCE(post_order, trx_number),
                        trx_number, invoice_num, customer_name, customer_id,
                        customer_tin, customer_email, customer_phone, customer_address,
                        customer_city, invoice_date, amount,
                        COALESCE(vat_amount, 0), status,
                        irn, qr_code, posted_at, error_message, api_response,
                        invoice_description, invoice_type, last_synced
                    FROM invoices_old
                """)
                conn.execute("""
                    INSERT OR IGNORE INTO invoice_lines
                        (post_order, trx_number, line_num, item_code, description,
                         quantity, unit_price, amount, tax_rate)
                    SELECT
                        COALESCE(
                            (SELECT post_order FROM invoices_old
                             WHERE trx_number = il.trx_number LIMIT 1),
                            il.trx_number
                        ),
                        il.trx_number, il.line_num, il.item_code, il.description,
                        il.quantity, il.unit_price, il.amount,
                        COALESCE(il.tax_rate, 0)
                    FROM invoice_lines_old il
                """)
                conn.commit()
                print("[MIGRATION] Done. Old tables kept as invoices_old/invoice_lines_old.")
        finally:
            conn.close()

init_db()

# === HELPERS ===
def to_float(val):
    if val is None: return 0.0
    if isinstance(val, Decimal): return float(val)
    try: return float(val)
    except: return 0.0

def to_str(val):
    if val is None: return ""
    return str(val).strip()

def find_col(columns, *candidates):
    for c in candidates:
        if c in columns: return c
    return None

# === SAGE SYNC ===
def sync_headers_from_sage(date_from=None, date_to=None):
    if not date_from:
        today = date.today()
        date_from = today.replace(day=1).strftime("%Y-%m-%d")
    if not date_to:
        today = date.today()
        date_to = (
            today.replace(day=31).strftime("%Y-%m-%d")
            if today.month == 12
            else today.replace(month=today.month + 1, day=1).strftime("%Y-%m-%d")
        )

    try:
        sage = pyodbc.connect(ODBC_CONN)
    except Exception as e:
        return {"ok": False, "error": f"ODBC: {e}"}

    try:
        cursor = sage.cursor()
        cursor.execute(
            'SELECT JrnlKey_TrxNumber, PostOrder, CustVendId, TransactionDate, MainAmount, '
            'Reference, Description, JournalEx FROM "JrnlHdr" '
            "WHERE Module='R' AND JournalEx IN (8, 9) "
            "AND TransactionDate>=? AND TransactionDate<=? ORDER BY TransactionDate DESC",
            (date_from, date_to),
        )
        headers = cursor.fetchall()

        # ── Build invoice number lookup from JrnlRow.InvNumForThisTrx ──
        # JrnlRow has an InvNumForThisTrx column on every line, keyed by PostOrder.
        # This is the authoritative invoice number — it is present even for recurring
        # invoices that have a blank JrnlHdr.Reference field.
        inv_num_by_po = {}
        try:
            # Pervasive SQL doesn't support != '' — filter empty strings in Python instead
            cursor.execute(
                'SELECT DISTINCT PostOrder, InvNumForThisTrx FROM "JrnlRow" '
                'WHERE InvNumForThisTrx IS NOT NULL '
                'AND PostOrder IS NOT NULL AND PostOrder != 0'
            )
            for row in cursor.fetchall():
                po, inv = row[0], to_str(row[1])
                if po and inv:  # skip empty strings here in Python
                    if po not in inv_num_by_po:
                        inv_num_by_po[po] = inv
            print(f"[SYNC] Invoice numbers from JrnlRow.InvNumForThisTrx: {len(inv_num_by_po)}")
        except Exception as e:
            print(f"[WARN] Could not load InvNumForThisTrx: {e}")

        # ── FIX: Build cust_map keyed by BOTH CustomerRecordNumber (int) ──
        # AND CustomerID (text), because JrnlHdr.CustVendId may store either.
        # Old customers may have matched by coincidence; new customers whose
        # CustVendId is a text CustomerID were silently returning {} and being
        # inserted with blank names / missing data.
        cust_map = {}
        try:
            cursor.execute(
                'SELECT CustomerRecordNumber, CustomerID, Customer_Bill_Name, '
                'Phone_Number, eMail_Address, SalesTaxResaleNum FROM "Customers"'
            )
            for cr in cursor.fetchall():
                rec = {
                    "id":    to_str(cr[1]),
                    "name":  to_str(cr[2]),
                    "phone": to_str(cr[3]),
                    "email": to_str(cr[4]),
                    "tin":   to_str(cr[5]),
                }
                # Key by integer CustomerRecordNumber
                if cr[0] is not None:
                    cust_map[cr[0]] = rec
                # ALSO key by text CustomerID (covers CustVendId text lookups)
                cust_id_str = to_str(cr[1])
                if cust_id_str:
                    cust_map[cust_id_str] = rec
        except Exception as e:
            print(f"[WARN] Customers query failed: {e}")

        # ── FIX: Build addr_map keyed by CustomerRecordNumber (int) ──
        # We also build a secondary lookup by CustomerID string in case
        # CustVendId is a text key and we need to cross-reference addresses.
        addr_map = {}
        addr_by_custid = {}
        try:
            # Join Address to Customers to get CustomerID alongside address
            cursor.execute(
                'SELECT a.CustomerRecordNumber, a.AddressLine1, a.AddressLine2, a.City, '
                'c.CustomerID FROM "Address" a '
                'LEFT JOIN "Customers" c ON a.CustomerRecordNumber = c.CustomerRecordNumber'
            )
            for ar in cursor.fetchall():
                addr_rec = {
                    "address": ", ".join(p for p in [to_str(ar[1]), to_str(ar[2])] if p),
                    "city": to_str(ar[3]),
                }
                if ar[0] not in addr_map:
                    addr_map[ar[0]] = addr_rec
                cust_id_str = to_str(ar[4])
                if cust_id_str and cust_id_str not in addr_by_custid:
                    addr_by_custid[cust_id_str] = addr_rec
        except Exception:
            # Fallback: plain Address query without JOIN (original behaviour)
            try:
                cursor.execute('SELECT CustomerRecordNumber, AddressLine1, AddressLine2, City FROM "Address"')
                for ar in cursor.fetchall():
                    if ar[0] not in addr_map:
                        addr_map[ar[0]] = {
                            "address": ", ".join(p for p in [to_str(ar[1]), to_str(ar[2])] if p),
                            "city": to_str(ar[3]),
                        }
            except Exception as e:
                print(f"[WARN] Address query failed: {e}")

    finally:
        sage.close()

    # ── Key by post_order (unique) not trx_number (reused for recurring invoices) ──
    # Sage reuses JrnlKey_TrxNumber for recurring monthly invoices — same TRX,
    # different PostOrder each month. Using trx_number as PK caused all but one
    # occurrence to be silently discarded on INSERT conflict.
    existing_po = {r["post_order"]: r["status"] for r in db_read("SELECT post_order, status FROM invoices")}
    now = datetime.now().isoformat()
    operations = []
    new_count = 0
    unresolved = []

    for hdr in headers:
        trx_num, post_order, cust_vendor_id, tx_date = hdr[0], hdr[1], hdr[2], hdr[3]
        main_amt, ref, desc = to_float(hdr[4]), to_str(hdr[5]), to_str(hdr[6])
        jrnl_ex = int(hdr[7]) if len(hdr) > 7 and hdr[7] is not None else 0
        tx_date_str = (
            tx_date.strftime("%Y-%m-%d")
            if isinstance(tx_date, (datetime, date))
            else str(tx_date)[:10]
        )

        # Invoice number: JrnlHdr.Reference is blank for recurring invoices.
        # JrnlRow.InvNumForThisTrx is always populated and is the true invoice number.
        # Priority: 1) JrnlHdr.Reference  2) JrnlRow.InvNumForThisTrx by PostOrder
        inv_num = ref or inv_num_by_po.get(post_order, "")
        if not inv_num:
            print(f"[WARN] No invoice number found for PostOrder={post_order} TRX={trx_num}")
            inv_num = f"PO-{post_order}"

        cust = (
            cust_map.get(cust_vendor_id)
            or cust_map.get(to_str(cust_vendor_id))
            or {}
        )
        if not cust:
            unresolved.append(cust_vendor_id)

        addr = (
            addr_map.get(cust_vendor_id)
            or addr_map.get(int(cust_vendor_id) if str(cust_vendor_id).isdigit() else -1, {})
            or addr_by_custid.get(to_str(cust_vendor_id))
            or {}
        )

        cust_name = cust.get("name", "") or desc or f"Unknown ({cust_vendor_id})"

        if jrnl_ex == 9 or main_amt < 0 or "CREDIT MEMO" in (ref or "").upper() or (ref or "").upper().startswith("CM/"):
            inv_type = "Credit Note"
        else:
            inv_type = "Invoice"

        # INSERT or UPDATE keyed on post_order — never overwrites a posted record
        if post_order in existing_po:
            if existing_po[post_order] != "posted":
                operations.append((
                    "UPDATE invoices SET trx_number=?,invoice_num=?,customer_name=?,customer_id=?,"
                    "customer_tin=?,customer_email=?,customer_phone=?,customer_address=?,customer_city=?,"
                    "invoice_date=?,amount=?,invoice_description=?,invoice_type=?,last_synced=? "
                    "WHERE post_order=?",
                    (trx_num, inv_num, cust_name, cust.get("id",""), cust.get("tin",""),
                     cust.get("email",""), cust.get("phone",""), addr.get("address",""),
                     addr.get("city",""), tx_date_str, main_amt, desc, inv_type, now, post_order),
                ))
        else:
            new_count += 1
            operations.append((
                "INSERT INTO invoices (post_order,trx_number,invoice_num,customer_name,customer_id,"
                "customer_tin,customer_email,customer_phone,customer_address,customer_city,"
                "invoice_date,amount,status,invoice_description,invoice_type,last_synced) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'pending',?,?,?)",
                (post_order, trx_num, inv_num, cust_name, cust.get("id",""), cust.get("tin",""),
                 cust.get("email",""), cust.get("phone",""), addr.get("address",""),
                 addr.get("city",""), tx_date_str, main_amt, desc, inv_type, now),
            ))

    if unresolved:
        print(f"[WARN] {len(set(str(x) for x in unresolved))} unresolved CustVendId(s). "
              f"Hit /api/debug-sync to inspect.")

    if operations:
        db_write_many(operations)

    return {
        "ok": True,
        "synced": len(headers),
        "new": new_count,
        "unresolved_customers": len(unresolved),
        "date_from": date_from,
        "date_to": date_to,
    }


# === FETCH LINE ITEMS (PostOrder FK) ===
def fetch_line_items(post_order):
    """Fetch Sage line items by PostOrder (unique, handles recurring invoices)."""
    try: sage = pyodbc.connect(ODBC_CONN)
    except Exception as e: return [], 0, f"ODBC: {e}"
    try:
        cursor = sage.cursor()
        print(f"[LINES] PostOrder={post_order}")
        jrnlrow_cols = [c.column_name for c in cursor.columns(table="JrnlRow")]
        jr_amount = find_col(jrnlrow_cols, "Amount")
        jr_qty = find_col(jrnlrow_cols, "Quantity", "StockingQuantity")
        jr_price = find_col(jrnlrow_cols, "UnitCost", "UnitPrice", "StockingUnitCost")
        jr_desc = find_col(jrnlrow_cols, "RowDescription", "Description", "ItemDescription", "LineDescription", "Memo")
        jr_itemrec = find_col(jrnlrow_cols, "ItemRecordNumber")
        jr_glacct = find_col(jrnlrow_cols, "GLAcntNumber")
        jr_rownum = find_col(jrnlrow_cols, "RowNumber")

        lineitem_cols = [c.column_name for c in cursor.columns(table="LineItem")]
        li_recnum = find_col(lineitem_cols, "ItemRecordNumber", "RecordNumber")
        li_itemid = find_col(lineitem_cols, "ItemID")
        li_desc = find_col(lineitem_cols, "ItemDescription", "SalesDescription")
        li_price = find_col(lineitem_cols, "SalesPrice1", "SalesPrice", "Price", "UnitPrice", "Cost")
        item_lookup = {}
        if li_recnum and li_itemid:
            select_parts = [li_recnum, li_itemid]
            if li_desc: select_parts.append(li_desc)
            if li_price: select_parts.append(li_price)
            try:
                cursor.execute(f'SELECT {", ".join(select_parts)} FROM "LineItem" WHERE {li_itemid} <> \'\'')
                for row in cursor.fetchall():
                    idx = 2; desc_val = ""; price_val = 0
                    if li_desc: desc_val = to_str(row[idx]); idx += 1
                    if li_price and idx < len(row): price_val = to_float(row[idx])
                    item_lookup[row[0]] = {"item_id": to_str(row[1]), "description": desc_val, "price": price_val}
            except: pass

        jr_select = [c for c in [jr_glacct, jr_amount, jr_qty, jr_price, jr_rownum, jr_itemrec, jr_desc] if c]
        jr_stt = find_col(jrnlrow_cols, "SalesTaxType")
        if jr_stt and jr_stt not in jr_select: jr_select.append(jr_stt)
        if not jr_select: return [], 0, "No usable JrnlRow columns"

        cursor.execute(f'SELECT {", ".join(jr_select)} FROM "JrnlRow" WHERE "PostOrder" = {post_order}')
        rc = [c[0] for c in cursor.description]; all_rows = cursor.fetchall()
        lines = []; vat_amount = 0.0
        for lr in all_rows:
            ld = dict(zip(rc, lr))
            qty = to_float(ld.get(jr_qty, 0)) if jr_qty else 0
            amount = to_float(ld.get(jr_amount, 0)) if jr_amount else 0
            unit_cost = to_float(ld.get(jr_price, 0)) if jr_price else 0
            item_recnum = ld.get(jr_itemrec, 0) if jr_itemrec else 0
            row_desc = to_str(ld.get(jr_desc, "")) if jr_desc else ""
            upper_desc = row_desc.upper()
            if ("VALUE ADDED TAX" in upper_desc or "VAT" in upper_desc) and item_recnum == 0 and qty == 0:
                vat_amount = abs(amount); continue
            item_info = item_lookup.get(item_recnum, {})
            item_id = item_info.get("item_id", ""); item_desc_val = item_info.get("description", "")
            sales_price = item_info.get("price", 0)
            line_desc = row_desc or item_desc_val or item_id or ""
            if unit_cost != 0:
                unit_price = abs(unit_cost)
            elif qty != 0 and amount != 0:
                unit_price = abs(amount / qty)
            elif sales_price > 0:
                unit_price = sales_price
            else:
                unit_price = abs(amount)
            if qty != 0 or item_recnum > 0:
                lines.append({"item_code": item_id or str(item_recnum), "description": line_desc or "Service",
                    "quantity": abs(qty) if qty != 0 else 1, "unit_price": unit_price,
                    "amount": abs(qty if qty != 0 else 1) * unit_price, "tax_rate": 0})

        if lines and vat_amount > 0:
            taxable_base = round(vat_amount / 0.075, 2); matched = False
            for line in lines:
                if abs(line["amount"] - taxable_base) < 0.02: line["tax_rate"] = 7.5; matched = True
            if not matched:
                subtotal = sum(l["amount"] for l in lines)
                if abs(subtotal - taxable_base) < 0.02:
                    for line in lines: line["tax_rate"] = 7.5
                    matched = True
            if not matched:
                remaining = taxable_base
                for line in sorted(lines, key=lambda l: l["amount"], reverse=True):
                    if remaining >= line["amount"] - 0.02: line["tax_rate"] = 7.5; remaining -= line["amount"]
                    if remaining < 0.02: break
        return lines, vat_amount, None
    except Exception as e: return [], 0, str(e)
    finally: sage.close()

# === BUILD PAYLOAD (shared by post and preview) ===
def build_payload(trx_number):
    inv = db_read_one("SELECT * FROM invoices WHERE post_order=?", (trx_number,))
    if not inv: return None, [], 0, "Invoice not found"
    lines, vat_amount, line_error = fetch_line_items(inv["post_order"] if inv else trx_number)
    if not lines:
        amt = abs(to_float(inv["amount"]))
        if amt > 0:
            sage_desc = to_str(inv.get("invoice_description","")) or to_str(inv.get("customer_name",""))
            lines = [{"item_code": inv["invoice_num"] or f"TRX-{trx_number}", "description": sage_desc or "Security Services",
                "quantity": 1, "unit_price": amt, "amount": amt, "tax_rate": 7.5}]
            if vat_amount == 0: vat_amount = round(amt - (amt / 1.075), 2)
        else: return None, [], 0, line_error or "No line items"

    cust_tin = inv["customer_tin"] or "23773131-0001"
    cust_email = inv["customer_email"] or "noemail@placeholder.com"
    cust_phone = inv["customer_phone"] or "+234"

    subtotal = sum(l["amount"] for l in lines if l["unit_price"] > 0)
    tax_amount = vat_amount
    grand_total = subtotal + tax_amount

    api_lines = []
    for i, line in enumerate(lines):
        if line["unit_price"] <= 0: continue
        lr = line.get("tax_rate", 0)
        line_ext = line["quantity"] * line["unit_price"]
        api_lines.append({
            "hsn_code": "2710.19",
            "item_name": line["description"] or "Service",
            "price_amount": line["unit_price"],
            "invoiced_quantity": line["quantity"],
            "line_extension_amount": line_ext,
            "discount_amount": 1,
            "tax_rate": lr,
            "tax_category_id": TAX_CAT_STANDARD if lr > 0 else TAX_CAT_EXEMPT,
            "uom": "EA",
            "sellers_item_identification": line["item_code"] or f"ITEM-{i+1}",
            "product_category": "Security Services",
        })

    if not api_lines: return None, lines, vat_amount, "No valid line items"

    inv_num = inv["invoice_num"] or f"TRX-{trx_number}"
    irn = f"{inv_num}-{(inv['invoice_date'] or '').replace('-', '')}"

    inv_type = inv.get("invoice_type") or "Invoice"
    if inv_type == "Credit Note": type_code = "381"
    elif inv_type == "Debit Note": type_code = "383"
    else: type_code = "394"

    payload = {
        "business_id": SUPPLIER["business_id"],
        "irn": irn,
        "document_identifier": inv_num,
        "issue_date": inv["invoice_date"],
        "invoice_type_code": type_code,
        "document_currency_code": "NGN",
        "tax_currency_code": "NGN",
        "accounting_supplier_party": {
            "party_name": SUPPLIER["name"],
            "tin": SUPPLIER["tin"],
            "email": SUPPLIER["email"],
            "telephone": SUPPLIER["telephone"],
            "business_description": "Security Services",
            "postal_address": {
                "street_name": SUPPLIER["street_name"],
                "city_name": SUPPLIER["city_name"],
                "postal_zone": SUPPLIER["postal_zone"],
                "country": SUPPLIER["country"],
            },
        },
        "accounting_customer_party": {
            "party_name": inv["customer_name"],
            "tin": cust_tin,
            "email": cust_email,
            "telephone": cust_phone,
            "business_description": "Customer",
            "postal_address": {
                "street_name": inv["customer_address"] or "N/A",
                "city_name": inv["customer_city"] or "Lagos",
                "postal_zone": "100001",
                "country": "NG",
            },
        },
        "legal_monetary_total": {
            "line_extension_amount": subtotal,
            "tax_exclusive_amount": subtotal,
            "tax_inclusive_amount": grand_total,
            "payable_amount": grand_total,
        },
        "invoice_line": api_lines,
    }
    return payload, lines, vat_amount, None

# === POST TO FIRS ===
def post_to_firs(trx_number):
    inv = db_read_one("SELECT * FROM invoices WHERE post_order=?", (trx_number,))
    if not inv: return {"ok": False, "error": "Not found"}
    if inv["status"] == "posted": return {"ok": False, "error": "Already posted", "irn": inv["irn"]}
    payload, lines, vat_amount, build_error = build_payload(trx_number)
    if not payload:
        db_write("UPDATE invoices SET status='failed', error_message=? WHERE post_order=?", (build_error[:500], trx_number))
        return {"ok": False, "error": build_error}
    ops = [("DELETE FROM invoice_lines WHERE post_order=?", (trx_number,)),
           ("UPDATE invoices SET vat_amount=? WHERE post_order=?", (vat_amount, trx_number))]
    for i, line in enumerate(lines):
        ops.append(("INSERT INTO invoice_lines (post_order,trx_number,line_num,item_code,description,quantity,unit_price,amount,tax_rate) VALUES (?,?,?,?,?,?,?,?,?)",
            (trx_number, inv["trx_number"], i+1, line["item_code"], line["description"], line["quantity"], line["unit_price"], line["amount"], line.get("tax_rate",0))))
    db_write_many(ops)
    try:
        resp = requests.post(f"{API_URL}/invoice/generate", headers=API_HEADERS, json=payload, timeout=30)
        resp_text = resp.text
        resp_json = {}
        try: resp_json = resp.json()
        except: pass
        if resp.status_code in (200, 201):
            data = resp_json.get("data", resp_json); irn = data.get("irn", "N/A"); qr_code = data.get("qr_code", "")
            db_write("UPDATE invoices SET status='posted', irn=?, qr_code=?, posted_at=?, error_message=NULL, api_response=? WHERE post_order=?",
                (irn, qr_code, datetime.now().isoformat(), resp_text[:5000], trx_number))
            generate_pdf(trx_number); return {"ok": True, "irn": irn, "status": "posted"}
        elif resp.status_code == 409:
            errors = resp_json.get("errors", {}); irn = errors.get("irn", resp_json.get("irn", "")); qr_code = errors.get("qr_code", resp_json.get("qr_code", ""))
            if irn:
                db_write("UPDATE invoices SET status='posted', irn=?, qr_code=?, posted_at=?, error_message=NULL, api_response=? WHERE post_order=?",
                    (irn, qr_code, datetime.now().isoformat(), resp_text[:5000], trx_number))
                generate_pdf(trx_number); return {"ok": True, "irn": irn, "status": "posted", "note": "Already on FIRS"}
            error_msg = resp_json.get("message", "409 conflict")
            db_write("UPDATE invoices SET status='failed', error_message=?, api_response=? WHERE post_order=?", (error_msg[:500], resp_text[:5000], trx_number))
            return {"ok": False, "error": error_msg, "status_code": resp.status_code, "api_response": resp_json or resp_text[:2000]}
        else:
            error_msg = resp_json.get("message", resp.text[:300])
            db_write("UPDATE invoices SET status='failed', error_message=?, api_response=? WHERE post_order=?", (error_msg[:500], resp_text[:5000], trx_number))
            return {"ok": False, "error": error_msg, "status_code": resp.status_code, "api_response": resp_json or resp_text[:2000]}
    except requests.exceptions.ConnectionError as e:
        db_write("UPDATE invoices SET status='failed', error_message=? WHERE post_order=?", (f"Connection: {str(e)[:200]}", trx_number))
        return {"ok": False, "error": f"Connection failed: {e}"}
    except Exception as e: return {"ok": False, "error": str(e)}

# === PDF ===
def generate_pdf(trx_number):
    from reportlab.lib.pagesizes import A4; from reportlab.lib import colors
    from reportlab.pdfgen import canvas; from reportlab.platypus import Table, TableStyle
    from reportlab.lib.utils import ImageReader
    inv = db_read_one("SELECT * FROM invoices WHERE post_order=?", (trx_number,))
    lines = db_read("SELECT * FROM invoice_lines WHERE post_order=? ORDER BY line_num", (trx_number,))
    if not inv: return None
    qr_img_reader = None
    if inv["qr_code"]:
        try:
            import qrcode; qr = qrcode.QRCode(version=1, box_size=4, border=2); qr.add_data(inv["qr_code"]); qr.make(fit=True)
            img = qr.make_image(fill_color="black", back_color="white"); buf = io.BytesIO(); img.save(buf, format="PNG"); buf.seek(0)
            qr_img_reader = ImageReader(buf)
        except: pass
    safe_name = (inv["invoice_num"] or f"TRX-{trx_number}").replace("/","_").replace("\\","_").replace(" ","_")
    pdf_path = os.path.join(PDF_DIR, f"{safe_name}.pdf")
    w, h = A4; c = canvas.Canvas(pdf_path, pagesize=A4)
    navy=colors.HexColor("#0f172a"); blue=colors.HexColor("#2563eb"); slate50=colors.HexColor("#f8fafc")
    slate200=colors.HexColor("#e2e8f0"); slate500=colors.HexColor("#64748b"); slate800=colors.HexColor("#1e293b"); green=colors.HexColor("#16a34a")
    y = h - 30
    c.setFillColor(navy); c.rect(0,y-60,w,70,fill=True,stroke=False)
    c.setFillColor(colors.white); c.setFont("Helvetica-Bold",16); c.drawString(30,y-25,SUPPLIER["name"])
    c.setFont("Helvetica",9); c.drawString(30,y-42,SUPPLIER["address"])
    c.setFillColor(green); c.roundRect(w-145,y-47,115,30,4,fill=True,stroke=False)
    c.setFillColor(colors.white); c.setFont("Helvetica-Bold",11); c.drawCentredString(w-87,y-37,"E-INVOICE"); y-=85
    c.setFillColor(slate800); c.setFont("Helvetica-Bold",22); c.drawString(30,y,"INVOICE"); y-=25
    for label, val in [("Invoice No:",inv["invoice_num"]),("Date:",inv["invoice_date"]),("IRN:",inv["irn"] or "Pending"),("Currency:","NGN")]:
        c.setFont("Helvetica-Bold",9); c.setFillColor(slate500); c.drawString(30,y,label)
        c.setFont("Helvetica",9); c.setFillColor(slate800); c.drawString(115,y,str(val)); y-=15
    if qr_img_reader: c.drawImage(qr_img_reader, w-140, y+5, 105, 105)
    y-=15
    c.setFillColor(slate50); c.rect(25,y-55,w-50,60,fill=True,stroke=False)
    c.setStrokeColor(slate200); c.rect(25,y-55,w-50,60,fill=False,stroke=True)
    c.setFillColor(blue); c.setFont("Helvetica-Bold",9); c.drawString(35,y-5,"BILL TO")
    c.setFillColor(slate800); c.setFont("Helvetica-Bold",11); c.drawString(35,y-20,inv["customer_name"] or "")
    c.setFont("Helvetica",8); c.setFillColor(slate500)
    addr = f"{inv['customer_address'] or ''}, {inv['customer_city'] or ''}".strip(", ")
    c.drawString(35,y-34,addr[:80])
    if inv["customer_tin"]: c.drawString(35,y-46,f"TIN: {inv['customer_tin']}")
    c.drawRightString(w-35,y-20,inv["customer_email"] or ""); c.drawRightString(w-35,y-34,inv["customer_phone"] or ""); y-=75
    c.setFillColor(slate800); c.setFont("Helvetica-Bold",10); c.drawString(30,y,"Line Items"); y-=5
    table_data = [["#","Description","Qty","Unit Price (N)","Tax","Amount (N)"]]; total=0.0
    for line in lines:
        qty=line["quantity"]; price=line["unit_price"]; amt=qty*price; total+=amt
        lr=to_float(line.get("tax_rate",0)); tax_label=f"{lr:g}%" if lr>0 else "0%"
        table_data.append([str(line["line_num"]),(line["description"] or "Service")[:40],f"{qty:g}",f"{price:,.2f}",tax_label,f"{amt:,.2f}"])
    col_widths=[25,220,35,85,40,85]; max_rows=int((y-120)/16); header_row=table_data[0]; data_rows=table_data[1:]; page_num=1
    while data_rows:
        chunk=data_rows[:max_rows]; data_rows=data_rows[max_rows:]; page_data=[header_row]+chunk
        t=Table(page_data, colWidths=col_widths)
        t.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),navy),("TEXTCOLOR",(0,0),(-1,0),colors.white),
            ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,0),8),
            ("FONTNAME",(0,1),(-1,-1),"Helvetica"),("FONTSIZE",(0,1),(-1,-1),7.5),("TEXTCOLOR",(0,1),(-1,-1),slate800),
            ("ALIGN",(0,0),(0,-1),"CENTER"),("ALIGN",(2,0),(-1,-1),"RIGHT"),
            *[("BACKGROUND",(0,i),(-1,i),slate50) for i in range(2,len(page_data),2)],
            ("LINEBELOW",(0,0),(-1,0),1,navy),("LINEBELOW",(0,-1),(-1,-1),0.5,slate200),
            ("TOPPADDING",(0,0),(-1,-1),3),("BOTTOMPADDING",(0,0),(-1,-1),3)]))
        tw,th=t.wrap(0,0); t.drawOn(c,30,y-th); y-=th+10
        if data_rows:
            c.setFont("Helvetica",7); c.setFillColor(slate500); c.drawRightString(w-30,25,f"Page {page_num}")
            c.showPage(); page_num+=1; y=h-50; c.setFillColor(slate800); c.setFont("Helvetica-Bold",10); c.drawString(30,y,"Line Items (continued)"); y-=5; max_rows=int((y-120)/16)
    y-=10
    stored_vat=to_float(inv.get("vat_amount",0))
    tax_amt = stored_vat if stored_vat > 0 else round(sum(line["quantity"]*line["unit_price"]*(to_float(line.get("tax_rate",0))/100) for line in lines if to_float(line.get("tax_rate",0))>0),2)
    grand=total+tax_amt; vat_label=f"VAT (7.5%):" if tax_amt>0 else "VAT:"; tx=w-230; bw=200
    c.setFillColor(slate50); c.rect(tx,y-65,bw,70,fill=True,stroke=False)
    c.setStrokeColor(slate200); c.rect(tx,y-65,bw,70,fill=False,stroke=True)
    c.setFont("Helvetica",9); c.setFillColor(slate500); c.drawString(tx+10,y-8,"Subtotal:"); c.drawString(tx+10,y-23,vat_label)
    c.setFillColor(slate800); c.drawRightString(tx+bw-10,y-8,f"N{total:,.2f}"); c.drawRightString(tx+bw-10,y-23,f"N{tax_amt:,.2f}")
    c.setStrokeColor(navy); c.line(tx+10,y-33,tx+bw-10,y-33)
    c.setFont("Helvetica-Bold",11); c.setFillColor(navy); c.drawString(tx+10,y-50,"TOTAL:"); c.drawRightString(tx+bw-10,y-50,f"N{grand:,.2f}")
    c.setFillColor(navy); c.rect(0,0,w,45,fill=True,stroke=False)
    c.setFillColor(colors.white); c.setFont("Helvetica-Bold",8); c.drawString(30,28,f"IRN: {inv['irn'] or 'Pending'}")
    c.setFont("Helvetica",7); c.drawString(30,15,"System-generated e-invoice. Validated by Nigeria E-Invoicing Portal (FIRS).")
    c.drawRightString(w-30,15,f"Page {page_num}"); c.save(); return pdf_path

# === ROUTES ===
@app.route("/")
def index():
    page = request.args.get("page", 1, type=int)
    q = request.args.get("q", "").strip()
    status_filter = request.args.get("status", "").strip()
    try:
        # Always fetch global stats from the full table (unaffected by search)
        all_stats = db_read("SELECT status, COUNT(*) as cnt FROM invoices GROUP BY status")
        stats = {"total": 0, "posted": 0, "pending": 0, "failed": 0, "credit_notes": 0, "invoices_count": 0}
        for s in all_stats: stats[s["status"]] = s["cnt"]; stats["total"] += s["cnt"]
        type_stats = db_read("SELECT invoice_type, COUNT(*) as cnt FROM invoices GROUP BY invoice_type")
        for t in type_stats:
            if t["invoice_type"] == "Credit Note": stats["credit_notes"] = t["cnt"]
            elif t["invoice_type"] == "Invoice": stats["invoices_count"] = t["cnt"]

        # Build WHERE clause for search + status filter
        where_parts = []
        params = []
        if q:
            where_parts.append(
                "(LOWER(customer_name) LIKE ? OR LOWER(customer_id) LIKE ? OR LOWER(invoice_num) LIKE ?)"
            )
            like = f"%{q.lower()}%"
            params += [like, like, like]
        if status_filter and status_filter in ("pending", "posted", "failed"):
            where_parts.append("status = ?")
            params.append(status_filter)

        where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        count_row = db_read_one(f"SELECT COUNT(*) as cnt FROM invoices {where_sql}", tuple(params))
        total = count_row["cnt"] if count_row else 0
        total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
        page = max(1, min(page, total_pages))
        offset = (page - 1) * PER_PAGE

        invoices = db_read(
            f"SELECT * FROM invoices {where_sql} ORDER BY invoice_date DESC, trx_number DESC LIMIT ? OFFSET ?",
            tuple(params) + (PER_PAGE, offset)
        )
    except:
        invoices=[]; stats={"total":0,"posted":0,"pending":0,"failed":0,"credit_notes":0,"invoices_count":0}; total=0; total_pages=1; page=1
    return render_template(
        "index.html",
        invoices=invoices, stats=stats,
        page=page, total_pages=total_pages, total=total,
        q=q, status_filter=status_filter
    )

@app.route("/api/sync", methods=["POST"])
def api_sync():
    data = request.get_json(silent=True) or {}
    return jsonify(sync_headers_from_sage(date_from=data.get("date_from"), date_to=data.get("date_to")))

@app.route("/api/post/<int:trx_number>", methods=["POST"])
def api_post(trx_number): return jsonify(post_to_firs(trx_number))

@app.route("/api/error-details/<int:trx_number>")
def api_error_details(trx_number):
    inv = db_read_one("SELECT trx_number, post_order, invoice_num, customer_name, status, error_message, api_response FROM invoices WHERE post_order=?", (trx_number,))
    if not inv: return jsonify({"ok": False, "error": "Invoice not found"})
    api_resp = inv.get("api_response") or ""
    parsed = None
    try:
        import json as _json; parsed = _json.loads(api_resp)
    except: pass
    return jsonify({"ok": True, "trx_number": trx_number, "invoice_num": inv["invoice_num"],
        "customer_name": inv["customer_name"], "status": inv["status"],
        "error_message": inv["error_message"] or "", "api_response": parsed or api_resp})

@app.route("/api/tax-categories", methods=["GET", "POST"])
def api_tax_categories():
    global TAX_CAT_STANDARD, TAX_CAT_EXEMPT
    if request.method == "POST":
        data = request.json or {}
        if "standard" in data: TAX_CAT_STANDARD = data["standard"]
        if "exempt" in data: TAX_CAT_EXEMPT = data["exempt"]
        return jsonify({"ok": True, "standard": TAX_CAT_STANDARD, "exempt": TAX_CAT_EXEMPT})
    return jsonify({"standard": TAX_CAT_STANDARD, "exempt": TAX_CAT_EXEMPT})

@app.route("/api/flick-tax-categories")
def api_flick_tax_categories():
    base = API_URL.replace("/v1", "")
    urls = [
        f"{base}/api/v1/invoice/resources/tax-categories",
        f"{API_URL}/invoice/resources/tax-categories",
    ]
    for url in urls:
        try:
            resp = requests.get(url, headers=API_HEADERS, timeout=15)
            if resp.status_code == 200:
                return jsonify({"ok": True, "url": url, "status_code": resp.status_code, "data": resp.json()})
        except: pass
    try:
        return jsonify({"ok": False, "urls_tried": urls, "last_status": resp.status_code, "last_body": resp.text[:2000]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.route("/api/preview-payload/<int:trx_number>")
def api_preview_payload(trx_number):
    inv = db_read_one("SELECT * FROM invoices WHERE post_order=?", (trx_number,))
    if not inv: return jsonify({"ok": False, "error": "Invoice not found"})
    payload, lines, vat_amount, error = build_payload(trx_number)
    if not payload: return jsonify({"ok": False, "error": error or "Failed to build payload"})
    subtotal = sum(l["amount"] for l in lines)
    return jsonify({"ok": True, "invoice_num": inv["invoice_num"], "customer_name": inv["customer_name"],
        "post_order": inv.get("post_order"), "subtotal": subtotal, "vat_amount": vat_amount,
        "grand_total": subtotal + vat_amount, "lines_count": len(lines),
        "api_url": f"{API_URL}/invoice/generate",
        "headers": {k: v for k, v in API_HEADERS.items() if k != "x-api-key"},
        "payload": payload})

@app.route("/api/post-bulk", methods=["POST"])
def api_post_bulk():
    pending = db_read("SELECT post_order, trx_number FROM invoices WHERE status='pending'"); results = []
    for row in pending: results.append({"trx": row["post_order"], **post_to_firs(row["post_order"])})
    posted = sum(1 for r in results if r.get("ok"))
    return jsonify({"ok": True, "posted": posted, "failed": len(results)-posted, "details": results})

@app.route("/api/stats")
def api_stats():
    try:
        all_stats = db_read("SELECT status, COUNT(*) as cnt FROM invoices GROUP BY status")
        stats = {"total": 0, "posted": 0, "pending": 0, "failed": 0}
        for s in all_stats: stats[s["status"]] = s["cnt"]; stats["total"] += s["cnt"]
        return jsonify({"ok": True, **stats})
    except Exception as e: return jsonify({"ok": False, "error": str(e)})

@app.route("/api/debug-lines/<int:trx_number>")
def api_debug_lines(trx_number):
    inv = db_read_one("SELECT * FROM invoices WHERE post_order=?", (trx_number,))
    lines, vat_amount, error = fetch_line_items(trx_number)  # trx_number here IS post_order from URL; subtotal = sum(l["amount"] for l in lines)
    return jsonify({"trx_number": trx_number, "post_order": inv.get("post_order") if inv else None,
        "invoice": {"invoice_num": inv["invoice_num"], "customer_name": inv["customer_name"], "amount": inv["amount"], "status": inv["status"]} if inv else None,
        "lines_found": len(lines), "lines": lines[:20], "vat_amount": vat_amount, "subtotal": subtotal, "grand_total": subtotal+vat_amount, "error": error})

@app.route("/api/debug-invoice-tables")
def api_debug_invoice_tables():
    """Show all Sage tables that might contain real invoice numbers for recurring invoices."""
    try:
        sage = pyodbc.connect(ODBC_CONN)
        cursor = sage.cursor()
        all_tables = [t.table_name for t in cursor.tables(tableType="TABLE")]
        results = {}
        # Check any table that sounds invoice/order related
        candidates = [t for t in all_tables if any(k in t.upper()
            for k in ["INVOICE","ORDER","SALES","AR","JRNL"])]
        for table in candidates:
            try:
                cols = [c.column_name for c in cursor.columns(table=table)]
                po_col  = find_col(cols, "PostOrder","PostOrderNumber")
                num_col = find_col(cols, "InvoiceNumber","InvoiceNum","ReferenceNumber",
                                   "Reference","DocNumber","SalesInvoiceNumber","OrderNumber")
                cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
                cnt = cursor.fetchone()[0]
                results[table] = {
                    "row_count": cnt,
                    "columns": cols[:20],
                    "has_post_order_col": po_col,
                    "has_inv_num_col": num_col,
                    "useful": bool(po_col and num_col)
                }
                if po_col and num_col and cnt > 0:
                    cursor.execute(f'SELECT TOP 3 {po_col}, {num_col} FROM "{table}" WHERE {po_col} IS NOT NULL AND {po_col} != 0')
                    results[table]["samples"] = [[r[0], to_str(r[1])] for r in cursor.fetchall()]
            except Exception as e:
                results[table] = {"error": str(e)}
        sage.close()
        useful = {k:v for k,v in results.items() if v.get("useful")}
        return jsonify({"ok": True, "useful_tables": useful, "all_candidates": list(results.keys()), "details": results})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.route("/api/debug-sync")
def api_debug_sync():
    """
    Diagnostic: shows what CustVendId values look like in JrnlHdr vs
    what CustomerRecordNumber/CustomerID look like in Customers.
    Use this to confirm which key format Sage is actually storing so you
    can verify the double-keyed cust_map fix is working correctly.
    """
    try:
        sage = pyodbc.connect(ODBC_CONN)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})
    try:
        cursor = sage.cursor()

        # Sample recent invoice headers
        cursor.execute(
            "SELECT TOP 15 JrnlKey_TrxNumber, CustVendId, Reference, TransactionDate "
            'FROM "JrnlHdr" WHERE Module=\'R\' AND JournalEx IN (8,9) '
            "ORDER BY TransactionDate DESC"
        )
        jrnl_samples = [
            {
                "trx_number": r[0],
                "CustVendId": r[1],
                "CustVendId_type": type(r[1]).__name__,
                "Reference": r[2],
                "Date": str(r[3])[:10] if r[3] else None,
            }
            for r in cursor.fetchall()
        ]

        # Sample customers table
        cursor.execute(
            "SELECT TOP 15 CustomerRecordNumber, CustomerID, Customer_Bill_Name "
            'FROM "Customers" ORDER BY CustomerRecordNumber DESC'
        )
        cust_samples = [
            {
                "CustomerRecordNumber": r[0],
                "CustomerRecordNumber_type": type(r[0]).__name__,
                "CustomerID": r[1],
                "CustomerID_type": type(r[1]).__name__,
                "Name": r[2],
            }
            for r in cursor.fetchall()
        ]

        # Check how many JrnlHdr rows have a CustVendId that matches
        # neither CustomerRecordNumber nor CustomerID in Customers
        cursor.execute(
            "SELECT COUNT(*) FROM \"JrnlHdr\" j WHERE Module='R' AND JournalEx IN (8,9) "
            "AND NOT EXISTS (SELECT 1 FROM \"Customers\" c WHERE c.CustomerRecordNumber = j.CustVendId) "
            "AND NOT EXISTS (SELECT 1 FROM \"Customers\" c WHERE c.CustomerID = j.CustVendId)"
        )
        unmatched_count = cursor.fetchone()[0]

        return jsonify({
            "ok": True,
            "unmatched_invoice_headers": unmatched_count,
            "note": (
                "If unmatched_invoice_headers > 0 and CustVendId_type is 'str', "
                "the double-key fix is needed. If it's already 0, both key types work."
            ),
            "jrnl_custvend_samples": jrnl_samples,
            "customers_samples": cust_samples,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})
    finally:
        sage.close()

@app.route("/download/<int:trx_number>")
def download_pdf(trx_number):
    inv = db_read_one("SELECT * FROM invoices WHERE post_order=?", (trx_number,))
    if not inv or inv["status"] != "posted": return "Not posted yet", 404
    safe_name = (inv["invoice_num"] or f"TRX-{trx_number}").replace("/","_").replace("\\","_").replace(" ","_")
    pdf_path = os.path.join(PDF_DIR, f"{safe_name}.pdf")
    if not os.path.exists(pdf_path): generate_pdf(trx_number)
    if os.path.exists(pdf_path): return send_file(pdf_path, as_attachment=True, download_name=f"{safe_name}.pdf")
    return "PDF generation failed", 500

if __name__ == "__main__":
    print("\n  Nigeria E-Invoicing Dashboard\n  =============================\n  http://localhost:5000\n")
    app.run(debug=False, host="0.0.0.0", port=5000)