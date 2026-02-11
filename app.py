"""
Nigeria E-Invoicing Dashboard
==============================
- Page ALWAYS loads (never crashes on DB lock)
- Sage sync via AJAX only (never blocks page load)
- Single threading lock prevents concurrent DB access
- Pagination (25 per page)
- Line items fetched on-demand when posting
"""

import os
import io
import sqlite3
import threading
import pyodbc
import requests
from datetime import datetime, date
from decimal import Decimal
from flask import Flask, render_template, jsonify, send_file, request

# ============================================================
# CONFIG
# ============================================================
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
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "einvoice.db")
PDF_DIR = os.path.join(BASE_DIR, "invoices")
os.makedirs(PDF_DIR, exist_ok=True)

PER_PAGE = 25

app = Flask(__name__)

# Single lock - ALL database access goes through this
_db_lock = threading.Lock()


# ============================================================
# SQLITE - thread-safe via lock, short-lived connections
# ============================================================

def _open_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def db_read(sql, params=()):
    with _db_lock:
        conn = _open_db()
        try:
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def db_read_one(sql, params=()):
    with _db_lock:
        conn = _open_db()
        try:
            row = conn.execute(sql, params).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()


def db_write(sql, params=()):
    with _db_lock:
        conn = _open_db()
        try:
            conn.execute(sql, params)
            conn.commit()
        finally:
            conn.close()


def db_write_many(operations):
    with _db_lock:
        conn = _open_db()
        try:
            for sql, params in operations:
                conn.execute(sql, params)
            conn.commit()
        finally:
            conn.close()


def init_db():
    with _db_lock:
        conn = _open_db()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS invoices (
                    trx_number       INTEGER PRIMARY KEY,
                    invoice_num      TEXT,
                    customer_name    TEXT,
                    customer_id      TEXT,
                    customer_tin     TEXT,
                    customer_email   TEXT,
                    customer_phone   TEXT,
                    customer_address TEXT,
                    customer_city    TEXT,
                    invoice_date     TEXT,
                    amount           REAL DEFAULT 0,
                    status           TEXT DEFAULT 'pending',
                    irn              TEXT,
                    qr_code          TEXT,
                    posted_at        TEXT,
                    error_message    TEXT,
                    last_synced      TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS invoice_lines (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    trx_number       INTEGER,
                    line_num         INTEGER,
                    item_code        TEXT,
                    description      TEXT,
                    quantity         REAL DEFAULT 1,
                    unit_price       REAL DEFAULT 0,
                    amount           REAL DEFAULT 0
                )
            """)
            conn.commit()
        finally:
            conn.close()


init_db()


# ============================================================
# HELPERS
# ============================================================

def to_float(val):
    if val is None:
        return 0.0
    if isinstance(val, Decimal):
        return float(val)
    try:
        return float(val)
    except:
        return 0.0


def to_str(val):
    if val is None:
        return ""
    return str(val).strip()


def find_col(columns, *candidates):
    for c in candidates:
        if c in columns:
            return c
    return None


def get_columns(cursor, table):
    return [list(row)[3] for row in cursor.columns(table=table)]


# ============================================================
# SAGE 50 - SYNC HEADERS (AJAX only, never on page load)
# ============================================================

def sync_headers_from_sage(date_from=None, date_to=None):
    """
    Sync invoice headers from Sage 50 ODBC.
    date_from / date_to: 'YYYY-MM-DD' strings to filter by TransactionDate.
    Defaults to current month if not specified.
    """
    # Default to current month (like Sage's "This Period")
    if not date_from:
        today = date.today()
        date_from = today.replace(day=1).strftime("%Y-%m-%d")
    if not date_to:
        today = date.today()
        # Last day of current month
        if today.month == 12:
            date_to = today.replace(day=31).strftime("%Y-%m-%d")
        else:
            date_to = (today.replace(month=today.month + 1, day=1)).strftime("%Y-%m-%d")

    # Step 1: Read from Sage (NO SQLite here)
    try:
        sage = pyodbc.connect(ODBC_CONN)
    except Exception as e:
        return {"ok": False, "error": f"ODBC connection failed: {e}"}

    try:
        cursor = sage.cursor()
        cursor.execute("""
            SELECT JrnlKey_TrxNumber, CustVendId, TransactionDate,
                   MainAmount, Reference, Description
            FROM "JrnlHdr"
            WHERE Module = 'R'
              AND TransactionDate >= ?
              AND TransactionDate <= ?
            ORDER BY TransactionDate DESC
        """, (date_from, date_to))
        headers = cursor.fetchall()

        cust_map = {}
        try:
            cursor.execute("""
                SELECT CustomerRecordNumber, CustomerID, Customer_Bill_Name,
                       Phone_Number, eMail_Address, SalesTaxResaleNum
                FROM "Customers"
            """)
            for cr in cursor.fetchall():
                cust_map[cr[0]] = {
                    "id": to_str(cr[1]), "name": to_str(cr[2]),
                    "phone": to_str(cr[3]), "email": to_str(cr[4]),
                    "tin": to_str(cr[5]),
                }
        except:
            pass

        addr_map = {}
        try:
            cursor.execute("""
                SELECT CustomerRecordNumber, AddressLine1, AddressLine2, City
                FROM "Address"
            """)
            for ar in cursor.fetchall():
                if ar[0] not in addr_map:
                    parts = [to_str(ar[1]), to_str(ar[2])]
                    addr_map[ar[0]] = {
                        "address": ", ".join(p for p in parts if p),
                        "city": to_str(ar[3]),
                    }
        except:
            pass
    finally:
        sage.close()

    # Step 2: Read existing statuses from SQLite (quick read)
    existing_map = {}
    for row in db_read("SELECT trx_number, status FROM invoices"):
        existing_map[row["trx_number"]] = row["status"]

    # Step 3: Build all operations, then write in ONE batch
    now = datetime.now().isoformat()
    operations = []
    new_count = 0

    for hdr in headers:
        trx_num = hdr[0]
        cust_recnum = hdr[1]
        tx_date = hdr[2]
        main_amt = to_float(hdr[3])
        ref = to_str(hdr[4])
        desc = to_str(hdr[5])
        inv_num = ref if ref else f"TRX-{trx_num}"

        if isinstance(tx_date, (datetime, date)):
            tx_date_str = tx_date.strftime("%Y-%m-%d")
        else:
            tx_date_str = str(tx_date)[:10]

        cust = cust_map.get(cust_recnum, {})
        addr = addr_map.get(cust_recnum, {})
        cust_name = cust.get("name", "") or desc

        if trx_num in existing_map:
            operations.append(("""
                UPDATE invoices SET
                    invoice_num=?, customer_name=?, customer_id=?,
                    customer_tin=?, customer_email=?, customer_phone=?,
                    customer_address=?, customer_city=?,
                    invoice_date=?, amount=?, last_synced=?
                WHERE trx_number=?
            """, (inv_num, cust_name, cust.get("id", ""), cust.get("tin", ""),
                  cust.get("email", ""), cust.get("phone", ""),
                  addr.get("address", ""), addr.get("city", ""),
                  tx_date_str, main_amt, now, trx_num)))
        else:
            new_count += 1
            operations.append(("""
                INSERT INTO invoices
                    (trx_number, invoice_num, customer_name, customer_id,
                     customer_tin, customer_email, customer_phone,
                     customer_address, customer_city,
                     invoice_date, amount, status, last_synced)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,'pending',?)
            """, (trx_num, inv_num, cust_name, cust.get("id", ""),
                  cust.get("tin", ""), cust.get("email", ""),
                  cust.get("phone", ""), addr.get("address", ""),
                  addr.get("city", ""), tx_date_str, main_amt, now)))

    if operations:
        db_write_many(operations)

    return {"ok": True, "synced": len(headers), "new": new_count,
            "date_from": date_from, "date_to": date_to}


# ============================================================
# SAGE 50 - LINE ITEMS (on-demand when posting)
# ============================================================

def fetch_line_items(trx_number):
    try:
        sage = pyodbc.connect(ODBC_CONN)
    except:
        return []

    try:
        cursor = sage.cursor()
        jrnlrow_cols = get_columns(cursor, "JrnlRow")
        lineitem_cols = get_columns(cursor, "LineItem")

        jrnlrow_fk = find_col(jrnlrow_cols,
            "JrnlKey_TrxNumber", "Journal", "JournalKey",
            "TrxNumber", "TransactionNumber")
        if not jrnlrow_fk:
            return []

        jr_amount = find_col(jrnlrow_cols, "Amount")
        jr_qty = find_col(jrnlrow_cols, "Quantity", "StockingQuantity")
        jr_price = find_col(jrnlrow_cols, "UnitCost", "UnitPrice", "StockingUnitCost")
        jr_desc = find_col(jrnlrow_cols, "RowDescription", "Description",
                           "ItemDescription", "LineDescription", "Memo")
        jr_itemrec = find_col(jrnlrow_cols, "ItemRecordNumber")

        li_recnum = find_col(lineitem_cols, "ItemRecordNumber", "RecordNumber")
        li_itemid = find_col(lineitem_cols, "ItemID")
        li_desc = find_col(lineitem_cols, "ItemDescription", "Description", "SalesDescription")

        item_lookup = {}
        if li_recnum and li_itemid:
            select_parts = [li_recnum, li_itemid]
            if li_desc:
                select_parts.append(li_desc)
            try:
                cursor.execute(f'SELECT {", ".join(select_parts)} FROM "LineItem" WHERE {li_itemid} <> \'\'')
                for row in cursor.fetchall():
                    item_lookup[row[0]] = {
                        "item_id": to_str(row[1]),
                        "description": to_str(row[2]) if li_desc else "",
                    }
            except:
                pass

        jr_select = []
        if jr_amount: jr_select.append(jr_amount)
        if jr_qty: jr_select.append(jr_qty)
        if jr_price: jr_select.append(jr_price)
        if jr_itemrec: jr_select.append(jr_itemrec)
        if jr_desc: jr_select.append(jr_desc)

        lines = []
        if jr_select:
            try:
                cursor.execute(
                    f'SELECT {", ".join(jr_select)} FROM "JrnlRow" WHERE "{jrnlrow_fk}" = {trx_number}'
                )
                rc = [c[0] for c in cursor.description]
                for lr in cursor.fetchall():
                    ld = dict(zip(rc, lr))
                    qty = to_float(ld.get(jr_qty, 0)) if jr_qty else 0
                    amount = to_float(ld.get(jr_amount, 0)) if jr_amount else 0
                    unit_cost = to_float(ld.get(jr_price, 0)) if jr_price else 0
                    item_recnum = ld.get(jr_itemrec, 0) if jr_itemrec else 0
                    row_desc = to_str(ld.get(jr_desc, "")) if jr_desc else ""

                    item_info = item_lookup.get(item_recnum, {})
                    item_id = item_info.get("item_id", "")
                    item_desc = item_info.get("description", "")
                    line_desc = row_desc or item_desc or item_id or ""
                    unit_price = abs(unit_cost) if unit_cost != 0 else abs(amount)

                    if (qty != 0 or item_recnum > 0) and unit_price > 0:
                        lines.append({
                            "item_code": item_id or str(item_recnum),
                            "description": line_desc or "Service",
                            "quantity": abs(qty) if qty != 0 else 1,
                            "unit_price": unit_price,
                            "amount": abs(qty if qty != 0 else 1) * unit_price,
                        })
            except:
                pass

        return lines
    finally:
        sage.close()


# ============================================================
# API SUBMISSION
# ============================================================

def post_to_firs(trx_number):
    inv = db_read_one("SELECT * FROM invoices WHERE trx_number=?", (trx_number,))
    if not inv:
        return {"ok": False, "error": "Invoice not found"}
    if inv["status"] == "posted":
        return {"ok": False, "error": "Already posted", "irn": inv["irn"]}

    lines = fetch_line_items(trx_number)
    if not lines:
        return {"ok": False, "error": "No line items found in Sage 50."}

    ops = [("DELETE FROM invoice_lines WHERE trx_number=?", (trx_number,))]
    for i, line in enumerate(lines):
        ops.append(("""
            INSERT INTO invoice_lines (trx_number, line_num, item_code,
                 description, quantity, unit_price, amount)
            VALUES (?,?,?,?,?,?,?)
        """, (trx_number, i+1, line["item_code"], line["description"],
              line["quantity"], line["unit_price"], line["amount"])))
    db_write_many(ops)

    cust_tin = inv["customer_tin"] or "23773131-0001"
    cust_email = inv["customer_email"] or "noemail@placeholder.com"
    cust_phone = inv["customer_phone"] or "+234"

    api_lines = []
    for line in lines:
        if line["unit_price"] <= 0:
            continue
        api_lines.append({
            "hsn_code": "2710.19",
            "price_amount": line["unit_price"],
            "discount_amount": 1,
            "uom": "ST",
            "invoiced_quantity": line["quantity"],
            "product_category": "Security Services",
            "tax_rate": 7.5,
            "tax_category_id": "STANDARD_VAT",
            "item_name": line["description"] or "Service",
            "sellers_item_identification": line["item_code"] or f"ITEM-{lines.index(line)+1}",
        })

    if not api_lines:
        return {"ok": False, "error": "No valid line items (all zero prices)"}

    payload = {
        "document_identifier": inv["invoice_num"],
        "issue_date": inv["invoice_date"],
        "invoice_type_code": "394",
        "document_currency_code": "NGN",
        "tax_currency_code": "NGN",
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
        "invoice_line": api_lines,
    }

    try:
        resp = requests.post(
            f"{API_URL}/invoice/generate",
            headers=API_HEADERS, json=payload, timeout=30,
        )
        resp_json = {}
        try:
            resp_json = resp.json()
        except:
            pass

        if resp.status_code in (200, 201):
            data = resp_json.get("data", resp_json)
            irn = data.get("irn", "N/A")
            qr_code = data.get("qr_code", "")
            db_write("""
                UPDATE invoices SET status='posted', irn=?, qr_code=?,
                    posted_at=?, error_message=NULL
                WHERE trx_number=?
            """, (irn, qr_code, datetime.now().isoformat(), trx_number))
            generate_pdf(trx_number)
            return {"ok": True, "irn": irn, "status": "posted"}
        else:
            error_msg = resp_json.get("message", resp.text[:300])
            db_write("""
                UPDATE invoices SET status='failed', error_message=?
                WHERE trx_number=?
            """, (error_msg[:500], trx_number))
            return {"ok": False, "error": error_msg}

    except requests.exceptions.ConnectionError as e:
        db_write("UPDATE invoices SET status='failed', error_message=? WHERE trx_number=?",
                 (f"Connection error: {str(e)[:200]}", trx_number))
        return {"ok": False, "error": f"Connection failed: {e}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ============================================================
# PDF GENERATION
# ============================================================

def generate_pdf(trx_number):
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.pdfgen import canvas
    from reportlab.platypus import Table, TableStyle
    from reportlab.lib.utils import ImageReader

    inv = db_read_one("SELECT * FROM invoices WHERE trx_number=?", (trx_number,))
    lines = db_read("SELECT * FROM invoice_lines WHERE trx_number=? ORDER BY line_num", (trx_number,))

    if not inv:
        return None

    qr_img_reader = None
    if inv["qr_code"]:
        try:
            import qrcode
            qr = qrcode.QRCode(version=1, box_size=4, border=2)
            qr.add_data(inv["qr_code"])
            qr.make(fit=True)
            img = qr.make_image(fill_color="black", back_color="white")
            buf = io.BytesIO()
            img.save(buf, format="PNG")
            buf.seek(0)
            qr_img_reader = ImageReader(buf)
        except:
            pass

    safe_name = (inv["invoice_num"] or f"TRX-{trx_number}").replace("/", "_").replace("\\", "_").replace(" ", "_")
    pdf_path = os.path.join(PDF_DIR, f"{safe_name}.pdf")

    w, h = A4
    c = canvas.Canvas(pdf_path, pagesize=A4)

    navy = colors.HexColor("#0f172a")
    blue = colors.HexColor("#2563eb")
    slate50 = colors.HexColor("#f8fafc")
    slate200 = colors.HexColor("#e2e8f0")
    slate500 = colors.HexColor("#64748b")
    slate800 = colors.HexColor("#1e293b")
    green = colors.HexColor("#16a34a")

    y = h - 30

    c.setFillColor(navy)
    c.rect(0, y - 60, w, 70, fill=True, stroke=False)
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(30, y - 25, SUPPLIER["name"])
    c.setFont("Helvetica", 9)
    c.drawString(30, y - 42, SUPPLIER["address"])
    c.setFillColor(green)
    c.roundRect(w - 145, y - 47, 115, 30, 4, fill=True, stroke=False)
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 11)
    c.drawCentredString(w - 87, y - 37, "E-INVOICE")
    y -= 85

    c.setFillColor(slate800)
    c.setFont("Helvetica-Bold", 22)
    c.drawString(30, y, "INVOICE")
    y -= 25

    for label, val in [("Invoice No:", inv["invoice_num"]), ("Date:", inv["invoice_date"]),
                       ("IRN:", inv["irn"] or "Pending"), ("Currency:", "NGN")]:
        c.setFont("Helvetica-Bold", 9); c.setFillColor(slate500); c.drawString(30, y, label)
        c.setFont("Helvetica", 9); c.setFillColor(slate800); c.drawString(115, y, str(val))
        y -= 15

    if qr_img_reader:
        c.drawImage(qr_img_reader, w - 140, y + 5, 105, 105)
    y -= 15

    c.setFillColor(slate50)
    c.rect(25, y - 55, w - 50, 60, fill=True, stroke=False)
    c.setStrokeColor(slate200)
    c.rect(25, y - 55, w - 50, 60, fill=False, stroke=True)
    c.setFillColor(blue); c.setFont("Helvetica-Bold", 9); c.drawString(35, y - 5, "BILL TO")
    c.setFillColor(slate800); c.setFont("Helvetica-Bold", 11); c.drawString(35, y - 20, inv["customer_name"] or "")
    c.setFont("Helvetica", 8); c.setFillColor(slate500)
    addr = f"{inv['customer_address'] or ''}, {inv['customer_city'] or ''}".strip(", ")
    c.drawString(35, y - 34, addr[:80])
    if inv["customer_tin"]: c.drawString(35, y - 46, f"TIN: {inv['customer_tin']}")
    c.drawRightString(w - 35, y - 20, inv["customer_email"] or "")
    c.drawRightString(w - 35, y - 34, inv["customer_phone"] or "")
    y -= 75

    c.setFillColor(slate800); c.setFont("Helvetica-Bold", 10); c.drawString(30, y, "Line Items"); y -= 5

    table_data = [["#", "Description", "Qty", "Unit Price (N)", "Amount (N)"]]
    total = 0.0
    for line in lines:
        qty = line["quantity"]; price = line["unit_price"]; amt = qty * price; total += amt
        table_data.append([str(line["line_num"]), (line["description"] or "Service")[:45],
                           f"{qty:g}", f"{price:,.2f}", f"{amt:,.2f}"])

    col_widths = [30, 250, 40, 90, 90]
    max_rows = int((y - 120) / 16)
    header_row = table_data[0]; data_rows = table_data[1:]; page_num = 1

    while data_rows:
        chunk = data_rows[:max_rows]; data_rows = data_rows[max_rows:]
        page_data = [header_row] + chunk
        t = Table(page_data, colWidths=col_widths)
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), navy), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("FONTSIZE", (0, 0), (-1, 0), 8),
            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"), ("FONTSIZE", (0, 1), (-1, -1), 7.5),
            ("TEXTCOLOR", (0, 1), (-1, -1), slate800),
            ("ALIGN", (0, 0), (0, -1), "CENTER"), ("ALIGN", (2, 0), (-1, -1), "RIGHT"),
            *[("BACKGROUND", (0, i), (-1, i), slate50) for i in range(2, len(page_data), 2)],
            ("LINEBELOW", (0, 0), (-1, 0), 1, navy), ("LINEBELOW", (0, -1), (-1, -1), 0.5, slate200),
            ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]))
        tw, th = t.wrap(0, 0); t.drawOn(c, 30, y - th); y -= th + 10
        if data_rows:
            c.setFont("Helvetica", 7); c.setFillColor(slate500); c.drawRightString(w - 30, 25, f"Page {page_num}")
            c.showPage(); page_num += 1; y = h - 50
            c.setFillColor(slate800); c.setFont("Helvetica-Bold", 10); c.drawString(30, y, "Line Items (continued)"); y -= 5
            max_rows = int((y - 120) / 16)

    y -= 10
    tax_rate = 7.5; tax_amt = total * (tax_rate / 100); grand = total + tax_amt
    tx = w - 230; bw = 200
    c.setFillColor(slate50); c.rect(tx, y - 65, bw, 70, fill=True, stroke=False)
    c.setStrokeColor(slate200); c.rect(tx, y - 65, bw, 70, fill=False, stroke=True)
    c.setFont("Helvetica", 9); c.setFillColor(slate500)
    c.drawString(tx + 10, y - 8, "Subtotal:"); c.drawString(tx + 10, y - 23, f"VAT ({tax_rate}%):")
    c.setFillColor(slate800)
    c.drawRightString(tx + bw - 10, y - 8, f"N{total:,.2f}"); c.drawRightString(tx + bw - 10, y - 23, f"N{tax_amt:,.2f}")
    c.setStrokeColor(navy); c.line(tx + 10, y - 33, tx + bw - 10, y - 33)
    c.setFont("Helvetica-Bold", 11); c.setFillColor(navy)
    c.drawString(tx + 10, y - 50, "TOTAL:"); c.drawRightString(tx + bw - 10, y - 50, f"N{grand:,.2f}")

    c.setFillColor(navy); c.rect(0, 0, w, 45, fill=True, stroke=False)
    c.setFillColor(colors.white); c.setFont("Helvetica-Bold", 8)
    c.drawString(30, 28, f"IRN: {inv['irn'] or 'Pending'}")
    c.setFont("Helvetica", 7)
    c.drawString(30, 15, "System-generated e-invoice. Validated by Nigeria E-Invoicing Portal (FIRS).")
    c.drawRightString(w - 30, 15, f"Page {page_num}")
    c.save()
    return pdf_path


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def index():
    """Page ALWAYS loads. If DB is locked, show empty + let sync fix it."""
    page = request.args.get("page", 1, type=int)

    try:
        total_row = db_read_one("SELECT COUNT(*) as cnt FROM invoices")
        total = total_row["cnt"] if total_row else 0
        total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
        page = max(1, min(page, total_pages))
        offset = (page - 1) * PER_PAGE

        invoices = db_read(
            "SELECT * FROM invoices ORDER BY invoice_date DESC, trx_number DESC LIMIT ? OFFSET ?",
            (PER_PAGE, offset)
        )

        all_stats = db_read("SELECT status, COUNT(*) as cnt FROM invoices GROUP BY status")
        stats = {"total": 0, "posted": 0, "pending": 0, "failed": 0}
        for s in all_stats:
            stats[s["status"]] = s["cnt"]
            stats["total"] += s["cnt"]

    except Exception as e:
        print(f"[WARN] Page load DB error (will recover): {e}")
        invoices = []
        stats = {"total": 0, "posted": 0, "pending": 0, "failed": 0}
        total = 0
        total_pages = 1
        page = 1

    return render_template("index.html",
        invoices=invoices, stats=stats,
        page=page, total_pages=total_pages, total=total,
    )


@app.route("/api/sync", methods=["POST"])
def api_sync():
    data = request.get_json(silent=True) or {}
    date_from = data.get("date_from")
    date_to = data.get("date_to")
    return jsonify(sync_headers_from_sage(date_from=date_from, date_to=date_to))


@app.route("/api/post/<int:trx_number>", methods=["POST"])
def api_post(trx_number):
    return jsonify(post_to_firs(trx_number))


@app.route("/api/post-bulk", methods=["POST"])
def api_post_bulk():
    pending = db_read("SELECT trx_number FROM invoices WHERE status='pending'")
    results = []
    for row in pending:
        r = post_to_firs(row["trx_number"])
        results.append({"trx": row["trx_number"], **r})
    posted = sum(1 for r in results if r.get("ok"))
    failed = len(results) - posted
    return jsonify({"ok": True, "posted": posted, "failed": failed, "details": results})


@app.route("/download/<int:trx_number>")
def download_pdf(trx_number):
    inv = db_read_one("SELECT * FROM invoices WHERE trx_number=?", (trx_number,))
    if not inv or inv["status"] != "posted":
        return "Invoice not posted yet", 404
    safe_name = (inv["invoice_num"] or f"TRX-{trx_number}").replace("/", "_").replace("\\", "_").replace(" ", "_")
    pdf_path = os.path.join(PDF_DIR, f"{safe_name}.pdf")
    if not os.path.exists(pdf_path):
        generate_pdf(trx_number)
    if os.path.exists(pdf_path):
        return send_file(pdf_path, as_attachment=True, download_name=f"{safe_name}.pdf")
    return "PDF generation failed", 500


if __name__ == "__main__":
    print("\n  Nigeria E-Invoicing Dashboard")
    print("  =============================")
    print("  http://localhost:5000\n")
    app.run(debug=False, host="0.0.0.0", port=5000)