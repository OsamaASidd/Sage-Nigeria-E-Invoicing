"""
Test: Fetch latest Sage 50 invoice and submit to Nigeria E-Invoicing API
=========================================================================
FULLY DYNAMIC - discovers all column names before querying.
All output written to test_output.txt
"""

import pyodbc
import requests
import json
import sys
import os
from datetime import datetime, date
from decimal import Decimal

# --- CONFIG ---
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

OUTPUT_FILE = "test_output.txt"
_outfile = None


def log(msg=""):
    """Write to both console and output file."""
    print(msg)
    if _outfile:
        _outfile.write(msg + "\n")
        _outfile.flush()


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


def get_columns(cursor, table):
    return [c.column_name for c in cursor.columns(table=table)]


def find_col(columns, *candidates):
    for c in candidates:
        if c in columns:
            return c
    return None


def main():
    global _outfile

    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
        except:
            pass

    _outfile = open(OUTPUT_FILE, "w", encoding="utf-8")

    try:
        _run()
    finally:
        _outfile.close()
        print(f"\n>>> Output saved to: {os.path.abspath(OUTPUT_FILE)}")


def _run():
    log("=" * 60)
    log("  TEST: Fetch Latest Invoice -> Submit to API")
    log("=" * 60)

    # ---- STEP 1: Connect ----
    log("\n[1] Connecting to Sage 50...")
    try:
        conn = pyodbc.connect(ODBC_CONN)
        log("    [OK] Connected")
    except Exception as e:
        log(f"    [FAIL] {e}")
        return

    cursor = conn.cursor()

    # ============================================================
    # STEP 1b: DISCOVER ALL TABLE SCHEMAS
    # ============================================================
    log("\n[1b] Discovering table schemas...")

    jrnlrow_cols = get_columns(cursor, "JrnlRow")
    log(f"    JrnlRow ({len(jrnlrow_cols)} cols): {jrnlrow_cols}")

    lineitem_cols = get_columns(cursor, "LineItem")
    log(f"    LineItem ({len(lineitem_cols)} cols): {lineitem_cols}")

    # ---- Find the JrnlRow foreign key to JrnlHdr ----
    jrnlrow_fk = find_col(jrnlrow_cols,
        "JrnlKey_TrxNumber",
        "Journal",
        "JournalKey",
        "TrxNumber",
        "TransactionNumber",
    )

    if not jrnlrow_fk:
        log("\n    [DIAG] No obvious FK column found. Probing JrnlRow sample...")
        cursor.execute("""
            SELECT JrnlKey_TrxNumber FROM "JrnlHdr"
            WHERE Module = 'R' ORDER BY TransactionDate DESC
        """)
        sample_trx = cursor.fetchone()
        if sample_trx:
            trx_val = sample_trx[0]
            log(f"    Looking for TRX value {trx_val} in JrnlRow columns...")
            cursor.execute('SELECT * FROM "JrnlRow"')
            sample_cols = [c[0] for c in cursor.description]
            sample_row = cursor.fetchone()
            if sample_row:
                d = dict(zip(sample_cols, sample_row))
                log(f"    Sample JrnlRow row (first row):")
                for col_name, col_val in d.items():
                    log(f"      {col_name} = {col_val!r}")

                for col_name, col_val in d.items():
                    if col_val == trx_val:
                        jrnlrow_fk = col_name
                        log(f"\n    [FOUND] FK column: {col_name} (matched value {trx_val})")
                        break

            if not jrnlrow_fk:
                log("\n    Trying each column as FK...")
                for candidate in jrnlrow_cols:
                    try:
                        cursor.execute(
                            f'SELECT COUNT(*) FROM "JrnlRow" WHERE "{candidate}" = {trx_val}'
                        )
                        count = cursor.fetchone()[0]
                        if count > 0:
                            log(f"      {candidate} = {trx_val} -> {count} rows [MATCH]")
                            jrnlrow_fk = candidate
                            break
                    except:
                        pass

    if jrnlrow_fk:
        log(f"\n    JrnlRow FK column: {jrnlrow_fk}")
    else:
        log("\n    [FAIL] Cannot determine JrnlRow FK column!")
        log("    Please check the JrnlRow column list above and report back.")
        conn.close()
        return

    # ---- Find JrnlRow data columns ----
    jr_amount = find_col(jrnlrow_cols, "Amount")
    jr_qty = find_col(jrnlrow_cols, "Quantity", "StockingQuantity")
    jr_price = find_col(jrnlrow_cols, "UnitCost", "UnitPrice", "StockingUnitCost")
    jr_desc = find_col(jrnlrow_cols, "RowDescription", "Description",
                       "ItemDescription", "LineDescription", "Memo")
    jr_itemrec = find_col(jrnlrow_cols, "ItemRecordNumber")
    jr_glacct = find_col(jrnlrow_cols, "GLAcntNumber")
    jr_rownum = find_col(jrnlrow_cols, "RowNumber")

    log(f"    JrnlRow mapping: FK={jrnlrow_fk}, Amt={jr_amount}, Qty={jr_qty}, "
        f"Price={jr_price}, Desc={jr_desc}, ItemRec={jr_itemrec}")

    # ---- Find LineItem columns ----
    li_recnum = find_col(lineitem_cols, "ItemRecordNumber", "RecordNumber",
                         "LineItemRecordNumber")
    li_itemid = find_col(lineitem_cols, "ItemID")
    li_desc = find_col(lineitem_cols, "ItemDescription", "Description",
                       "SalesDescription")
    li_price = find_col(lineitem_cols, "SalesPrice1", "SalesPrice", "Price",
                        "UnitPrice", "Cost")

    log(f"    LineItem mapping: RecNum={li_recnum}, ID={li_itemid}, "
        f"Desc={li_desc}, Price={li_price}")

    # ============================================================
    # STEP 1c: Build LineItem lookup
    # ============================================================
    log("\n    Building LineItem lookup...")
    item_lookup = {}
    if li_recnum and li_itemid:
        select_parts = [li_recnum, li_itemid]
        if li_desc:
            select_parts.append(li_desc)
        if li_price:
            select_parts.append(li_price)
        select_str = ", ".join(select_parts)
        try:
            cursor.execute(f'SELECT {select_str} FROM "LineItem" WHERE {li_itemid} <> \'\'')
            for row in cursor.fetchall():
                rec = row[0]
                item_lookup[rec] = {
                    "item_id": to_str(row[1]),
                    "description": to_str(row[2]) if li_desc else "",
                    "price": to_float(row[3]) if li_price and len(row) > 3 else (
                        to_float(row[2]) if li_price and not li_desc and len(row) > 2 else 0
                    ),
                }
            log(f"    Loaded {len(item_lookup)} items (keyed by {li_recnum})")
        except Exception as e:
            log(f"    [WARN] LineItem query failed: {e}")
            try:
                cursor.execute(f'SELECT {li_recnum}, {li_itemid} FROM "LineItem" WHERE {li_itemid} <> \'\'')
                for row in cursor.fetchall():
                    item_lookup[row[0]] = {"item_id": to_str(row[1]), "description": "", "price": 0}
                log(f"    Loaded {len(item_lookup)} items (minimal - ID only)")
            except Exception as e2:
                log(f"    [WARN] Minimal query also failed: {e2}")
    else:
        log(f"    [WARN] Cannot build item lookup (missing columns)")

    # ============================================================
    # STEP 2: Get latest sales invoice
    # ============================================================
    log("\n[2] Fetching latest sales invoice (Module='R')...")
    cursor.execute("""
        SELECT JrnlKey_TrxNumber, CustVendId, TransactionDate,
               MainAmount, Reference, Description
        FROM "JrnlHdr"
        WHERE Module = 'R'
        ORDER BY TransactionDate DESC
    """)
    row = cursor.fetchone()

    if not row:
        log("    [FAIL] No Module='R' invoices found!")
        conn.close()
        return

    hdr_cols = [c[0] for c in cursor.description]
    hdr = dict(zip(hdr_cols, row))

    trx_num = hdr["JrnlKey_TrxNumber"]
    cust_recnum = hdr["CustVendId"]
    tx_date = hdr["TransactionDate"]
    main_amt = to_float(hdr["MainAmount"])
    ref = to_str(hdr["Reference"])
    desc = to_str(hdr["Description"])
    inv_num = ref if ref else f"TRX-{trx_num}"

    if isinstance(tx_date, (datetime, date)):
        tx_date_str = tx_date.strftime("%Y-%m-%d")
    else:
        tx_date_str = str(tx_date)[:10]

    log(f"    Invoice:    {inv_num}")
    log(f"    TRX#:       {trx_num}")
    log(f"    Date:       {tx_date_str}")
    log(f"    Amount:     N{main_amt:,.2f}")
    log(f"    Desc:       {desc}")
    log(f"    CustVendId: {cust_recnum}")

    # ============================================================
    # STEP 3: Get customer
    # ============================================================
    log("\n[3] Looking up customer...")
    cursor.execute(f"""
        SELECT CustomerID, Customer_Bill_Name, Contact, Phone_Number,
               eMail_Address, SalesTaxResaleNum
        FROM "Customers"
        WHERE CustomerRecordNumber = {cust_recnum}
    """)
    cust_row = cursor.fetchone()
    if cust_row:
        cc = [c[0] for c in cursor.description]
        cust = dict(zip(cc, cust_row))
        cust_id = to_str(cust.get("CustomerID", ""))
        cust_name = to_str(cust.get("Customer_Bill_Name", "")) or cust_id
        cust_phone = to_str(cust.get("Phone_Number", ""))
        cust_email = to_str(cust.get("eMail_Address", ""))
        cust_tin = to_str(cust.get("SalesTaxResaleNum", ""))
        log(f"    ID:    {cust_id}")
        log(f"    Name:  {cust_name}")
        log(f"    TIN:   {cust_tin or '(none)'}")
    else:
        cust_name = desc
        cust_phone = ""
        cust_email = ""
        cust_tin = ""
        log(f"    [WARN] Not found, using: {desc}")

    # Get address
    cust_address = ""
    cust_city = ""
    cust_zip = ""
    try:
        cursor.execute(f"""
            SELECT AddressLine1, AddressLine2, City, State, Zip, Country
            FROM "Address"
            WHERE CustomerRecordNumber = {cust_recnum}
        """)
        addr_row = cursor.fetchone()
        if addr_row:
            ac = [c[0] for c in cursor.description]
            ad = dict(zip(ac, addr_row))
            parts = [to_str(ad.get("AddressLine1", "")), to_str(ad.get("AddressLine2", ""))]
            cust_address = ", ".join(p for p in parts if p)
            cust_city = to_str(ad.get("City", ""))
            cust_zip = to_str(ad.get("Zip", ""))
            log(f"    Addr:  {cust_address}")
            log(f"    City:  {cust_city}")
    except Exception as e:
        log(f"    [WARN] Address: {e}")

    # ============================================================
    # STEP 4: Get line items (using discovered columns)
    # ============================================================
    log("\n[4] Fetching line items from JrnlRow...")

    jr_select_cols = []
    if jr_glacct: jr_select_cols.append(jr_glacct)
    if jr_amount: jr_select_cols.append(jr_amount)
    if jr_qty: jr_select_cols.append(jr_qty)
    if jr_price: jr_select_cols.append(jr_price)
    if jr_rownum: jr_select_cols.append(jr_rownum)
    if jr_itemrec: jr_select_cols.append(jr_itemrec)
    if jr_desc: jr_select_cols.append(jr_desc)

    jr_select_str = ", ".join(jr_select_cols)
    log(f"    SELECT {jr_select_str}")
    log(f"    WHERE {jrnlrow_fk} = {trx_num}")

    def fetch_lines(cur, trx, items_lookup):
        cur.execute(f'SELECT {jr_select_str} FROM "JrnlRow" WHERE "{jrnlrow_fk}" = {trx}')
        rc = [c[0] for c in cur.description]
        result = []
        all_rows = cur.fetchall()
        log(f"    Total JrnlRow entries for TRX {trx}: {len(all_rows)}")

        for lr in all_rows:
            ld = dict(zip(rc, lr))
            qty = to_float(ld.get(jr_qty, 0)) if jr_qty else 0
            amount = to_float(ld.get(jr_amount, 0)) if jr_amount else 0
            unit_cost = to_float(ld.get(jr_price, 0)) if jr_price else 0
            item_recnum = ld.get(jr_itemrec, 0) if jr_itemrec else 0
            row_desc = to_str(ld.get(jr_desc, "")) if jr_desc else ""

            item_info = items_lookup.get(item_recnum, {})
            item_id = item_info.get("item_id", "")
            item_desc = item_info.get("description", "")
            sales_price = item_info.get("price", 0)

            line_desc = row_desc or item_desc or item_id or ""
            unit_price = abs(unit_cost) if unit_cost != 0 else (
                sales_price if sales_price > 0 else abs(amount)
            )

            log(f"      ItemRec={item_recnum} ItemID={item_id!r} "
                f"Desc={line_desc!r} Qty={qty} Price={unit_cost} Amt={amount}")

            if qty != 0 or item_recnum > 0:
                result.append({
                    "item_code": item_id or str(item_recnum),
                    "description": line_desc or "Service",
                    "quantity": abs(qty) if qty != 0 else 1,
                    "unit_price": unit_price,
                    "discount": 0,
                    "tax_amount": 0,
                })
        return result

    lines = fetch_lines(cursor, trx_num, item_lookup)

    # If no lines, search next 50 invoices
    if not lines:
        log("\n    [WARN] No usable lines. Searching recent invoices...")
        cursor.execute("""
            SELECT JrnlKey_TrxNumber, CustVendId, TransactionDate,
                   MainAmount, Reference, Description
            FROM "JrnlHdr"
            WHERE Module = 'R'
            ORDER BY TransactionDate DESC
        """)
        search_cols = [c[0] for c in cursor.description]
        cursor.fetchone()
        for attempt_row in cursor.fetchmany(50):
            att = dict(zip(search_cols, attempt_row))
            t = att["JrnlKey_TrxNumber"]
            cursor2 = conn.cursor()
            test_lines = fetch_lines(cursor2, t, item_lookup)
            if test_lines:
                trx_num = t
                inv_num = to_str(att["Reference"]) or f"TRX-{t}"
                cust_recnum = att["CustVendId"]
                tx_date = att["TransactionDate"]
                main_amt = to_float(att["MainAmount"])
                desc = to_str(att["Description"])
                if isinstance(tx_date, (datetime, date)):
                    tx_date_str = tx_date.strftime("%Y-%m-%d")
                else:
                    tx_date_str = str(tx_date)[:10]
                lines = test_lines

                cursor.execute(f"""
                    SELECT CustomerID, Customer_Bill_Name, Phone_Number,
                           eMail_Address, SalesTaxResaleNum
                    FROM "Customers"
                    WHERE CustomerRecordNumber = {cust_recnum}
                """)
                cr = cursor.fetchone()
                if cr:
                    cd = dict(zip([c[0] for c in cursor.description], cr))
                    cust_name = to_str(cd.get("Customer_Bill_Name", "")) or to_str(cd.get("CustomerID", ""))
                    cust_phone = to_str(cd.get("Phone_Number", ""))
                    cust_email = to_str(cd.get("eMail_Address", ""))
                    cust_tin = to_str(cd.get("SalesTaxResaleNum", ""))

                log(f"\n    Found: {inv_num} | {cust_name} | {tx_date_str} | "
                    f"N{main_amt:,.2f} | {len(lines)} lines")
                break

    conn.close()

    if not lines:
        log("\n    [FAIL] No invoice with usable line items found.")
        return

    log(f"\n    Usable lines: {len(lines)}")

    # ============================================================
    # STEP 5: Build payload
    # ============================================================
    log("\n[5] Building API payload...")

    if not cust_tin:
        cust_tin = "23773131-0001"
        log(f"    [WARN] No customer TIN -- using test TIN: {cust_tin}")

    api_lines = []
    for i, line in enumerate(lines):
        # API requires discount_amount >= 1, so use 1 when no discount
        discount = line["discount"] if line["discount"] >= 1 else 1

        # Skip lines with zero price
        if line["unit_price"] <= 0:
            log(f"    Line {i+1}: SKIPPED (zero price) - {line['description']}")
            continue

        api_lines.append({
            "hsn_code": "2710.19",
            "price_amount": line["unit_price"],
            "discount_amount": discount,
            "uom": "ST",
            "invoiced_quantity": line["quantity"],
            "product_category": "Security Services",
            "tax_rate": 7.5,
            "tax_category_id": "STANDARD_VAT",
            "item_name": line["description"] or f"Line Item {i+1}",
            "sellers_item_identification": line["item_code"] or f"ITEM-{i+1}",
        })
        log(f"    Line {i+1}: {line['description']} | "
            f"Qty={line['quantity']} x N{line['unit_price']:,.2f}")

    if not api_lines:
        log("\n    [FAIL] No lines with valid price after filtering.")
        return

    log(f"\n    API lines (after filtering): {len(api_lines)}")

    payload = {
        "document_identifier": inv_num,
        "issue_date": tx_date_str,
        "invoice_type_code": "394",
        "document_currency_code": "NGN",
        "tax_currency_code": "NGN",
        "accounting_customer_party": {
            "party_name": cust_name,
            "tin": cust_tin,
            "email": cust_email or "noemail@placeholder.com",
            "telephone": cust_phone or "+234",
            "business_description": "Customer",
            "postal_address": {
                "street_name": cust_address or "N/A",
                "city_name": cust_city or "Lagos",
                "postal_zone": cust_zip or "100001",
                "country": "NG",
            },
        },
        "invoice_line": api_lines,
    }

    log("\n" + "-" * 60)
    log("  FULL API PAYLOAD")
    log("-" * 60)
    log(json.dumps(payload, indent=2))

    # ============================================================
    # STEP 6: Submit
    # ============================================================
    log("\n" + "=" * 60)
    log("[6] Submitting to Nigeria E-Invoicing API...")
    log(f"    URL: {API_URL}/invoice/generate")
    log("=" * 60)

    try:
        response = requests.post(
            f"{API_URL}/invoice/generate",
            headers=API_HEADERS,
            json=payload,
            timeout=30,
        )

        log(f"\n    HTTP Status: {response.status_code}")
        log(f"\n    Response Body:")
        try:
            resp_json = response.json()
            log(json.dumps(resp_json, indent=2))

            if response.status_code in (200, 201):
                irn = (
                    resp_json.get("irn")
                    or resp_json.get("data", {}).get("irn")
                    or "N/A"
                )
                log(f"\n    [OK] SUCCESS!")
                log(f"    IRN: {irn}")
            else:
                log(f"\n    [FAIL] API returned status {response.status_code}")
        except:
            log(response.text[:2000])

    except requests.exceptions.ConnectionError as e:
        log(f"\n    [FAIL] Connection error: {e}")
    except requests.exceptions.Timeout:
        log(f"\n    [FAIL] Request timed out")
    except Exception as e:
        log(f"\n    [FAIL] {e}")

    log("\n" + "=" * 60)
    log("  TEST COMPLETE")
    log("=" * 60)


if __name__ == "__main__":
    main()