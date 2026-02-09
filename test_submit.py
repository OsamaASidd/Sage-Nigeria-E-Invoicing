"""
Test: Fetch latest Sage 50 invoice and submit to Nigeria E-Invoicing API
=========================================================================
Uses ONLY confirmed columns from JrnlRow:
  GLAcntNumber, Amount, Quantity, UnitCost, RowNumber,
  ItemRecordNumber, RowDescription, JrnlKey_TrxNumber (via WHERE)
"""

import pyodbc
import requests
import json
import sys
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


def main():
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
        except:
            pass

    print("=" * 60)
    print("  TEST: Fetch Latest Invoice -> Submit to API")
    print("=" * 60)

    # ---- STEP 1: Connect ----
    print("\n[1] Connecting to Sage 50...")
    try:
        conn = pyodbc.connect(ODBC_CONN)
        print("    [OK] Connected")
    except Exception as e:
        print(f"    [FAIL] {e}")
        return

    cursor = conn.cursor()

    # ---- Build LineItem lookup (ItemRecordNumber -> ItemID + Description) ----
    print("\n    Building LineItem lookup...")
    item_lookup = {}  # {record_number_int: {item_id, description, price}}
    try:
        # First check LineItem columns
        li_cols = [c.column_name for c in cursor.columns(table="LineItem")]
        print(f"    LineItem columns: {li_cols[:15]}...")

        # Find the record number column
        li_recnum_col = None
        for candidate in ["RecordNumber", "LineItemRecordNumber", "ItemRecordNumber"]:
            if candidate in li_cols:
                li_recnum_col = candidate
                break

        if li_recnum_col:
            cursor.execute(f"""
                SELECT {li_recnum_col}, ItemID, Description, SalesPrice1
                FROM "LineItem"
                WHERE ItemID <> ''
            """)
            for row in cursor.fetchall():
                rec = row[0]
                item_lookup[rec] = {
                    "item_id": to_str(row[1]),
                    "description": to_str(row[2]),
                    "price": to_float(row[3]),
                }
            print(f"    Loaded {len(item_lookup)} line items (keyed by {li_recnum_col})")
        else:
            # Fallback: just read all and use row index
            cursor.execute('SELECT ItemID, Description, SalesPrice1 FROM "LineItem" WHERE ItemID <> \'\'')
            idx = 1
            for row in cursor.fetchall():
                item_lookup[idx] = {
                    "item_id": to_str(row[0]),
                    "description": to_str(row[1]),
                    "price": to_float(row[2]),
                }
                idx += 1
            print(f"    Loaded {len(item_lookup)} line items (sequential)")
    except Exception as e:
        print(f"    [WARN] LineItem lookup failed: {e}")

    # ---- STEP 2: Get latest sales invoice ----
    print("\n[2] Fetching latest sales invoice (Module='R')...")
    cursor.execute("""
        SELECT JrnlKey_TrxNumber, CustVendId, TransactionDate,
               MainAmount, Reference, Description
        FROM "JrnlHdr"
        WHERE Module = 'R'
        ORDER BY TransactionDate DESC
    """)
    row = cursor.fetchone()

    if not row:
        print("    [FAIL] No Module='R' invoices found!")
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

    print(f"    Invoice:    {inv_num}")
    print(f"    TRX#:       {trx_num}")
    print(f"    Date:       {tx_date_str}")
    print(f"    Amount:     N{main_amt:,.2f}")
    print(f"    Desc:       {desc}")
    print(f"    CustVendId: {cust_recnum}")

    # ---- STEP 3: Get customer ----
    print("\n[3] Looking up customer...")
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
        print(f"    ID:    {cust_id}")
        print(f"    Name:  {cust_name}")
        print(f"    TIN:   {cust_tin or '(none)'}")
    else:
        cust_name = desc
        cust_phone = ""
        cust_email = ""
        cust_tin = ""
        print(f"    [WARN] Not found, using: {desc}")

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
            print(f"    Addr:  {cust_address}")
            print(f"    City:  {cust_city}")
    except Exception as e:
        print(f"    [WARN] Address: {e}")

    # ---- STEP 4: Get line items ----
    # JrnlRow ACTUAL columns:
    #   Amount, Quantity, UnitCost, RowDescription, ItemRecordNumber,
    #   GLAcntNumber, RowNumber (NO: UnitPrice, ItemID, Description, TaxAmount, DiscountAmount)
    print("\n[4] Fetching line items from JrnlRow...")

    def fetch_lines(cur, trx, items_lookup):
        """Fetch JrnlRow lines using only confirmed columns."""
        cur.execute(f"""
            SELECT GLAcntNumber, Amount, Quantity, UnitCost,
                   RowNumber, ItemRecordNumber, RowDescription
            FROM "JrnlRow"
            WHERE JrnlKey_TrxNumber = {trx}
        """)
        rc = [c[0] for c in cur.description]
        result = []
        all_rows = cur.fetchall()
        print(f"    Total JrnlRow entries for TRX {trx}: {len(all_rows)}")

        for lr in all_rows:
            ld = dict(zip(rc, lr))
            qty = to_float(ld.get("Quantity", 0))
            amount = to_float(ld.get("Amount", 0))
            unit_cost = to_float(ld.get("UnitCost", 0))
            item_recnum = ld.get("ItemRecordNumber", 0)
            row_desc = to_str(ld.get("RowDescription", ""))

            # Look up item details from LineItem table
            item_info = items_lookup.get(item_recnum, {})
            item_id = item_info.get("item_id", "")
            item_desc = item_info.get("description", "")
            sales_price = item_info.get("price", 0)

            # Use best available description
            line_desc = row_desc or item_desc or item_id or ""

            # Use sales price if unit cost is 0
            unit_price = abs(unit_cost) if unit_cost != 0 else (sales_price if sales_price > 0 else abs(amount))

            print(f"      ItemRec={item_recnum} ItemID={item_id!r} "
                  f"Desc={line_desc!r} Qty={qty} UnitCost={unit_cost} Amt={amount}")

            # Keep lines with quantity or linked item
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
        print("\n    [WARN] No usable lines. Searching recent invoices...")
        cursor.execute("""
            SELECT JrnlKey_TrxNumber, CustVendId, TransactionDate,
                   MainAmount, Reference, Description
            FROM "JrnlHdr"
            WHERE Module = 'R'
            ORDER BY TransactionDate DESC
        """)
        search_cols = [c[0] for c in cursor.description]
        # Skip first (already tried)
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

                # Re-fetch customer
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

                print(f"\n    Found: {inv_num} | {cust_name} | {tx_date_str} | "
                      f"N{main_amt:,.2f} | {len(lines)} lines")
                break

    conn.close()

    if not lines:
        print("\n    [FAIL] No invoice with usable line items found.")
        return

    print(f"\n    Usable lines: {len(lines)}")

    # ---- STEP 5: Build payload ----
    print("\n[5] Building API payload...")

    if not cust_tin:
        cust_tin = "23773131-0001"
        print(f"    [WARN] No customer TIN -- using test TIN: {cust_tin}")

    api_lines = []
    for i, line in enumerate(lines):
        api_lines.append({
            "hsn_code": "2710.19",
            "price_amount": line["unit_price"],
            "discount_amount": line["discount"],
            "uom": "ST",
            "invoiced_quantity": line["quantity"],
            "product_category": "Security Services",
            "tax_rate": 7.5,
            "tax_category_id": "STANDARD_VAT",
            "item_name": line["description"] or f"Line Item {i+1}",
            "sellers_item_identification": line["item_code"] or f"ITEM-{i+1}",
        })
        print(f"    Line {i+1}: {line['description']} | "
              f"Qty={line['quantity']} x N{line['unit_price']:,.2f}")

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

    print("\n" + "-" * 60)
    print("  FULL API PAYLOAD")
    print("-" * 60)
    print(json.dumps(payload, indent=2))

    # ---- STEP 6: Submit ----
    print("\n" + "=" * 60)
    print("[6] Submitting to Nigeria E-Invoicing API...")
    print(f"    URL: {API_URL}/invoice/generate")
    print("=" * 60)

    try:
        response = requests.post(
            f"{API_URL}/invoice/generate",
            headers=API_HEADERS,
            json=payload,
            timeout=30,
        )

        print(f"\n    HTTP Status: {response.status_code}")
        print(f"\n    Response Body:")
        try:
            resp_json = response.json()
            print(json.dumps(resp_json, indent=2))

            if response.status_code in (200, 201):
                irn = (
                    resp_json.get("irn")
                    or resp_json.get("data", {}).get("irn")
                    or "N/A"
                )
                print(f"\n    [OK] SUCCESS!")
                print(f"    IRN: {irn}")
            else:
                print(f"\n    [FAIL] API returned status {response.status_code}")
        except:
            print(response.text[:2000])

    except requests.exceptions.ConnectionError as e:
        print(f"\n    [FAIL] Connection error: {e}")
    except requests.exceptions.Timeout:
        print(f"\n    [FAIL] Request timed out")
    except Exception as e:
        print(f"\n    [FAIL] {e}")

    print("\n" + "=" * 60)
    print("  TEST COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()