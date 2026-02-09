"""
Sage 50 Data Reader
====================
Reads invoice data from Sage 50 via ODBC (Pervasive) or CSV export.

WORKING CONNECTION:
    Driver: Pervasive ODBC Client Interface
    Server: localhost
    DBQ:    PROTONSECURITYSERVIC
    UID:    Peachtree
    PWD:    cool123

DISCOVERED SCHEMA (PROTON SECURITY):

  JrnlHdr (transaction headers):
    Module: ' '=blank, 'G'=General, 'P'=AP, 'R'=AR/Sales
    CustVendId = integer -> Customers.CustomerRecordNumber
    Key columns: JrnlKey_TrxNumber, TransactionDate, MainAmount, Reference, Description

  JrnlRow (line items):
    ACTUAL columns: GLAcntNumber, Amount, Quantity, UnitCost, RowNumber,
                    ItemRecordNumber, RowDescription, CustomerRecordNumber, ...
    NOTE: NO UnitPrice, NO ItemID (string), NO Description, NO TaxAmount, NO DiscountAmount
    ItemRecordNumber = integer -> LineItem table

  Customers:
    CustomerID (string like 'ATL'), Customer_Bill_Name, eMail_Address,
    CustomerRecordNumber (integer PK)

  Address:
    CustomerRecordNumber, Name, AddressLine1, AddressLine2, City, State, Zip, Country
    NOTE: NO EMail, NO RecordID

  LineItem:
    ItemID (string), Description, SalesPrice1, ItemClass
"""

import csv
import os
import logging
from datetime import datetime, date
from decimal import Decimal

from config import (
    SAGE_ODBC_DRIVER, SAGE_ODBC_SERVER, SAGE_ODBC_DBQ,
    SAGE_ODBC_USER, SAGE_ODBC_PASSWORD,
    SAGE_CSV_INVOICES_PATH, SAGE_CSV_CUSTOMERS_PATH,
    SAGE_ODBC_DSN,
)

logger = logging.getLogger(__name__)


def to_float(val):
    if val is None:
        return 0.0
    if isinstance(val, Decimal):
        return float(val)
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def to_str(val):
    if val is None:
        return ""
    return str(val).strip()


class SageODBCReader:
    """
    Reads data directly from Sage 50's Pervasive/Actian database.

    Key schema facts for this installation:
      - Sales invoices: JrnlHdr.Module = 'R'
      - CustVendId is integer (CustomerRecordNumber)
      - JrnlRow uses: RowDescription, UnitCost, ItemRecordNumber, Quantity, Amount
      - JrnlRow does NOT have: Description, UnitPrice, ItemID, TaxAmount, DiscountAmount
    """

    def __init__(self):
        self.conn = None
        self._item_lookup = None  # lazy-loaded

    def connect(self):
        try:
            import pyodbc
        except ImportError:
            print("[FAIL] pyodbc not installed. Run: pip install pyodbc")
            return False
        try:
            conn_str = (
                f"Driver={{{SAGE_ODBC_DRIVER}}};"
                f"ServerName={SAGE_ODBC_SERVER};"
                f"DBQ={SAGE_ODBC_DBQ};"
                f"UID={SAGE_ODBC_USER};"
                f"PWD={SAGE_ODBC_PASSWORD};"
            )
            self.conn = pyodbc.connect(conn_str)
            logger.info("Connected to Sage 50 via ODBC")
            return True
        except Exception as e:
            logger.error(f"ODBC connection failed: {e}")
            print(f"[FAIL] Connection failed: {e}")
            return False

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    # ----------------------------------------------------------------
    # ITEM LOOKUP (ItemRecordNumber -> ItemID, Description, Price)
    # ----------------------------------------------------------------

    def _build_item_lookup(self):
        """Build lookup: {ItemRecordNumber(int): {item_id, description, price}}"""
        if self._item_lookup is not None:
            return self._item_lookup

        self._item_lookup = {}
        if not self.conn and not self.connect():
            return self._item_lookup

        cursor = self.conn.cursor()
        try:
            # Discover the record number column name
            li_cols = [c.column_name for c in cursor.columns(table="LineItem")]
            recnum_col = None
            for candidate in ["RecordNumber", "LineItemRecordNumber", "ItemRecordNumber"]:
                if candidate in li_cols:
                    recnum_col = candidate
                    break

            if recnum_col:
                cursor.execute(f"""
                    SELECT {recnum_col}, ItemID, Description, SalesPrice1
                    FROM "LineItem" WHERE ItemID <> ''
                """)
                for row in cursor.fetchall():
                    self._item_lookup[row[0]] = {
                        "item_id": to_str(row[1]),
                        "description": to_str(row[2]),
                        "price": to_float(row[3]),
                    }
            else:
                # Fallback: read sequentially (record numbers may not match)
                cursor.execute('SELECT ItemID, Description, SalesPrice1 FROM "LineItem" WHERE ItemID <> \'\'')
                idx = 1
                for row in cursor.fetchall():
                    self._item_lookup[idx] = {
                        "item_id": to_str(row[0]),
                        "description": to_str(row[1]),
                        "price": to_float(row[2]),
                    }
                    idx += 1

            logger.info(f"Built item lookup: {len(self._item_lookup)} items")
        except Exception as e:
            logger.warning(f"Could not build item lookup: {e}")

        return self._item_lookup

    # ----------------------------------------------------------------
    # COMPANY INFO
    # ----------------------------------------------------------------

    def get_company_info(self):
        if not self.conn and not self.connect():
            return {}
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM "Company"')
        cols = [c[0] for c in cursor.description]
        row = cursor.fetchone()
        if row:
            data = dict(zip(cols, row))
            return {k: v for k, v in data.items() if v and str(v).strip()}
        return {}

    # ----------------------------------------------------------------
    # CUSTOMERS
    # ----------------------------------------------------------------

    def _build_customer_map(self):
        """Returns (by_recnum, by_custid) dicts."""
        if not self.conn and not self.connect():
            return {}, {}

        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT CustomerID, Customer_Bill_Name, Contact, Phone_Number,
                   FAX_Number, eMail_Address, SalesTaxResaleNum,
                   CustomerRecordNumber, Balance
            FROM "Customers" WHERE CustomerID <> ''
        """)
        cols = [c[0] for c in cursor.description]

        by_recnum = {}
        by_custid = {}

        for row in cursor.fetchall():
            d = dict(zip(cols, row))
            rec_num = d.get("CustomerRecordNumber", 0)
            cust_id = to_str(d.get("CustomerID", ""))
            info = {
                "customer_id": cust_id,
                "record_number": rec_num,
                "name": to_str(d.get("Customer_Bill_Name", "")) or cust_id,
                "contact": to_str(d.get("Contact", "")),
                "phone": to_str(d.get("Phone_Number", "")),
                "fax": to_str(d.get("FAX_Number", "")),
                "email": to_str(d.get("eMail_Address", "")),
                "tax_resale_num": to_str(d.get("SalesTaxResaleNum", "")),
                "balance": to_float(d.get("Balance", 0)),
                "address": "", "city": "", "state": "", "zip": "", "country": "NG",
            }
            by_recnum[rec_num] = info
            by_custid[cust_id] = info

        # Enrich with addresses
        try:
            cursor.execute("""
                SELECT CustomerRecordNumber, Name, AddressLine1, AddressLine2,
                       City, State, Zip, Country
                FROM "Address" WHERE CustomerRecordNumber > 0
            """)
            for row in cursor.fetchall():
                ac = [c[0] for c in cursor.description]
                d = dict(zip(ac, row))
                rec_num = d.get("CustomerRecordNumber", 0)
                if rec_num in by_recnum:
                    cust = by_recnum[rec_num]
                    parts = [to_str(d.get("AddressLine1", "")), to_str(d.get("AddressLine2", ""))]
                    addr = ", ".join(p for p in parts if p)
                    if addr and not cust["address"]:
                        cust["address"] = addr
                    for field, col in [("city", "City"), ("state", "State"),
                                       ("zip", "Zip"), ("country", "Country")]:
                        val = to_str(d.get(col, ""))
                        if val and not cust[field]:
                            cust[field] = val
                    name = to_str(d.get("Name", ""))
                    if name and not cust["name"]:
                        cust["name"] = name
        except Exception as e:
            logger.warning(f"Could not read Address table: {e}")

        return by_recnum, by_custid

    def get_customers(self):
        _, by_custid = self._build_customer_map()
        return by_custid

    def get_customer(self, customer_id):
        return self.get_customers().get(customer_id)

    # ----------------------------------------------------------------
    # SALES INVOICES
    # ----------------------------------------------------------------

    def get_sales_invoices(self, from_date=None, to_date=None, limit=None):
        """
        Read sales invoices from JrnlHdr + JrnlRow.
        Module='R' for AR/Sales. CustVendId = integer CustomerRecordNumber.

        JrnlRow columns used:
          GLAcntNumber, Amount, Quantity, UnitCost, RowNumber,
          ItemRecordNumber, RowDescription
        """
        if not self.conn and not self.connect():
            return {}

        cursor = self.conn.cursor()
        cust_by_recnum, _ = self._build_customer_map()
        item_lookup = self._build_item_lookup()

        # Query headers
        query = """
            SELECT JrnlKey_TrxNumber, CustVendId, TransactionDate,
                   MainAmount, Reference, Description
            FROM "JrnlHdr"
            WHERE Module = 'R'
        """
        params = []
        if from_date:
            query += " AND TransactionDate >= ?"
            params.append(str(from_date))
        if to_date:
            query += " AND TransactionDate <= ?"
            params.append(str(to_date))
        query += " ORDER BY TransactionDate DESC"

        try:
            cursor.execute(query, params) if params else cursor.execute(query)
        except Exception as e:
            logger.warning(f"Parameterized query failed: {e}")
            cursor.execute("""
                SELECT JrnlKey_TrxNumber, CustVendId, TransactionDate,
                       MainAmount, Reference, Description
                FROM "JrnlHdr" WHERE Module = 'R'
                ORDER BY TransactionDate DESC
            """)

        hdr_cols = [c[0] for c in cursor.description]
        headers = cursor.fetchall()
        if limit:
            headers = headers[:limit]

        logger.info(f"Found {len(headers)} AR headers (Module='R')")

        invoices = {}
        for row in headers:
            hdr = dict(zip(hdr_cols, row))
            trx_num = hdr["JrnlKey_TrxNumber"]
            cust_vend_id = hdr.get("CustVendId", 0)
            ref = to_str(hdr.get("Reference", ""))
            desc = to_str(hdr.get("Description", ""))
            inv_num = ref if ref else f"TRX-{trx_num}"

            cust = cust_by_recnum.get(cust_vend_id, {})

            tx_date = hdr.get("TransactionDate", "")
            if isinstance(tx_date, (datetime, date)):
                tx_date = tx_date.strftime("%Y-%m-%d")
            else:
                tx_date = str(tx_date)[:10]

            unique_key = inv_num
            if unique_key in invoices:
                unique_key = f"{inv_num}-{trx_num}"

            invoices[unique_key] = {
                "invoice_number": inv_num,
                "trx_number": trx_num,
                "date": tx_date,
                "customer_id": cust.get("customer_id", str(cust_vend_id)),
                "customer_name": cust.get("name", "") or desc,
                "customer_email": cust.get("email", ""),
                "customer_phone": cust.get("phone", ""),
                "customer_address": cust.get("address", ""),
                "customer_city": cust.get("city", ""),
                "customer_state": cust.get("state", ""),
                "customer_zip": cust.get("zip", ""),
                "customer_tin": cust.get("tax_resale_num", ""),
                "main_amount": to_float(hdr.get("MainAmount", 0)),
                "description": desc,
                "lines": [],
            }

        # Fetch line items using CORRECT JrnlRow columns
        for inv_key, inv_data in invoices.items():
            trx = inv_data["trx_number"]
            try:
                cursor.execute(f"""
                    SELECT GLAcntNumber, Amount, Quantity, UnitCost,
                           RowNumber, ItemRecordNumber, RowDescription
                    FROM "JrnlRow"
                    WHERE JrnlKey_TrxNumber = {trx}
                """)
                row_cols = [c[0] for c in cursor.description]
                for line_row in cursor.fetchall():
                    ld = dict(zip(row_cols, line_row))

                    amount = to_float(ld.get("Amount", 0))
                    qty = to_float(ld.get("Quantity", 0))
                    unit_cost = to_float(ld.get("UnitCost", 0))
                    item_recnum = ld.get("ItemRecordNumber", 0)
                    row_desc = to_str(ld.get("RowDescription", ""))

                    # Resolve item from LineItem table
                    item_info = item_lookup.get(item_recnum, {})
                    item_id = item_info.get("item_id", "")
                    item_desc = item_info.get("description", "")
                    sales_price = item_info.get("price", 0)

                    # Best description
                    line_desc = row_desc or item_desc or item_id or ""
                    # Best unit price
                    unit_price = abs(unit_cost) if unit_cost != 0 else (
                        sales_price if sales_price > 0 else abs(amount)
                    )

                    # Keep lines with quantity or linked item
                    if qty != 0 or item_recnum > 0:
                        inv_data["lines"].append({
                            "item_code": item_id or str(item_recnum),
                            "description": line_desc or "Service",
                            "quantity": abs(qty) if qty != 0 else 1,
                            "unit_price": unit_price,
                            "discount": 0,
                            "tax_amount": 0,
                            "tax_rate": 7.5,
                            "line_total": abs(amount),
                            "gl_account": to_str(ld.get("GLAcntNumber", "")),
                            "row_number": ld.get("RowNumber", 0),
                        })

            except Exception as e:
                logger.warning(f"Error reading lines for TRX {trx}: {e}")

        invoices = {k: v for k, v in invoices.items() if v["lines"]}
        logger.info(f"Read {len(invoices)} sales invoices with line items")
        return invoices

    def get_invoice_by_reference(self, reference):
        return self.get_sales_invoices().get(reference)

    # ----------------------------------------------------------------
    # LINE ITEMS / INVENTORY
    # ----------------------------------------------------------------

    def get_line_items(self):
        if not self.conn and not self.connect():
            return {}
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT ItemID, Description, ItemClass, SalesPrice1,
                       CostMethod, GLSalesAcct, GLInventAcct
                FROM "LineItem" WHERE ItemID <> ''
            """)
            cols = [c[0] for c in cursor.description]
            items = {}
            for row in cursor.fetchall():
                d = dict(zip(cols, row))
                item_id = to_str(d.get("ItemID", ""))
                if item_id:
                    items[item_id] = {
                        "item_id": item_id,
                        "description": to_str(d.get("Description", "")),
                        "item_class": to_str(d.get("ItemClass", "")),
                        "sales_price": to_float(d.get("SalesPrice1", 0)),
                        "gl_sales_acct": to_str(d.get("GLSalesAcct", "")),
                    }
            return items
        except Exception as e:
            logger.warning(f"Error reading LineItem: {e}")
            return {}

    # ----------------------------------------------------------------
    # TAX INFO
    # ----------------------------------------------------------------

    def get_tax_authorities(self):
        if not self.conn and not self.connect():
            return []
        cursor = self.conn.cursor()
        try:
            cursor.execute('SELECT * FROM "Tax_Authority"')
            cols = [c[0] for c in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.warning(f"Error reading Tax_Authority: {e}")
            return []

    def get_tax_codes(self):
        if not self.conn and not self.connect():
            return []
        cursor = self.conn.cursor()
        try:
            cursor.execute('SELECT * FROM "Tax_Code"')
            cols = [c[0] for c in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.warning(f"Error reading Tax_Code: {e}")
            return []

    # ----------------------------------------------------------------
    # DISCOVERY HELPERS
    # ----------------------------------------------------------------

    def list_tables(self):
        if not self.conn and not self.connect():
            return []
        cursor = self.conn.cursor()
        return sorted([t.table_name for t in cursor.tables(tableType="TABLE")])

    def list_columns(self, table_name):
        if not self.conn and not self.connect():
            return []
        cursor = self.conn.cursor()
        return [{"name": c.column_name, "type": c.type_name}
                for c in cursor.columns(table=table_name)]

    def sample_table(self, table_name, rows=5):
        if not self.conn and not self.connect():
            return [], []
        cursor = self.conn.cursor()
        cursor.execute(f'SELECT * FROM "{table_name}"')
        cols = [c[0] for c in cursor.description]
        return cols, cursor.fetchmany(rows)


# ============================================================
# CSV EXPORT READER (Fallback)
# ============================================================

class SageCSVReader:
    COLUMN_MAP = {
        "invoice_number": "Invoice Number", "invoice_date": "Date",
        "customer_id": "Customer ID", "customer_name": "Customer Name",
        "item_code": "Item Code", "item_description": "Item Description",
        "quantity": "Quantity", "unit_price": "Unit Price",
        "discount": "Discount", "tax_rate": "Tax Rate", "line_total": "Line Total",
    }

    def __init__(self, invoices_path=None, customers_path=None):
        self.invoices_path = invoices_path or SAGE_CSV_INVOICES_PATH
        self.customers_path = customers_path or SAGE_CSV_CUSTOMERS_PATH

    def read_invoices(self):
        if not os.path.exists(self.invoices_path):
            print(f"[FAIL] File not found: {self.invoices_path}")
            return {}
        invoices = {}
        col = self.COLUMN_MAP
        with open(self.invoices_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                inv_num = row.get(col["invoice_number"], "").strip()
                if not inv_num:
                    continue
                if inv_num not in invoices:
                    invoices[inv_num] = {
                        "invoice_number": inv_num,
                        "date": self._parse_date(row.get(col["invoice_date"], "")),
                        "customer_id": row.get(col["customer_id"], "").strip(),
                        "customer_name": row.get(col["customer_name"], "").strip(),
                        "lines": [],
                    }
                invoices[inv_num]["lines"].append({
                    "item_code": row.get(col["item_code"], "").strip(),
                    "description": row.get(col["item_description"], "").strip(),
                    "quantity": self._parse_float(row.get(col["quantity"], "0")),
                    "unit_price": self._parse_float(row.get(col["unit_price"], "0")),
                    "discount": self._parse_float(row.get(col["discount"], "0")),
                    "tax_rate": self._parse_float(row.get(col["tax_rate"], "7.5")),
                    "line_total": self._parse_float(row.get(col["line_total"], "0")),
                })
        return invoices

    @staticmethod
    def _parse_date(date_str):
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return date_str.strip()

    @staticmethod
    def _parse_float(value):
        try:
            return float(str(value).replace(",", "").strip())
        except (ValueError, TypeError):
            return 0.0


# ============================================================
# DATABASE DISCOVERY
# ============================================================

def discover_sage_database():
    reader = SageODBCReader()
    if not reader.connect():
        return

    print("\n" + "=" * 60)
    print("  SAGE 50 DATABASE DISCOVERY")
    print("=" * 60)

    tables = reader.list_tables()
    print(f"\nFound {len(tables)} tables:")
    for t in tables:
        print(f"   {t}")

    for table in ["JrnlHdr", "JrnlRow", "Customers", "Address",
                  "LineItem", "Company", "Tax_Authority", "Tax_Code"]:
        if table in tables:
            cols = reader.list_columns(table)
            print(f"\n{table} ({len(cols)} columns):")
            for col in cols:
                print(f"   - {col['name']} ({col['type']})")

    print("\n" + "=" * 60)
    print("  SAMPLE SALES INVOICES (Module='R')")
    print("=" * 60)

    invoices = reader.get_sales_invoices(limit=5)
    if invoices:
        for inv_num, inv in invoices.items():
            print(f"\n  Invoice: {inv_num}")
            print(f"  Date: {inv['date']} | Customer: {inv['customer_name']} ({inv['customer_id']})")
            print(f"  Amount: N{inv['main_amount']:,.2f} | Lines: {len(inv['lines'])}")
            for line in inv["lines"]:
                print(f"    -> {line['item_code']} | {line['description']} | "
                      f"Qty: {line['quantity']} x {line['unit_price']} = {line['line_total']}")
    else:
        print("  No sales invoices with line items found.")

    print("\n" + "=" * 60)
    print("  CUSTOMERS (first 20)")
    print("=" * 60)
    customers = reader.get_customers()
    for i, (cid, c) in enumerate(customers.items()):
        if i >= 20:
            print(f"  ... and {len(customers) - 20} more")
            break
        print(f"  {cid}: {c['name']} | {c['phone']} | {c['email']} | {c['city']}")

    reader.close()
    print(f"\n[OK] Discovery complete!")


if __name__ == "__main__":
    discover_sage_database()