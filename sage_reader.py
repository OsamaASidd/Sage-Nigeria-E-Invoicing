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

KEY TABLES (96 total):
    JrnlHdr     - Journal headers (all transactions). Module='A' = AR (Sales)
    JrnlRow     - Journal line items, linked by JrnlKey_TrxNumber
    Customers   - Customer master data
    Address     - Customer/vendor addresses (linked by RecordType + ID)
    Company     - Company info
    LineItem    - Inventory items
    Tax_Authority - Tax authorities
    Tax_Code    - Tax codes
    Chart       - Chart of accounts
"""

import csv
import os
import logging
from datetime import datetime, date

from config import (
    SAGE_ODBC_DRIVER, SAGE_ODBC_SERVER, SAGE_ODBC_DBQ,
    SAGE_ODBC_USER, SAGE_ODBC_PASSWORD,
    SAGE_CSV_INVOICES_PATH, SAGE_CSV_CUSTOMERS_PATH,
    SAGE_ODBC_DSN,
)

logger = logging.getLogger(__name__)


# ============================================================
# SAGE 50 ODBC READER (Primary method)
# ============================================================

class SageODBCReader:
    """
    Reads data directly from Sage 50's Pervasive/Actian database.

    Sage 50 Journal Structure:
        JrnlHdr.Module values:
            'A' = Accounts Receivable (Sales Invoices, Credit Memos)
            'P' = Accounts Payable (Purchase Invoices)
            'G' = General Journal
            'C' = Cash Receipts
            'D' = Cash Disbursements
            'I' = Inventory Adjustments
            'R' = Payroll

        JrnlHdr.JournalEx values:
            1 = Regular transaction
            2 = Voided transaction

        JrnlRow links via JrnlKey_TrxNumber
    """

    def __init__(self):
        self.conn = None

    def connect(self):
        """Establish ODBC connection to Sage 50."""
        try:
            import pyodbc
        except ImportError:
            print("❌ pyodbc not installed. Run: pip install pyodbc")
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
            print(f"❌ Connection failed: {e}")
            return False

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    # ----------------------------------------------------------------
    # COMPANY INFO
    # ----------------------------------------------------------------

    def get_company_info(self):
        """Read company details from Company table."""
        if not self.conn and not self.connect():
            return {}

        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM "Company"')
        cols = [c[0] for c in cursor.description]
        row = cursor.fetchone()
        if row:
            data = dict(zip(cols, row))
            # Filter out empty/null values
            return {k: v for k, v in data.items() if v and str(v).strip()}
        return {}

    # ----------------------------------------------------------------
    # CUSTOMERS
    # ----------------------------------------------------------------

    def get_customers(self):
        """Read all customers with their addresses."""
        if not self.conn and not self.connect():
            return {}

        cursor = self.conn.cursor()

        # Get customer master data
        cursor.execute("""
            SELECT CustomerID, Customer_Type, Contact, Phone_Number, FAX_Number,
                   AccountNumber, Balance, CustomerSince, GLAcntNumber,
                   SalesTaxResaleNum
            FROM "Customers"
            WHERE CustomerID <> ''
        """)
        cols = [c[0] for c in cursor.description]
        customers = {}
        for row in cursor.fetchall():
            d = dict(zip(cols, row))
            cust_id = d["CustomerID"].strip()
            customers[cust_id] = {
                "customer_id": cust_id,
                "name": "",
                "contact": d.get("Contact", "").strip(),
                "phone": d.get("Phone_Number", "").strip(),
                "fax": d.get("FAX_Number", "").strip(),
                "balance": d.get("Balance", 0),
                "tax_resale_num": d.get("SalesTaxResaleNum", "").strip(),
                "address": "",
                "city": "",
                "state": "",
                "zip": "",
                "country": "NG",
                "email": "",
            }

        # Get addresses - RecordType 1 = Customer
        try:
            cursor.execute("""
                SELECT RecordID, Name, Address1, Address2, City, State,
                       Zip, Country, EMail, RecordType
                FROM "Address"
                WHERE RecordType = 1
            """)
            addr_cols = [c[0] for c in cursor.description]
            for row in cursor.fetchall():
                d = dict(zip(addr_cols, row))
                cust_id = d.get("RecordID", "").strip()
                if cust_id in customers:
                    customers[cust_id]["name"] = d.get("Name", "").strip()
                    addr_parts = [
                        d.get("Address1", "").strip(),
                        d.get("Address2", "").strip(),
                    ]
                    customers[cust_id]["address"] = ", ".join(p for p in addr_parts if p)
                    customers[cust_id]["city"] = d.get("City", "").strip()
                    customers[cust_id]["state"] = d.get("State", "").strip()
                    customers[cust_id]["zip"] = d.get("Zip", "").strip()
                    customers[cust_id]["country"] = d.get("Country", "NG").strip() or "NG"
                    customers[cust_id]["email"] = d.get("EMail", "").strip()
        except Exception as e:
            logger.warning(f"Could not read Address table: {e}")
            # Fallback: use CustomerID as name
            for cust_id in customers:
                if not customers[cust_id]["name"]:
                    customers[cust_id]["name"] = cust_id

        return customers

    def get_customer(self, customer_id):
        """Get a single customer by ID."""
        customers = self.get_customers()
        return customers.get(customer_id)

    # ----------------------------------------------------------------
    # SALES INVOICES
    # ----------------------------------------------------------------

    def get_sales_invoices(self, from_date=None, to_date=None, limit=None):
        """
        Read sales invoices from JrnlHdr + JrnlRow.

        Sales invoices: Module='A' (Accounts Receivable), JournalEx=1 (not voided)

        Args:
            from_date: Start date (YYYY-MM-DD string or date object)
            to_date:   End date (YYYY-MM-DD string or date object)
            limit:     Max number of invoices to return

        Returns: dict of {invoice_number: {header_info, lines[]}}
        """
        if not self.conn and not self.connect():
            return {}

        cursor = self.conn.cursor()

        # Build query for AR journal headers (sales invoices)
        query = """
            SELECT JrnlKey_TrxNumber, CustVendId, TransactionDate,
                   MainAmount, Reference, Description, Module,
                   JrnlKey_Journal, JrnlKey_Per, JournalEx,
                   JournalGUID, PostOrder
            FROM "JrnlHdr"
            WHERE Module = 'A' AND JournalEx = 1
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
            # Fallback without params
            logger.warning(f"Parameterized query failed, using basic: {e}")
            cursor.execute("""
                SELECT JrnlKey_TrxNumber, CustVendId, TransactionDate,
                       MainAmount, Reference, Description, Module,
                       JrnlKey_Journal, JrnlKey_Per, JournalEx
                FROM "JrnlHdr"
                WHERE Module = 'A' AND JournalEx = 1
                ORDER BY TransactionDate DESC
            """)

        hdr_cols = [c[0] for c in cursor.description]
        headers = cursor.fetchall()

        if limit:
            headers = headers[:limit]

        # Load all customers for lookup
        customers = self.get_customers()

        # Build invoice dict
        invoices = {}
        trx_numbers = []

        for row in headers:
            hdr = dict(zip(hdr_cols, row))
            trx_num = hdr["JrnlKey_TrxNumber"]
            cust_id = str(hdr.get("CustVendId", "")).strip()
            ref = str(hdr.get("Reference", "")).strip()

            # Use Reference as invoice number if available, else TrxNumber
            inv_num = ref if ref else f"TRX-{trx_num}"

            # Get customer info
            cust = customers.get(cust_id, {})

            # Parse transaction date
            tx_date = hdr.get("TransactionDate", "")
            if isinstance(tx_date, (datetime, date)):
                tx_date = tx_date.strftime("%Y-%m-%d")
            else:
                tx_date = str(tx_date)[:10]

            invoices[inv_num] = {
                "invoice_number": inv_num,
                "trx_number": trx_num,
                "date": tx_date,
                "customer_id": cust_id,
                "customer_name": cust.get("name", cust_id),
                "customer_email": cust.get("email", ""),
                "customer_phone": cust.get("phone", ""),
                "customer_address": cust.get("address", ""),
                "customer_city": cust.get("city", ""),
                "customer_state": cust.get("state", ""),
                "customer_zip": cust.get("zip", ""),
                "customer_tin": cust.get("tax_resale_num", ""),
                "main_amount": float(hdr.get("MainAmount", 0)),
                "description": str(hdr.get("Description", "")).strip(),
                "lines": [],
            }
            trx_numbers.append(trx_num)

        # Now fetch line items for all invoices
        if trx_numbers:
            # Fetch lines in batches to avoid query limits
            for inv_num, inv_data in invoices.items():
                trx = inv_data["trx_number"]
                try:
                    cursor.execute(f"""
                        SELECT GLAcntNumber, Amount, Description, Quantity,
                               UnitPrice, RowNumber, ItemID, JrnlKey_TrxNumber,
                               TaxAmount, DiscountAmount
                        FROM "JrnlRow"
                        WHERE JrnlKey_TrxNumber = {trx}
                    """)
                    row_cols = [c[0] for c in cursor.description]
                    for line_row in cursor.fetchall():
                        ld = dict(zip(row_cols, line_row))

                        amount = float(ld.get("Amount", 0))
                        qty = float(ld.get("Quantity", 0))
                        unit_price = float(ld.get("UnitPrice", 0))
                        tax_amt = float(ld.get("TaxAmount", 0))
                        discount = float(ld.get("DiscountAmount", 0))

                        # Skip zero-amount GL posting lines (debit/credit entries)
                        # Keep lines that have quantity or item info
                        item_id = str(ld.get("ItemID", "")).strip()
                        desc = str(ld.get("Description", "")).strip()

                        if qty != 0 or item_id:
                            inv_data["lines"].append({
                                "item_code": item_id,
                                "description": desc,
                                "quantity": abs(qty) if qty != 0 else 1,
                                "unit_price": abs(unit_price) if unit_price != 0 else abs(amount),
                                "discount": abs(discount),
                                "tax_amount": abs(tax_amt),
                                "tax_rate": 7.5,  # Default Nigeria VAT
                                "line_total": abs(amount),
                                "gl_account": str(ld.get("GLAcntNumber", "")).strip(),
                                "row_number": ld.get("RowNumber", 0),
                            })

                except Exception as e:
                    logger.warning(f"Error reading lines for TRX {trx}: {e}")

        # Remove invoices with no line items
        invoices = {k: v for k, v in invoices.items() if v["lines"]}

        logger.info(f"Read {len(invoices)} sales invoices from Sage 50")
        return invoices

    def get_invoice_by_reference(self, reference):
        """Get a single invoice by its reference number."""
        invoices = self.get_sales_invoices()
        return invoices.get(reference)

    # ----------------------------------------------------------------
    # LINE ITEMS / INVENTORY
    # ----------------------------------------------------------------

    def get_line_items(self):
        """Read inventory/service items from LineItem table."""
        if not self.conn and not self.connect():
            return {}

        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT ItemID, Description, ItemClass, SalesPrice1,
                       CostMethod, GLSalesAcct, GLInventAcct
                FROM "LineItem"
                WHERE ItemID <> ''
            """)
            cols = [c[0] for c in cursor.description]
            items = {}
            for row in cursor.fetchall():
                d = dict(zip(cols, row))
                item_id = d["ItemID"].strip()
                items[item_id] = {
                    "item_id": item_id,
                    "description": d.get("Description", "").strip(),
                    "item_class": d.get("ItemClass", "").strip(),
                    "sales_price": float(d.get("SalesPrice1", 0)),
                    "gl_sales_acct": d.get("GLSalesAcct", "").strip(),
                }
            return items
        except Exception as e:
            logger.warning(f"Error reading LineItem: {e}")
            return {}

    # ----------------------------------------------------------------
    # TAX INFO
    # ----------------------------------------------------------------

    def get_tax_authorities(self):
        """Read tax authorities."""
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
        """Read tax codes."""
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
        """List all tables in the database."""
        if not self.conn and not self.connect():
            return []
        cursor = self.conn.cursor()
        return sorted([t.table_name for t in cursor.tables(tableType="TABLE")])

    def list_columns(self, table_name):
        """List columns for a table."""
        if not self.conn and not self.connect():
            return []
        cursor = self.conn.cursor()
        return [
            {"name": c.column_name, "type": c.type_name}
            for c in cursor.columns(table=table_name)
        ]

    def sample_table(self, table_name, rows=5):
        """Read sample rows from any table."""
        if not self.conn and not self.connect():
            return [], []
        cursor = self.conn.cursor()
        cursor.execute(f'SELECT * FROM "{table_name}"')
        cols = [c[0] for c in cursor.description]
        data = cursor.fetchmany(rows)
        return cols, data


# ============================================================
# CSV EXPORT READER (Fallback method)
# ============================================================

class SageCSVReader:
    """Reads invoice data from Sage 50 CSV exports."""

    COLUMN_MAP = {
        "invoice_number": "Invoice Number",
        "invoice_date": "Date",
        "customer_id": "Customer ID",
        "customer_name": "Customer Name",
        "item_code": "Item Code",
        "item_description": "Item Description",
        "quantity": "Quantity",
        "unit_price": "Unit Price",
        "discount": "Discount",
        "tax_rate": "Tax Rate",
        "line_total": "Line Total",
    }

    def __init__(self, invoices_path=None, customers_path=None):
        self.invoices_path = invoices_path or SAGE_CSV_INVOICES_PATH
        self.customers_path = customers_path or SAGE_CSV_CUSTOMERS_PATH

    def read_invoices(self):
        """Read and group invoice data from CSV."""
        if not os.path.exists(self.invoices_path):
            logger.error(f"CSV file not found: {self.invoices_path}")
            print(f"❌ File not found: {self.invoices_path}")
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
        date_str = date_str.strip()
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return date_str

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
    """Interactive discovery of Sage 50 database structure."""
    reader = SageODBCReader()
    if not reader.connect():
        return

    print("\n" + "=" * 60)
    print("  SAGE 50 DATABASE DISCOVERY")
    print("=" * 60)

    tables = reader.list_tables()
    print(f"\n📋 Found {len(tables)} tables:")
    for t in tables:
        print(f"   {t}")

    # Show key table structures
    key_tables = ["JrnlHdr", "JrnlRow", "Customers", "Address",
                  "LineItem", "Company", "Tax_Authority", "Tax_Code"]

    for table in key_tables:
        if table in tables:
            cols = reader.list_columns(table)
            print(f"\n📋 {table} ({len(cols)} columns):")
            for col in cols:
                print(f"   - {col['name']} ({col['type']})")

    # Show sample sales invoices
    print("\n" + "=" * 60)
    print("  SAMPLE SALES INVOICES (Module='A')")
    print("=" * 60)

    invoices = reader.get_sales_invoices(limit=5)
    for inv_num, inv in invoices.items():
        print(f"\n  Invoice: {inv_num}")
        print(f"  Date: {inv['date']} | Customer: {inv['customer_name']} ({inv['customer_id']})")
        print(f"  Amount: {inv['main_amount']} | Lines: {len(inv['lines'])}")
        for line in inv["lines"]:
            print(f"    → {line['item_code']} | {line['description']} | "
                  f"Qty: {line['quantity']} × {line['unit_price']} = {line['line_total']}")

    # Show customers
    print("\n" + "=" * 60)
    print("  CUSTOMERS")
    print("=" * 60)
    customers = reader.get_customers()
    for cid, c in customers.items():
        print(f"  {cid}: {c['name']} | {c['phone']} | {c['email']} | {c['city']}")

    reader.close()
    print(f"\n✅ Discovery complete!")


if __name__ == "__main__":
    discover_sage_database()