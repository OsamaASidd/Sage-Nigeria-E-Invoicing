"""
Sage 50 Data Reader
====================
Reads invoice data from Sage 50 via CSV export or ODBC connection.
"""

import csv
import os
import logging
from datetime import datetime
from config import (
    SAGE_CSV_INVOICES_PATH, SAGE_CSV_CUSTOMERS_PATH,
    SAGE_ODBC_DSN, SAGE_ODBC_USER, SAGE_ODBC_PASSWORD,
)

logger = logging.getLogger(__name__)


# ============================================================
# METHOD 1: CSV EXPORT READER (Recommended starting point)
# ============================================================

class SageCSVReader:
    """
    Reads invoice data from Sage 50 CSV exports.
    
    HOW TO EXPORT FROM SAGE 50:
    ---------------------------
    1. Open Sage 50 Accounting
    2. Go to Reports & Forms → Accounts Receivable → Sales Journal
       OR Reports → Accounts Receivable → Invoice Register
    3. Set date range for invoices you want to submit
    4. Click "Export" → Choose CSV format
    5. Save to the path configured in config.py
    
    EXPECTED CSV COLUMNS (adjust mapping below if different):
    Invoice Number, Date, Customer ID, Customer Name, 
    Item Code, Item Description, Quantity, Unit Price, 
    Discount, Tax Rate, Line Total
    """

    # ============================================================
    # COLUMN MAPPING — UPDATE THESE to match your Sage 50 CSV export
    # ============================================================
    COLUMN_MAP = {
        # Invoice Header
        "invoice_number": "Invoice Number",       # or "Invoice #", "Reference"
        "invoice_date": "Date",                    # or "Invoice Date"
        "customer_id": "Customer ID",              # or "Customer No"
        "customer_name": "Customer Name",          # or "Bill To"

        # Invoice Lines  
        "item_code": "Item Code",                  # or "Item ID", "Product Code"
        "item_description": "Item Description",    # or "Description"
        "quantity": "Quantity",                     # or "Qty"
        "unit_price": "Unit Price",                # or "Price", "Rate"
        "discount": "Discount",                    # or "Discount Amount"
        "tax_rate": "Tax Rate",                    # or "Tax %", "VAT Rate"
        "line_total": "Line Total",                # or "Amount", "Extended Amount"
    }

    def __init__(self, invoices_path=None, customers_path=None):
        self.invoices_path = invoices_path or SAGE_CSV_INVOICES_PATH
        self.customers_path = customers_path or SAGE_CSV_CUSTOMERS_PATH

    def read_invoices(self):
        """
        Read and group invoice data from CSV.
        
        Returns: dict of {invoice_number: {header, lines[]}}
        """
        if not os.path.exists(self.invoices_path):
            logger.error(f"CSV file not found: {self.invoices_path}")
            print(f"❌ File not found: {self.invoices_path}")
            print("   Please export invoices from Sage 50 first.")
            return {}

        invoices = {}
        col = self.COLUMN_MAP

        with open(self.invoices_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)

            # Show available columns for debugging
            if reader.fieldnames:
                logger.info(f"CSV columns found: {reader.fieldnames}")

            for row in reader:
                inv_num = row.get(col["invoice_number"], "").strip()
                if not inv_num:
                    continue

                # Create invoice entry if first time seeing this number
                if inv_num not in invoices:
                    invoices[inv_num] = {
                        "invoice_number": inv_num,
                        "date": self._parse_date(row.get(col["invoice_date"], "")),
                        "customer_id": row.get(col["customer_id"], "").strip(),
                        "customer_name": row.get(col["customer_name"], "").strip(),
                        "lines": [],
                    }

                # Add line item
                invoices[inv_num]["lines"].append({
                    "item_code": row.get(col["item_code"], "").strip(),
                    "description": row.get(col["item_description"], "").strip(),
                    "quantity": self._parse_float(row.get(col["quantity"], "0")),
                    "unit_price": self._parse_float(row.get(col["unit_price"], "0")),
                    "discount": self._parse_float(row.get(col["discount"], "0")),
                    "tax_rate": self._parse_float(row.get(col["tax_rate"], "7.5")),
                    "line_total": self._parse_float(row.get(col["line_total"], "0")),
                })

        logger.info(f"Read {len(invoices)} invoices from CSV")
        return invoices

    def read_customers(self):
        """Read customer data from a separate CSV export (optional)."""
        if not os.path.exists(self.customers_path):
            return {}

        customers = {}
        with open(self.customers_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cust_id = row.get("Customer ID", row.get("Customer No", "")).strip()
                if cust_id:
                    customers[cust_id] = {
                        "name": row.get("Customer Name", "").strip(),
                        "email": row.get("E-mail", row.get("Email", "")).strip(),
                        "phone": row.get("Telephone", row.get("Phone", "")).strip(),
                        "address": row.get("Address", row.get("Street", "")).strip(),
                        "city": row.get("City", "").strip(),
                        "postal_code": row.get("Postal Code", row.get("Zip", "")).strip(),
                    }
        return customers

    @staticmethod
    def _parse_date(date_str):
        """Parse date from various Sage 50 formats."""
        date_str = date_str.strip()
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%d-%m-%Y", "%m-%d-%Y"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return date_str  # Return as-is if no format matches

    @staticmethod
    def _parse_float(value):
        """Safely parse a float from string."""
        try:
            return float(str(value).replace(",", "").strip())
        except (ValueError, TypeError):
            return 0.0


# ============================================================
# METHOD 2: ODBC CONNECTION (For automation)
# ============================================================

class SageODBCReader:
    """
    Reads invoice data directly from Sage 50's Pervasive/Actian database.
    
    PREREQUISITES:
    - Pervasive PSQL or Actian Zen ODBC driver installed
    - ODBC DSN configured pointing to Sage 50 company data folder
    - pip install pyodbc
    
    SETUP ODBC DSN:
    1. Open "ODBC Data Sources (64-bit)" from Windows search
    2. System DSN → Add → Select "Pervasive ODBC Engine Interface"
    3. Data Source Name: "Sage50Company"
    4. Database Name: browse to your Sage company data folder
       (e.g., C:\\Sage\\Peachtree\\Company\\YourCompanyName)
    """

    # Common Sage 50 table names (may vary by version/region)
    TABLES = {
        "invoice_headers": "JOURNALHEADER",
        "invoice_lines": "JOURNALROW",
        "customers": "CUSTOMER",
        "items": "ITEM",
    }

    # Journal Key for Sales = 3 in most Sage 50 versions
    SALES_JOURNAL_KEY = 3

    def __init__(self, dsn=None, user=None, password=None):
        self.dsn = dsn or SAGE_ODBC_DSN
        self.user = user or SAGE_ODBC_USER
        self.password = password or SAGE_ODBC_PASSWORD
        self.conn = None

    def connect(self):
        """Establish ODBC connection."""
        try:
            import pyodbc
        except ImportError:
            print("❌ pyodbc not installed. Run: pip install pyodbc")
            return False

        try:
            conn_str = f"DSN={self.dsn}"
            if self.user:
                conn_str += f";UID={self.user}"
            if self.password:
                conn_str += f";PWD={self.password}"

            self.conn = pyodbc.connect(conn_str)
            print("✅ Connected to Sage 50 database via ODBC")
            return True
        except Exception as e:
            print(f"❌ ODBC connection failed: {e}")
            return False

    def list_tables(self):
        """List all tables in the Sage 50 database (for discovery)."""
        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()
        tables = []
        for table in cursor.tables(tableType="TABLE"):
            tables.append(table.table_name)
        return tables

    def list_columns(self, table_name):
        """List columns in a specific table (for discovery)."""
        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()
        columns = []
        for col in cursor.columns(table=table_name):
            columns.append({"name": col.column_name, "type": col.type_name})
        return columns

    def read_invoices(self, from_date=None, to_date=None):
        """
        Read sales invoices from Sage 50 database.
        
        NOTE: Column names below are common but may differ in your version.
        Use list_tables() and list_columns() to discover the actual schema.
        """
        if not self.conn:
            if not self.connect():
                return {}

        cursor = self.conn.cursor()

        # Query invoice headers
        query = f"""
            SELECT * FROM {self.TABLES['invoice_headers']}
            WHERE JournalKey = {self.SALES_JOURNAL_KEY}
        """
        if from_date:
            query += f" AND TransactionDate >= '{from_date}'"
        if to_date:
            query += f" AND TransactionDate <= '{to_date}'"

        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        
        invoices = {}
        for row in cursor.fetchall():
            row_dict = dict(zip(columns, row))
            inv_num = str(row_dict.get("Reference", row_dict.get("InvoiceNumber", "")))
            invoices[inv_num] = {
                "invoice_number": inv_num,
                "date": str(row_dict.get("TransactionDate", "")),
                "customer_id": str(row_dict.get("CustomerID", "")),
                "customer_name": str(row_dict.get("CustomerName", "")),
                "lines": [],
            }

        # Query invoice lines
        for inv_num in invoices:
            cursor.execute(f"""
                SELECT * FROM {self.TABLES['invoice_lines']}
                WHERE Reference = ?
            """, inv_num)
            line_columns = [col[0] for col in cursor.description]
            
            for row in cursor.fetchall():
                row_dict = dict(zip(line_columns, row))
                invoices[inv_num]["lines"].append({
                    "item_code": str(row_dict.get("ItemID", "")),
                    "description": str(row_dict.get("Description", "")),
                    "quantity": float(row_dict.get("Quantity", 0)),
                    "unit_price": float(row_dict.get("UnitPrice", 0)),
                    "discount": float(row_dict.get("DiscountAmount", 0)),
                    "tax_rate": float(row_dict.get("TaxRate", 7.5)),
                    "line_total": float(row_dict.get("Amount", 0)),
                })

        return invoices

    def close(self):
        if self.conn:
            self.conn.close()


# ============================================================
# DATABASE DISCOVERY HELPER
# ============================================================

def discover_sage_database():
    """
    Helper to explore the Sage 50 database structure.
    Run this first to understand your table/column names.
    """
    reader = SageODBCReader()
    if not reader.connect():
        return

    print("\n" + "=" * 60)
    print("SAGE 50 DATABASE DISCOVERY")
    print("=" * 60)

    tables = reader.list_tables()
    print(f"\nFound {len(tables)} tables:")
    for t in sorted(tables):
        print(f"  📋 {t}")

    # Show columns for key tables
    key_tables = ["JOURNALHEADER", "JOURNALROW", "CUSTOMER", "ITEM",
                  "SalesInvoice", "SalesInvoiceLine", "Invoice", "InvoiceLine"]

    for table in key_tables:
        if table in tables:
            print(f"\n📋 Columns in {table}:")
            columns = reader.list_columns(table)
            for col in columns:
                print(f"   - {col['name']} ({col['type']})")

    reader.close()


if __name__ == "__main__":
    # Run discovery to see your database structure
    discover_sage_database()
