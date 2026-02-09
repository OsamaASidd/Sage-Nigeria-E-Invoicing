"""
Nigeria E-Invoicing Integration for Sage 50
=============================================
Reads sales invoices from Sage 50 via ODBC and submits to
the Nigeria E-Invoicing Portal (Flick Network API).

USAGE:
    python main.py                    # Interactive menu
    python main.py --test             # Test API + Sage 50 connection
    python main.py --submit           # Submit invoices from Sage 50 ODBC
    python main.py --submit-csv       # Submit invoices from CSV export
    python main.py --list-invoices    # List submitted invoices
    python main.py --fetch-resources  # Download HS codes & resources
    python main.py --discover-db      # Explore Sage 50 ODBC tables
    python main.py --export-mappings  # Export customer/item lists for mapping
"""

import sys
import os
import csv
import json
import logging
from datetime import datetime

# Fix Windows console encoding for Unicode characters
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from config import SUBMISSION_LOG_FILE, SUPPLIER
from api_client import EInvoiceAPIClient
from sage_reader import SageODBCReader, SageCSVReader, discover_sage_database
from transformer import InvoiceTransformer

# Setup logging
os.makedirs("logs", exist_ok=True)
os.makedirs("resources", exist_ok=True)
os.makedirs("mappings", exist_ok=True)

# Create handlers with proper encoding
file_handler = logging.FileHandler("logs/integration.log", encoding="utf-8")
file_handler.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[file_handler, stream_handler],
)
logger = logging.getLogger(__name__)


class EInvoiceIntegration:
    """Main integration orchestrator."""

    def __init__(self):
        self.api = EInvoiceAPIClient()
        self.transformer = InvoiceTransformer()
        self.submitted_irns = self._load_submission_log()

    # ================================================================
    # CORE WORKFLOW: ODBC -> Transform -> Submit
    # ================================================================

    def submit_invoices_from_sage(self, from_date=None, to_date=None, dry_run=False):
        """
        Main workflow: Read from Sage 50 ODBC -> Transform -> Validate -> Submit
        """
        print("\n" + "=" * 60)
        print("  SUBMITTING INVOICES FROM SAGE 50 (ODBC)")
        print("=" * 60)

        # Step 1: Connect to Sage 50
        reader = SageODBCReader()
        if not reader.connect():
            print("[FAIL] Cannot connect to Sage 50. Check config.py ODBC settings.")
            return
        print("[OK] Connected to Sage 50")

        # Step 2: Read sales invoices
        print(f"\nReading sales invoices...")
        if from_date:
            print(f"  From: {from_date}")
        if to_date:
            print(f"  To:   {to_date}")

        invoices = reader.get_sales_invoices(from_date=from_date, to_date=to_date)
        reader.close()

        if not invoices:
            print("[FAIL] No sales invoices found for the given date range.")
            return

        print(f"\nFound {len(invoices)} sales invoices\n")

        # Step 3: Process each invoice
        results = {"submitted": 0, "failed": 0, "skipped": 0, "dry_run": 0}

        for inv_num, sage_invoice in invoices.items():
            print(f"{'~' * 50}")
            print(f"  {inv_num} | {sage_invoice['customer_name']} | "
                  f"{sage_invoice['date']} | N{sage_invoice['main_amount']:,.2f} | "
                  f"{len(sage_invoice['lines'])} lines")

            # Skip already submitted
            if inv_num in self.submitted_irns:
                print(f"   [SKIP] Already submitted (IRN: {self.submitted_irns[inv_num]})")
                results["skipped"] += 1
                continue

            # Transform
            try:
                payload = self.transformer.transform(sage_invoice)
            except Exception as e:
                print(f"   [FAIL] Transform error: {e}")
                logger.exception(f"Transform error for {inv_num}")
                results["failed"] += 1
                continue

            # Validate
            is_valid, errors = self.transformer.validate(payload)
            if not is_valid:
                print(f"   [FAIL] Validation failed:")
                for err in errors:
                    print(f"      - {err}")
                results["failed"] += 1
                continue

            # Dry run -- show payload but don't submit
            if dry_run:
                print(f"   [DRY RUN] payload:")
                print(f"      {json.dumps(payload, indent=2)[:500]}")
                results["dry_run"] += 1
                continue

            # Submit
            print(f"   Submitting...")
            response = self.api.generate_invoice(payload)

            if response["success"]:
                irn = (
                    response["data"].get("irn")
                    or response["data"].get("data", {}).get("irn")
                    or "UNKNOWN"
                )
                print(f"   [OK] SUCCESS! IRN: {irn}")
                self._log_submission(inv_num, irn, "SUCCESS", payload)
                results["submitted"] += 1
            else:
                error_msg = response.get("error", "Unknown error")
                print(f"   [FAIL] [{response['status']}]: {error_msg[:200]}")
                self._log_submission(inv_num, "", "FAILED", payload, error_msg)
                results["failed"] += 1

        # Summary
        print(f"\n{'=' * 60}")
        print(f"  SUMMARY")
        print(f"     Submitted: {results['submitted']}")
        print(f"     Failed:    {results['failed']}")
        print(f"     Skipped:   {results['skipped']}")
        if dry_run:
            print(f"     Dry run:   {results['dry_run']}")
        print(f"{'=' * 60}\n")

    # ================================================================
    # CSV WORKFLOW (Fallback)
    # ================================================================

    def submit_invoices_from_csv(self, csv_path=None):
        """Read CSV -> Transform -> Validate -> Submit."""
        print("\n" + "=" * 60)
        print("  SUBMITTING INVOICES FROM CSV EXPORT")
        print("=" * 60)

        reader = SageCSVReader(invoices_path=csv_path) if csv_path else SageCSVReader()
        invoices = reader.read_invoices()

        if not invoices:
            print("[FAIL] No invoices found in CSV.")
            return

        print(f"\nFound {len(invoices)} invoices in CSV")
        results = {"submitted": 0, "failed": 0, "skipped": 0}

        for inv_num, sage_invoice in invoices.items():
            print(f"\n{'~' * 40}")
            print(f"Processing: {inv_num} | {sage_invoice['customer_name']}")

            if inv_num in self.submitted_irns:
                print(f"  [SKIP] Already submitted")
                results["skipped"] += 1
                continue

            try:
                payload = self.transformer.transform(sage_invoice)
            except Exception as e:
                print(f"  [FAIL] Transform error: {e}")
                results["failed"] += 1
                continue

            is_valid, errors = self.transformer.validate(payload)
            if not is_valid:
                print(f"  [FAIL] Validation failed:")
                for err in errors:
                    print(f"     - {err}")
                results["failed"] += 1
                continue

            print(f"  Submitting...")
            response = self.api.generate_invoice(payload)

            if response["success"]:
                irn = (
                    response["data"].get("irn")
                    or response["data"].get("data", {}).get("irn")
                    or "UNKNOWN"
                )
                print(f"  [OK] IRN: {irn}")
                self._log_submission(inv_num, irn, "SUCCESS", payload)
                results["submitted"] += 1
            else:
                print(f"  [FAIL] {response.get('error', '')[:200]}")
                self._log_submission(inv_num, "", "FAILED", payload, response.get("error", ""))
                results["failed"] += 1

        print(f"\nSUMMARY: Submitted: {results['submitted']} | "
              f"Failed: {results['failed']} | Skipped: {results['skipped']}")

    # ================================================================
    # EXPORT MAPPING TEMPLATES
    # ================================================================

    def export_mapping_templates(self):
        """
        Read customers and items from Sage 50 and export CSV templates
        for the user to fill in TIN, HS codes, and categories.
        """
        print("\nExporting mapping templates from Sage 50...")

        reader = SageODBCReader()
        if not reader.connect():
            return

        # Export customer TIN template
        customers = reader.get_customers()
        tin_file = "mappings/customer_tin_map.csv"

        # Load existing mappings to preserve filled-in data
        existing_tins = {}
        if os.path.exists(tin_file):
            with open(tin_file, "r", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    cid = row.get("customer_id", "").strip()
                    if cid:
                        existing_tins[cid] = row

        with open(tin_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "customer_id", "tin", "email", "phone", "address",
                "city", "postal_code", "business_description",
            ])
            writer.writeheader()
            for cid, c in customers.items():
                existing = existing_tins.get(cid, {})
                writer.writerow({
                    "customer_id": cid,
                    "tin": existing.get("tin", ""),
                    "email": existing.get("email", "") or c.get("email", ""),
                    "phone": existing.get("phone", "") or c.get("phone", ""),
                    "address": existing.get("address", "") or c.get("address", ""),
                    "city": existing.get("city", "") or c.get("city", ""),
                    "postal_code": existing.get("postal_code", "") or c.get("zip", ""),
                    "business_description": existing.get("business_description", ""),
                })

        print(f"  [OK] {tin_file} -- {len(customers)} customers (fill in TIN column)")

        # Export HS code template
        items = reader.get_line_items()
        hsn_file = "mappings/hsn_code_map.csv"

        existing_hsn = {}
        if os.path.exists(hsn_file):
            with open(hsn_file, "r", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    ic = row.get("item_code", "").strip()
                    if ic:
                        existing_hsn[ic] = row

        with open(hsn_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["item_code", "description", "hsn_code"])
            writer.writeheader()
            for iid, item in items.items():
                existing = existing_hsn.get(iid, {})
                writer.writerow({
                    "item_code": iid,
                    "description": item.get("description", ""),
                    "hsn_code": existing.get("hsn_code", ""),
                })

        print(f"  [OK] {hsn_file} -- {len(items)} items (fill in hsn_code column)")

        # Export product category template
        cat_file = "mappings/product_category_map.csv"

        existing_cats = {}
        if os.path.exists(cat_file):
            with open(cat_file, "r", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    ic = row.get("item_code", "").strip()
                    if ic:
                        existing_cats[ic] = row

        with open(cat_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["item_code", "description", "category"])
            writer.writeheader()
            for iid, item in items.items():
                existing = existing_cats.get(iid, {})
                writer.writerow({
                    "item_code": iid,
                    "description": item.get("description", ""),
                    "category": existing.get("category", ""),
                })

        print(f"  [OK] {cat_file} -- {len(items)} items (fill in category column)")

        reader.close()
        print("\nNext steps:")
        print("   1. Fill in TIN for each customer in customer_tin_map.csv")
        print("   2. Fill in HS codes for each item in hsn_code_map.csv")
        print("   3. Fill in categories in product_category_map.csv")
        print("   4. Run: python main.py -> Submit Invoices")

    # ================================================================
    # RESOURCE MANAGEMENT
    # ================================================================

    def fetch_and_save_resources(self):
        """Download all resources (HS codes, currencies, etc.)."""
        print("\nFetching resources from API...")

        resources = {
            "hs_codes": self.api.get_hs_codes,
            "service_codes": self.api.get_service_codes,
            "currencies": self.api.get_currencies,
            "countries": self.api.get_countries,
            "all_resources": self.api.get_all_resources,
        }

        for name, fetch_func in resources.items():
            print(f"  Fetching {name}...", end=" ")
            result = fetch_func()
            if result["success"]:
                filepath = f"resources/{name}.json"
                with open(filepath, "w") as f:
                    json.dump(result["data"], f, indent=2)
                print(f"[OK] -> {filepath}")
            else:
                print(f"[FAIL] {result['error'][:100]}")

    # ================================================================
    # INVOICE MANAGEMENT
    # ================================================================

    def list_submitted_invoices(self):
        """List all invoices from the API."""
        print("\nFetching invoice list from API...")
        result = self.api.search_invoices()
        if result["success"]:
            data = result["data"]
            if isinstance(data, dict) and "data" in data:
                invoices = data["data"]
                if isinstance(invoices, list):
                    print(f"\nFound {len(invoices)} invoices:")
                    for inv in invoices[:20]:
                        print(f"  IRN: {inv.get('irn', 'N/A')} | "
                              f"Date: {inv.get('issue_date', '')} | "
                              f"Status: {inv.get('status', '')}")
                    return
            print(json.dumps(data, indent=2)[:2000])
        else:
            print(f"[FAIL] Error: {result['error']}")

    def download_invoice(self, irn):
        """Download a specific invoice by IRN."""
        print(f"\nDownloading: {irn}")
        result = self.api.download_invoice(irn)
        if result["success"]:
            filepath = f"logs/invoice_{irn}.json"
            with open(filepath, "w") as f:
                json.dump(result["data"], f, indent=2)
            print(f"[OK] Saved: {filepath}")
            print(json.dumps(result["data"], indent=2)[:2000])
        else:
            print(f"[FAIL] {result['error']}")

    def update_payment(self, irn, status, reference):
        """Update payment status for an invoice."""
        print(f"\nUpdating {irn}: {status}")
        result = self.api.update_payment_status(irn, status, reference)
        if result["success"]:
            print(f"[OK] Updated: {json.dumps(result['data'], indent=2)}")
        else:
            print(f"[FAIL] {result['error']}")

    # ================================================================
    # TEST
    # ================================================================

    def test_connections(self):
        """Test both API and Sage 50 connections."""
        print("\n" + "=" * 60)
        print("  CONNECTION TEST")
        print("=" * 60)

        # Test API
        print("\n1. Testing API connection...")
        api_ok = self.api.test_connection()

        # Test Sage 50 ODBC
        print("\n2. Testing Sage 50 ODBC connection...")
        reader = SageODBCReader()
        sage_ok = reader.connect()
        if sage_ok:
            invoices = reader.get_sales_invoices(limit=3)
            customers = reader.get_customers()
            company = reader.get_company_info()

            print(f"   Tables accessible: OK")
            print(f"   Sales invoices found: {len(invoices)}")
            print(f"   Customers found: {len(customers)}")
            print(f"   Company: {company.get('CompanyName', 'N/A')}")

            if invoices:
                inv = list(invoices.values())[0]
                print(f"\n   Sample invoice: {inv['invoice_number']}")
                print(f"     Customer: {inv['customer_name']}")
                print(f"     Date: {inv['date']}")
                print(f"     Amount: N{inv['main_amount']:,.2f}")
                print(f"     Lines: {len(inv['lines'])}")

            reader.close()

        print(f"\n{'=' * 60}")
        print(f"  API:     {'OK' if api_ok else 'FAILED'}")
        print(f"  Sage 50: {'OK' if sage_ok else 'FAILED'}")
        print(f"{'=' * 60}\n")

    def submit_test_invoice(self):
        """Submit a test invoice to verify API works."""
        test_payload = {
            "document_identifier": f"TEST-{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "issue_date": datetime.now().strftime("%Y-%m-%d"),
            "invoice_type_code": "394",
            "document_currency_code": "NGN",
            "tax_currency_code": "NGN",
            "accounting_customer_party": {
                "party_name": "Test Customer",
                "email": "test@example.com",
                "tin": "23773131-0001",
                "telephone": "+234",
                "business_description": "Test",
                "postal_address": {
                    "street_name": "1 Test Street",
                    "city_name": "Lagos",
                    "postal_zone": "100001",
                    "country": "NG",
                },
            },
            "invoice_line": [
                {
                    "hsn_code": "2710.19",
                    "price_amount": 100,
                    "discount_amount": 0,
                    "uom": "ST",
                    "invoiced_quantity": 1,
                    "product_category": "Test",
                    "tax_rate": 7.5,
                    "tax_category_id": "STANDARD_VAT",
                    "item_name": "Test Product",
                    "sellers_item_identification": "TEST001",
                },
            ],
        }

        print("\nTest Invoice Payload:")
        print(json.dumps(test_payload, indent=2))
        confirm = input("\nSubmit? (y/n): ").strip().lower()
        if confirm == "y":
            response = self.api.generate_invoice(test_payload)
            if response["success"]:
                print(f"\n[OK] SUCCESS: {json.dumps(response['data'], indent=2)}")
            else:
                print(f"\n[FAIL] [{response['status']}]: {response['error']}")

    # ================================================================
    # LOGGING
    # ================================================================

    def _log_submission(self, invoice_number, irn, status, payload, error=""):
        """Log each submission attempt."""
        os.makedirs(os.path.dirname(SUBMISSION_LOG_FILE), exist_ok=True)
        file_exists = os.path.exists(SUBMISSION_LOG_FILE)

        with open(SUBMISSION_LOG_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    "timestamp", "invoice_number", "irn", "status",
                    "customer", "amount", "error",
                ])
            writer.writerow([
                datetime.now().isoformat(),
                invoice_number,
                irn,
                status,
                payload.get("accounting_customer_party", {}).get("party_name", ""),
                "",
                error[:500],
            ])

        if status == "SUCCESS" and irn:
            self.submitted_irns[invoice_number] = irn

    def _load_submission_log(self):
        """Load previously submitted invoices to avoid duplicates."""
        submitted = {}
        if os.path.exists(SUBMISSION_LOG_FILE):
            with open(SUBMISSION_LOG_FILE, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get("status") == "SUCCESS" and row.get("irn"):
                        submitted[row["invoice_number"]] = row["irn"]
        return submitted


# ================================================================
# INTERACTIVE MENU
# ================================================================

def interactive_menu():
    integration = EInvoiceIntegration()

    while True:
        print("\n" + "=" * 60)
        print("  NIGERIA E-INVOICING -- SAGE 50 INTEGRATION")
        print("=" * 60)
        print("  1.  Test Connections (API + Sage 50)")
        print("  2.  Submit Invoices from Sage 50 (ODBC)")
        print("  3.  Dry Run -- Preview Without Submitting")
        print("  4.  Submit Invoices from CSV Export")
        print("  5.  List Submitted Invoices (API)")
        print("  6.  Download Invoice by IRN")
        print("  7.  Update Payment Status")
        print("  8.  Fetch Resources (HS Codes, etc.)")
        print("  9.  Export Mapping Templates from Sage 50")
        print("  10. Discover Sage 50 Database")
        print("  11. Submit Test Invoice")
        print("  12. Exit")
        print()

        choice = input("Select (1-12): ").strip()

        if choice == "1":
            integration.test_connections()

        elif choice == "2":
            from_d = input("From date (YYYY-MM-DD, Enter=all): ").strip() or None
            to_d = input("To date (YYYY-MM-DD, Enter=all): ").strip() or None
            integration.submit_invoices_from_sage(from_date=from_d, to_date=to_d)

        elif choice == "3":
            from_d = input("From date (YYYY-MM-DD, Enter=all): ").strip() or None
            to_d = input("To date (YYYY-MM-DD, Enter=all): ").strip() or None
            integration.submit_invoices_from_sage(
                from_date=from_d, to_date=to_d, dry_run=True
            )

        elif choice == "4":
            path = input("CSV path (Enter=default): ").strip()
            integration.submit_invoices_from_csv(path if path else None)

        elif choice == "5":
            integration.list_submitted_invoices()

        elif choice == "6":
            irn = input("Enter IRN: ").strip()
            if irn:
                integration.download_invoice(irn)

        elif choice == "7":
            irn = input("Enter IRN: ").strip()
            status = input("Status (PAID/REJECTED/PARTIAL): ").strip()
            ref = input("Payment reference: ").strip()
            if irn and status:
                integration.update_payment(irn, status, ref)

        elif choice == "8":
            integration.fetch_and_save_resources()

        elif choice == "9":
            integration.export_mapping_templates()

        elif choice == "10":
            discover_sage_database()

        elif choice == "11":
            integration.submit_test_invoice()

        elif choice == "12":
            print("Goodbye!")
            break

        else:
            print("Invalid option.")


# ================================================================
# CLI ENTRY POINT
# ================================================================

if __name__ == "__main__":
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        integration = EInvoiceIntegration()

        if arg == "--test":
            integration.test_connections()
        elif arg == "--submit":
            from_d = sys.argv[2] if len(sys.argv) > 2 else None
            to_d = sys.argv[3] if len(sys.argv) > 3 else None
            integration.submit_invoices_from_sage(from_date=from_d, to_date=to_d)
        elif arg == "--submit-csv":
            path = sys.argv[2] if len(sys.argv) > 2 else None
            integration.submit_invoices_from_csv(path)
        elif arg == "--dry-run":
            integration.submit_invoices_from_sage(dry_run=True)
        elif arg == "--fetch-resources":
            integration.fetch_and_save_resources()
        elif arg == "--list-invoices":
            integration.list_submitted_invoices()
        elif arg == "--discover-db":
            discover_sage_database()
        elif arg == "--export-mappings":
            integration.export_mapping_templates()
        else:
            print(f"Unknown: {arg}")
            print("Use: --test, --submit, --submit-csv, --dry-run, "
                  "--fetch-resources, --list-invoices, --discover-db, --export-mappings")
    else:
        interactive_menu()