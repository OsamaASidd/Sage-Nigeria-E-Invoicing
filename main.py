"""
Nigeria E-Invoicing Integration for Sage 50
=============================================

USAGE:
    python main.py                    # Interactive menu
    python main.py --test             # Test API connection
    python main.py --submit-csv       # Submit invoices from CSV export
    python main.py --fetch-resources  # Download HS codes & resources
    python main.py --list-invoices    # List submitted invoices
    python main.py --discover-db      # Discover Sage 50 ODBC tables

SETUP:
    1. pip install requests pyodbc    (pyodbc only if using ODBC method)
    2. Edit config.py with your details
    3. Fill in mapping CSVs in mappings/ folder
    4. Export invoices from Sage 50 as CSV
    5. Run: python main.py
"""

import sys
import os
import csv
import json
import logging
from datetime import datetime

from config import SUBMISSION_LOG_FILE, SUPPLIER
from api_client import EInvoiceAPIClient
from sage_reader import SageCSVReader, SageODBCReader, discover_sage_database
from transformer import InvoiceTransformer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/integration.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class EInvoiceIntegration:
    """Main integration orchestrator."""

    def __init__(self):
        self.api = EInvoiceAPIClient()
        self.transformer = InvoiceTransformer()
        self.submitted_irns = self._load_submission_log()

    # ================================================================
    # CORE WORKFLOW
    # ================================================================

    def submit_invoices_from_csv(self, csv_path=None):
        """
        Main workflow: Read CSV → Transform → Validate → Submit → Log
        """
        print("\n" + "=" * 60)
        print("📄 SUBMITTING INVOICES FROM CSV EXPORT")
        print("=" * 60)

        # Step 1: Read from Sage CSV
        reader = SageCSVReader(invoices_path=csv_path) if csv_path else SageCSVReader()
        invoices = reader.read_invoices()

        if not invoices:
            print("❌ No invoices found in CSV. Check file path and column mapping.")
            return

        print(f"\n📋 Found {len(invoices)} invoices in CSV")

        # Step 2: Process each invoice
        results = {"submitted": 0, "failed": 0, "skipped": 0}

        for inv_num, sage_invoice in invoices.items():
            print(f"\n{'─' * 40}")
            print(f"Processing: {inv_num} | Customer: {sage_invoice['customer_name']}")
            print(f"  Date: {sage_invoice['date']} | Lines: {len(sage_invoice['lines'])}")

            # Skip if already submitted
            if inv_num in self.submitted_irns:
                print(f"  ⏭️  Already submitted (IRN: {self.submitted_irns[inv_num]})")
                results["skipped"] += 1
                continue

            # Step 3: Transform
            try:
                payload = self.transformer.transform(sage_invoice)
            except Exception as e:
                print(f"  ❌ Transform error: {e}")
                results["failed"] += 1
                continue

            # Step 4: Validate
            is_valid, errors = self.transformer.validate(payload)
            if not is_valid:
                print(f"  ❌ Validation failed:")
                for err in errors:
                    print(f"     - {err}")
                results["failed"] += 1
                continue

            # Step 5: Submit
            print(f"  📤 Submitting to e-invoicing API...")
            response = self.api.generate_invoice(payload)

            if response["success"]:
                irn = response["data"].get("irn", response["data"].get("data", {}).get("irn", "UNKNOWN"))
                print(f"  ✅ SUCCESS! IRN: {irn}")
                self._log_submission(inv_num, irn, "SUCCESS", payload)
                results["submitted"] += 1
            else:
                error_msg = response.get("error", "Unknown error")
                print(f"  ❌ FAILED [{response['status']}]: {error_msg}")
                self._log_submission(inv_num, "", "FAILED", payload, error_msg)
                results["failed"] += 1

        # Summary
        print(f"\n{'=' * 60}")
        print(f"📊 SUMMARY")
        print(f"   ✅ Submitted: {results['submitted']}")
        print(f"   ❌ Failed:    {results['failed']}")
        print(f"   ⏭️  Skipped:  {results['skipped']}")
        print(f"{'=' * 60}")

    def submit_single_invoice(self, invoice_data):
        """Submit a single manually-constructed invoice payload."""
        print(f"\n📤 Submitting invoice: {invoice_data.get('document_identifier', 'N/A')}")

        is_valid, errors = self.transformer.validate(invoice_data)
        if not is_valid:
            print("❌ Validation errors:")
            for e in errors:
                print(f"   - {e}")
            return None

        response = self.api.generate_invoice(invoice_data)
        if response["success"]:
            print(f"✅ SUCCESS! Response: {json.dumps(response['data'], indent=2)}")
        else:
            print(f"❌ FAILED: {response['error']}")
        return response

    # ================================================================
    # RESOURCE MANAGEMENT
    # ================================================================

    def fetch_and_save_resources(self):
        """Download all available resources (HS codes, currencies, etc.)."""
        print("\n📥 Fetching resources from API...")

        resources = {
            "hs_codes": self.api.get_hs_codes,
            "service_codes": self.api.get_service_codes,
            "currencies": self.api.get_currencies,
            "countries": self.api.get_countries,
            "all_resources": self.api.get_all_resources,
        }

        os.makedirs("resources", exist_ok=True)

        for name, fetch_func in resources.items():
            print(f"  Fetching {name}...", end=" ")
            result = fetch_func()
            if result["success"]:
                filepath = f"resources/{name}.json"
                with open(filepath, "w") as f:
                    json.dump(result["data"], f, indent=2)
                print(f"✅ Saved to {filepath}")
            else:
                print(f"❌ {result['error']}")

    # ================================================================
    # INVOICE MANAGEMENT
    # ================================================================

    def list_submitted_invoices(self):
        """List all invoices from the API."""
        print("\n📋 Fetching invoice list...")
        result = self.api.search_invoices()
        if result["success"]:
            print(json.dumps(result["data"], indent=2))
        else:
            print(f"❌ Error: {result['error']}")

    def download_invoice(self, irn):
        """Download a specific invoice by IRN."""
        print(f"\n📥 Downloading invoice: {irn}")
        result = self.api.download_invoice(irn)
        if result["success"]:
            filepath = f"logs/invoice_{irn}.json"
            with open(filepath, "w") as f:
                json.dump(result["data"], f, indent=2)
            print(f"✅ Saved to {filepath}")
            print(json.dumps(result["data"], indent=2))
        else:
            print(f"❌ Error: {result['error']}")

    def update_payment(self, irn, status, reference):
        """Update payment status for an invoice."""
        print(f"\n💰 Updating payment for {irn}: {status}")
        result = self.api.update_payment_status(irn, status, reference)
        if result["success"]:
            print(f"✅ Payment updated: {json.dumps(result['data'], indent=2)}")
        else:
            print(f"❌ Error: {result['error']}")

    # ================================================================
    # LOGGING
    # ================================================================

    def _log_submission(self, invoice_number, irn, status, payload, error=""):
        """Log each submission attempt to CSV."""
        os.makedirs(os.path.dirname(SUBMISSION_LOG_FILE), exist_ok=True)
        file_exists = os.path.exists(SUBMISSION_LOG_FILE)

        with open(SUBMISSION_LOG_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["timestamp", "invoice_number", "irn", "status",
                                 "customer", "amount", "error"])
            writer.writerow([
                datetime.now().isoformat(),
                invoice_number,
                irn,
                status,
                payload.get("accounting_customer_party", {}).get("party_name", ""),
                "",  # amount placeholder
                error,
            ])

        if irn:
            self.submitted_irns[invoice_number] = irn

    def _load_submission_log(self):
        """Load previously submitted invoices to avoid duplicates."""
        submitted = {}
        if os.path.exists(SUBMISSION_LOG_FILE):
            with open(SUBMISSION_LOG_FILE, "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get("status") == "SUCCESS" and row.get("irn"):
                        submitted[row["invoice_number"]] = row["irn"]
        return submitted


# ================================================================
# INTERACTIVE MENU
# ================================================================

def interactive_menu():
    """Interactive CLI menu."""
    integration = EInvoiceIntegration()

    while True:
        print("\n" + "=" * 60)
        print("  NIGERIA E-INVOICING — SAGE 50 INTEGRATION")
        print("=" * 60)
        print("  1. 🔌 Test API Connection")
        print("  2. 📄 Submit Invoices from CSV Export")
        print("  3. 📋 List Submitted Invoices")
        print("  4. 📥 Download Invoice by IRN")
        print("  5. 💰 Update Payment Status")
        print("  6. 📦 Fetch Resources (HS Codes, etc.)")
        print("  7. 🔍 Discover Sage 50 Database (ODBC)")
        print("  8. 🧪 Submit Test Invoice")
        print("  9. ❌ Exit")
        print()

        choice = input("Select option (1-9): ").strip()

        if choice == "1":
            integration.api.test_connection()

        elif choice == "2":
            path = input("CSV path (Enter for default): ").strip()
            integration.submit_invoices_from_csv(path if path else None)

        elif choice == "3":
            integration.list_submitted_invoices()

        elif choice == "4":
            irn = input("Enter IRN: ").strip()
            if irn:
                integration.download_invoice(irn)

        elif choice == "5":
            irn = input("Enter IRN: ").strip()
            status = input("Status (PAID/REJECTED/PARTIAL): ").strip()
            ref = input("Payment reference: ").strip()
            if irn and status:
                integration.update_payment(irn, status, ref)

        elif choice == "6":
            integration.fetch_and_save_resources()

        elif choice == "7":
            discover_sage_database()

        elif choice == "8":
            submit_test_invoice(integration)

        elif choice == "9":
            print("Goodbye! 👋")
            break

        else:
            print("Invalid option. Try again.")


def submit_test_invoice(integration):
    """Submit a test invoice to verify the API works."""
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
            "business_description": "Test Business",
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
                "product_category": "Test Category",
                "tax_rate": 7.5,
                "tax_category_id": "STANDARD_VAT",
                "item_name": "Test Product",
                "sellers_item_identification": "TEST001",
            }
        ],
    }

    print("\n🧪 Test payload:")
    print(json.dumps(test_payload, indent=2))
    confirm = input("\nSubmit? (y/n): ").strip().lower()
    if confirm == "y":
        integration.submit_single_invoice(test_payload)


# ================================================================
# CLI ENTRY POINT
# ================================================================

if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    os.makedirs("resources", exist_ok=True)

    if len(sys.argv) > 1:
        arg = sys.argv[1]
        integration = EInvoiceIntegration()

        if arg == "--test":
            integration.api.test_connection()
        elif arg == "--submit-csv":
            path = sys.argv[2] if len(sys.argv) > 2 else None
            integration.submit_invoices_from_csv(path)
        elif arg == "--fetch-resources":
            integration.fetch_and_save_resources()
        elif arg == "--list-invoices":
            integration.list_submitted_invoices()
        elif arg == "--discover-db":
            discover_sage_database()
        else:
            print(f"Unknown argument: {arg}")
            print("Use: --test, --submit-csv, --fetch-resources, --list-invoices, --discover-db")
    else:
        interactive_menu()
