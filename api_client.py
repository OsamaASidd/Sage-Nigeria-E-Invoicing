"""
Nigeria E-Invoicing API Client
===============================
Handles all communication with the Cryptware Systems e-invoicing API.
"""

import requests
import json
import logging
from config import API_BASE_URL, API_KEY

logger = logging.getLogger(__name__)


class EInvoiceAPIClient:
    """Client for Nigeria E-Invoicing API (Cryptware Systems)"""

    def __init__(self, base_url=None, api_key=None):
        self.base_url = (base_url or API_BASE_URL).rstrip("/")
        self.headers = {
            "Content-Type": "application/json",
            "User-Agent": "SageX3-EInvoicing-Integration/1.0",
            "x-api-key": api_key or API_KEY,
        }

    def _request(self, method, endpoint, payload=None, params=None):
        """Make an API request with error handling."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.info(f"API {method} -> {url}")

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                json=payload,
                params=params,
                timeout=30,
            )

            logger.info(f"Response [{response.status_code}]: {response.text[:500]}")

            if response.status_code in (200, 201):
                try:
                    return {"success": True, "data": response.json(), "status": response.status_code}
                except json.JSONDecodeError:
                    return {"success": True, "data": response.text, "status": response.status_code}
            else:
                return {
                    "success": False,
                    "error": response.text,
                    "status": response.status_code,
                }
        except requests.exceptions.Timeout:
            return {"success": False, "error": "Request timed out", "status": 0}
        except requests.exceptions.ConnectionError as e:
            return {"success": False, "error": f"Connection error: {e}", "status": 0}
        except Exception as e:
            return {"success": False, "error": str(e), "status": 0}

    # ----------------------------------------------------------------
    # INVOICE ENDPOINTS
    # ----------------------------------------------------------------

    def generate_invoice(self, invoice_data):
        """POST /invoice/generate - Submit a new invoice."""
        return self._request("POST", "/invoice/generate", invoice_data)

    def search_invoices(self, page=1, limit=20, status=None, from_date=None, to_date=None, transaction_category=None):
        """GET /invoice - List invoices with optional filters."""
        params = {"page": page, "limit": limit}
        if status:
            params["status"] = status
        if from_date:
            params["from_date"] = from_date
        if to_date:
            params["to_date"] = to_date
        if transaction_category:
            params["transaction_category"] = transaction_category
        return self._request("GET", "/invoice", params=params)

    def get_invoice_details(self, invoice_id):
        """GET /invoice/{id} - Get full invoice details by UUID or IRN."""
        return self._request("GET", f"/invoice/{invoice_id}")

    def get_invoice_status(self, invoice_id):
        """GET /invoice/{id}/status - Get invoice processing status."""
        return self._request("GET", f"/invoice/{invoice_id}/status")

    def download_qr_code(self, invoice_id):
        """GET /invoice/{id}/qrcode - Download QR code PNG. Note: invoice_id must be UUID, not IRN."""
        return self._request("GET", f"/invoice/{invoice_id}/qrcode")

    def update_payment_status(self, irn, payment_status, reference):
        """PATCH /invoice/{irn} - Update payment status."""
        payload = {
            "payment_status": payment_status,
            "reference": reference,
        }
        return self._request("PATCH", f"/invoice/{irn}", payload)

    def transmit_invoice(self, irn):
        """POST /invoice/transmit/{irn} - Transmit signed invoice to NRS."""
        return self._request("POST", f"/invoice/transmit/{irn}")

    def retry_invoice(self, invoice_id):
        """POST /invoice/{id}/retry - Retry a failed invoice."""
        return self._request("POST", f"/invoice/{invoice_id}/retry")

    def get_statistics(self):
        """GET /invoice/statistics - Get invoice statistics."""
        return self._request("GET", "/invoice/statistics")

    # ----------------------------------------------------------------
    # REFERENCE DATA ENDPOINTS  (was /resources/*)
    # ----------------------------------------------------------------

    def get_countries(self):
        """GET /reference-data/countries"""
        return self._request("GET", "/reference-data/countries")

    def get_currencies(self):
        """GET /reference-data/currencies"""
        return self._request("GET", "/reference-data/currencies")

    def get_tax_categories(self):
        """GET /reference-data/tax-categories"""
        return self._request("GET", "/reference-data/tax-categories")

    def get_payment_means(self):
        """GET /reference-data/payment-means"""
        return self._request("GET", "/reference-data/payment-means")

    def get_invoice_types(self):
        """GET /reference-data/invoice-types"""
        return self._request("GET", "/reference-data/invoice-types")

    def get_service_codes(self):
        """GET /reference-data/service-codes"""
        return self._request("GET", "/reference-data/service-codes")

    def get_vat_exemptions(self):
        """GET /reference-data/vat-exemptions"""
        return self._request("GET", "/reference-data/vat-exemptions")

    # ----------------------------------------------------------------
    # HEALTH CHECK
    # ----------------------------------------------------------------

    def test_connection(self):
        """Test API connectivity via reference data."""
        print("  Testing API...")
        result = self.get_countries()
        if result["success"]:
            print("  [OK] API connection successful!")
            return True
        else:
            print(f"  [FAIL] API failed: {result['error'][:100]}")
            return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = EInvoiceAPIClient()
    client.test_connection()