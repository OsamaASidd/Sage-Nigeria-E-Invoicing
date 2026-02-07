"""
Nigeria E-Invoicing API Client
===============================
Handles all communication with the Flick Network e-invoicing API.
"""

import requests
import json
import logging
from datetime import datetime, date
from config import API_BASE_URL, PARTICIPANT_ID, API_KEY

logger = logging.getLogger(__name__)


class EInvoiceAPIClient:
    """Client for Nigeria E-Invoicing API (Flick Network)"""

    def __init__(self, base_url=None, participant_id=None, api_key=None):
        self.base_url = (base_url or API_BASE_URL).rstrip("/")
        self.headers = {
            "Content-Type": "application/json",
            "User-Agent": "Sage50-EInvoicing-Integration/1.0",
            "participant-id": participant_id or PARTICIPANT_ID,
            "x-api-key": api_key or API_KEY,
        }

    def _request(self, method, endpoint, payload=None):
        """Make an API request with error handling."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.info(f"API {method} → {url}")

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                json=payload,
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
        """
        Submit a new invoice to the e-invoicing system.
        
        POST /invoice/generate
        
        Returns: IRN (Invoice Reference Number) on success.
        """
        return self._request("POST", "/invoice/generate", invoice_data)

    def search_invoices(self):
        """
        List/search all invoices.
        
        GET /invoice/search
        """
        return self._request("GET", "/invoice/search")

    def download_invoice(self, irn):
        """
        Download invoice details by IRN.
        
        GET /invoice/download/{irn}
        """
        return self._request("GET", f"/invoice/download/{irn}")

    def get_invoice_details(self, irn):
        """
        Get invoice details/QR code by IRN.
        
        GET /invoice/details/{irn}
        """
        return self._request("GET", f"/invoice/details/{irn}")

    def update_payment_status(self, irn, payment_status, reference):
        """
        Update payment status on an existing invoice.
        
        PATCH /invoice/{irn}
        
        Args:
            irn: Invoice Reference Number
            payment_status: e.g. "PAID", "REJECTED", "PARTIAL"
            reference: Payment reference number
        """
        payload = {
            "payment_status": payment_status,
            "reference": reference,
        }
        return self._request("PATCH", f"/invoice/{irn}", payload)

    # ----------------------------------------------------------------
    # RESOURCE ENDPOINTS (for lookups)
    # ----------------------------------------------------------------

    def get_all_resources(self):
        """GET /resources/all"""
        return self._request("GET", "/resources/all")

    def get_hs_codes(self):
        """GET /resources/hs-codes"""
        return self._request("GET", "/resources/hs-codes")

    def get_service_codes(self):
        """GET /resources/services-codes"""
        return self._request("GET", "/resources/services-codes")

    def get_currencies(self):
        """GET /resources/currencies"""
        return self._request("GET", "/resources/currencies")

    def get_countries(self):
        """GET /resources/countries"""
        return self._request("GET", "/resources/countries")

    # ----------------------------------------------------------------
    # HEALTH CHECK
    # ----------------------------------------------------------------

    def test_connection(self):
        """Test API connectivity by fetching resources."""
        print("Testing API connection...")
        result = self.get_all_resources()
        if result["success"]:
            print("✅ API connection successful!")
            return True
        else:
            print(f"❌ API connection failed: {result['error']}")
            return False


# Quick test
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = EInvoiceAPIClient()
    client.test_connection()
