"""
Invoice Transformer
====================
Maps Sage 50 invoice data to Nigeria E-Invoicing API format.
Handles lookup mappings for TIN, HS codes, and product categories.
"""

import csv
import os
import logging
from config import (
    SUPPLIER, DEFAULT_CURRENCY, DEFAULT_TAX_RATE,
    DEFAULT_TAX_CATEGORY, DEFAULT_UOM, DEFAULT_COUNTRY,
    CUSTOMER_TIN_MAP_FILE, HSN_CODE_MAP_FILE, PRODUCT_CATEGORY_MAP_FILE,
)

logger = logging.getLogger(__name__)


class InvoiceTransformer:
    """Transforms Sage 50 invoice data into API-ready JSON."""

    def __init__(self):
        self.customer_tin_map = self._load_csv_map(CUSTOMER_TIN_MAP_FILE, "customer_id", "tin")
        self.hsn_code_map = self._load_csv_map(HSN_CODE_MAP_FILE, "item_code", "hsn_code")
        self.category_map = self._load_csv_map(PRODUCT_CATEGORY_MAP_FILE, "item_code", "category")
        self.customer_extra = self._load_customer_extra()

    def transform(self, sage_invoice):
        """
        Transform a single Sage 50 invoice dict into API payload.
        
        Args:
            sage_invoice: {
                "invoice_number": "INV-001",
                "date": "2025-03-05",
                "customer_id": "CUST001",
                "customer_name": "Customer Name",
                "lines": [
                    {
                        "item_code": "ITEM001",
                        "description": "Product Name",
                        "quantity": 2,
                        "unit_price": 10.0,
                        "discount": 1.0,
                        "tax_rate": 7.5,
                    }
                ]
            }
        
        Returns: dict ready to POST to /invoice/generate
        """
        inv = sage_invoice
        cust_id = inv["customer_id"]
        cust_extra = self.customer_extra.get(cust_id, {})

        # Build customer party
        customer_tin = self.customer_tin_map.get(cust_id, "")
        if not customer_tin:
            logger.warning(f"⚠️  No TIN found for customer '{cust_id}' ({inv['customer_name']})")

        customer_party = {
            "party_name": inv["customer_name"],
            "tin": customer_tin,
            "email": cust_extra.get("email", ""),
            "telephone": cust_extra.get("phone", ""),
            "business_description": cust_extra.get("business_description", ""),
            "postal_address": {
                "street_name": cust_extra.get("address", ""),
                "city_name": cust_extra.get("city", ""),
                "postal_zone": cust_extra.get("postal_code", ""),
                "country": DEFAULT_COUNTRY,
            },
        }

        # Build invoice lines
        invoice_lines = []
        for line in inv["lines"]:
            item_code = line["item_code"]
            hsn_code = self.hsn_code_map.get(item_code, "")
            category = self.category_map.get(item_code, "")

            if not hsn_code:
                logger.warning(f"⚠️  No HS code for item '{item_code}' ({line['description']})")

            invoice_lines.append({
                "hsn_code": hsn_code,
                "price_amount": line["unit_price"],
                "discount_amount": line.get("discount", 0),
                "uom": DEFAULT_UOM,
                "invoiced_quantity": line["quantity"],
                "product_category": category,
                "tax_rate": line.get("tax_rate", DEFAULT_TAX_RATE),
                "tax_category_id": DEFAULT_TAX_CATEGORY,
                "item_name": line["description"],
                "sellers_item_identification": item_code,
            })

        # Build final payload
        payload = {
            "document_identifier": inv["invoice_number"],
            "issue_date": inv["date"],
            "invoice_type_code": "394",  # Standard Invoice
            "document_currency_code": DEFAULT_CURRENCY,
            "tax_currency_code": DEFAULT_CURRENCY,
            "accounting_customer_party": customer_party,
            "invoice_line": invoice_lines,
        }

        return payload

    def validate(self, payload):
        """
        Validate the transformed payload before submission.
        Returns: (is_valid: bool, errors: list[str])
        """
        errors = []

        # Required fields
        if not payload.get("document_identifier"):
            errors.append("Missing document_identifier (invoice number)")
        if not payload.get("issue_date"):
            errors.append("Missing issue_date")

        # Customer validation
        cust = payload.get("accounting_customer_party", {})
        if not cust.get("party_name"):
            errors.append("Missing customer party_name")
        if not cust.get("tin"):
            errors.append(f"Missing customer TIN for '{cust.get('party_name', 'Unknown')}'")

        # Line validation
        lines = payload.get("invoice_line", [])
        if not lines:
            errors.append("No invoice lines")

        for i, line in enumerate(lines):
            if not line.get("hsn_code"):
                errors.append(f"Line {i+1}: Missing HS code for '{line.get('item_name', 'Unknown')}'")
            if not line.get("price_amount") or line["price_amount"] <= 0:
                errors.append(f"Line {i+1}: Invalid price_amount")
            if not line.get("invoiced_quantity") or line["invoiced_quantity"] <= 0:
                errors.append(f"Line {i+1}: Invalid invoiced_quantity")

        return (len(errors) == 0, errors)

    # ----------------------------------------------------------------
    # MAPPING LOADERS
    # ----------------------------------------------------------------

    @staticmethod
    def _load_csv_map(filepath, key_col, value_col):
        """Load a CSV file as a key→value dictionary."""
        mapping = {}
        if not os.path.exists(filepath):
            logger.warning(f"Mapping file not found: {filepath}")
            return mapping

        with open(filepath, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                k = row.get(key_col, "").strip()
                v = row.get(value_col, "").strip()
                if k and v:
                    mapping[k] = v

        logger.info(f"Loaded {len(mapping)} entries from {filepath}")
        return mapping

    def _load_customer_extra(self):
        """Load extra customer details (email, phone, address) from TIN map file."""
        extra = {}
        if not os.path.exists(CUSTOMER_TIN_MAP_FILE):
            return extra

        with open(CUSTOMER_TIN_MAP_FILE, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cust_id = row.get("customer_id", "").strip()
                if cust_id:
                    extra[cust_id] = {
                        "email": row.get("email", "").strip(),
                        "phone": row.get("phone", "").strip(),
                        "address": row.get("address", "").strip(),
                        "city": row.get("city", "").strip(),
                        "postal_code": row.get("postal_code", "").strip(),
                        "business_description": row.get("business_description", "").strip(),
                    }
        return extra
