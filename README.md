# Nigeria E-Invoicing Integration for Sage 50

Middleware integration between **Sage 50 Accounting 2021** and the **Nigeria E-Invoicing Portal** (Flick Network API).

## Quick Start

### 1. Install Dependencies
```bash
pip install requests
pip install pyodbc    # Only if using ODBC method
```

### 2. Configure
Edit `config.py`:
- API credentials (already set for preprod)
- Supplier/company details
- Sage 50 CSV export path

### 3. Fill Mapping Files
The API requires data that Sage 50 doesn't have natively. Fill these CSVs:

| File | Purpose | Required Columns |
|------|---------|-----------------|
| `mappings/customer_tin_map.csv` | Customer TIN numbers + contact info | customer_id, tin, email, phone, address, city |
| `mappings/hsn_code_map.csv` | Item → HS tariff code mapping | item_code, hsn_code |
| `mappings/product_category_map.csv` | Item → product category | item_code, category |

> **Tip:** Run `python main.py` → Option 6 to download valid HS codes from the API.

### 4. Export Invoices from Sage 50
1. Open Sage 50 → **Reports & Forms** → **Accounts Receivable**
2. Select **Sales Journal** or **Invoice Register**
3. Set date range → **Export → CSV**
4. Save to the path in `config.py` (default: `C:\Sage50Export\invoices.csv`)

### 5. Run
```bash
python main.py                    # Interactive menu
python main.py --test             # Test API connection
python main.py --submit-csv       # Submit all invoices from CSV
python main.py --fetch-resources  # Download HS codes & resources
python main.py --list-invoices    # List submitted invoices
python main.py --discover-db      # Explore Sage 50 ODBC tables
```

## Project Structure
```
nigeria-einvoicing/
├── main.py                          # Entry point & CLI menu
├── config.py                        # All configuration
├── api_client.py                    # E-invoicing API client
├── sage_reader.py                   # Sage 50 data reader (CSV + ODBC)
├── transformer.py                   # Sage → API format mapping
├── sample_sage_export.csv           # Sample CSV for testing
├── mappings/
│   ├── customer_tin_map.csv         # Customer ID → TIN mapping
│   ├── hsn_code_map.csv             # Item code → HS code mapping
│   └── product_category_map.csv     # Item code → category mapping
├── logs/
│   ├── integration.log              # Application log
│   └── submission_log.csv           # Submitted invoice tracker
└── resources/                       # Downloaded API resources (auto-created)
```

## Workflow

```
Sage 50 → CSV Export → main.py reads CSV
                          ↓
                    Loads mapping files (TIN, HS codes, categories)
                          ↓
                    Transforms to API JSON format
                          ↓
                    Validates all required fields
                          ↓
                    POST /invoice/generate
                          ↓
                    Logs IRN to submission_log.csv
                          ↓
                    Skips already-submitted invoices on next run
```

## API Endpoints Used

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/invoice/generate` | POST | Submit new invoice |
| `/invoice/search` | GET | List all invoices |
| `/invoice/download/{irn}` | GET | Download invoice details |
| `/invoice/details/{irn}` | GET | Get invoice QR code |
| `/invoice/{irn}` | PATCH | Update payment status |
| `/resources/hs-codes` | GET | Get valid HS codes |
| `/resources/all` | GET | Get all reference data |

## Sage 50 CSV Column Mapping

If your Sage 50 export has different column names, update `COLUMN_MAP` in `sage_reader.py`:

```python
COLUMN_MAP = {
    "invoice_number": "Invoice Number",   # Change to match your export
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
```

## Environment

- **API:** Preprod (https://preprod-ng.flick.network/v1)
- **For production:** Update `API_BASE_URL`, `PARTICIPANT_ID`, and `API_KEY` in config.py
