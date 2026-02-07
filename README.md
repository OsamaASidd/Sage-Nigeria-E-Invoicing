# Nigeria E-Invoicing Integration for Sage 50

Middleware integration between **Sage 50 Accounting 2021** and the **Nigeria E-Invoicing Portal** (Flick Network API).

Reads sales invoices directly from Sage 50 via ODBC (Pervasive database) and submits them to the Nigeria e-invoicing API.

## Quick Start

### 1. Install Dependencies
```bash
pip install requests pyodbc
```

### 2. Configure Sage 50 Data Access
In Sage 50:
1. **Maintain → Users → Set up security**
2. Click **Crystal Reports/Data Access** tab
3. Under "Access from Outside Sage 50", select **With the following login information**
4. Set a 7-character password (min 1 letter + 1 number)
5. Click Close

### 3. Configure `config.py`
Update with your details:
- `SUPPLIER` — your client's company info and TIN
- `SAGE_ODBC_*` — Sage 50 connection (already set for PROTON SECURITY)
- `API_*` — E-invoicing API credentials (already set for preprod)

### 4. Export Mapping Templates
```bash
python main.py → Option 9 (Export Mapping Templates)
```
This reads all customers and items from Sage 50 and creates CSV files in `mappings/` for you to fill in:
- **customer_tin_map.csv** — Add TIN numbers for each customer
- **hsn_code_map.csv** — Add HS tariff codes for each item
- **product_category_map.csv** — Add product categories

### 5. Run
```bash
python main.py                         # Interactive menu
python main.py --test                  # Test API + Sage 50 connections
python main.py --submit                # Submit all invoices
python main.py --submit 2025-01-01     # Submit from date
python main.py --dry-run               # Preview without submitting
python main.py --export-mappings       # Export mapping templates
python main.py --fetch-resources       # Download HS codes from API
```

## Workflow

```
Sage 50 (Pervasive DB)
    ↓ ODBC Connection
Read JrnlHdr (Module='A' = Sales)
    ↓
Read JrnlRow (line items)
    ↓
Read Customers + Address (names, contact info)
    ↓
Merge with mapping CSVs (TIN, HS codes, categories)
    ↓
Transform to API JSON format
    ↓
Validate all required fields
    ↓
POST /invoice/generate
    ↓
Log IRN to submission_log.csv
    ↓
Skip already-submitted on next run
```

## Project Structure
```
nigeria-einvoicing/
├── main.py              # Entry point & CLI menu
├── config.py            # All configuration (ODBC + API credentials)
├── api_client.py        # E-invoicing API client
├── sage_reader.py       # Sage 50 ODBC + CSV readers
├── transformer.py       # Sage → API format mapping
├── sage_test.py         # Standalone ODBC connection test
├── mappings/
│   ├── customer_tin_map.csv       # Customer → TIN (fill in)
│   ├── hsn_code_map.csv           # Item → HS code (fill in)
│   └── product_category_map.csv   # Item → category (fill in)
├── logs/
│   ├── integration.log            # App log
│   └── submission_log.csv         # Submitted invoice tracker
└── resources/                     # Downloaded API resources
```

## Sage 50 ODBC Connection

| Setting | Value |
|---------|-------|
| Driver | Pervasive ODBC Client Interface |
| Server | localhost |
| Database | PROTONSECURITYSERVIC |
| User | Peachtree |
| Password | *(set in Sage 50 security)* |

Key Sage 50 tables used:
- **JrnlHdr** — Transaction headers (Module='A' for AR/Sales)
- **JrnlRow** — Transaction line items
- **Customers** — Customer master data
- **Address** — Customer/vendor addresses
- **LineItem** — Inventory/service items
- **Company** — Company info

## API Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/invoice/generate` | POST | Submit new invoice |
| `/invoice/search` | GET | List all invoices |
| `/invoice/download/{irn}` | GET | Download invoice details |
| `/invoice/details/{irn}` | GET | Get invoice QR code |
| `/invoice/{irn}` | PATCH | Update payment status |
| `/resources/hs-codes` | GET | Get valid HS codes |
| `/resources/all` | GET | Get all reference data |

## Notes
- Uses **32-bit Python** (required for 32-bit Pervasive ODBC driver)
- Sage 50 can remain open while reading data
- Duplicate submissions are prevented via `submission_log.csv`
- For production: update API_BASE_URL, PARTICIPANT_ID, API_KEY in config.py