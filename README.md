# Nigeria E-Invoicing Dashboard

Flask web app that syncs Sage 50 sales invoices and posts them to FIRS via the Flick Network API.

## Setup

```bash
pip install flask pyodbc requests reportlab qrcode Pillow
```

## Run

```bash
python app.py
```

Open http://localhost:5000

## Usage

1. **Sync from Sage** – Reads all sales invoices from Sage 50 into local SQLite
2. **Post to FIRS** – Submits individual or all pending invoices to the API
3. **Download PDF** – Get invoice PDF with IRN and scannable QR code

## Files

- `app.py` – Flask app (routes, Sage sync, API posting, PDF generation)
- `templates/index.html` – Dashboard UI
- `einvoice.db` – SQLite tracking database (auto-created)
- `invoices/` – Generated PDF invoices
