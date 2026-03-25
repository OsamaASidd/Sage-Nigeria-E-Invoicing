"""
migrate_db.py
=============
One-time migration: switches PRIMARY KEY from trx_number → post_order.

Run ONCE before starting the new app.py:
    python migrate_db.py

Safe to run multiple times — detects if already migrated and skips.
"""

import sqlite3, os, shutil, sys
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.path.join(BASE_DIR, "einvoice.db")

def main():
    if not os.path.exists(DB_PATH):
        print(f"[ERROR] Database not found at: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    # ── Check current schema ──────────────────────────────────────────────────
    info = conn.execute("PRAGMA table_info(invoices)").fetchall()
    if not info:
        print("[INFO] No 'invoices' table found — nothing to migrate.")
        conn.close()
        return

    pk_col = next((r[1] for r in info if r[5] == 1), None)
    col_names = [r[1] for r in info]

    if pk_col == "post_order":
        print("[OK] Already on new schema (PK=post_order). Nothing to do.")
        conn.close()
        return

    if pk_col != "trx_number":
        print(f"[WARN] Unexpected PK column '{pk_col}'. Aborting to be safe.")
        conn.close()
        sys.exit(1)

    # ── Backup ────────────────────────────────────────────────────────────────
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = DB_PATH.replace(".db", f"_backup_{ts}.db")
    shutil.copy2(DB_PATH, backup_path)
    print(f"[BACKUP] Saved backup → {backup_path}")

    # ── Count rows before migration ───────────────────────────────────────────
    total_inv   = conn.execute("SELECT COUNT(*) FROM invoices").fetchone()[0]
    posted_inv  = conn.execute("SELECT COUNT(*) FROM invoices WHERE status='posted'").fetchone()[0]
    total_lines = conn.execute("SELECT COUNT(*) FROM invoice_lines").fetchone()[0]
    print(f"[INFO] invoices: {total_inv} rows ({posted_inv} posted), invoice_lines: {total_lines} rows")

    # ── Check for duplicate post_order values in old table ───────────────────
    # If post_order is NULL or 0 for some rows, we fall back to trx_number
    null_po = conn.execute(
        "SELECT COUNT(*) FROM invoices WHERE post_order IS NULL OR post_order = 0"
    ).fetchone()[0]
    if null_po:
        print(f"[WARN] {null_po} invoice(s) have NULL/0 post_order — will use trx_number as fallback PK")

    dup_po = conn.execute("""
        SELECT post_order, COUNT(*) as cnt FROM invoices
        WHERE post_order IS NOT NULL AND post_order != 0
        GROUP BY post_order HAVING cnt > 1
    """).fetchall()
    if dup_po:
        print(f"[WARN] {len(dup_po)} duplicate post_order value(s) found — only first occurrence kept per PK")

    print("\n[MIGRATING] Starting schema migration...")

    try:
        # Step 1: Rename old tables
        conn.execute("ALTER TABLE invoices RENAME TO invoices_old")
        try:
            conn.execute("ALTER TABLE invoice_lines RENAME TO invoice_lines_old")
            has_lines_old = True
        except:
            has_lines_old = False
        conn.commit()
        print("[STEP 1/5] Renamed old tables → invoices_old / invoice_lines_old")

        # Step 2: Create new invoices table (post_order PK)
        conn.execute("""
            CREATE TABLE invoices (
                post_order          INTEGER PRIMARY KEY,
                trx_number          INTEGER,
                invoice_num         TEXT,
                customer_name       TEXT,
                customer_id         TEXT,
                customer_tin        TEXT,
                customer_email      TEXT,
                customer_phone      TEXT,
                customer_address    TEXT,
                customer_city       TEXT,
                invoice_date        TEXT,
                amount              REAL    DEFAULT 0,
                vat_amount          REAL    DEFAULT 0,
                status              TEXT    DEFAULT 'pending',
                irn                 TEXT,
                qr_code             TEXT,
                posted_at           TEXT,
                error_message       TEXT,
                api_response        TEXT,
                invoice_description TEXT,
                invoice_type        TEXT    DEFAULT 'Invoice',
                last_synced         TEXT
            )
        """)
        conn.commit()
        print("[STEP 2/5] Created new invoices table (PK=post_order)")

        # Step 3: Create new invoice_lines table (post_order FK)
        conn.execute("""
            CREATE TABLE invoice_lines (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                post_order  INTEGER,
                trx_number  INTEGER,
                line_num    INTEGER,
                item_code   TEXT,
                description TEXT,
                quantity    REAL    DEFAULT 1,
                unit_price  REAL    DEFAULT 0,
                amount      REAL    DEFAULT 0,
                tax_rate    REAL    DEFAULT 0
            )
        """)
        conn.commit()
        print("[STEP 3/5] Created new invoice_lines table")

        # Step 4: Copy invoices — use COALESCE(post_order, trx_number) as PK
        # INSERT OR IGNORE so duplicate post_orders keep the first occurrence
        # Handle old tables that may or may not have vat_amount / api_response
        old_cols = [r[1] for r in conn.execute("PRAGMA table_info(invoices_old)").fetchall()]

        vat_expr      = "COALESCE(vat_amount, 0)" if "vat_amount"      in old_cols else "0"
        api_expr      = "api_response"             if "api_response"    in old_cols else "NULL"
        desc_expr     = "invoice_description"      if "invoice_description" in old_cols else "NULL"
        type_expr     = "invoice_type"             if "invoice_type"    in old_cols else "'Invoice'"
        synced_expr   = "last_synced"              if "last_synced"     in old_cols else "NULL"

        conn.execute(f"""
            INSERT OR IGNORE INTO invoices
                (post_order, trx_number, invoice_num, customer_name, customer_id,
                 customer_tin, customer_email, customer_phone, customer_address, customer_city,
                 invoice_date, amount, vat_amount, status,
                 irn, qr_code, posted_at, error_message, api_response,
                 invoice_description, invoice_type, last_synced)
            SELECT
                COALESCE(NULLIF(post_order, 0), trx_number),
                trx_number, invoice_num, customer_name, customer_id,
                customer_tin, customer_email, customer_phone, customer_address, customer_city,
                invoice_date, amount, {vat_expr}, status,
                irn, qr_code, posted_at, error_message, {api_expr},
                {desc_expr}, {type_expr}, {synced_expr}
            FROM invoices_old
        """)
        conn.commit()

        migrated_inv = conn.execute("SELECT COUNT(*) FROM invoices").fetchone()[0]
        print(f"[STEP 4/5] Copied {migrated_inv}/{total_inv} invoices")

        # Step 5: Copy invoice_lines — resolve post_order via invoices_old lookup
        if has_lines_old:
            old_line_cols = [r[1] for r in conn.execute("PRAGMA table_info(invoice_lines_old)").fetchall()]
            tax_expr = "COALESCE(il.tax_rate, 0)" if "tax_rate" in old_line_cols else "0"

            conn.execute(f"""
                INSERT OR IGNORE INTO invoice_lines
                    (post_order, trx_number, line_num, item_code, description,
                     quantity, unit_price, amount, tax_rate)
                SELECT
                    COALESCE(
                        NULLIF(
                            (SELECT post_order FROM invoices_old
                             WHERE trx_number = il.trx_number LIMIT 1),
                            0
                        ),
                        il.trx_number
                    ),
                    il.trx_number, il.line_num, il.item_code, il.description,
                    il.quantity, il.unit_price, il.amount, {tax_expr}
                FROM invoice_lines_old il
            """)
            conn.commit()
            migrated_lines = conn.execute("SELECT COUNT(*) FROM invoice_lines").fetchone()[0]
            print(f"[STEP 5/5] Copied {migrated_lines}/{total_lines} invoice lines")
        else:
            print("[STEP 5/5] No invoice_lines_old table found — skipped")

        # Indexes
        conn.execute("CREATE INDEX IF NOT EXISTS idx_invoices_trx      ON invoices(trx_number)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_invoices_status    ON invoices(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_invoices_customer  ON invoices(customer_id)")
        conn.commit()

        # ── Verification ──────────────────────────────────────────────────────
        new_total  = conn.execute("SELECT COUNT(*) FROM invoices").fetchone()[0]
        new_posted = conn.execute("SELECT COUNT(*) FROM invoices WHERE status='posted'").fetchone()[0]
        pk_check   = conn.execute("PRAGMA table_info(invoices)").fetchall()
        new_pk     = next((r[1] for r in pk_check if r[5] == 1), None)

        print(f"\n[VERIFY] New schema PK column : {new_pk}")
        print(f"[VERIFY] invoices rows         : {new_total} (was {total_inv})")
        print(f"[VERIFY] Posted invoices        : {new_posted} (was {posted_inv})")

        if new_pk != "post_order":
            print("[ERROR] PK is not post_order — migration may have failed!")
            sys.exit(1)
        if new_posted < posted_inv:
            print(f"[ERROR] Lost {posted_inv - new_posted} posted invoice(s)! Check invoices_old.")
            sys.exit(1)

        print("\n[SUCCESS] Migration complete.")
        print(f"  Backup  : {backup_path}")
        print(f"  Old tables kept: invoices_old, invoice_lines_old (safe to delete after testing)\n")

    except Exception as e:
        conn.close()
        print(f"\n[FATAL] Migration failed: {e}")
        print(f"  Restoring from backup {backup_path} ...")
        shutil.copy2(backup_path, DB_PATH)
        print("  Restored. No changes made.")
        sys.exit(1)

    conn.close()

if __name__ == "__main__":
    main()