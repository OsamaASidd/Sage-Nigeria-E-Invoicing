"""
fix_db.py
=========
Cleans the database by removing all non-posted records so a fresh
sync from Sage will repopulate them correctly via InvNumForThisTrx.

Posted invoices (with IRN) are NEVER touched.

Run ONCE, then restart Flask and sync Jan-Apr 2026.

    python fix_db.py
"""
import sqlite3, os, shutil
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.path.join(BASE_DIR, "einvoice.db")

def main():
    if not os.path.exists(DB_PATH):
        print(f"[ERROR] Database not found: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row

    # ── Schema check ──────────────────────────────────────────────────────────
    info  = conn.execute("PRAGMA table_info(invoices)").fetchall()
    pk    = next((r[1] for r in info if r[5] == 1), None)
    cols  = [r[1] for r in info]
    print(f"[INFO] Schema PK: {pk}")
    print(f"[INFO] Columns:   {cols}")

    if pk not in ("post_order", "trx_number"):
        print("[WARN] Unexpected PK — aborting to be safe.")
        conn.close()
        return

    # ── Count current state ───────────────────────────────────────────────────
    total   = conn.execute("SELECT COUNT(*) FROM invoices").fetchone()[0]
    posted  = conn.execute("SELECT COUNT(*) FROM invoices WHERE status='posted'").fetchone()[0]
    pending = conn.execute("SELECT COUNT(*) FROM invoices WHERE status='pending'").fetchone()[0]
    failed  = conn.execute("SELECT COUNT(*) FROM invoices WHERE status='failed'").fetchone()[0]

    print(f"\n[INFO] Current DB state:")
    print(f"       Total    : {total}")
    print(f"       Posted   : {posted}  ← THESE WILL BE KEPT")
    print(f"       Pending  : {pending} ← will be deleted")
    print(f"       Failed   : {failed}  ← will be deleted")

    if total == 0:
        print("\n[OK] Database is already empty.")
        conn.close()
        return

    if pending == 0 and failed == 0:
        print("\n[OK] No pending/failed records to clean.")
        conn.close()
        return

    # Show a sample of what will be deleted
    samples = conn.execute(
        "SELECT * FROM invoices WHERE status != 'posted' LIMIT 10"
    ).fetchall()
    print(f"\n[PREVIEW] First {len(samples)} records that will be deleted:")
    for r in samples:
        d = dict(r)
        pk_val    = d.get("post_order") or d.get("trx_number")
        inv_num   = d.get("invoice_num", "")
        cust_id   = d.get("customer_id", "")
        inv_date  = d.get("invoice_date", "")
        status    = d.get("status", "")
        print(f"    PK={pk_val:>10}  {inv_date}  {inv_num:<30}  {cust_id:<15}  [{status}]")

    print(f"\n  Will DELETE {pending + failed} records (pending + failed).")
    print(f"  Will KEEP   {posted} posted records (with IRN).")
    confirm = input("\n  Proceed? (yes/no): ").strip().lower()

    if confirm != "yes":
        print("[CANCELLED] No changes made.")
        conn.close()
        return

    # ── Backup ────────────────────────────────────────────────────────────────
    ts          = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = DB_PATH.replace(".db", f"_backup_before_fix_{ts}.db")
    shutil.copy2(DB_PATH, backup_path)
    print(f"\n[BACKUP] Saved to: {backup_path}")

    # ── Delete ────────────────────────────────────────────────────────────────
    # Get the PK column for invoice_lines FK
    pk_col = pk  # "post_order" or "trx_number"

    conn.execute(f"""
        DELETE FROM invoice_lines
        WHERE {pk_col} IN (
            SELECT {pk_col} FROM invoices WHERE status != 'posted'
        )
    """)
    lines_deleted = conn.total_changes

    conn.execute("DELETE FROM invoices WHERE status != 'posted'")
    inv_deleted = conn.total_changes - lines_deleted

    conn.commit()

    # ── Verify ────────────────────────────────────────────────────────────────
    new_total  = conn.execute("SELECT COUNT(*) FROM invoices").fetchone()[0]
    new_posted = conn.execute("SELECT COUNT(*) FROM invoices WHERE status='posted'").fetchone()[0]

    print(f"\n[DONE]  Deleted {inv_deleted} invoices, {lines_deleted} invoice_lines")
    print(f"[VERIFY] invoices remaining : {new_total}")
    print(f"[VERIFY] posted preserved   : {new_posted}")

    if new_posted != posted:
        print("[ERROR] Posted count changed! Restore from backup immediately.")
    else:
        print("\n[SUCCESS] Database cleaned.")
        print(f"\nNext steps:")
        print(f"  1. Restart Flask (python app.py)")
        print(f"  2. Sync date range: 2026-01-01 → 2026-04-30")
        print(f"  3. All invoices will be re-inserted with correct invoice numbers from JrnlRow.InvNumForThisTrx")
        print(f"\n  Backup at: {backup_path}")

    conn.close()

if __name__ == "__main__":
    main()