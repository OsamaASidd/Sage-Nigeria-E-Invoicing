"""
Diagnostic: Verify Custom Fields on JrnlHdr + Test Write
==========================================================
Run AFTER creating custom fields in Sage 50:
  Maintain -> Custom Fields -> Transaction tab
"""

import pyodbc
import sys

ODBC_CONN = (
    "Driver={Pervasive ODBC Client Interface};"
    "ServerName=localhost;DBQ=PROTONSECURITYSERVIC;"
    "UID=Peachtree;PWD=cool123;"
)

OUTPUT_FILE = "diag_customfields.txt"
_outfile = None


def log(msg=""):
    print(msg)
    if _outfile:
        _outfile.write(msg + "\n")
        _outfile.flush()


def get_columns_info(cursor, table):
    """Get column info using index-based access (safe for all pyodbc versions)."""
    cols = []
    for row in cursor.columns(table=table):
        vals = list(row)
        name = vals[3] if len(vals) > 3 else str(row)
        typ = vals[5] if len(vals) > 5 else "UNKNOWN"
        size = vals[6] if len(vals) > 6 else 0
        cols.append({"name": name, "type": str(typ), "size": size})
    return cols


def main():
    global _outfile

    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
        except:
            pass

    _outfile = open(OUTPUT_FILE, "w", encoding="utf-8")

    try:
        _run()
    finally:
        _outfile.close()
        print(f"\n>>> Output saved to: {OUTPUT_FILE}")


def _run():
    log("=" * 70)
    log("  DIAGNOSTIC: CUSTOM FIELDS ON JrnlHdr")
    log("=" * 70)

    conn = pyodbc.connect(ODBC_CONN)
    cursor = conn.cursor()
    log("\n[OK] Connected to Sage 50")

    # ================================================================
    # 1. FIND ALL CUSTOM FIELD COLUMNS ON JrnlHdr
    # ================================================================
    log("\n" + "-" * 70)
    log("  [1] ALL JrnlHdr COLUMNS CONTAINING 'custom' OR 'udf'")
    log("-" * 70)

    all_cols = get_columns_info(cursor, "JrnlHdr")
    custom_cols = []

    for c in all_cols:
        name_lower = c["name"].lower()
        if ("custom" in name_lower or "udf" in name_lower
                or "userfield" in name_lower):
            custom_cols.append(c)

    if custom_cols:
        log(f"\n  Found {len(custom_cols)} custom field columns:\n")
        for c in custom_cols:
            log(f"    {c['name']:40s}  type={c['type']:15s}  size={c['size']}")
    else:
        log("\n  [WARN] No custom field columns found on JrnlHdr!")
        log("  Make sure you created them in Sage 50:")
        log("    Maintain -> Custom Fields -> Transaction tab")
        log("\n  Listing ALL JrnlHdr columns for reference:\n")
        for c in all_cols:
            log(f"    {c['name']:40s}  type={c['type']:15s}  size={c['size']}")

    # ================================================================
    # 2. CHECK CURRENT VALUES ON LATEST INVOICE
    # ================================================================
    log("\n" + "-" * 70)
    log("  [2] CURRENT VALUES OF CUSTOM FIELDS ON LATEST INVOICE")
    log("-" * 70)

    cursor.execute("""
        SELECT JrnlKey_TrxNumber, Reference, Description
        FROM "JrnlHdr"
        WHERE Module = 'R'
        ORDER BY TransactionDate DESC
    """)
    hdr = cursor.fetchone()

    if not hdr:
        log("\n  [FAIL] No Module='R' invoices found!")
        conn.close()
        return

    trx_num = hdr[0]
    log(f"\n  TRX#: {trx_num}  Ref: {hdr[1]}  Desc: {hdr[2]}")

    if custom_cols:
        col_names = ", ".join(f'"{c["name"]}"' for c in custom_cols)
        cursor.execute(f"""
            SELECT {col_names}
            FROM "JrnlHdr"
            WHERE JrnlKey_TrxNumber = {trx_num}
        """)
        row = cursor.fetchone()
        if row:
            log(f"\n  Current custom field values:")
            for i, c in enumerate(custom_cols):
                log(f"    {c['name']:40s} = {row[i]!r}")

    # ================================================================
    # 3. TEST WRITE - IRN
    # ================================================================
    log("\n" + "-" * 70)
    log("  [3] TESTING WRITE: IRN TO FIRST CUSTOM FIELD")
    log("-" * 70)

    if not custom_cols:
        log("\n  [SKIP] No custom field columns to test")
        log("  Create them first in Sage 50 UI, then re-run")
        conn.close()
        return

    irn_col = custom_cols[0]["name"]
    test_irn = "TRX-1-C1B44E6C-20260701"

    log(f"\n  Target column: {irn_col}")
    log(f"  Test IRN value: {test_irn}")

    cursor.execute(f"""
        SELECT "{irn_col}" FROM "JrnlHdr"
        WHERE JrnlKey_TrxNumber = {trx_num}
    """)
    original_val = cursor.fetchone()[0]
    log(f"  Original value: {original_val!r}")

    try:
        cursor.execute(f"""
            UPDATE "JrnlHdr"
            SET "{irn_col}" = ?
            WHERE JrnlKey_TrxNumber = {trx_num}
        """, test_irn)
        conn.commit()
        log(f"  [OK] UPDATE executed!")

        cursor.execute(f"""
            SELECT "{irn_col}" FROM "JrnlHdr"
            WHERE JrnlKey_TrxNumber = {trx_num}
        """)
        new_val = cursor.fetchone()[0]
        log(f"  Verified value: {new_val!r}")

        if str(new_val).strip() == test_irn:
            log(f"\n  [SUCCESS] IRN WRITE CONFIRMED!")
        else:
            log(f"\n  [WARN] Value mismatch after write")

    except Exception as e:
        log(f"  [FAIL] Cannot write: {e}")
        log(f"  Pervasive may not allow UPDATE on this column.")
        conn.close()
        return

    # ================================================================
    # 4. TEST WRITE - QR CODE
    # ================================================================
    log("\n" + "-" * 70)
    log("  [4] TESTING WRITE: QR CODE TO SECOND CUSTOM FIELD")
    log("-" * 70)

    if len(custom_cols) >= 2:
        qr_col = custom_cols[1]["name"]
        test_qr = "BcGy0Cu390qHNItQe3SyW6/FZ50tL/KBCEOSevjycpRn72lyWp4ZkL3oZ+vPsEXE"
        qr_col_size = custom_cols[1]["size"]

        log(f"\n  Target column: {qr_col} (max size: {qr_col_size})")
        log(f"  Test QR ({len(test_qr)} chars): {test_qr[:50]}...")

        if qr_col_size and int(qr_col_size) < 100:
            log(f"  [WARN] Column may be too small for full QR (typically 300-500 chars)")

        try:
            cursor.execute(f"""
                UPDATE "JrnlHdr"
                SET "{qr_col}" = ?
                WHERE JrnlKey_TrxNumber = {trx_num}
            """, test_qr)
            conn.commit()

            cursor.execute(f"""
                SELECT "{qr_col}" FROM "JrnlHdr"
                WHERE JrnlKey_TrxNumber = {trx_num}
            """)
            new_val = cursor.fetchone()[0]
            log(f"  Verified: {new_val!r}")

            if str(new_val).strip() == test_qr:
                log(f"\n  [SUCCESS] QR CODE WRITE CONFIRMED!")
            else:
                log(f"  [WARN] Possible truncation")
                log(f"  Written: {len(test_qr)} chars, Read: {len(str(new_val).strip())} chars")

        except Exception as e:
            log(f"  [FAIL] Cannot write QR: {e}")
    else:
        log("\n  [SKIP] Only 1 custom field found")
        log("  Add more custom fields in Sage 50: Maintain -> Custom Fields -> Transaction")

    # ================================================================
    # 5. TEST WRITE - STATUS
    # ================================================================
    if len(custom_cols) >= 3:
        log("\n" + "-" * 70)
        log("  [5] TESTING WRITE: STATUS TO THIRD CUSTOM FIELD")
        log("-" * 70)

        status_col = custom_cols[2]["name"]
        try:
            cursor.execute(f"""
                UPDATE "JrnlHdr"
                SET "{status_col}" = ?
                WHERE JrnlKey_TrxNumber = {trx_num}
            """, "Submitted")
            conn.commit()

            cursor.execute(f"""
                SELECT "{status_col}" FROM "JrnlHdr"
                WHERE JrnlKey_TrxNumber = {trx_num}
            """)
            new_val = cursor.fetchone()[0]
            log(f"  {status_col} = {new_val!r}")

            if str(new_val).strip() == "Submitted":
                log(f"  [SUCCESS] STATUS WRITE CONFIRMED!")
        except Exception as e:
            log(f"  [FAIL] Cannot write status: {e}")

    # ================================================================
    # SUMMARY
    # ================================================================
    log("\n" + "=" * 70)
    log("  SUMMARY")
    log("=" * 70)
    log(f"\n  Custom fields found: {len(custom_cols)}")
    if custom_cols:
        for c in custom_cols:
            log(f"    - {c['name']} ({c['type']}, size={c['size']})")
    log(f"""
  NEXT STEPS:
    1. Upload this output file
    2. Verify in Sage 50: open TRX-1, check Custom Fields section
    3. I'll update the integration to write IRN+QR after API submission
    4. Then customize your invoice print template to show them
    """)

    conn.close()
    log("[DONE]")


if __name__ == "__main__":
    main()