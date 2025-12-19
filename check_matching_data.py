"""
Diagnostic script to check why only 20 ATM transactions show in matching
when database has 100 records
"""
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

# Get database connection from environment
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "root")
DB_NAME = os.getenv("DB_NAME", "recon_db_render")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create database connection
engine = create_engine(DATABASE_URL)

print("=" * 70)
print("🔍 MATCHING DATA DIAGNOSTIC")
print("=" * 70)

with engine.connect() as conn:
    # 1. Total records in each table
    print("\n📊 TOTAL RECORDS IN EACH TABLE:")
    print("-" * 70)
    
    atm_count = conn.execute(text("SELECT COUNT(*) FROM atm_transactions")).scalar()
    switch_count = conn.execute(text("SELECT COUNT(*) FROM switch_transactions")).scalar()
    flexcube_count = conn.execute(text("SELECT COUNT(*) FROM flexcube_transactions")).scalar()
    
    print(f"ATM Transactions:      {atm_count} records")
    print(f"SWITCH Transactions:   {switch_count} records")
    print(f"FLEXCUBE Transactions: {flexcube_count} records")
    
    # 2. Unique RRNs in each table
    print("\n📋 UNIQUE RRNs IN EACH TABLE:")
    print("-" * 70)
    
    atm_unique = conn.execute(text("""
        SELECT COUNT(DISTINCT TRIM(CAST(rrn AS VARCHAR)))
        FROM atm_transactions 
        WHERE rrn IS NOT NULL AND rrn != ''
    """)).scalar()
    
    switch_unique = conn.execute(text("""
        SELECT COUNT(DISTINCT TRIM(CAST(rrn AS VARCHAR)))
        FROM switch_transactions 
        WHERE rrn IS NOT NULL AND rrn != ''
    """)).scalar()
    
    flexcube_unique = conn.execute(text("""
        SELECT COUNT(DISTINCT TRIM(CAST(rrn AS VARCHAR)))
        FROM flexcube_transactions 
        WHERE rrn IS NOT NULL AND rrn != ''
    """)).scalar()
    
    print(f"ATM Unique RRNs:      {atm_unique}")
    print(f"SWITCH Unique RRNs:   {switch_unique}")
    print(f"FLEXCUBE Unique RRNs: {flexcube_unique}")
    
    # 3. RRNs with NULL or empty values
    print("\n❌ NULL OR EMPTY RRNs:")
    print("-" * 70)
    
    atm_null = conn.execute(text("""
        SELECT COUNT(*) FROM atm_transactions 
        WHERE rrn IS NULL OR rrn = ''
    """)).scalar()
    
    switch_null = conn.execute(text("""
        SELECT COUNT(*) FROM switch_transactions 
        WHERE rrn IS NULL OR rrn = ''
    """)).scalar()
    
    flexcube_null = conn.execute(text("""
        SELECT COUNT(*) FROM flexcube_transactions 
        WHERE rrn IS NULL OR rrn = ''
    """)).scalar()
    
    print(f"ATM NULL/Empty RRNs:      {atm_null} records")
    print(f"SWITCH NULL/Empty RRNs:   {switch_null} records")
    print(f"FLEXCUBE NULL/Empty RRNs: {flexcube_null} records")
    
    # 4. Duplicate RRNs (same RRN multiple times in one table)
    print("\n🔄 DUPLICATE RRNs IN EACH TABLE:")
    print("-" * 70)
    
    atm_dupes = conn.execute(text("""
        SELECT COUNT(*) as dup_count
        FROM (
            SELECT TRIM(CAST(rrn AS VARCHAR)) as rrn, COUNT(*) as cnt
            FROM atm_transactions
            WHERE rrn IS NOT NULL AND rrn != ''
            GROUP BY TRIM(CAST(rrn AS VARCHAR))
            HAVING COUNT(*) > 1
        ) dupes
    """)).scalar()
    
    print(f"ATM Duplicate RRNs:   {atm_dupes} RRNs have duplicates")
    
    # 5. Overall matching summary
    print("\n🎯 OVERALL MATCHING SUMMARY:")
    print("-" * 70)
    
    summary = conn.execute(text("""
        WITH combined_rrn AS (
            SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'ATM' AS source
            FROM atm_transactions
            WHERE rrn IS NOT NULL AND rrn != ''

            UNION ALL
            
            SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'SWITCH' AS source
            FROM switch_transactions
            WHERE rrn IS NOT NULL AND rrn != ''

            UNION ALL
            
            SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'FLEXCUBE' AS source
            FROM flexcube_transactions
            WHERE rrn IS NOT NULL AND rrn != ''
        ),
        rrn_summary AS (
            SELECT 
                rrn,
                COUNT(DISTINCT source) AS match_count,
                STRING_AGG(DISTINCT source, ', ' ORDER BY source) AS matched_sources
            FROM combined_rrn
            GROUP BY rrn
        )
        SELECT 
            COUNT(*) AS total_unique_rrns,
            SUM(CASE WHEN match_count = 3 THEN 1 ELSE 0 END) AS fully_matched,
            SUM(CASE WHEN match_count = 2 THEN 1 ELSE 0 END) AS partially_matched,
            SUM(CASE WHEN match_count = 1 THEN 1 ELSE 0 END) AS unmatched
        FROM rrn_summary
    """)).fetchone()
    
    print(f"Total Unique RRNs:    {summary[0]}")
    print(f"Fully Matched:        {summary[1]} (all 3 sources)")
    print(f"Partially Matched:    {summary[2]} (2 sources)")
    print(f"Unmatched:            {summary[3]} (1 source only)")
    
    # 6. Sample of ATM RRNs to check data quality
    print("\n📝 SAMPLE ATM RRNs (First 10):")
    print("-" * 70)
    
    sample_rrns = conn.execute(text("""
        SELECT DISTINCT TRIM(CAST(rrn AS VARCHAR)) as rrn
        FROM atm_transactions
        WHERE rrn IS NOT NULL AND rrn != ''
        LIMIT 10
    """)).fetchall()
    
    for idx, row in enumerate(sample_rrns, 1):
        print(f"{idx}. RRN: {row[0]}")
    
    print("\n" + "=" * 70)
    print("✅ DIAGNOSTIC COMPLETE")
    print("=" * 70)
    
    # 7. Explanation
    print("\n�� EXPLANATION:")
    print("-" * 70)
    print("If 'Total Records' is 100 but 'Unique RRNs' is 20, then:")
    print("  → Multiple transactions share the same RRN")
    print("  → Matching is done by unique RRN, not by record count")
    print("  → This is CORRECT behavior for reconciliation")
    print("\nIf many RRNs are NULL/empty:")
    print("  → Those records are excluded from matching")
    print("  → Check file upload mapping for RRN column")
    print("=" * 70)

