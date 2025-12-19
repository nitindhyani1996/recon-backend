"""
Create mixed matching results: 90% matched, 8% partial, 2% unmatched

This script modifies your existing data to create a realistic reconciliation scenario.
"""

import sys
import os
sys.path.insert(0, '.')

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Database connection
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://recon_db_e19b_user:dLi83rJHXVnuFPi8QZ6r7XAsgZ87Fhqx@dpg-ctc4o023esus73afpc0g-a.oregon-postgres.render.com/recon_db_e19b"
)
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
db = Session()

def create_mixed_results():
    """
    Modify existing data to create:
    - 90% matched (exists in ATM + SWITCH + FLEXCUBE)
    - 8% partial (exists in ATM + SWITCH, not in FLEXCUBE)  
    - 2% unmatched (exists in ATM only)
    """
    
    print("🔍 Analyzing current data...")
    
    # Get all ATM RRNs
    atm_rrns = db.execute(text("""
        SELECT rrn FROM atm_transaction 
        WHERE rrn IS NOT NULL AND rrn != ''
        ORDER BY id
        LIMIT 100
    """)).fetchall()
    
    atm_rrn_list = [row[0] for row in atm_rrns]
    total_atm = len(atm_rrn_list)
    
    print(f"📊 Total ATM transactions: {total_atm}")
    
    if total_atm == 0:
        print("❌ No ATM transactions found!")
        return
    
    # Calculate split
    matched_count = int(total_atm * 0.90)  # 90%
    partial_count = int(total_atm * 0.08)  # 8%
    unmatched_count = total_atm - matched_count - partial_count  # Remaining
    
    print(f"\n🎯 Target Distribution:")
    print(f"  ✅ Matched: {matched_count} ({matched_count/total_atm*100:.1f}%)")
    print(f"  ⚠️  Partial: {partial_count} ({partial_count/total_atm*100:.1f}%)")
    print(f"  ❌ Unmatched: {unmatched_count} ({unmatched_count/total_atm*100:.1f}%)")
    
    # Split RRNs
    matched_rrns = atm_rrn_list[:matched_count]
    partial_rrns = atm_rrn_list[matched_count:matched_count + partial_count]
    unmatched_rrns = atm_rrn_list[matched_count + partial_count:]
    
    print(f"\n🔧 Modifying data...")
    
    # Step 1: Ensure MATCHED RRNs exist in all 3 tables
    print(f"  1. Ensuring {matched_count} RRNs exist in SWITCH and FLEXCUBE...")
    for rrn in matched_rrns:
        # Check and add to SWITCH if missing
        switch_exists = db.execute(text(
            "SELECT COUNT(*) FROM switch_transaction WHERE rrn = :rrn"
        ), {"rrn": rrn}).scalar()
        
        if switch_exists == 0:
            db.execute(text("""
                INSERT INTO switch_transaction 
                (rrn, terminal_id, amount, transaction_datetime, direction, uploaded_by, created_at)
                VALUES (:rrn, 'TEST001', 100.00, NOW(), 'INBOUND', 10, NOW())
            """), {"rrn": rrn})
            db.execute(text("""
                INSERT INTO switch_transaction 
                (rrn, terminal_id, amount, transaction_datetime, direction, uploaded_by, created_at)
                VALUES (:rrn, 'TEST001', 100.00, NOW(), 'OUTBOUND', 10, NOW())
            """), {"rrn": rrn})
        
        # Check and add to FLEXCUBE if missing
        flex_exists = db.execute(text(
            "SELECT COUNT(*) FROM flexcube_transaction WHERE rrn = :rrn"
        ), {"rrn": rrn}).scalar()
        
        if flex_exists == 0:
            db.execute(text("""
                INSERT INTO flexcube_transaction 
                (rrn, dr, transaction_datetime, uploaded_by, created_at)
                VALUES (:rrn, 100.00, NOW(), 10, NOW())
            """), {"rrn": rrn})
    
    # Step 2: Ensure PARTIAL RRNs exist in SWITCH but NOT in FLEXCUBE
    print(f"  2. Creating {partial_count} partial matches (removing from FLEXCUBE)...")
    for rrn in partial_rrns:
        # Ensure exists in SWITCH
        switch_exists = db.execute(text(
            "SELECT COUNT(*) FROM switch_transaction WHERE rrn = :rrn"
        ), {"rrn": rrn}).scalar()
        
        if switch_exists == 0:
            db.execute(text("""
                INSERT INTO switch_transaction 
                (rrn, terminal_id, amount, transaction_datetime, direction, uploaded_by, created_at)
                VALUES (:rrn, 'TEST001', 100.00, NOW(), 'INBOUND', 10, NOW())
            """), {"rrn": rrn})
            db.execute(text("""
                INSERT INTO switch_transaction 
                (rrn, terminal_id, amount, transaction_datetime, direction, uploaded_by, created_at)
                VALUES (:rrn, 'TEST001', 100.00, NOW(), 'OUTBOUND', 10, NOW())
            """), {"rrn": rrn})
        
        # Delete from FLEXCUBE
        db.execute(text(
            "DELETE FROM flexcube_transaction WHERE rrn = :rrn"
        ), {"rrn": rrn})
    
    # Step 3: Ensure UNMATCHED RRNs don't exist in SWITCH or FLEXCUBE
    print(f"  3. Creating {unmatched_count} unmatched (removing from SWITCH and FLEXCUBE)...")
    for rrn in unmatched_rrns:
        db.execute(text(
            "DELETE FROM switch_transaction WHERE rrn = :rrn"
        ), {"rrn": rrn})
        db.execute(text(
            "DELETE FROM flexcube_transaction WHERE rrn = :rrn"
        ), {"rrn": rrn})
    
    db.commit()
    
    print("\n✅ Data modification complete!")
    print("\n📋 Sample RRNs:")
    print(f"  Matched (first 3): {matched_rrns[:3]}")
    print(f"  Partial (first 3): {partial_rrns[:3] if len(partial_rrns) >= 3 else partial_rrns}")
    print(f"  Unmatched (first 3): {unmatched_rrns[:3] if len(unmatched_rrns) >= 3 else unmatched_rrns}")
    
    print("\n🎯 Next Steps:")
    print("  1. Run matching engine: GET /api/v1/matching-engine?userId=10&category=1")
    print("  2. View results: GET /api/v1/recon-atm-matching")
    print("  3. You should see ~90% matched, ~8% partial, ~2% unmatched")

if __name__ == "__main__":
    try:
        create_mixed_results()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()
