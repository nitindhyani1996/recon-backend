"""
Test Script: Generate and Test 1000 Records with Mixed Matching Scenarios

This script will:
1. Generate 1000 test records (ATM, SWITCH, FLEXCUBE)
2. Create different matching scenarios:
   - Fully Matched (all 3 sources)
   - Partially Matched (2 sources)
   - Unmatched (1 source only)
3. Test storing results in ReconMatchingSummary TEXT columns
4. Measure performance and storage size
"""

import sys
import os
import random
import json
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.models.atm_transaction import ATMTransaction
from app.models.SwitchTransaction import SwitchTransaction
from app.models.FlexcubeTransaction import FlexcubeTransaction
from app.models.ReconMatchingSummary import ReconMatchingSummary
from app.db.database import get_db, engine

load_dotenv()

print("=" * 80)
print("🧪 TESTING 1000 RECORDS WITH MIXED MATCHING SCENARIOS")
print("=" * 80)

# Configuration
TOTAL_RRNS = 1000
FULLY_MATCHED = 600   # 60% - in all 3 sources
PARTIALLY_MATCHED = 300  # 30% - in 2 sources only
UNMATCHED = 100      # 10% - in 1 source only

print(f"\n📊 Test Configuration:")
print(f"  Total RRNs: {TOTAL_RRNS}")
print(f"  Fully Matched: {FULLY_MATCHED} ({FULLY_MATCHED/TOTAL_RRNS*100}%)")
print(f"  Partially Matched: {PARTIALLY_MATCHED} ({PARTIALLY_MATCHED/TOTAL_RRNS*100}%)")
print(f"  Unmatched: {UNMATCHED} ({UNMATCHED/TOTAL_RRNS*100}%)")

# Generate test data
def generate_rrn(index):
    """Generate RRN like: 251218XXXXXX"""
    date_part = "251218"  # Dec 18, 2025
    number_part = str(index).zfill(6)
    return f"{date_part}{number_part}"

def random_datetime():
    """Generate random datetime in December 2025"""
    start = datetime(2025, 12, 1)
    days = random.randint(0, 30)
    hours = random.randint(0, 23)
    minutes = random.randint(0, 59)
    return start + timedelta(days=days, hours=hours, minutes=minutes)

def random_terminal():
    """Generate random terminal ID"""
    return f"IZY000{random.randint(1, 99):02d}"

def random_amount():
    """Generate random amount"""
    return random.choice([5000, 10000, 15000, 20000, 50000, 100000, 200000, 500000])

currencies = ["BWP", "EUR", "GBP", "KES", "USD", "ZAR", "ZMW"]
transaction_types = ["Withdrawal", "Deposit", "Balance Inquiry", "Transfer"]
directions = ["INBOUND", "OUTBOUND"]
response_codes = ["0", "5", "51", "68"]

print("\n🔨 Generating test data...")

# Create session
Session = sessionmaker(bind=engine)
db = Session()

try:
    # Clear existing test data
    print("  🗑️  Clearing existing test data...")
    db.query(ReconMatchingSummary).filter(
        ReconMatchingSummary.recon_reference_number.like('TEST_%')
    ).delete(synchronize_session=False)
    db.query(ATMTransaction).delete(synchronize_session=False)
    db.query(SwitchTransaction).delete(synchronize_session=False)
    db.query(FlexcubeTransaction).delete(synchronize_session=False)
    db.commit()
    
    # Track RRNs by category
    fully_matched_rrns = []
    partially_matched_rrns = []
    unmatched_rrns = []
    
    print(f"  ✅ Generating {FULLY_MATCHED} fully matched records...")
    # 1. Generate FULLY MATCHED records (ATM + SWITCH + FLEXCUBE)
    for i in range(FULLY_MATCHED):
        rrn = generate_rrn(i)
        dt = random_datetime()
        terminal = random_terminal()
        amount = random_amount()
        currency = random.choice(currencies)
        
        fully_matched_rrns.append(rrn)
        
        # ATM Transaction
        atm = ATMTransaction(
            datetime=dt,
            terminalid=terminal,
            rrn=rrn,
            amount=amount,
            currency=currency,
            transactiontype=random.choice(transaction_types),
            responsecode=random.choice(response_codes),
            uploaded_by=10
        )
        db.add(atm)
        
        # SWITCH Transaction (INBOUND + OUTBOUND)
        for direction in directions:
            switch = SwitchTransaction(
                datetime=dt,
                direction=direction,
                rrn=rrn,
                terminalid=terminal,
                amountminor=amount,
                currency=currency,
                responsecode=random.choice(response_codes),
                uploaded_by=10
            )
            db.add(switch)
        
        # FLEXCUBE Transaction  
        flexcube = FlexcubeTransaction(
            posted_datetime=dt,
            rrn=int(rrn),
            dr=amount,
            currency=currency,
            status="Posted",
            uploaded_by=10
        )
        db.add(flexcube)
    
    print(f"  ⚠️  Generating {PARTIALLY_MATCHED} partially matched records...")
    # 2. Generate PARTIALLY MATCHED records (ATM + SWITCH only, missing FLEXCUBE)
    for i in range(PARTIALLY_MATCHED):
        rrn = generate_rrn(FULLY_MATCHED + i)
        dt = random_datetime()
        terminal = random_terminal()
        amount = random_amount()
        currency = random.choice(currencies)
        
        partially_matched_rrns.append(rrn)
        
        # ATM Transaction
        atm = ATMTransaction(
            datetime=dt,
            terminalid=terminal,
            rrn=rrn,
            amount=amount,
            currency=currency,
            transactiontype=random.choice(transaction_types),
            responsecode=random.choice(response_codes),
            uploaded_by=10
        )
        db.add(atm)
        
        # SWITCH Transaction (INBOUND + OUTBOUND)
        for direction in directions:
            switch = SwitchTransaction(
                datetime=dt,
                direction=direction,
                rrn=rrn,
                terminalid=terminal,
                amountminor=amount,
                currency=currency,
                responsecode=random.choice(response_codes),
                uploaded_by=10
            )
            db.add(switch)
        
        # NO FLEXCUBE TRANSACTION (that's why it's partial!)
    
    print(f"  ❌ Generating {UNMATCHED} unmatched records...")
    # 3. Generate UNMATCHED records (ATM only, no SWITCH or FLEXCUBE)
    for i in range(UNMATCHED):
        rrn = generate_rrn(FULLY_MATCHED + PARTIALLY_MATCHED + i)
        dt = random_datetime()
        terminal = random_terminal()
        amount = random_amount()
        currency = random.choice(currencies)
        
        unmatched_rrns.append(rrn)
        
        # ATM Transaction only
        atm = ATMTransaction(
            datetime=dt,
            terminalid=terminal,
            rrn=rrn,
            amount=amount,
            currency=currency,
            transactiontype=random.choice(transaction_types),
            responsecode=random.choice(response_codes),
            uploaded_by=10
        )
        db.add(atm)
        
        # NO SWITCH OR FLEXCUBE (that's why it's unmatched!)
    
    print("  💾 Committing to database...")
    db.commit()
    
    print("\n✅ Test data generated successfully!")
    print(f"  Total ATM records: {TOTAL_RRNS}")
    print(f"  Total SWITCH records: {(FULLY_MATCHED + PARTIALLY_MATCHED) * 2}")
    print(f"  Total FLEXCUBE records: {FULLY_MATCHED}")
    
    # Verify counts
    print("\n🔍 Verifying data in database...")
    atm_count = db.query(ATMTransaction).count()
    switch_count = db.query(SwitchTransaction).count()
    flexcube_count = db.query(FlexcubeTransaction).count()
    
    print(f"  ATM: {atm_count} records")
    print(f"  SWITCH: {switch_count} records")
    print(f"  FLEXCUBE: {flexcube_count} records")
    
    # Now test storing in ReconMatchingSummary
    print("\n📝 Testing ReconMatchingSummary storage...")
    
    # Prepare data for TEXT columns
    matched_text = ",".join(fully_matched_rrns)
    partially_matched_text = ",".join(partially_matched_rrns)
    unmatched_text = ",".join(unmatched_rrns)
    
    # Calculate sizes
    matched_size = len(matched_text.encode('utf-8'))
    partial_size = len(partially_matched_text.encode('utf-8'))
    unmatched_size = len(unmatched_text.encode('utf-8'))
    total_size = matched_size + partial_size + unmatched_size
    
    print(f"\n📊 Storage size analysis:")
    print(f"  Matched ({FULLY_MATCHED} RRNs): {matched_size:,} bytes ({matched_size/1024:.2f} KB)")
    print(f"  Partially Matched ({PARTIALLY_MATCHED} RRNs): {partial_size:,} bytes ({partial_size/1024:.2f} KB)")
    print(f"  Unmatched ({UNMATCHED} RRNs): {unmatched_size:,} bytes ({unmatched_size/1024:.2f} KB)")
    print(f"  TOTAL: {total_size:,} bytes ({total_size/1024:.2f} KB)")
    print(f"  PostgreSQL TEXT limit: 1,073,741,824 bytes (1 GB)")
    print(f"  Usage: {total_size/1073741824*100:.4f}%")
    
    # Create ReconMatchingSummary record
    print("\n💾 Saving to ReconMatchingSummary...")
    
    import time
    start_time = time.time()
    
    summary = ReconMatchingSummary(
        recon_reference_number=f"TEST_{datetime.now().strftime('%Y%m%d%H%M%S')}",
        matched=matched_text,
        un_matched=unmatched_text,
        partially_matched=partially_matched_text,
        added_by=10
    )
    
    db.add(summary)
    db.commit()
    db.refresh(summary)
    
    save_time = time.time() - start_time
    
    print(f"  ✅ Saved successfully in {save_time:.3f} seconds")
    print(f"  Summary ID: {summary.id}")
    print(f"  Reference: {summary.recon_reference_number}")
    
    # Test retrieval
    print("\n🔍 Testing retrieval...")
    
    start_time = time.time()
    retrieved = db.query(ReconMatchingSummary).filter_by(id=summary.id).first()
    retrieval_time = time.time() - start_time
    
    print(f"  ✅ Retrieved in {retrieval_time:.3f} seconds")
    
    # Parse and count
    start_time = time.time()
    matched_list = retrieved.matched.split(',') if retrieved.matched else []
    partial_list = retrieved.partially_matched.split(',') if retrieved.partially_matched else []
    unmatched_list = retrieved.un_matched.split(',') if retrieved.un_matched else []
    parse_time = time.time() - start_time
    
    print(f"  ✅ Parsed in {parse_time:.3f} seconds")
    print(f"  Matched count: {len(matched_list)}")
    print(f"  Partially matched count: {len(partial_list)}")
    print(f"  Unmatched count: {len(unmatched_list)}")
    
    # Test search for specific RRN
    print("\n🔎 Testing search for specific RRN...")
    test_rrn = fully_matched_rrns[random.randint(0, len(fully_matched_rrns)-1)]
    
    start_time = time.time()
    found = test_rrn in retrieved.matched
    search_time = time.time() - start_time
    
    print(f"  Searching for RRN: {test_rrn}")
    print(f"  Found: {found}")
    print(f"  Search time: {search_time:.6f} seconds")
    
    # Performance summary
    print("\n⏱️  PERFORMANCE SUMMARY:")
    print("=" * 80)
    print(f"  Save time: {save_time:.3f} seconds")
    print(f"  Retrieval time: {retrieval_time:.3f} seconds")
    print(f"  Parse time: {parse_time:.3f} seconds")
    print(f"  Search time: {search_time:.6f} seconds")
    print(f"  Total time: {save_time + retrieval_time + parse_time:.3f} seconds")
    
    # Final verdict
    print("\n" + "=" * 80)
    print("🎯 VERDICT:")
    print("=" * 80)
    
    if total_size < 1_000_000:  # < 1 MB
        print("  ✅ EXCELLENT - Storage size is very small")
    elif total_size < 10_000_000:  # < 10 MB
        print("  ✅ GOOD - Storage size is manageable")
    else:
        print("  ⚠️  WARNING - Storage size is large")
    
    if save_time < 1:
        print("  ✅ FAST - Save time is quick")
    else:
        print("  ⚠️  SLOW - Save time is noticeable")
    
    if retrieval_time < 1:
        print("  ✅ FAST - Retrieval time is quick")
    else:
        print("  ⚠️  SLOW - Retrieval time is noticeable")
    
    if search_time < 0.01:
        print("  ✅ INSTANT - Search is very fast")
    elif search_time < 0.1:
        print("  ✅ FAST - Search is acceptable")
    else:
        print("  ⚠️  SLOW - Search is noticeable")
    
    print("\n💡 RECOMMENDATIONS:")
    if total_size > 1_000_000 or search_time > 0.1:
        print("  ⚠️  Consider using a separate details table for better performance")
        print("  ⚠️  TEXT columns work but will degrade with more data")
    else:
        print("  ✅ Current TEXT column approach works fine for this data size")
        print("  ✅ Can handle 1000 records without issues")
    
    print("\n" + "=" * 80)
    print("✅ TEST COMPLETED SUCCESSFULLY!")
    print("=" * 80)
    
except Exception as e:
    print(f"\n❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    db.rollback()
finally:
    db.close()
