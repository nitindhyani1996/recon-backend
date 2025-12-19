"""
Test script to verify bulk upload optimization.
"""

import asyncio
import time
from app.db.database import SessionLocal
from app.services.optimized_bulk_upload import OptimizedBulkUploadService
from datetime import datetime

async def test_bulk_upload():
    """Test bulk upload with 1000 mock records."""
    
    db = SessionLocal()
    
    try:
        # Generate 1000 test records
        print("Generating 1000 test records...")
        test_records = []
        for i in range(1000):
            test_records.append({
                "datetime": datetime.now(),
                "terminalid": f"TERM{i:06d}",
                "location": f"Location {i}",
                "atmindex": f"ATM{i:04d}",
                "pan_masked": f"****1234",
                "account_masked": f"****5678",
                "transactiontype": "WITHDRAWAL",
                "amount": 100.00 + i,
                "currency": "USD",
                "stan": f"{i:06d}",
                "rrn": f"RRN{i:012d}",
                "auth": f"AUTH{i:06d}",
                "responsecode": "00",
                "responsedesc": "Approved"
            })
        
        print(f"✅ Generated {len(test_records)} records")
        print()
        
        # Test optimized upload
        print("Testing OPTIMIZED bulk upload...")
        start_time = time.time()
        
        result = await OptimizedBulkUploadService.saveATMFileData(
            db=db,
            mapped_df=test_records,
            uploaded_file_id=999  # Test file ID
        )
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        print()
        print("=" * 60)
        print("RESULTS:")
        print("=" * 60)
        print(f"Status: {result['status']}")
        print(f"Records Saved: {result['recordsSaved']}")
        print(f"Duplicates Skipped: {result.get('duplicateRecords', 0)}")
        print(f"Total Processed: {result.get('totalProcessed', 0)}")
        print(f"Time Taken: {elapsed:.2f} seconds")
        print(f"Records/Second: {result['recordsSaved']/elapsed:.2f}")
        print("=" * 60)
        
        if elapsed < 15:
            print("✅ Performance: EXCELLENT (< 15 seconds)")
        elif elapsed < 30:
            print("✅ Performance: GOOD (< 30 seconds)")
        else:
            print("⚠️  Performance: SLOW (> 30 seconds)")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        db.close()

if __name__ == "__main__":
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║         BULK UPLOAD PERFORMANCE TEST                        ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print()
    
    asyncio.run(test_bulk_upload())
