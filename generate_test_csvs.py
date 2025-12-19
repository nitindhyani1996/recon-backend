"""
Generate test CSV files for mixed matching results: 90% matched, 8% partial, 2% unmatched
"""

import csv
from datetime import datetime, timedelta
import random

def generate_atm_csv(filename="atm_100_test.csv", count=100):
    """Generate ATM CSV with all 100 RRNs"""
    print(f"📝 Generating {filename} with {count} transactions...")
    
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['RRN', 'Amount', 'TransactionType', 'TerminalID', 'TransactionDateTime'])
        
        base_time = datetime(2025, 12, 18, 10, 0, 0)
        transaction_types = ['Withdrawal', 'Deposit', 'Balance Inquiry']
        terminals = ['ATM001', 'ATM002', 'ATM003', 'ATM004', 'ATM005', 'ATM006']
        
        for i in range(1, count + 1):
            rrn = f"TEST{i:03d}"
            amount = round(random.uniform(50, 500), 2)
            txn_type = random.choice(transaction_types)
            terminal = random.choice(terminals)
            txn_time = base_time + timedelta(minutes=i)
            
            writer.writerow([
                rrn,
                f"{amount:.2f}",
                txn_type,
                terminal,
                txn_time.strftime('%Y-%m-%d %H:%M:%S')
            ])
    
    print(f"  ✅ Created {filename} with {count} transactions")

def generate_switch_csv(filename="switch_98_test.csv", count=98):
    """Generate SWITCH CSV with 98 RRNs (missing last 2 for unmatched)"""
    print(f"📝 Generating {filename} with {count} unique RRNs (x2 for INBOUND/OUTBOUND)...")
    
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['RRN', 'Amount', 'Direction', 'TerminalID', 'TransactionDateTime'])
        
        base_time = datetime(2025, 12, 18, 10, 0, 0)
        terminals = ['ATM001', 'ATM002', 'ATM003', 'ATM004', 'ATM005', 'ATM006']
        
        for i in range(1, count + 1):
            rrn = f"TEST{i:03d}"
            amount = round(random.uniform(50, 500), 2)
            terminal = random.choice(terminals)
            txn_time = base_time + timedelta(minutes=i)
            
            # Add INBOUND
            writer.writerow([
                rrn,
                f"{amount:.2f}",
                'INBOUND',
                terminal,
                txn_time.strftime('%Y-%m-%d %H:%M:%S')
            ])
            
            # Add OUTBOUND
            writer.writerow([
                rrn,
                f"{amount:.2f}",
                'OUTBOUND',
                terminal,
                txn_time.strftime('%Y-%m-%d %H:%M:%S')
            ])
    
    print(f"  ✅ Created {filename} with {count} unique RRNs ({count * 2} total rows)")
    print(f"  ⚠️  Missing TEST{count+1:03d} and TEST{count+2:03d} (creates 2 UNMATCHED)")

def generate_flexcube_csv(filename="flexcube_90_test.csv", count=90):
    """Generate FLEXCUBE CSV with 90 RRNs (missing 10 for partial matches)"""
    print(f"📝 Generating {filename} with {count} transactions...")
    
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['RRN', 'DR', 'CR', 'TransactionDateTime'])
        
        base_time = datetime(2025, 12, 18, 10, 0, 0)
        
        for i in range(1, count + 1):
            rrn = f"TEST{i:03d}"
            amount = round(random.uniform(50, 500), 2)
            txn_time = base_time + timedelta(minutes=i)
            
            writer.writerow([
                rrn,
                f"{amount:.2f}",
                "0",
                txn_time.strftime('%Y-%m-%d %H:%M:%S')
            ])
    
    print(f"  ✅ Created {filename} with {count} transactions")
    print(f"  ⚠️  Missing TEST{count+1:03d} to TEST098 (creates 8 PARTIAL MATCHED)")

def main():
    print("╔════════════════════════════════════════════════════════════════════╗")
    print("║  🎯 Generating Test CSV Files for Mixed Matching (90% / 8% / 2%)  ║")
    print("╚════════════════════════════════════════════════════════════════════╝")
    print()
    
    # Generate all 3 CSV files
    generate_atm_csv("atm_100_test.csv", 100)
    print()
    generate_switch_csv("switch_98_test.csv", 98)
    print()
    generate_flexcube_csv("flexcube_90_test.csv", 90)
    
    print()
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print()
    print("📊 Expected Matching Results:")
    print("  ✅ MATCHED (90):    TEST001 - TEST090 (in all 3 tables)")
    print("  ⚠️  PARTIAL (8):     TEST091 - TEST098 (ATM + SWITCH only)")
    print("  ❌ UNMATCHED (2):   TEST099 - TEST100 (ATM only)")
    print()
    print("🚀 Next Steps:")
    print("  1. Upload atm_100_test.csv via bulk upload")
    print("  2. Upload switch_98_test.csv via bulk upload")
    print("  3. Upload flexcube_90_test.csv via bulk upload")
    print("  4. Run matching engine")
    print("  5. View results - you'll see 90% / 8% / 2% split!")
    print()
    print("✅ CSV files created successfully!")

if __name__ == "__main__":
    main()
