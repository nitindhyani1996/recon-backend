"""
Generate test CSV files with CORRECT column names for the system
90% matched, 8% partial, 2% unmatched
"""

import csv
from datetime import datetime, timedelta
import random

def generate_atm_csv(filename="atm_100_correct.csv", count=100):
    """Generate ATM CSV with ATMIndex column (required for detection)"""
    print(f"📝 Generating {filename} with {count} transactions...")
    
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        # ATMIndex is REQUIRED for ATM file detection
        writer.writerow(['ATMIndex', 'RRN', 'Amount', 'TransactionType', 'TerminalID', 'TransactionDateTime'])
        
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
                i,  # ATMIndex - REQUIRED!
                rrn,
                f"{amount:.2f}",
                txn_type,
                terminal,
                txn_time.strftime('%Y-%m-%d %H:%M:%S')
            ])
    
    print(f"  ✅ Created {filename} with {count} transactions (with ATMIndex column)")

def generate_switch_csv(filename="switch_98_correct.csv", count=98):
    """Generate SWITCH CSV with MTI column (required for detection)"""
    print(f"📝 Generating {filename} with {count} unique RRNs (x2 for INBOUND/OUTBOUND)...")
    
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        # MTI is REQUIRED for SWITCH file detection
        writer.writerow(['MTI', 'RRN', 'Amount', 'Direction', 'TerminalID', 'TransactionDateTime'])
        
        base_time = datetime(2025, 12, 18, 10, 0, 0)
        terminals = ['ATM001', 'ATM002', 'ATM003', 'ATM004', 'ATM005', 'ATM006']
        
        row_count = 0
        for i in range(1, count + 1):
            rrn = f"TEST{i:03d}"
            amount = round(random.uniform(50, 500), 2)
            terminal = random.choice(terminals)
            txn_time = base_time + timedelta(minutes=i)
            
            # INBOUND transaction
            writer.writerow([
                '0200',  # MTI - REQUIRED! (Message Type Indicator)
                rrn,
                f"{amount:.2f}",
                'INBOUND',
                terminal,
                txn_time.strftime('%Y-%m-%d %H:%M:%S')
            ])
            row_count += 1
            
            # OUTBOUND transaction (same RRN)
            writer.writerow([
                '0210',  # MTI - REQUIRED!
                rrn,
                f"{amount:.2f}",
                'OUTBOUND',
                terminal,
                (txn_time + timedelta(seconds=5)).strftime('%Y-%m-%d %H:%M:%S')
            ])
            row_count += 1
    
    print(f"  ✅ Created {filename} with {count} unique RRNs ({row_count} total rows with MTI column)")

def generate_flexcube_csv(filename="flexcube_90_correct.csv", count=90):
    """Generate FLEXCUBE CSV with FCTxnID column (required for detection)"""
    print(f"📝 Generating {filename} with {count} transactions...")
    
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        # FCTxnID is REQUIRED for FLEXCUBE file detection
        writer.writerow(['FCTxnID', 'RRN', 'DR', 'CR', 'TransactionDateTime'])
        
        base_time = datetime(2025, 12, 18, 10, 0, 0)
        
        for i in range(1, count + 1):
            rrn = f"TEST{i:03d}"
            amount = round(random.uniform(50, 500), 2)
            txn_time = base_time + timedelta(minutes=i)
            
            # Randomly choose DR or CR
            is_debit = random.choice([True, False])
            dr_amount = f"{amount:.2f}" if is_debit else "0"
            cr_amount = "0" if is_debit else f"{amount:.2f}"
            
            writer.writerow([
                f"FC{i:08d}",  # FCTxnID - REQUIRED! (e.g., FC00000001)
                rrn,
                dr_amount,
                cr_amount,
                txn_time.strftime('%Y-%m-%d %H:%M:%S')
            ])
    
    print(f"  ✅ Created {filename} with {count} transactions (with FCTxnID column)")

if __name__ == "__main__":
    print("=" * 70)
    print("🚀 GENERATING TEST CSV FILES WITH CORRECT COLUMN NAMES")
    print("=" * 70)
    print()
    print("📋 Required columns for file detection:")
    print("  • ATM File:      Must have 'ATMIndex' column")
    print("  • SWITCH File:   Must have 'MTI' column")
    print("  • FLEXCUBE File: Must have 'FCTxnID' column")
    print()
    print("-" * 70)
    
    # Generate all 3 files
    generate_atm_csv("atm_100_correct.csv", 100)
    generate_switch_csv("switch_98_correct.csv", 98)  # Missing TEST099, TEST100
    generate_flexcube_csv("flexcube_90_correct.csv", 90)  # Missing TEST091-TEST100
    
    print()
    print("=" * 70)
    print("✅ ALL FILES GENERATED SUCCESSFULLY!")
    print("=" * 70)
    print()
    print("📊 Expected Results After Upload:")
    print("  • 90 MATCHED:     TEST001-TEST090 (in all 3 tables)")
    print("  • 8 PARTIAL:      TEST091-TEST098 (ATM + SWITCH only, missing FLEXCUBE)")
    print("  • 2 UNMATCHED:    TEST099-TEST100 (ATM only, missing SWITCH + FLEXCUBE)")
    print("  • TOTAL:          100 transactions")
    print()
    print("🎯 Now upload these files in Postman:")
    print("  1. atm_100_correct.csv")
    print("  2. switch_98_correct.csv")
    print("  3. flexcube_90_correct.csv")
    print()
