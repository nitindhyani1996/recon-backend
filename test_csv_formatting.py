#!/usr/bin/env python3
"""Test to check what's happening during CSV formatting"""

import sys
sys.path.append('/Users/mac/Documents/projects/render-fastapi-recon')

from app.db.database import SessionLocal
from app.services.MatchingRuleService import MatchingRuleService
from datetime import datetime

db = SessionLocal()

try:
    # Simulate what happens in saveReconMatchingSummary
    atm_txns = MatchingRuleService.getAllAtmTransactions(db)
    atm_0 = [t for t in atm_txns if t['rrn'] == 'RRN0000000'][0]
    
    print("=== Original ATM Dict ===")
    print(f"datetime: {atm_0.get('datetime')}")
    print(f"type: {type(atm_0.get('datetime'))}")
    
    # Simulate the partially_matched dict structure
    match_dict = {
        "ATM": atm_0,
        "Switch": None,
        "Flexcube": None
    }
    
    print("\n=== Match Dict ===")
    print(f"match_dict['ATM']['datetime']: {match_dict['ATM'].get('datetime')}")
    
    # Simulate what happens in saveReconMatchingSummary (lines 303-309)
    partially_transactions = []
    atm = match_dict.get("ATM", {})
    
    print("\n=== Building CSV Transaction Dict ===")
    print(f"atm.get('datetime'): {atm.get('datetime')}")
    print(f"type: {type(atm.get('datetime'))}")
    
    # The exact line from the code
    date_str = str(atm.get("datetime") or atm.get("transaction_datetime") or "")
    print(f"date_str result: '{date_str}'")
    
    partially_transactions.append({
        "rrn": atm.get("rrn") or "",
        "txn_type": atm.get("transactiontype") or atm.get("transaction_type") or "",
        "terminal_id": atm.get("terminalid") or atm.get("terminal_id") or "",
        "date": date_str,
        "amount": str(float(atm.get("amount"))),
        "result": "PARTIAL"
    })
    
    print("\n=== CSV Transaction Dict ===")
    print(partially_transactions[0])
    
    # Now test the CSV formatter
    from app.utils.recon_data_formatter import ReconDataFormatter
    csv_output = ReconDataFormatter.format_matched_data_csv(partially_transactions)
    
    print("\n=== CSV Output ===")
    print(csv_output)
    
finally:
    db.close()
