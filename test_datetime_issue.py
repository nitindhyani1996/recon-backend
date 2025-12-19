#!/usr/bin/env python3
"""Test to check datetime value in ATM transaction"""

import sys
sys.path.append('/Users/mac/Documents/projects/render-fastapi-recon')

from app.db.database import SessionLocal
from app.services.MatchingRuleService import MatchingRuleService

db = SessionLocal()

try:
    # Get ATM transactions
    atm_txns = MatchingRuleService.getAllAtmTransactions(db)
    atm_0 = [t for t in atm_txns if t['rrn'] == 'RRN0000000'][0]
    
    print("=== ATM Transaction RRN0000000 ===")
    print(f"datetime value: {atm_0.get('datetime')}")
    print(f"datetime type: {type(atm_0.get('datetime'))}")
    print(f"str(datetime): '{str(atm_0.get('datetime'))}'")
    print(f"bool(datetime): {bool(atm_0.get('datetime'))}")
    
    # Test the exact line from the code
    date_value = str(atm_0.get("datetime") or atm_0.get("transaction_datetime") or "")
    print(f"\nFormatted date (from line 298): '{date_value}'")
    
finally:
    db.close()
