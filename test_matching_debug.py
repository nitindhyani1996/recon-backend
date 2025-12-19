import sys
sys.path.append('/Users/mac/Documents/projects/render-fastapi-recon')

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.services.MatchingRuleService import MatchingRuleService

# Create database session
DATABASE_URL = "postgresql://postgres:root@localhost:5432/recon_db_render"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
db = SessionLocal()

# Get transactions
print("Fetching transactions...")
atm_transactions = MatchingRuleService.getAllAtmTransactions(db)
switch_transactions = MatchingRuleService.getAllSwitchTransactions(db)
flexcube_transactions = MatchingRuleService.getAllFlexcubeTransactions(db)

print(f"ATM: {len(atm_transactions)} records")
print(f"SWITCH: {len(switch_transactions)} records")
print(f"FLEXCUBE: {len(flexcube_transactions)} records")

# Check RRN0000000
atm_0 = [t for t in atm_transactions if t.get('rrn') == 'RRN0000000']
switch_0 = [t for t in switch_transactions if t.get('rrn') == 'RRN0000000']
flex_0 = [t for t in flexcube_transactions if t.get('rrn') == 'RRN0000000']

print(f"\n=== RRN0000000 Details ===")
print(f"ATM records: {len(atm_0)}")
if atm_0:
    print(f"  ATM RRN field: '{atm_0[0].get('rrn')}'")
    
print(f"SWITCH records: {len(switch_0)}")
if switch_0:
    print(f"  SWITCH RRN field: '{switch_0[0].get('rrn')}'")
    
print(f"FLEXCUBE records: {len(flex_0)}")
if flex_0:
    print(f"  FLEXCUBE RRN field: '{flex_0[0].get('rrn')}'")
    print(f"  FLEXCUBE dr: {flex_0[0].get('dr')}")
    print(f"  FLEXCUBE cr: {flex_0[0].get('cr')}")
else:
    print("  NO FLEXCUBE RECORD FOUND!")
    # Show first few FLEXCUBE RRNs
    print(f"\n  First 5 FLEXCUBE RRNs:")
    for i, t in enumerate(flexcube_transactions[:5]):
        print(f"    {i}: '{t.get('rrn')}'")

# Get matching rule
matching_rule = MatchingRuleService.getMatchingRuleJson(db, userId=10, category=1)
print(f"\nMatching rule: {matching_rule}")

# Now run the matching
print("\n=== Running Matching Logic ===")
import asyncio
result = asyncio.run(MatchingRuleService.match_three_way_async(
    atm_transactions, switch_transactions, flexcube_transactions, matching_rule[0]
))
print(f"Matched: {len(result['matched'])}")
print(f"Partially Matched: {len(result['partially_matched'])}")
print(f"Unmatched: {len(result['unmatched'])}")

if result['matched']:
    print(f"\nFirst matched RRN: {result['matched'][0]['ATM'].get('rrn')}")
elif result['partially_matched']:
    print(f"\nFirst partial RRN: {result['partially_matched'][0]['ATM'].get('rrn')}")

db.close()
