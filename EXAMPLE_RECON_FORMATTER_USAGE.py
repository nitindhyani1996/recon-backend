"""
Example: How to use ReconDataFormatter with ReconMatchingSummary

This shows how to:
1. Format matching results for storage in TEXT columns
2. Retrieve and parse data for frontend
3. Update your matching service to use the formatter
"""

from sqlalchemy.orm import Session
from app.models.ReconMatchingSummary import ReconMatchingSummary
from app.models.atm_transaction import ATMTransaction
from app.models.SwitchTransaction import SwitchTransaction
from app.models.FlexcubeTransaction import FlexcubeTransaction
from app.utils.recon_data_formatter import ReconDataFormatter
from datetime import datetime
from typing import List, Dict, Any


class ReconMatchingServiceWithFormatter:
    """
    Enhanced matching service that formats data for ReconMatchingSummary
    """
    
    @staticmethod
    def save_matching_results(
        db: Session,
        recon_reference: str,
        fully_matched_rrns: List[str],
        partially_matched_rrns: List[str],
        unmatched_rrns: List[str],
        added_by: int = 10
    ) -> ReconMatchingSummary:
        """
        Save matching results with properly formatted data
        
        Args:
            db: Database session
            recon_reference: Unique reference number (e.g., "RECON20251218001")
            fully_matched_rrns: List of RRNs matched in all 3 sources
            partially_matched_rrns: List of RRNs matched in 2 sources
            unmatched_rrns: List of RRNs matched in 1 source only
            added_by: User ID who ran the reconciliation
            
        Returns:
            ReconMatchingSummary instance
        """
        
        # Fetch and format fully matched transactions
        matched_data = []
        if fully_matched_rrns:
            atm_txns = db.query(ATMTransaction).filter(
                ATMTransaction.rrn.in_(fully_matched_rrns)
            ).all()
            
            for atm in atm_txns:
                matched_data.append({
                    'rrn': atm.rrn,
                    'txn_type': atm.transactiontype or 'Unknown',
                    'terminal_id': atm.terminalid or '',
                    'date': atm.datetime,
                    'amount': float(atm.amount) if atm.amount else 0.0,
                    'result': 'MATCHED'
                })
        
        # Fetch and format partially matched transactions
        partial_data = []
        if partially_matched_rrns:
            atm_txns = db.query(ATMTransaction).filter(
                ATMTransaction.rrn.in_(partially_matched_rrns)
            ).all()
            
            for atm in atm_txns:
                partial_data.append({
                    'rrn': atm.rrn,
                    'txn_type': atm.transactiontype or 'Unknown',
                    'terminal_id': atm.terminalid or '',
                    'date': atm.datetime,
                    'amount': float(atm.amount) if atm.amount else 0.0,
                    'result': 'PARTIAL'
                })
        
        # Fetch and format unmatched transactions
        unmatched_data = []
        if unmatched_rrns:
            atm_txns = db.query(ATMTransaction).filter(
                ATMTransaction.rrn.in_(unmatched_rrns)
            ).all()
            
            for atm in atm_txns:
                unmatched_data.append({
                    'rrn': atm.rrn,
                    'txn_type': atm.transactiontype or 'Unknown',
                    'terminal_id': atm.terminalid or '',
                    'date': atm.datetime,
                    'amount': float(atm.amount) if atm.amount else 0.0,
                    'result': 'UNMATCHED'
                })
        
        # Format data using ReconDataFormatter
        matched_csv = ReconDataFormatter.format_matched_data_csv(matched_data)
        partial_csv = ReconDataFormatter.format_matched_data_csv(partial_data)
        unmatched_csv = ReconDataFormatter.format_matched_data_csv(unmatched_data)
        
        # Create summary record
        summary = ReconMatchingSummary(
            recon_reference_number=recon_reference,
            matched=matched_csv,
            partially_matched=partial_csv,
            un_matched=unmatched_csv,
            added_by=added_by
        )
        
        db.add(summary)
        db.commit()
        db.refresh(summary)
        
        return summary
    
    @staticmethod
    def get_matching_results_for_frontend(
        db: Session,
        recon_reference: str
    ) -> Dict[str, Any]:
        """
        Get matching results formatted for frontend display
        
        Args:
            db: Database session
            recon_reference: Reconciliation reference number
            
        Returns:
            {
                "recon_reference": "RECON20251218001",
                "matched": [
                    {
                        "RRN": "251218000001",
                        "Txn Type": "Withdrawal",
                        "Terminal Id": "IZY00055",
                        "Date": "2025-12-18 10:30:00",
                        "Amount": "10000.00",
                        "Result": "MATCHED"
                    },
                    ...
                ],
                "partially_matched": [...],
                "unmatched": [...],
                "summary": {
                    "total_matched": 600,
                    "total_partial": 300,
                    "total_unmatched": 100
                }
            }
        """
        
        # Fetch summary record
        summary = db.query(ReconMatchingSummary).filter(
            ReconMatchingSummary.recon_reference_number == recon_reference
        ).first()
        
        if not summary:
            return {
                "error": "Reconciliation not found",
                "recon_reference": recon_reference
            }
        
        # Parse and format data for frontend
        matched = ReconDataFormatter.get_frontend_format(summary.matched or "")
        partially_matched = ReconDataFormatter.get_frontend_format(summary.partially_matched or "")
        unmatched = ReconDataFormatter.get_frontend_format(summary.un_matched or "")
        
        return {
            "recon_reference": summary.recon_reference_number,
            "created_at": summary.created_at.isoformat() if summary.created_at else None,
            "matched": matched,
            "partially_matched": partially_matched,
            "unmatched": unmatched,
            "summary": {
                "total_matched": len(matched),
                "total_partial": len(partially_matched),
                "total_unmatched": len(unmatched),
                "total_transactions": len(matched) + len(partially_matched) + len(unmatched)
            }
        }


# Example API endpoint usage
"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.db.database import get_db

router = APIRouter(prefix="/api/v1/reconciliation", tags=["Reconciliation"])

@router.post("/run-matching")
async def run_matching_engine(db: Session = Depends(get_db)):
    '''
    Run matching engine and save results
    '''
    # ... your existing matching logic ...
    
    # Example results
    fully_matched_rrns = ["251218000001", "251218000002", ...]  # 600 RRNs
    partially_matched_rrns = ["251218000601", "251218000602", ...]  # 300 RRNs
    unmatched_rrns = ["251218000901", "251218000902", ...]  # 100 RRNs
    
    # Generate unique reference
    recon_ref = f"RECON{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Save with proper formatting
    summary = ReconMatchingServiceWithFormatter.save_matching_results(
        db=db,
        recon_reference=recon_ref,
        fully_matched_rrns=fully_matched_rrns,
        partially_matched_rrns=partially_matched_rrns,
        unmatched_rrns=unmatched_rrns,
        added_by=10
    )
    
    return {
        "success": True,
        "message": "Matching completed successfully",
        "recon_reference": summary.recon_reference_number,
        "summary": {
            "matched": len(fully_matched_rrns),
            "partial": len(partially_matched_rrns),
            "unmatched": len(unmatched_rrns)
        }
    }


@router.get("/results/{recon_reference}")
async def get_reconciliation_results(
    recon_reference: str,
    db: Session = Depends(get_db)
):
    '''
    Get reconciliation results formatted for frontend
    
    Returns data in format:
    {
        "matched": [
            {
                "RRN": "251218000001",
                "Txn Type": "Withdrawal",
                "Terminal Id": "IZY00055",
                "Date": "2025-12-18 10:30:00",
                "Amount": "10000.00",
                "Result": "MATCHED"
            },
            ...
        ],
        "partially_matched": [...],
        "unmatched": [...]
    }
    '''
    results = ReconMatchingServiceWithFormatter.get_matching_results_for_frontend(
        db=db,
        recon_reference=recon_reference
    )
    
    return results
"""


if __name__ == "__main__":
    print("=" * 80)
    print("ReconMatching Service Example with Frontend Format")
    print("=" * 80)
    
    print("\n✅ Data Format for Frontend:")
    print("-" * 80)
    
    example_data = {
        "RRN": "251218000001",
        "Txn Type": "Withdrawal",
        "Terminal Id": "IZY00055",
        "Date": "2025-12-18 10:30:00",
        "Amount": "10000.00",
        "Result": "MATCHED"
    }
    
    print("Each transaction has these fields:")
    for key, value in example_data.items():
        print(f"  • {key}: {value}")
    
    print("\n✅ Storage Format in Database:")
    print("-" * 80)
    print("TEXT column stores as CSV:")
    print("RRN|Txn Type|Terminal Id|Date|Amount|Result;...")
    print("\nExample:")
    print("251218000001|Withdrawal|IZY00055|2025-12-18 10:30:00|10000.00|MATCHED")
    
    print("\n✅ Size Estimate:")
    print("-" * 80)
    print("Per transaction: ~70 bytes")
    print("1000 transactions: ~70 KB")
    print("10000 transactions: ~700 KB")
    
    print("\n" + "=" * 80)
