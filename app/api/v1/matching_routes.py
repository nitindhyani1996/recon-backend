"""
API Routes for Matching Display - Shows Matched, Unmatched, Partially Matched

Endpoints:
- GET /api/v1/matching/summary - Overall matching statistics
- GET /api/v1/matching/fully-matched - List fully matched transactions
- GET /api/v1/matching/partially-matched - List partially matched transactions
- GET /api/v1/matching/unmatched - List unmatched transactions
- GET /api/v1/matching/rrn/{rrn} - Get details for specific RRN
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from app.db.database import get_db
from app.services.matching_display_service import MatchingDisplayService
from app.utils.json_encoder import clean_nan_values
from typing import Optional

router = APIRouter(prefix="/api/v1/matching", tags=["Matching Display"])


@router.get("/summary")
async def get_matching_summary(db: Session = Depends(get_db)):
    """
    Get overall matching summary with counts and percentages.
    
    Returns:
        - total_rrns: Total unique RRNs across all sources
        - fully_matched: Count of RRNs in all 3 sources
        - partially_matched: Count of RRNs in 2 sources  
        - unmatched: Count of RRNs in only 1 source
        - match_percentage: Percentage of fully matched RRNs
    """
    result = MatchingDisplayService.get_matching_summary(db)
    return clean_nan_values(result)


@router.get("/fully-matched")
async def get_fully_matched_transactions(
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(50, ge=1, le=500, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by RRN"),
    db: Session = Depends(get_db)
):
    """
    Get list of fully matched transactions (present in all 3 sources: ATM, SWITCH, FLEXCUBE).
    
    Each record shows:
    - RRN
    - ATM transaction details
    - SWITCH transaction details
    - FLEXCUBE transaction details
    
    Supports pagination and search.
    """
    result = MatchingDisplayService.get_fully_matched_data(
        db=db,
        offset=offset,
        limit=limit,
        search_rrn=search
    )
    return clean_nan_values(result)


@router.get("/partially-matched")
async def get_partially_matched_transactions(
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(50, ge=1, le=500, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by RRN"),
    db: Session = Depends(get_db)
):
    """
    Get list of partially matched transactions (present in 2 out of 3 sources).
    
    Each record shows:
    - RRN
    - Which sources matched (e.g., "ATM, SWITCH")
    - Which source is missing (e.g., "FLEXCUBE")
    - Available transaction details
    
    Useful for identifying reconciliation gaps.
    """
    result = MatchingDisplayService.get_partially_matched_data(
        db=db,
        offset=offset,
        limit=limit,
        search_rrn=search
    )
    return clean_nan_values(result)


@router.get("/unmatched")
async def get_unmatched_transactions(
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(50, ge=1, le=500, description="Items per page"),
    source: Optional[str] = Query(None, description="Filter by source: ATM, SWITCH, or FLEXCUBE"),
    search: Optional[str] = Query(None, description="Search by RRN"),
    db: Session = Depends(get_db)
):
    """
    Get list of unmatched transactions (present in only 1 source, no match found).
    
    Each record shows:
    - RRN
    - Source (ATM, SWITCH, or FLEXCUBE)
    - Transaction details from that source
    
    Can filter by specific source to see:
    - ATM transactions with no SWITCH/FLEXCUBE match
    - SWITCH transactions with no ATM/FLEXCUBE match
    - FLEXCUBE transactions with no ATM/SWITCH match
    
    Useful for identifying missing data or reconciliation issues.
    """
    result = MatchingDisplayService.get_unmatched_data(
        db=db,
        offset=offset,
        limit=limit,
        source_filter=source,
        search_rrn=search
    )
    return clean_nan_values(result)


@router.get("/rrn/{rrn}")
async def get_matching_details_by_rrn(
    rrn: str,
    db: Session = Depends(get_db)
):
    """
    Get detailed matching information for a specific RRN.
    
    Shows:
    - Match status (FULLY_MATCHED, PARTIALLY_MATCHED, UNMATCHED)
    - Which sources have data for this RRN
    - Complete details from each source
    
    Useful for drilling down into specific transaction details.
    """
    result = MatchingDisplayService.get_matching_details_by_rrn(db, rrn)
    return clean_nan_values(result)


@router.get("/export/fully-matched")
async def export_fully_matched(
    db: Session = Depends(get_db)
):
    """
    Export all fully matched transactions (no pagination).
    
    WARNING: Can return large dataset. Use with caution.
    Consider adding limit or implementing streaming export.
    """
    result = MatchingDisplayService.get_fully_matched_data(
        db=db,
        offset=0,
        limit=10000  # Large limit for export
    )
    return clean_nan_values(result)


@router.get("/stats/by-source")
async def get_stats_by_source(db: Session = Depends(get_db)):
    """
    Get statistics showing unmatched counts per source.
    
    Returns:
        - atm_only: Transactions only in ATM
        - switch_only: Transactions only in SWITCH
        - flexcube_only: Transactions only in FLEXCUBE
    """
    from sqlalchemy import text
    
    query = text("""
        WITH combined_rrn AS (
            SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'ATM' AS source
            FROM atm_transactions WHERE rrn IS NOT NULL
            UNION ALL
            SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'SWITCH' AS source
            FROM switch_transactions WHERE rrn IS NOT NULL
            UNION ALL
            SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'FLEXCUBE' AS source
            FROM flexcube_transactions WHERE rrn IS NOT NULL
        ),
        rrn_counts AS (
            SELECT rrn, source, COUNT(*) as count
            FROM combined_rrn
            GROUP BY rrn, source
        ),
        single_source AS (
            SELECT rrn, source
            FROM rrn_counts
            WHERE rrn IN (
                SELECT rrn
                FROM combined_rrn
                GROUP BY rrn
                HAVING COUNT(DISTINCT source) = 1
            )
        )
        SELECT 
            source,
            COUNT(*) as count
        FROM single_source
        GROUP BY source
    """)
    
    results = db.execute(query).fetchall()
    
    stats = {
        "atm_only": 0,
        "switch_only": 0,
        "flexcube_only": 0
    }
    
    for row in results:
        if row.source == 'ATM':
            stats['atm_only'] = row.count
        elif row.source == 'SWITCH':
            stats['switch_only'] = row.count
        elif row.source == 'FLEXCUBE':
            stats['flexcube_only'] = row.count
    
    return {
        "status": "success",
        "unmatched_by_source": stats,
        "total_unmatched": sum(stats.values())
    }
