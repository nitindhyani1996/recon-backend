"""
Enhanced Matching Display Service - Shows Matched, Unmatched, and Partially Matched Data

This service provides detailed views of:
1. Fully Matched (all 3 sources: ATM + SWITCH + FLEXCUBE)
2. Partially Matched (2 out of 3 sources)
3. Unmatched (only 1 source, no match found)

Supports pagination, filtering, and export functionality.
"""

from sqlalchemy.orm import Session
from sqlalchemy import text, func
from typing import Dict, List, Any, Optional
import json
from datetime import datetime


class MatchingDisplayService:
    """Service to retrieve and display matching results."""
    
    @staticmethod
    def get_matching_summary(db: Session) -> Dict[str, Any]:
        """
        Get overall matching summary with counts.
        
        Returns:
            {
                "total_rrns": 1000,
                "fully_matched": 850,
                "partially_matched": 100,
                "unmatched": 50,
                "match_percentage": 85.0
            }
        """
        query = text("""
            WITH combined_rrn AS (
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'ATM' AS source
                FROM atm_transactions
                WHERE rrn IS NOT NULL AND rrn != ''

                UNION ALL
                
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'SWITCH' AS source
                FROM switch_transactions
                WHERE rrn IS NOT NULL AND rrn != ''

                UNION ALL
                
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'FLEXCUBE' AS source
                FROM flexcube_transactions
                WHERE rrn IS NOT NULL AND rrn != ''
            ),
            rrn_summary AS (
                SELECT 
                    rrn,
                    COUNT(DISTINCT source) AS match_count,
                    STRING_AGG(DISTINCT source, ', ' ORDER BY source) AS matched_sources
                FROM combined_rrn
                GROUP BY rrn
            )
            SELECT 
                COUNT(*) AS total_rrns,
                SUM(CASE WHEN match_count = 3 THEN 1 ELSE 0 END) AS fully_matched,
                SUM(CASE WHEN match_count = 2 THEN 1 ELSE 0 END) AS partially_matched,
                SUM(CASE WHEN match_count = 1 THEN 1 ELSE 0 END) AS unmatched
            FROM rrn_summary
        """)
        
        result = db.execute(query).fetchone()
        
        total = result.total_rrns or 0
        fully_matched = result.fully_matched or 0
        partially_matched = result.partially_matched or 0
        unmatched = result.unmatched or 0
        
        match_percentage = (fully_matched / total * 100) if total > 0 else 0
        
        return {
            "total_rrns": total,
            "fully_matched": fully_matched,
            "partially_matched": partially_matched,
            "unmatched": unmatched,
            "match_percentage": round(match_percentage, 2),
            "summary": {
                "matched": {
                    "count": fully_matched,
                    "percentage": round(fully_matched / total * 100, 2) if total > 0 else 0
                },
                "partially_matched": {
                    "count": partially_matched,
                    "percentage": round(partially_matched / total * 100, 2) if total > 0 else 0
                },
                "unmatched": {
                    "count": unmatched,
                    "percentage": round(unmatched / total * 100, 2) if total > 0 else 0
                }
            }
        }
    
    @staticmethod
    def get_fully_matched_data(
        db: Session, 
        offset: int = 0, 
        limit: int = 50,
        search_rrn: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get fully matched transactions (present in all 3 sources).
        
        Returns paginated list with ATM, SWITCH, and FLEXCUBE details.
        """
        search_clause = ""
        if search_rrn:
            search_clause = f"AND rrn LIKE '%{search_rrn}%'"
        
        query = text(f"""
            WITH combined_rrn AS (
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'ATM' AS source
                FROM atm_transactions
                WHERE rrn IS NOT NULL AND rrn != ''

                UNION ALL
                
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'SWITCH' AS source
                FROM switch_transactions
                WHERE rrn IS NOT NULL AND rrn != ''

                UNION ALL
                
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'FLEXCUBE' AS source
                FROM flexcube_transactions
                WHERE rrn IS NOT NULL AND rrn != ''
            ),
            matched_rrns AS (
                SELECT rrn
                FROM combined_rrn
                GROUP BY rrn
                HAVING COUNT(DISTINCT source) = 3
            )
            SELECT 
                m.rrn,
                -- ATM details
                a.datetime AS atm_datetime,
                a.terminalid AS atm_terminal,
                a.location AS atm_location,
                a.amount AS atm_amount,
                a.transactiontype AS atm_type,
                a.responsecode AS atm_response,
                -- SWITCH details
                s.datetime AS switch_datetime,
                s.direction AS switch_direction,
                s.mti AS switch_mti,
                s.amountminor AS switch_amount,
                s.responsecode AS switch_response,
                -- FLEXCUBE details
                f.fc_txn_id AS flexcube_txn_id,
                f.dr AS flexcube_dr,
                f.status AS flexcube_status,
                f.description AS flexcube_description
            FROM matched_rrns m
            LEFT JOIN atm_transactions a ON TRIM(CAST(a.rrn AS VARCHAR)) = m.rrn
            LEFT JOIN switch_transactions s ON TRIM(CAST(s.rrn AS VARCHAR)) = m.rrn
            LEFT JOIN flexcube_transactions f ON TRIM(CAST(f.rrn AS VARCHAR)) = m.rrn
            WHERE 1=1 {search_clause}
            ORDER BY a.datetime DESC
            LIMIT :limit OFFSET :offset
        """)
        
        # Get total count
        count_query = text(f"""
            WITH combined_rrn AS (
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn
                FROM atm_transactions WHERE rrn IS NOT NULL
                UNION ALL
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn
                FROM switch_transactions WHERE rrn IS NOT NULL
                UNION ALL
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn
                FROM flexcube_transactions WHERE rrn IS NOT NULL
            )
            SELECT COUNT(DISTINCT rrn) as total
            FROM combined_rrn
            WHERE rrn IN (
                SELECT rrn
                FROM combined_rrn
                GROUP BY rrn
                HAVING COUNT(*) >= 3
            ) {search_clause}
        """)
        
        results = db.execute(query, {"limit": limit, "offset": offset}).fetchall()
        total = db.execute(count_query).scalar() or 0
        
        data = []
        for row in results:
            data.append({
                "rrn": row.rrn,
                "match_status": "FULLY_MATCHED",
                "atm": {
                    "datetime": row.atm_datetime.isoformat() if row.atm_datetime else None,
                    "terminal": row.atm_terminal,
                    "location": row.atm_location,
                    "amount": float(row.atm_amount) if row.atm_amount else None,
                    "type": row.atm_type,
                    "response": row.atm_response
                },
                "switch": {
                    "datetime": row.switch_datetime.isoformat() if row.switch_datetime else None,
                    "direction": row.switch_direction,
                    "mti": row.switch_mti,
                    "amount": float(row.switch_amount) if row.switch_amount else None,
                    "response": row.switch_response
                },
                "flexcube": {
                    "txn_id": row.flexcube_txn_id,
                    "dr": float(row.flexcube_dr) if row.flexcube_dr else None,
                    "status": row.flexcube_status,
                    "description": row.flexcube_description
                }
            })
        
        return {
            "status": "success",
            "match_type": "fully_matched",
            "total": total,
            "offset": offset,
            "limit": limit,
            "data": data
        }
    
    @staticmethod
    def get_partially_matched_data(
        db: Session,
        offset: int = 0,
        limit: int = 50,
        search_rrn: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get partially matched transactions (present in 2 out of 3 sources).
        
        Shows which sources matched and which is missing.
        """
        search_clause = ""
        if search_rrn:
            search_clause = f"AND rrn LIKE '%{search_rrn}%'"
        
        query = text(f"""
            WITH combined_rrn AS (
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'ATM' AS source
                FROM atm_transactions
                WHERE rrn IS NOT NULL

                UNION ALL
                
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'SWITCH' AS source
                FROM switch_transactions
                WHERE rrn IS NOT NULL

                UNION ALL
                
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'FLEXCUBE' AS source
                FROM flexcube_transactions
                WHERE rrn IS NOT NULL
            ),
            partially_matched_rrns AS (
                SELECT 
                    rrn,
                    STRING_AGG(DISTINCT source, ', ' ORDER BY source) AS matched_sources
                FROM combined_rrn
                GROUP BY rrn
                HAVING COUNT(DISTINCT source) = 2
            )
            SELECT 
                m.rrn,
                m.matched_sources,
                -- ATM details (if exists)
                a.datetime AS atm_datetime,
                a.terminalid AS atm_terminal,
                a.amount AS atm_amount,
                a.transactiontype AS atm_type,
                -- SWITCH details (if exists)
                s.datetime AS switch_datetime,
                s.direction AS switch_direction,
                s.amountminor AS switch_amount,
                -- FLEXCUBE details (if exists)
                f.fc_txn_id AS flexcube_txn_id,
                f.dr AS flexcube_dr,
                f.status AS flexcube_status
            FROM partially_matched_rrns m
            LEFT JOIN atm_transactions a ON TRIM(CAST(a.rrn AS VARCHAR)) = m.rrn
            LEFT JOIN switch_transactions s ON TRIM(CAST(s.rrn AS VARCHAR)) = m.rrn
            LEFT JOIN flexcube_transactions f ON TRIM(CAST(f.rrn AS VARCHAR)) = m.rrn
            WHERE 1=1 {search_clause}
            ORDER BY m.rrn
            LIMIT :limit OFFSET :offset
        """)
        
        count_query = text(f"""
            WITH combined_rrn AS (
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn
                FROM atm_transactions WHERE rrn IS NOT NULL
                UNION ALL
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn
                FROM switch_transactions WHERE rrn IS NOT NULL
                UNION ALL
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn
                FROM flexcube_transactions WHERE rrn IS NOT NULL
            )
            SELECT COUNT(DISTINCT rrn) as total
            FROM combined_rrn
            WHERE rrn IN (
                SELECT rrn
                FROM combined_rrn
                GROUP BY rrn
                HAVING COUNT(DISTINCT rrn) = 2
            ) {search_clause}
        """)
        
        results = db.execute(query, {"limit": limit, "offset": offset}).fetchall()
        total = db.execute(count_query).scalar() or 0
        
        data = []
        for row in results:
            sources = row.matched_sources.split(', ')
            missing_source = [s for s in ['ATM', 'FLEXCUBE', 'SWITCH'] if s not in sources][0] if len(sources) == 2 else None
            
            data.append({
                "rrn": row.rrn,
                "match_status": "PARTIALLY_MATCHED",
                "matched_sources": sources,
                "missing_source": missing_source,
                "atm": {
                    "datetime": row.atm_datetime.isoformat() if row.atm_datetime else None,
                    "terminal": row.atm_terminal,
                    "amount": float(row.atm_amount) if row.atm_amount else None,
                    "type": row.atm_type
                } if 'ATM' in sources else None,
                "switch": {
                    "datetime": row.switch_datetime.isoformat() if row.switch_datetime else None,
                    "direction": row.switch_direction,
                    "amount": float(row.switch_amount) if row.switch_amount else None
                } if 'SWITCH' in sources else None,
                "flexcube": {
                    "txn_id": row.flexcube_txn_id,
                    "dr": float(row.flexcube_dr) if row.flexcube_dr else None,
                    "status": row.flexcube_status
                } if 'FLEXCUBE' in sources else None
            })
        
        return {
            "status": "success",
            "match_type": "partially_matched",
            "total": total,
            "offset": offset,
            "limit": limit,
            "data": data
        }
    
    @staticmethod
    def get_unmatched_data(
        db: Session,
        offset: int = 0,
        limit: int = 50,
        source_filter: Optional[str] = None,  # 'ATM', 'SWITCH', or 'FLEXCUBE'
        search_rrn: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get unmatched transactions (present in only 1 source).
        
        Can filter by specific source (ATM only, SWITCH only, etc.)
        """
        search_clause = ""
        if search_rrn:
            search_clause = f"AND rrn LIKE '%{search_rrn}%'"
        
        source_clause = ""
        if source_filter:
            source_clause = f"AND source = '{source_filter}'"
        
        query = text(f"""
            WITH combined_rrn AS (
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'ATM' AS source, 
                       datetime, terminalid, amount, transactiontype, responsecode
                FROM atm_transactions
                WHERE rrn IS NOT NULL

                UNION ALL
                
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'SWITCH' AS source,
                       datetime, terminalid, amountminor as amount, direction as transactiontype, responsecode
                FROM switch_transactions
                WHERE rrn IS NOT NULL

                UNION ALL
                
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'FLEXCUBE' AS source,
                       NULL as datetime, NULL as terminalid, dr as amount, 
                       status as transactiontype, NULL as responsecode
                FROM flexcube_transactions
                WHERE rrn IS NOT NULL
            ),
            unmatched_rrns AS (
                SELECT rrn, source
                FROM combined_rrn
                GROUP BY rrn, source
                HAVING COUNT(*) = 1 AND rrn IN (
                    SELECT rrn
                    FROM combined_rrn
                    GROUP BY rrn
                    HAVING COUNT(DISTINCT source) = 1
                )
            )
            SELECT 
                u.rrn,
                u.source,
                c.datetime,
                c.terminalid,
                c.amount,
                c.transactiontype,
                c.responsecode
            FROM unmatched_rrns u
            JOIN combined_rrn c ON u.rrn = c.rrn AND u.source = c.source
            WHERE 1=1 {search_clause} {source_clause}
            ORDER BY c.datetime DESC
            LIMIT :limit OFFSET :offset
        """)
        
        count_query = text(f"""
            WITH combined_rrn AS (
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'ATM' AS source
                FROM atm_transactions WHERE rrn IS NOT NULL
                UNION ALL
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'SWITCH' AS source
                FROM switch_transactions WHERE rrn IS NOT NULL
                UNION ALL
                SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'FLEXCUBE' AS source
                FROM flexcube_transactions WHERE rrn IS NOT NULL
            )
            SELECT COUNT(*) as total
            FROM (
                SELECT rrn, source
                FROM combined_rrn
                GROUP BY rrn, source
                HAVING rrn IN (
                    SELECT rrn
                    FROM combined_rrn
                    GROUP BY rrn
                    HAVING COUNT(DISTINCT source) = 1
                )
            ) u
            WHERE 1=1 {search_clause} {source_clause}
        """)
        
        results = db.execute(query, {"limit": limit, "offset": offset}).fetchall()
        total = db.execute(count_query).scalar() or 0
        
        data = []
        for row in results:
            data.append({
                "rrn": row.rrn,
                "match_status": "UNMATCHED",
                "source": row.source,
                "datetime": row.datetime.isoformat() if row.datetime else None,
                "terminal": row.terminalid,
                "amount": float(row.amount) if row.amount else None,
                "type": row.transactiontype,
                "response": row.responsecode
            })
        
        return {
            "status": "success",
            "match_type": "unmatched",
            "source_filter": source_filter,
            "total": total,
            "offset": offset,
            "limit": limit,
            "data": data
        }
    
    @staticmethod
    def get_matching_details_by_rrn(db: Session, rrn: str) -> Dict[str, Any]:
        """
        Get detailed matching information for a specific RRN.
        
        Shows all sources and their data for this RRN.
        """
        query = text("""
            SELECT 
                TRIM(CAST(a.rrn AS VARCHAR)) as rrn,
                -- ATM
                a.datetime AS atm_datetime,
                a.terminalid AS atm_terminal,
                a.location AS atm_location,
                a.amount AS atm_amount,
                a.transactiontype AS atm_type,
                a.responsecode AS atm_response,
                a.responsedesc AS atm_response_desc,
                -- SWITCH
                s.datetime AS switch_datetime,
                s.direction AS switch_direction,
                s.mti AS switch_mti,
                s.amountminor AS switch_amount,
                s.source AS switch_source,
                s.destination AS switch_destination,
                -- FLEXCUBE
                f.fc_txn_id AS flexcube_txn_id,
                f.dr AS flexcube_dr,
                f.status AS flexcube_status,
                f.description AS flexcube_description,
                f.account_masked AS flexcube_account
            FROM atm_transactions a
            FULL OUTER JOIN switch_transactions s ON TRIM(CAST(a.rrn AS VARCHAR)) = TRIM(CAST(s.rrn AS VARCHAR))
            FULL OUTER JOIN flexcube_transactions f ON TRIM(CAST(COALESCE(a.rrn, s.rrn) AS VARCHAR)) = TRIM(CAST(f.rrn AS VARCHAR))
            WHERE TRIM(CAST(COALESCE(a.rrn, s.rrn, f.rrn) AS VARCHAR)) = :rrn
        """)
        
        result = db.execute(query, {"rrn": rrn.strip()}).fetchone()
        
        if not result:
            return {
                "status": "not_found",
                "message": f"No data found for RRN: {rrn}"
            }
        
        sources_found = []
        if result.atm_datetime:
            sources_found.append("ATM")
        if result.switch_datetime:
            sources_found.append("SWITCH")
        if result.flexcube_txn_id:
            sources_found.append("FLEXCUBE")
        
        match_status = "UNMATCHED"
        if len(sources_found) == 3:
            match_status = "FULLY_MATCHED"
        elif len(sources_found) == 2:
            match_status = "PARTIALLY_MATCHED"
        
        return {
            "status": "success",
            "rrn": rrn,
            "match_status": match_status,
            "sources_found": sources_found,
            "source_count": len(sources_found),
            "details": {
                "atm": {
                    "found": "ATM" in sources_found,
                    "datetime": result.atm_datetime.isoformat() if result.atm_datetime else None,
                    "terminal": result.atm_terminal,
                    "location": result.atm_location,
                    "amount": float(result.atm_amount) if result.atm_amount else None,
                    "type": result.atm_type,
                    "response": result.atm_response,
                    "response_desc": result.atm_response_desc
                },
                "switch": {
                    "found": "SWITCH" in sources_found,
                    "datetime": result.switch_datetime.isoformat() if result.switch_datetime else None,
                    "direction": result.switch_direction,
                    "mti": result.switch_mti,
                    "amount": float(result.switch_amount) if result.switch_amount else None,
                    "source": result.switch_source,
                    "destination": result.switch_destination
                },
                "flexcube": {
                    "found": "FLEXCUBE" in sources_found,
                    "txn_id": result.flexcube_txn_id,
                    "dr": float(result.flexcube_dr) if result.flexcube_dr else None,
                    "status": result.flexcube_status,
                    "description": result.flexcube_description,
                    "account": result.flexcube_account
                }
            }
        }
