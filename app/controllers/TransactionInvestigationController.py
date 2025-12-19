"""
Transaction Investigation Controller
Handles API requests for transaction investigation
"""

from sqlalchemy.orm import Session
from app.services.TransactionInvestigationService import TransactionInvestigationService
from typing import Dict, Any


class TransactionInvestigationController:
    """Controller for transaction investigation endpoints"""

    @staticmethod
    async def get_transaction_by_rrn(db: Session, rrn: str) -> Dict[str, Any]:
        """
        Get transaction data from all sources by RRN
        
        Args:
            db: Database session
            rrn: Reference Retrieval Number
            
        Returns:
            Response with transaction data from all sources
        """
        if not rrn or not rrn.strip():
            return {
                "success": False,
                "status_code": 400,
                "message": "RRN is required",
                "data": None
            }

        try:
            result = TransactionInvestigationService.get_all_transaction_data_by_rrn(db, rrn.strip())
            
            # Check if any data was found
            if result.get("total_sources", 0) == 0 and not result.get("error"):
                return {
                    "success": False,
                    "status_code": 404,
                    "message": f"No transaction found with RRN: {rrn}",
                    "data": result
                }

            return {
                "success": True,
                "status_code": 200,
                "message": "Transaction data fetched successfully",
                "data": result
            }

        except Exception as e:
            return {
                "success": False,
                "status_code": 500,
                "message": f"Error fetching transaction data: {str(e)}",
                "data": None
            }

    @staticmethod
    async def get_atm_transaction_by_rrn(db: Session, rrn: str) -> Dict[str, Any]:
        """
        Get ATM transaction data by RRN
        
        Args:
            db: Database session
            rrn: Reference Retrieval Number
            
        Returns:
            Response with ATM transaction data
        """
        if not rrn or not rrn.strip():
            return {
                "success": False,
                "status_code": 400,
                "message": "RRN is required",
                "data": None
            }

        try:
            result = TransactionInvestigationService.get_atm_transaction_by_rrn(db, rrn.strip())
            
            if not result:
                return {
                    "success": False,
                    "status_code": 404,
                    "message": f"No ATM transaction found with RRN: {rrn}",
                    "data": None
                }

            return {
                "success": True,
                "status_code": 200,
                "message": "ATM transaction data fetched successfully",
                "data": result
            }

        except Exception as e:
            return {
                "success": False,
                "status_code": 500,
                "message": f"Error fetching ATM transaction: {str(e)}",
                "data": None
            }

    @staticmethod
    async def get_switch_transaction_by_rrn(db: Session, rrn: str) -> Dict[str, Any]:
        """
        Get Switch transaction data by RRN
        
        Args:
            db: Database session
            rrn: Reference Retrieval Number
            
        Returns:
            Response with Switch transaction data
        """
        if not rrn or not rrn.strip():
            return {
                "success": False,
                "status_code": 400,
                "message": "RRN is required",
                "data": None
            }

        try:
            result = TransactionInvestigationService.get_switch_transaction_by_rrn(db, rrn.strip())
            
            if not result:
                return {
                    "success": False,
                    "status_code": 404,
                    "message": f"No Switch transaction found with RRN: {rrn}",
                    "data": None
                }

            return {
                "success": True,
                "status_code": 200,
                "message": "Switch transaction data fetched successfully",
                "data": result
            }

        except Exception as e:
            return {
                "success": False,
                "status_code": 500,
                "message": f"Error fetching Switch transaction: {str(e)}",
                "data": None
            }

    @staticmethod
    async def get_cbs_transaction_by_rrn(db: Session, rrn: str) -> Dict[str, Any]:
        """
        Get CBS/Flexcube transaction data by RRN
        
        Args:
            db: Database session
            rrn: Reference Retrieval Number
            
        Returns:
            Response with CBS transaction data
        """
        if not rrn or not rrn.strip():
            return {
                "success": False,
                "status_code": 400,
                "message": "RRN is required",
                "data": None
            }

        try:
            result = TransactionInvestigationService.get_cbs_transaction_by_rrn(db, rrn.strip())
            
            if not result:
                return {
                    "success": False,
                    "status_code": 404,
                    "message": f"No CBS transaction found with RRN: {rrn}",
                    "data": None
                }

            return {
                "success": True,
                "status_code": 200,
                "message": "CBS transaction data fetched successfully",
                "data": result
            }

        except Exception as e:
            return {
                "success": False,
                "status_code": 500,
                "message": f"Error fetching CBS transaction: {str(e)}",
                "data": None
            }
