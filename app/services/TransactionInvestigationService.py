"""
Transaction Investigation Service
Fetches transaction data from multiple sources (ATM, Switch, CBS/Flexcube) by RRN
"""

from sqlalchemy.orm import Session
from app.models.atm_transaction import ATMTransaction
from app.models.SwitchTransaction import SwitchTransaction
from app.models.FlexcubeTransaction import FlexcubeTransaction
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class TransactionInvestigationService:
    """Service for investigating transactions across multiple sources"""

    @staticmethod
    def get_atm_transaction_by_rrn(db: Session, rrn: str) -> Optional[Dict[str, Any]]:
        """
        Fetch ATM transaction data by RRN
        
        Args:
            db: Database session
            rrn: Reference Retrieval Number
            
        Returns:
            Dictionary with ATM transaction data or None if not found
        """
        try:
            transaction = db.query(ATMTransaction).filter(
                ATMTransaction.rrn == rrn
            ).first()

            if not transaction:
                return None

            return {
                "rrn": transaction.rrn,
                "amount": float(transaction.amount) if transaction.amount else None,
                "date": transaction.datetime.isoformat() if transaction.datetime else None,
                "terminal_id": transaction.terminalid,
                "card_number": transaction.pan_masked,
                "account_number": transaction.account_masked,
                "transaction_type": transaction.transactiontype,
                "currency": transaction.currency,
                "stan": transaction.stan,
                "auth_code": transaction.auth,
                "response_code": transaction.responsecode,
                "response_description": transaction.responsedesc,
                "location": transaction.location,
                "atm_index": transaction.atmindex,
                "created_at": transaction.created_at.isoformat() if hasattr(transaction, 'created_at') and transaction.created_at else None
            }

        except Exception as e:
            logger.error(f"Error fetching ATM transaction by RRN {rrn}: {str(e)}")
            return None

    @staticmethod
    def get_switch_transaction_by_rrn(db: Session, rrn: str) -> Optional[Dict[str, Any]]:
        """
        Fetch Switch transaction data by RRN
        Returns both INBOUND and OUTBOUND transactions if they exist
        
        Args:
            db: Database session
            rrn: Reference Retrieval Number
            
        Returns:
            Dictionary with Switch transaction data or None if not found
        """
        try:
            transactions = db.query(SwitchTransaction).filter(
                SwitchTransaction.rrn == rrn
            ).all()

            if not transactions:
                return None

            # Separate INBOUND and OUTBOUND
            inbound = None
            outbound = None

            for txn in transactions:
                txn_data = {
                    "rrn": txn.rrn,
                    "amount": float(txn.amountminor) if txn.amountminor else None,
                    "date": txn.datetime.isoformat() if txn.datetime else None,
                    "mti": txn.mti,
                    "processing_code": txn.processingcode,
                    "stan": txn.stan,
                    "terminal_id": txn.terminalid,
                    "source": txn.source,
                    "destination": txn.destination,
                    "currency_code": txn.currency,
                    "response_code": txn.responsecode,
                    "auth_id": txn.authid,
                    "card_number": txn.pan_masked,
                    "direction": txn.direction,
                    "created_at": txn.created_at.isoformat() if hasattr(txn, 'created_at') and txn.created_at else None
                }

                if txn.direction and txn.direction.upper() == 'INBOUND':
                    inbound = txn_data
                elif txn.direction and txn.direction.upper() == 'OUTBOUND':
                    outbound = txn_data

            return {
                "rrn": rrn,
                "inbound": inbound,
                "outbound": outbound,
                "has_pair": inbound is not None and outbound is not None
            }

        except Exception as e:
            logger.error(f"Error fetching Switch transaction by RRN {rrn}: {str(e)}")
            return None

    @staticmethod
    def get_cbs_transaction_by_rrn(db: Session, rrn: str) -> Optional[Dict[str, Any]]:
        """
        Fetch CBS/Flexcube transaction data by RRN
        
        Args:
            db: Database session
            rrn: Reference Retrieval Number
            
        Returns:
            Dictionary with CBS transaction data or None if not found
        """
        try:
            transaction = db.query(FlexcubeTransaction).filter(
                FlexcubeTransaction.rrn == rrn
            ).first()

            if not transaction:
                return None

            # Determine transaction type based on DR/CR
            txn_type = None
            amount = None
            
            if transaction.dr and float(transaction.dr) > 0:
                txn_type = "Debit"
                amount = float(transaction.dr)
            elif transaction.cr and float(transaction.cr) > 0:
                txn_type = "Credit"
                amount = float(transaction.cr)

            return {
                "rrn": transaction.rrn,
                "amount": amount,
                "transaction_type": txn_type,
                "date": transaction.posted_datetime.isoformat() if transaction.posted_datetime else None,
                "fc_txn_id": transaction.fc_txn_id,
                "stan": transaction.stan,
                "account_number": transaction.account_masked,
                "dr": float(transaction.dr) if transaction.dr else 0,
                "cr": float(transaction.cr) if transaction.cr else 0,
                "currency": transaction.currency,
                "status": transaction.status,
                "description": transaction.description,
                "created_at": transaction.created_at.isoformat() if hasattr(transaction, 'created_at') and transaction.created_at else None
            }

        except Exception as e:
            logger.error(f"Error fetching CBS transaction by RRN {rrn}: {str(e)}")
            return None

    @staticmethod
    def get_all_transaction_data_by_rrn(db: Session, rrn: str) -> Dict[str, Any]:
        """
        Fetch transaction data from all sources (ATM, Switch, CBS) by RRN
        
        Args:
            db: Database session
            rrn: Reference Retrieval Number
            
        Returns:
            Dictionary with data from all sources
        """
        try:
            atm_data = TransactionInvestigationService.get_atm_transaction_by_rrn(db, rrn)
            switch_data = TransactionInvestigationService.get_switch_transaction_by_rrn(db, rrn)
            cbs_data = TransactionInvestigationService.get_cbs_transaction_by_rrn(db, rrn)

            # Determine matching status
            sources_found = []
            if atm_data:
                sources_found.append("ATM")
            if switch_data:
                sources_found.append("Switch")
            if cbs_data:
                sources_found.append("CBS")

            matching_status = "UNMATCHED"
            if len(sources_found) == 3:
                matching_status = "MATCHED"
            elif len(sources_found) == 2:
                matching_status = "PARTIAL"

            return {
                "rrn": rrn,
                "atm": atm_data,
                "switch": switch_data,
                "cbs": cbs_data,
                "matching_status": matching_status,
                "sources_found": sources_found,
                "total_sources": len(sources_found)
            }

        except Exception as e:
            logger.error(f"Error fetching all transaction data by RRN {rrn}: {str(e)}")
            return {
                "rrn": rrn,
                "atm": None,
                "switch": None,
                "cbs": None,
                "matching_status": "ERROR",
                "sources_found": [],
                "total_sources": 0,
                "error": str(e)
            }
