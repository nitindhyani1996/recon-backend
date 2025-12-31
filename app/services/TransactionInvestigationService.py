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
    def get_atm_transaction_by_rrn(db: Session, rrn: str):
        try:
            txns = (
                db.query(ATMTransaction)
                .filter(ATMTransaction.rrn == rrn)
                .all()
            )

            if not txns:
                return []

            atm_list = []
            for txn in txns:
                atm_list.append({
                    "rrn": txn.rrn,
                    "amount": float(txn.amount) if txn.amount else None,
                    "date": txn.datetime.isoformat() if txn.datetime else None,
                    "terminal_id": txn.terminalid,
                    "card_number": txn.pan_masked,
                    "account_number": txn.account_masked,
                    "transaction_type": txn.transactiontype,
                    "currency": txn.currency,
                    "stan": txn.stan,
                    "auth_code": txn.auth,
                    "response_code": txn.responsecode,
                    "response_description": txn.responsedesc,
                    "location": txn.location,
                    "atm_index": txn.atmindex,
                    "created_at": txn.created_at.isoformat() if txn.created_at else None
                })

            return atm_list

        except Exception as e:
            logger.error(f"Error fetching ATM transaction by RRN {rrn}: {str(e)}")
            return []


    @staticmethod
    def get_switch_transaction_by_rrn(db: Session, rrn: str) -> Optional[Dict[str, Any]]:
        """
        Fetch single Switch transaction by RRN
        (No inbound / outbound separation)
        """
        try:
            txn = (
                db.query(SwitchTransaction)
                .filter(SwitchTransaction.rrn == rrn)
                .all()
            )

            if not txn:
                return []
            switch_list = []
            for txn in txn:
                switch_list.append({
                "rrn": txn.rrn,
                "amount": float(txn.amountminor) if txn.amountminor is not None else None,
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
                # "amountminor": float(txn.amountminor) if txn.amountminor is not None else None,
                "created_at": txn.created_at.isoformat() if getattr(txn, "created_at", None) else None
            })
            return switch_list

        except Exception as e:
            logger.error(f"Error fetching Switch transaction by RRN {rrn}: {str(e)}")
            return None

    @staticmethod
    def get_cbs_transaction_by_rrn(db: Session, rrn: str):
        try:
            txns = (
                db.query(FlexcubeTransaction)
                .filter(FlexcubeTransaction.rrn == rrn)
                .all()
            )

            if not txns:
                return []

            cbs_list = []
            for txn in txns:
                txn_type = None
                amount = None

                if txn.dr and float(txn.dr) > 0:
                    txn_type = "Debit"
                    amount = float(txn.dr)
                elif txn.cr and float(txn.cr) > 0:
                    txn_type = "Credit"
                    amount = float(txn.cr)

                cbs_list.append({
                    "rrn": txn.rrn,
                    "amount": amount,
                    "transaction_type": txn_type,
                    "date": txn.posted_datetime.isoformat() if txn.posted_datetime else None,
                    "fc_txn_id": txn.fc_txn_id,
                    "stan": txn.stan,
                    "account_number": txn.account_masked,
                    "dr": float(txn.dr) if txn.dr else 0,
                    "cr": float(txn.cr) if txn.cr else 0,
                    "currency": txn.currency,
                    "status": txn.status,
                    "description": txn.description,
                    "created_at": txn.created_at.isoformat() if txn.created_at else None
                })

            return cbs_list

        except Exception as e:
            logger.error(f"Error fetching CBS transaction by RRN {rrn}: {str(e)}")
            return []


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
            print('atm_data',atm_data)
            print('switch_data',switch_data)
            print('cbs_data',cbs_data)

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
