from sqlalchemy.orm import Session
from fastapi import HTTPException
from app.models.MasterTable import Transaction

class MasterTransactionService:

    @staticmethod
    def patch(db: Session, ids: list[int], recon_reference_number: str, payload: dict):
        txns = db.query(Transaction).filter(
            Transaction.id.in_(ids)
        ).all()

        if not txns:
            raise HTTPException(
                status_code=404,
                detail="Master transaction(s) not found"
            )

        for txn in txns:
            for field, value in payload.items():
                if hasattr(txn, field):
                    setattr(txn, field, value)

            # ✅ ensure recon ref is set on master
            txn.recon_reference_number = recon_reference_number

        db.commit()
        return txns
