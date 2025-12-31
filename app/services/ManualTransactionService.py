from sqlalchemy.orm import Session
from app.models.ManualTransaction import ManualTransaction

class ManualTransactionService:

    @staticmethod
    def create(db: Session, payload: dict):
        manual_txn = ManualTransaction(**payload)
        db.add(manual_txn)
        db.commit()
        db.refresh(manual_txn)
        return manual_txn
