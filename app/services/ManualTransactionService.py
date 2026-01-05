from sqlalchemy.orm import Session
from fastapi import HTTPException
from sqlalchemy.dialects.postgresql import insert
from app.models.ManualTransaction import ManualTransaction
from sqlalchemy.dialects.postgresql import insert

class ManualTransactionService:

    @staticmethod
    def create(db: Session, payload: dict):
        stmt = insert(ManualTransaction).values(**payload)

        # block insert ONLY when both fields match
        stmt = stmt.on_conflict_do_nothing(
            constraint="uq_recon_rrn_source_ref"
        )

        result = db.execute(stmt)
        db.commit()

        # return existing or newly inserted row
        return db.query(ManualTransaction).filter(
            ManualTransaction.recon_reference_number == payload["recon_reference_number"],
            ManualTransaction.source_reference_number == payload["source_reference_number"]
        ).first()


    @staticmethod
    def patch(db: Session, recon_reference_number: str, payload: dict):
        txns = db.query(ManualTransaction).filter(
        ManualTransaction.recon_reference_number == recon_reference_number
        ).all()

        if not txns:
            raise HTTPException(status_code=404, detail="Transaction not found")

        for txn in txns:
            for field, value in payload.items():
                if hasattr(txn, field):
                    setattr(txn, field, value)

        db.commit()

        return txns

    
    # @staticmethod
    # def get_all(db: Session):
    #     return db.query(ManualTransaction).all()
    
    @staticmethod
    def get_all_json(
        db: Session,
        username: str
    ):
        results = (
            db.query(
                ManualTransaction.recon_reference_number,
                ManualTransaction.channel_id,
                ManualTransaction.source_id,
                ManualTransaction.json_file
            )
            .filter(
                ManualTransaction.reconciled_status == "PENDING",
                ManualTransaction.created_by == username
            )
            .all()
        )
        return [{"recon_reference_number": r[0],"channel_id": r[1],"source_id": r[2], "json_file": r[3]} for r in results]


