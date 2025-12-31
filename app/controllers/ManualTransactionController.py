from sqlalchemy.orm import Session
from app.services.ManualTransactionService import ManualTransactionService

class ManualTransactionController:

    async def create_manual_transaction(self, db: Session, payload: dict):
        return ManualTransactionService.create(db, payload)
