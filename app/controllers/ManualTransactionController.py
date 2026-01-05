from sqlalchemy.orm import Session
from app.services.ManualTransactionService import ManualTransactionService

class ManualTransactionController:

    async def create_manual_transaction(self, db: Session, payload: dict):
        return ManualTransactionService.create(db, payload)

    async def patch_manual_transaction(
        self, db: Session, txn_id: int, payload: dict
    ):
        return ManualTransactionService.patch(db, txn_id, payload)
    
    # async def get_all_manual_transactions(self, db: Session):
    #     return ManualTransactionService.get_all(db)
    
    # async def get_all_manual_transactions(self, db: Session):
    #     return ManualTransactionService.get_all(
    #         db,
    #         username="Ackim"
    #     )
    async def get_all_manual_transactions(self, db: Session):
        return ManualTransactionService.get_all_json(
            db=db,
            username="Ackim"   
        )