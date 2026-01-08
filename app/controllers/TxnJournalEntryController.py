from sqlalchemy.orm import Session
from app.services.TxnJournalEntryService import TxnJournalEntryService

class TxnJournalEntryController:

    async def create_journal_entries(self, db: Session, payload: list):
        return TxnJournalEntryService.create_many(db, payload)
# from sqlalchemy.orm import Session
# from app.services.TxnJournalEntryService import TxnJournalEntryService
# from app.services.MasterTransactionService import MasterTransactionService


# class TxnJournalEntryController:

#     async def create_journal_entries(self, db: Session, payload: list):
#         # 1️⃣ Create journal entries (existing behavior)
#         result = TxnJournalEntryService.create_many(db, payload)

#         # 2️⃣ Update master transactions (added behavior)
#         for item in payload:
#             recon_reference_number = item.get("recon_reference_number")

#             if recon_reference_number:
#                 MasterTransactionService.patch(
#                     db=db,
#                     recon_reference_number=recon_reference_number,
#                     payload={
#                         "reconciled_mode": True,
#                         "updated_by": item.get("created_by")
#                     }
#                 )

#         return result
