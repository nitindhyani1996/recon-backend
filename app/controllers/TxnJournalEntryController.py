from sqlalchemy.orm import Session
from app.services.TxnJournalEntryService import TxnJournalEntryService

class TxnJournalEntryController:

    async def create_journal_entries(self, db: Session, payload: list):
        return TxnJournalEntryService.create_many(db, payload)
