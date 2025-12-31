from sqlalchemy.orm import Session
from app.services.TxnJournalEntryService import TxnJournalEntryService

class TxnJournalEntryController:

    async def create_journal_entry(self, db: Session, payload: dict):
        return TxnJournalEntryService.create(db, payload)
