from sqlalchemy.orm import Session
from app.models.TxnJournalEntry import TxnJournalEntry

class TxnJournalEntryService:

    @staticmethod
    def create(db: Session, payload: dict):
        entry = TxnJournalEntry(**payload)
        db.add(entry)
        db.commit()
        db.refresh(entry)
        return entry
