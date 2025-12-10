from datetime import datetime
from sqlalchemy.orm import Session
from app.models.transaction import Transaction

class SampleService:
    @staticmethod
    def return_message(db: Session):
        dummy = Transaction(
            amount=500.75,
            transaction_datetime=datetime.now(),
            transaction_type="Debit",
            atm_terminal="ATM-567",
            card="9999-XXXX-XXXX-1111",
            status_response="Success",
            transaction_id="TEST123456",
            channel_type="ATM",
            response_code="00",
            description="Dummy test transaction"
        )

        db.add(dummy)
        # db.add_all(dummy)
        db.commit()
        db.refresh(dummy)
        print("Inserted Transaction ID:", dummy.transaction_id)
        return {"message": "Inserted", "id": dummy.id}
