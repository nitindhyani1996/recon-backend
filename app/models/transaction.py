from sqlalchemy import Column, Integer, String, Float, DateTime
from app.db.database import Base
from sqlalchemy.sql import func


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)  # Auto increment ID
    amount = Column(Float, nullable=False)
    transaction_datetime = Column(DateTime, nullable=False)
    transaction_type = Column(String(50), nullable=False)
    atm_terminal = Column(String(100), nullable=True)
    card = Column(String(50), nullable=True)
    status_response = Column(String(50), nullable=True)
    transaction_id = Column(String(100), nullable=False, unique=True)
    channel_type = Column(String(50), nullable=True)
    response_code = Column(String(50), nullable=True)
    description = Column(String(255), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)