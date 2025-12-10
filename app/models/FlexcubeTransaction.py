from sqlalchemy import Column, BigInteger, String, Text, TIMESTAMP, Numeric
from sqlalchemy.sql import func
from app.db.database import Base

class FlexcubeTransaction(Base):
    __tablename__ = "flexcube_transactions"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    posted_datetime = Column(TIMESTAMP, nullable=True)
    fc_txn_id = Column(String(100))
    rrn = Column(BigInteger)
    stan = Column(BigInteger)
    account_masked = Column(String(50))
    dr = Column(Numeric)
    currency = Column(String(10))
    status = Column(String(50))
    description = Column(Text)
    cr = Column(Numeric)
    uploaded_by = Column(BigInteger, nullable=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
