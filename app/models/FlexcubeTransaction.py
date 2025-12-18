from sqlalchemy import Column, BigInteger, String, Text, TIMESTAMP, Numeric
from sqlalchemy.sql import func
from app.db.database import Base

class FlexcubeTransaction(Base):
    __tablename__ = "flexcube_transactions"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    posted_datetime = Column(TIMESTAMP, nullable=True)
    fc_txn_id = Column(String(100))
    rrn = Column(BigInteger,nullable=True)
    stan = Column(BigInteger,nullable=True)
    account_masked = Column(String(50),nullable=True)
    dr = Column(Numeric,nullable=True)
    currency = Column(String(10),nullable=True)
    status = Column(String(50),nullable=True)
    description = Column(Text,nullable=True)
    # cr = Column(Numeric,nullable=True)
    uploaded_by = Column(BigInteger, nullable=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
