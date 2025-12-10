from sqlalchemy import DECIMAL, BigInteger, Column, String, TIMESTAMP
from app.db.database import Base
from sqlalchemy.sql import func

class ATMTransaction(Base):
    __tablename__ = "atm_transactions"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    datetime = Column(TIMESTAMP, nullable=True)
    terminalid = Column(String(50), nullable=True)
    location = Column(String(255), nullable=True)
    atmindex = Column(String(50), nullable=True)
    pan_masked = Column(String(30), nullable=True)
    account_masked = Column(String(30), nullable=True)
    transactiontype = Column(String(50), nullable=True)
    amount = Column(DECIMAL(15, 2), nullable=True)
    currency = Column(String(10), nullable=True)
    stan = Column(String(20), nullable=True)
    rrn = Column(String(50))
    auth = Column(String(20), nullable=True)
    responsecode = Column(String(10), nullable=True)
    responsedesc = Column(String(255), nullable=True)
    uploaded_by = Column(BigInteger, nullable=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
