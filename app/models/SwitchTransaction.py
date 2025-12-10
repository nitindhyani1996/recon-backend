from sqlalchemy import Column, BigInteger, String, Integer, Numeric, TIMESTAMP, func
from app.db.database import Base

class SwitchTransaction(Base):
    __tablename__ = "switch_transactions"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    datetime = Column(TIMESTAMP, nullable=True)
    direction = Column(String(50), nullable=True)
    mti = Column(Integer, nullable=True)
    pan_masked = Column(String(50), nullable=True)
    processingcode = Column(Integer, nullable=True)
    amountminor = Column(Numeric, nullable=True)
    currency = Column(String(10), nullable=True)
    stan = Column(BigInteger, nullable=True)
    rrn = Column(String(50))
    terminalid = Column(String(50), nullable=True)
    source = Column(String(50), nullable=True)
    destination = Column(String(50), nullable=True)
    responsecode = Column(String(50))
    authid = Column(String(50))
    uploaded_by = Column(BigInteger, nullable=True) 
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
