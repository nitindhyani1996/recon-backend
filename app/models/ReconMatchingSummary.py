from sqlalchemy import TIMESTAMP, BigInteger, Column, String, Text
from sqlalchemy.sql import func
from app.db.database import Base

class ReconMatchingSummary(Base):
    __tablename__ = "recon_matching_summary"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    recon_reference_number = Column(String(100), nullable=False, unique=True)

    matched = Column(Text, nullable=True)
    un_matched = Column(Text, nullable=True)
    partially_matched = Column(Text, nullable=True)
    added_by = Column(BigInteger)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
