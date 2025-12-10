from sqlalchemy import Column, BigInteger, String, Text, TIMESTAMP, func
from app.db.database import Base

class UploadedFile(Base):
    __tablename__ = "uploaded_files"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    file_description = Column(Text, nullable=True)
    uploaded_by = Column(BigInteger, nullable=True)
    status = Column(String(50), nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
