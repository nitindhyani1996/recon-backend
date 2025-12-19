from sqlalchemy import Column, Integer, String, JSON, DateTime, func
from app.db.database import Base

class MatchingRule(Base):
    __tablename__ = "matching_rules"

    id = Column(Integer, primary_key=True, index=True)
    basic_details = Column(JSON, nullable=False)
    classification = Column(JSON, nullable=False)
    rule_category = Column(Integer, nullable=False)
    matchcondition = Column(JSON, nullable=False)  # snake_case
    tolerance = Column(JSON, nullable=False)
    added_by = Column(String(100), nullable=True)
