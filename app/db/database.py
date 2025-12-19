from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "1234")
DB_NAME = os.getenv("DB_NAME", "recon_db")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create engine with optimized connection pooling for bulk operations
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=10,              # Maintain 10 connections in pool
    max_overflow=20,           # Allow 20 extra connections when needed
    pool_timeout=30,           # Wait 30 seconds for connection
    pool_recycle=3600,         # Recycle connections after 1 hour
    pool_pre_ping=True,        # Verify connections before use
    connect_args={
        "connect_timeout": 10,  # Connection timeout
        "options": "-c statement_timeout=300000"  # 5 minute query timeout (300 seconds)
    }
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
