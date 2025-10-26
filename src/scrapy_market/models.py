import os
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, Text, Float, DateTime, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import func

Base = declarative_base()

class MarketEntry(Base):
    __tablename__ = "market_entries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_url = Column(Text, nullable=False)
    profile_url = Column(Text)
    website_url = Column(Text)
    company_name = Column(Text)

    rating = Column(Float)
    reviews_count = Column(Integer)
    hourly_rate = Column(String(100))
    min_project_size = Column(String(100))
    team_size = Column(String(100))

    locations = Column(JSONB)
    services_offered = Column(JSONB)
    case_studies_count = Column(Integer)

    last_crawled_at = Column(DateTime(timezone=False))
    created_at = Column(DateTime(timezone=False), server_default=func.now())
    updated_at = Column(DateTime(timezone=False), server_default=func.now(), onupdate=func.now())

def database_url_from_env():
    user = os.getenv("POSTGRES_USER", "market")
    password = os.getenv("POSTGRES_PASSWORD", "marketpass")
    host = os.getenv("DB_HOST", "db")
    port = os.getenv("DB_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "marketdb")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
