import os
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, UniqueConstraint
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('POSTGRES_DB')}"

Base = declarative_base()

class MarketEntry(Base):
    __tablename__ = "market_entries"
    id = Column(Integer, primary_key=True)
    source_url = Column(Text, nullable=False)
    company_name = Column(Text, nullable=False)
    services_offered = Column(Text)
    industries_served = Column(Text)
    tech_stack = Column(Text)
    pricing_models = Column(Text)
    locations = Column(Text)
    certifications = Column(Text)
    case_studies_count = Column(Integer)
    last_crawled_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    __table_args__ = (UniqueConstraint("source_url", "company_name", name="uq_source_company"),)

class PostgresPipeline:
    def open_spider(self, spider):
        self.engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def process_item(self, item, spider):
        session = self.Session()
        try:
            exists = session.query(MarketEntry).filter_by(source_url=item.get("source_url","").strip(), company_name=item.get("company_name","").strip()).one_or_none()
            if exists:
                exists.services_offered = stringify(item.get("services_offered"))
                exists.industries_served = stringify(item.get("industries_served"))
                exists.tech_stack = stringify(item.get("tech_stack"))
                exists.pricing_models = stringify(item.get("pricing_models"))
                exists.locations = stringify(item.get("locations"))
                exists.certifications = stringify(item.get("certifications"))
                exists.case_studies_count = to_int(item.get("case_studies_count"))
                exists.last_crawled_at = datetime.utcnow()
                exists.updated_at = datetime.utcnow()
                session.add(exists)
            else:
                row = MarketEntry(
                    source_url=item.get("source_url","").strip(),
                    company_name=item.get("company_name","").strip(),
                    services_offered=stringify(item.get("services_offered")),
                    industries_served=stringify(item.get("industries_served")),
                    tech_stack=stringify(item.get("tech_stack")),
                    pricing_models=stringify(item.get("pricing_models")),
                    locations=stringify(item.get("locations")),
                    certifications=stringify(item.get("certifications")),
                    case_studies_count=to_int(item.get("case_studies_count")),
                    last_crawled_at=datetime.utcnow(),
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
                session.add(row)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
        return item

def stringify(v):
    if v is None:
        return None
    if isinstance(v, (list, tuple)):
        return ", ".join([str(x).strip() for x in v if str(x).strip()])
    return str(v).strip()

def to_int(v):
    try:
        return int(str(v).strip())
    except Exception:
        return None
