from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import Base, MarketEntry, database_url_from_env

class PostgresPipeline:
    def open_spider(self, spider):
        self.engine = create_engine(database_url_from_env())
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def close_spider(self, spider):
        try:
            self.session.commit()
        finally:
            self.session.close()

    def process_item(self, item, spider):
        last_crawled = item.get("last_crawled_at")
        if isinstance(last_crawled, str):
            try:
                last_crawled = datetime.fromisoformat(last_crawled)
            except Exception:
                last_crawled = datetime.now(datetime.timezone.utc)

        entry = MarketEntry(
            source_url=item.get("source_url"),
            profile_url=item.get("profile_url"),
            website_url=item.get("website_url"),
            company_name=item.get("company_name"),
            rating=item.get("rating"),
            reviews_count=item.get("reviews_count"),
            hourly_rate=item.get("hourly_rate"),
            min_project_size=item.get("min_project_size"),
            team_size=item.get("team_size"),
            locations=item.get("locations") or [],
            services_offered=item.get("services_offered") or [],
            case_studies_count=item.get("case_studies_count"),
            last_crawled_at=last_crawled or datetime.now(datetime.timezone.utc),
        )
        self.session.add(entry)
        self.session.commit()
        return item
