import scrapy

class MarketItem(scrapy.Item):
    source_url = scrapy.Field()
    profile_url = scrapy.Field()
    website_url = scrapy.Field()
    company_name = scrapy.Field()
    rating = scrapy.Field()
    reviews_count = scrapy.Field()
    hourly_rate = scrapy.Field()
    min_project_size = scrapy.Field()
    team_size = scrapy.Field()
    locations = scrapy.Field()
    services_offered = scrapy.Field()
    case_studies_count = scrapy.Field()
    last_crawled_at = scrapy.Field()
