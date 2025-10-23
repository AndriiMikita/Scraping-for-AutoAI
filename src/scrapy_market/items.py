import scrapy

class MarketItem(scrapy.Item):
    source_url = scrapy.Field()
    company_name = scrapy.Field()
    services_offered = scrapy.Field()
    industries_served = scrapy.Field()
    tech_stack = scrapy.Field()
    pricing_models = scrapy.Field()
    locations = scrapy.Field()
    certifications = scrapy.Field()
    case_studies_count = scrapy.Field()
    last_crawled_at = scrapy.Field()
