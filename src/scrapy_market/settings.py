import os

BOT_NAME = "scrapy_market"
SPIDER_MODULES = ["src.scrapy_market.spiders"]
NEWSPIDER_MODULE = "src.scrapy_market.spiders"
ROBOTSTXT_OBEY = True
DOWNLOAD_DELAY = 1.0
CONCURRENT_REQUESTS = 4
ITEM_PIPELINES = {
    "src.scrapy_market.pipelines.PostgresPipeline": 300,
}
LOG_LEVEL = os.getenv("SCRAPY_LOG_LEVEL", "INFO")
