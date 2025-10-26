import os, re
from datetime import datetime
import scrapy
from ..items import MarketItem

GF_CATEGORIES_DEFAULT = [
    "https://www.goodfirms.co/artificial-intelligence",
    "https://www.goodfirms.co/internet-of-things",
    "https://www.goodfirms.co/robotic-process-automation",
]

class GoodFirmsSpider(scrapy.Spider):
    name = "goodfirms"
    allowed_domains = ["goodfirms.co", "www.goodfirms.co"]
    handle_httpstatus_list = [429]
    custom_settings = {
        "CONCURRENT_REQUESTS": int(os.getenv("SCRAPY_CONCURRENT_REQUESTS", "1")),
        "DOWNLOAD_DELAY": float(os.getenv("SCRAPY_DOWNLOAD_DELAY", "2.0")),
        "AUTOTHROTTLE_ENABLED": True,
        "RETRY_TIMES": int(os.getenv("SCRAPY_RETRY_TIMES", "8")),
        "RETRY_HTTP_CODES": [429, 500, 502, 503, 504],
        "USER_AGENT": os.getenv("SCRAPY_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36"),
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Upgrade-Insecure-Requests": "1",
            "Referer": "https://www.goodfirms.co/",
        },
    }

    async def start(self):
        seeds_env = os.getenv("GOODFIRMS_SEED_URLS", "")
        seeds = [s.strip() for s in seeds_env.split(",") if s.strip()] or GF_CATEGORIES_DEFAULT
        seeds = [s for s in seeds if s.startswith("http")]
        max_pages = int(os.getenv("GOODFIRMS_MAX_PAGES", os.getenv("MAX_PAGES", "25")))
        for url in seeds:
            yield scrapy.Request(url, callback=self.parse_listing, cb_kwargs={"seed": url, "page": 1, "max_pages": max_pages}, dont_filter=True)

    def parse_listing(self, response, seed, page, max_pages):
        if response.status == 429:
            yield response.request.replace(dont_filter=True, priority=-10)
            return
        cards = response.css("li.firm-wrapper, li.firm-list-item, li.company-list-item, div.firm-card, article.firm-wrapper")
        for c in cards:
            it = MarketItem()
            it["source_url"] = response.url
            it["company_name"] = self._clean(c.css("h3.firm-name a::text, a.firm-name::text, a.visit-profile::text").get() or c.attrib.get("entity-name"))
            prof = c.css("h3.firm-name a::attr(href), a.visit-profile::attr(href)").get()
            it["profile_url"] = response.urljoin(prof) if prof else None
            it["website_url"] = c.css("a.visit-website.web-url::attr(href)").get()
            r = c.css(".firm-rating .rating-number::text, .rating-number::text").get() or c.css('meta[itemprop="ratingValue"]::attr(content)').get()
            it["rating"] = self._to_float(r)
            rv = self._int(" ".join(c.css(".firm-rating a::text, a[href*='#review']::text, .reviews-count::text, .review-count::text").getall()))
            it["reviews_count"] = self._to_int(rv)
            it["hourly_rate"] = self._norm(c.css(".firm-services-list .firm-pricing span::text, .pricing span::text").get())
            it["min_project_size"] = None
            it["team_size"] = self._norm(c.css(".firm-services-list .firm-employees span::text, .employees span::text").get())
            loc = self._norm(c.css(".firm-services-list .firm-location span::text, .location::text").get())
            it["locations"] = [loc] if loc else []
            focus = [self._clean(x) for x in c.xpath('.//div[contains(@class,"firm-focus-item-name")]/text()').getall()]
            it["services_offered"] = [x for x in focus if x]
            it["case_studies_count"] = None
            it["last_crawled_at"] = datetime.utcnow().isoformat()
            if it.get("profile_url"):
                yield response.follow(it["profile_url"], callback=self.parse_profile, cb_kwargs={"it": it}, dont_filter=True, priority=5)
            elif it.get("company_name"):
                yield it
        if page < max_pages:
            nxt = response.css('a[rel="next"]::attr(href), li.page-item.next a::attr(href), a.next::attr(href), link[rel="next"]::attr(href)').get()
            if not nxt:
                nxt = self._next(seed, page + 1)
            if nxt:
                yield response.follow(nxt, callback=self.parse_listing, cb_kwargs={"seed": seed, "page": page + 1, "max_pages": max_pages}, dont_filter=True, priority=-1)

    def parse_profile(self, response, it):
        if response.status == 429:
            yield response.request.replace(dont_filter=True, priority=-10)
            return
        if not it.get("company_name"):
            it["company_name"] = self._clean(response.css("h1::text, h1 span::text, .company-title::text").get())
        if it.get("rating") is None:
            it["rating"] = self._to_float(response.css(".rating-number::text").get() or response.css('meta[itemprop="ratingValue"]::attr(content)').get())
        if it.get("reviews_count") is None:
            it["reviews_count"] = self._to_int(self._int(" ".join(response.css("a[href*='#review']::text, .reviews-count::text, .review-count::text").getall())))
        if not it.get("hourly_rate"):
            it["hourly_rate"] = self._norm(response.css(".firm-pricing span::text, .pricing span::text").get())
        if not it.get("team_size"):
            it["team_size"] = self._norm(response.css(".firm-employees span::text, .employees span::text").get())
        if not it.get("locations"):
            loc = self._norm(response.css(".firm-location span::text, .location::text").get())
            it["locations"] = [loc] if loc else []
        if not it.get("website_url"):
            it["website_url"] = response.css("a.visit-website.web-url::attr(href)").get()
        yield it

    def _next(self, seed, n):
        if "page=" in seed:
            return re.sub(r"([?&])page=\d+", r"\1page=%d" % n, seed)
        return f"{seed}{'&' if '?' in seed else '?'}page={n}"

    def _clean(self, s):
        if not s: return None
        return " ".join(str(s).split())

    def _norm(self, s):
        if not s: return None
        return re.sub(r"\s+", " ", s).strip()

    def _int(self, s):
        if not s: return None
        m = re.search(r"(\d[\d,]*)", s)
        return m.group(1).replace(",", "") if m else None

    def _to_int(self, v):
        try: return int(v)
        except: return None

    def _to_float(self, v):
        try: return float(str(v).replace(",", "."))
        except: return None
