import os
import re
from datetime import datetime
import scrapy
from ..items import MarketItem

CATEGORIES_DEFAULT = [
    "https://clutch.co/developers/artificial-intelligence",
    "https://clutch.co/developers/machine-learning",
    "https://clutch.co/developers/robotic-process-automation",
    "https://clutch.co/developers/automation",
    "https://clutch.co/developers/internet-of-things",
    "https://clutch.co/hardware",
    "https://clutch.co/developers/robotics",
]

class ClutchAgenciesSpider(scrapy.Spider):
    name = "clutch"
    allowed_domains = ["clutch.co"]
    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "CONCURRENT_REQUESTS": 2,
        "DOWNLOAD_DELAY": 1.0,
        "AUTOTHROTTLE_ENABLED": True,
        "RETRY_HTTP_CODES": [403, 429, 500, 502, 503, 504],
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Upgrade-Insecure-Requests": "1",
            "DNT": "1",
        },
    }

    async def start(self):
        seeds_env = os.getenv("CLUTCH_SEED_URLS", "")
        seeds = [s.strip() for s in seeds_env.split(",") if s.strip()] or CATEGORIES_DEFAULT
        seeds = [s for s in seeds if s.startswith("http")]
        max_pages = int(os.getenv("MAX_PAGES", "25"))
        for url in seeds:
            yield scrapy.Request(
                url=url,
                callback=self.parse_listing,
                cb_kwargs={"seed": url, "page": 1, "max_pages": max_pages},
                dont_filter=True,
            )

    def parse_listing(self, response, seed, page, max_pages):
        cards = response.css("div.provider, li.provider-row, div.provider-row, article.provider")
        for card in cards:
            item = MarketItem()
            item["source_url"] = response.url

            name = card.css("h3.provider__title a.provider__title-link::text, h3 a.provider__title-link::text, h3 a::text").get()
            item["company_name"] = self._clean(name)

            prof = card.css("h3.provider__title a.provider__title-link::attr(href), h3 a.provider__title-link::attr(href), h3 a::attr(href)").get()
            item["profile_url"] = response.urljoin(prof) if prof else None

            rating = card.css('meta[itemprop="ratingValue"]::attr(content)').get()
            if not rating:
                rating = self._first_num(card.css(".provider__rating .sg-rating__number::text").get())
            item["rating"] = self._to_float(rating)

            reviews = card.css('meta[itemprop="reviewCount"]::attr(content)').get()
            if not reviews:
                rv_text = " ".join(card.css(".provider__rating .sg-rating__reviews::text").getall())
                reviews = self._first_int(rv_text)
            item["reviews_count"] = self._to_int(reviews)

            item["min_project_size"] = self._norm(card.xpath('normalize-space(.//div[contains(@class,"min-project-size")])').get())
            item["hourly_rate"] = self._norm(card.xpath('normalize-space(.//div[contains(@class,"hourly-rate")])').get())
            item["team_size"] = self._norm(card.xpath('normalize-space(.//div[contains(@class,"employees-count")])').get())

            loc = self._norm(card.xpath('normalize-space(.//div[contains(@class,"location")])').get())
            item["locations"] = [loc] if loc else []

            services = [self._clean(t) for t in card.css(".provider__services-list .provider__services-list-item::text").getall()]
            services = [re.sub(r"^\s*\d+%?\s*", "", s) for s in services if s]
            item["services_offered"] = [s for s in services if s]

            cs_text = " ".join(card.css(".provider__project-highlight-projects-link::text").getall())
            item["case_studies_count"] = self._first_int(cs_text)

            item["website_url"] = None
            item["last_crawled_at"] = datetime.utcnow().isoformat()

            if item.get("profile_url"):
                yield response.follow(item["profile_url"], callback=self.parse_profile, cb_kwargs={"item": item}, dont_filter=True)
            else:
                if item.get("company_name"):
                    yield item

        if page < max_pages:
            nxt = response.css('a[rel="next"]::attr(href), li.pager-next a::attr(href), a.next::attr(href), link[rel="next"]::attr(href)').get()
            if not nxt:
                nxt = self._guess_next_url(seed, page + 1)
            if nxt:
                yield response.follow(nxt, callback=self.parse_listing, cb_kwargs={"seed": seed, "page": page + 1, "max_pages": max_pages}, dont_filter=True)

    def parse_profile(self, response, item):
        if not item.get("company_name"):
            item["company_name"] = self._clean(response.css("h1::text, h1 span::text").get())

        if item.get("rating") is None:
            r = response.css('meta[itemprop="ratingValue"]::attr(content)').get() or self._first_num(response.css(".sg-rating__number::text").get())
            item["rating"] = self._to_float(r)

        if item.get("reviews_count") is None:
            rc = response.css('meta[itemprop="reviewCount"]::attr(content)').get() or self._first_int(" ".join(response.css(".sg-rating__reviews::text").getall()))
            item["reviews_count"] = self._to_int(rc)

        if not item.get("hourly_rate"):
            item["hourly_rate"] = self._norm(response.xpath('normalize-space(//div[contains(@class,"hourly-rate")])').get())

        if not item.get("min_project_size"):
            item["min_project_size"] = self._norm(response.xpath('normalize-space(//div[contains(@class,"min-project-size")])').get())

        if not item.get("team_size"):
            item["team_size"] = self._norm(response.xpath('normalize-space(//div[contains(@class,"employees-count")])').get())

        if not item.get("locations"):
            loc = self._norm(response.xpath('normalize-space(//div[contains(@class,"location")])').get())
            item["locations"] = [loc] if loc else []

        if not item.get("services_offered"):
            services = [self._clean(t) for t in response.css(".provider__services-list .provider__services-list-item::text").getall()]
            services = [re.sub(r"^\s*\d+%?\s*", "", s) for s in services if s]
            item["services_offered"] = [s for s in services if s]

        if item.get("case_studies_count") is None:
            cs_text = " ".join(response.css(".provider__project-highlight-projects-link::text").getall())
            item["case_studies_count"] = self._first_int(cs_text)

        if not item.get("website_url"):
            btn = response.css('a.website-link__item::attr(href)').get()
            if btn and "u=" in btn:
                from urllib.parse import urlparse, parse_qs, unquote
                try:
                    u = parse_qs(urlparse(btn).query).get("u", [None])[0]
                    item["website_url"] = unquote(u) if u else None
                except Exception:
                    item["website_url"] = None

        yield item

    def _guess_next_url(self, seed, next_page):
        if "page=" in seed:
            return re.sub(r"([?&])page=\d+", r"\1page=%d" % next_page, seed)
        sep = "&" if "?" in seed else "?"
        return f"{seed}{sep}page={next_page}"

    def _clean(self, s):
        if not s:
            return None
        return " ".join(str(s).split())

    def _norm(self, s):
        if not s:
            return None
        return re.sub(r"\s+", " ", s).strip()

    def _first_num(self, s):
        if not s:
            return None
        m = re.search(r"(\d+(?:[.,]\d+)?)", s)
        return m.group(1).replace(",", ".") if m else None

    def _first_int(self, s):
        if not s:
            return None
        m = re.search(r"(\d[\d,]*)", s)
        return m.group(1).replace(",", "") if m else None

    def _to_int(self, v):
        try:
            return int(v)
        except Exception:
            return None

    def _to_float(self, v):
        try:
            return float(str(v).replace(",", "."))
        except Exception:
            return None
