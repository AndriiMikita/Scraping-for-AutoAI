import os
import json
from datetime import datetime
import scrapy
from ..items import MarketItem

class AgenciesSpider(scrapy.Spider):
    name = "agencies"

    def start_requests(self):
        seeds_env = os.getenv("SEED_URLS", "")
        seeds = [s.strip() for s in seeds_env.split(",") if s.strip()]
        for url in seeds:
            yield scrapy.Request(url=url, callback=self.parse_listing, dont_filter=True)

    def parse_listing(self, response):
        for href in response.css("a::attr(href)").getall():
            if self.is_candidate_profile_link(href):
                yield response.follow(href, callback=self.parse_profile)
        next_links = response.css("a[rel='next']::attr(href), a.next::attr(href)").getall()
        for href in next_links:
            yield response.follow(href, callback=self.parse_listing)

    def parse_profile(self, response):
        item = MarketItem()
        item["source_url"] = response.url
        item["company_name"] = self.extract_company_name(response)
        item["services_offered"] = self.extract_services(response)
        item["industries_served"] = self.extract_industries(response)
        item["tech_stack"] = self.extract_tech_stack(response)
        item["pricing_models"] = self.extract_pricing(response)
        item["locations"] = self.extract_locations(response)
        item["certifications"] = self.extract_certifications(response)
        item["case_studies_count"] = self.extract_case_studies_count(response)
        item["last_crawled_at"] = datetime.utcnow().isoformat()
        if item["company_name"]:
            yield item

    def is_candidate_profile_link(self, href):
        if not href:
            return False
        lowered = href.lower()
        bad = ["#", "mailto:", "tel:"]
        if any(x in lowered for x in bad):
            return False
        tokens = ["company", "agency", "profile", "vendor", "partner"]
        return any(t in lowered for t in tokens)

    def extract_company_name(self, response):
        name = response.css("h1::text").get()
        if not name:
            data = self.extract_ld_json(response)
            if data and isinstance(data, dict):
                name = data.get("name")
        return self.clean(name)

    def extract_services(self, response):
        services = response.css("ul li::text").getall()
        if not services:
            data = self.extract_ld_json(response)
            if isinstance(data, dict):
                services = data.get("knowsAbout") or data.get("services")
        return [self.clean(s) for s in services if self.clean(s)]

    def extract_industries(self, response):
        labels = response.xpath("//*[contains(translate(text(),'INDUSTRIES','industries'),'industries')]/following::ul[1]/li/text()").getall()
        return [self.clean(x) for x in labels if self.clean(x)]

    def extract_tech_stack(self, response):
        labels = response.xpath("//*[contains(translate(text(),'STACK','stack'),'stack') or contains(translate(text(),'TECH','tech'),'tech')]/following::ul[1]/li/text()").getall()
        return [self.clean(x) for x in labels if self.clean(x)]

    def extract_pricing(self, response):
        labels = response.xpath("//*[contains(translate(text(),'PRIC','pric'),'pric')]/following::ul[1]/li/text()").getall()
        return [self.clean(x) for x in labels if self.clean(x)]

    def extract_locations(self, response):
        labels = response.xpath("//*[contains(translate(text(),'LOCATION','location'),'location')]/following::ul[1]/li/text()").getall()
        if not labels:
            data = self.extract_ld_json(response)
            if isinstance(data, dict):
                addr = data.get("address")
                if isinstance(addr, dict):
                    city = addr.get("addressLocality")
                    country = addr.get("addressCountry")
                    labels = [", ".join([x for x in [city, country] if x])]
        return [self.clean(x) for x in labels if self.clean(x)]

    def extract_certifications(self, response):
        labels = response.xpath("//*[contains(translate(text(),'CERT','cert'),'cert')]/following::ul[1]/li/text()").getall()
        return [self.clean(x) for x in labels if self.clean(x)]

    def extract_case_studies_count(self, response):
        text = "".join(response.xpath("//*[contains(translate(text(),'CASE','case'),'case') and contains(translate(text(),'STUD','stud'),'stud')]/text()").getall())
        numbers = [int(s) for s in "".join(ch if ch.isdigit() else " " for ch in text).split() if s.isdigit()]
        if numbers:
            return numbers[0]
        return None

    def extract_ld_json(self, response):
        try:
            blocks = response.xpath("//script[@type='application/ld+json']/text()").getall()
            for b in blocks:
                data = json.loads(b)
                if isinstance(data, list) and data:
                    data = data[0]
                if isinstance(data, dict):
                    return data
        except Exception:
            return None
        return None

    def clean(self, s):
        if not s:
            return None
        return " ".join(str(s).split())
