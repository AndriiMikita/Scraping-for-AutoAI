.PHONY: up down scrape export dump load reset logs

up:
	docker compose up -d --build

down:
	docker compose down

scrape:
	docker compose run --rm scraper bash -lc "python -m src.scripts.wait_for_postgres && scrapy crawl agencies"

export:
	docker compose run --rm scraper python -m src.scripts.export_data outputs/market_data.json outputs/market_data.xml outputs/market_data.csv

dump:
	mkdir -p dumps
	docker compose exec -T db sh -c "pg_dump -U $$POSTGRES_USER -d $$POSTGRES_DB -t market_entries" > dumps/market_entries.sql

load:
	docker compose exec -T db sh -c "psql -U $$POSTGRES_USER -d $$POSTGRES_DB" < dumps/market_entries.sql

logs:
	docker compose logs -f scraper

reset:
	docker compose down -v
	rm -rf dumps outputs
	mkdir -p dumps outputs
