import json
import csv
import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor

def db_url():
    user = os.getenv("POSTGRES_USER", "market")
    password = os.getenv("POSTGRES_PASSWORD", "marketpass")
    host = os.getenv("DB_HOST", "db")
    port = os.getenv("DB_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "marketdb")
    return f"dbname={db} user={user} password={password} host={host} port={port}"

def fetch_rows():
    conn = psycopg2.connect(db_url())
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                  id,
                  source_url,
                  profile_url,
                  website_url,
                  company_name,
                  rating,
                  reviews_count,
                  hourly_rate,
                  min_project_size,
                  team_size,
                  locations,
                  services_offered,
                  case_studies_count,
                  last_crawled_at
                FROM market_entries
                ORDER BY id ASC
            """)
            return cur.fetchall()
    finally:
        conn.close()

def write_json(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2, default=str)

def write_xml(path, rows):
    from xml.sax.saxutils import escape
    with open(path, "w", encoding="utf-8") as f:
        f.write("<market>\n")
        for r in rows:
            f.write("  <entry>\n")
            for k, v in r.items():
                if isinstance(v, (list, dict)):
                    v = json.dumps(v, ensure_ascii=False)
                f.write(f"    <{k}>{escape(str(v) if v is not None else '')}</{k}>\n")
            f.write("  </entry>\n")
        f.write("</market>\n")

def write_csv(path, rows):
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            pass
        return
    keys = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            r2 = {}
            for k, v in r.items():
                if isinstance(v, (list, dict)):
                    r2[k] = json.dumps(v, ensure_ascii=False)
                else:
                    r2[k] = v
            w.writerow(r2)

def main():
    out_json = sys.argv[1]
    out_xml = sys.argv[2]
    out_csv = sys.argv[3]
    rows = fetch_rows()
    write_json(out_json, rows)
    write_xml(out_xml, rows)
    write_csv(out_csv, rows)

if __name__ == "__main__":
    main()
