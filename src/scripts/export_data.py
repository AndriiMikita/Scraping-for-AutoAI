import os
import csv
import json
from xml.etree.ElementTree import Element, SubElement, ElementTree
import psycopg2
from psycopg2.extras import RealDictCursor
from pathlib import Path

def fetch_rows():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT","5432")),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
    )
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id, source_url, company_name, services_offered, industries_served, tech_stack, pricing_models, locations, certifications, case_studies_count, last_crawled_at, created_at, updated_at FROM market_entries ORDER BY id")
        rows = cur.fetchall()
    conn.close()
    return rows

def write_json(path, rows):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2, default=str)

def write_csv(path, rows):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write("")
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: (",".join(v) if isinstance(v, list) else v) for k, v in r.items()})

def write_xml(path, rows):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    root = Element("market_entries")
    for r in rows:
        e = SubElement(root, "entry")
        for k, v in r.items():
            c = SubElement(e, k)
            if isinstance(v, list):
                c.text = ", ".join(v)
            else:
                c.text = "" if v is None else str(v)
    tree = ElementTree(root)
    tree.write(path, encoding="utf-8", xml_declaration=True)

def main():
    import sys
    out_json = sys.argv[1] if len(sys.argv) > 1 else "outputs/market_data.json"
    out_xml = sys.argv[2] if len(sys.argv) > 2 else "outputs/market_data.xml"
    out_csv = sys.argv[3] if len(sys.argv) > 3 else "outputs/market_data.csv"
    rows = fetch_rows()
    write_json(out_json, rows)
    write_xml(out_xml, rows)
    write_csv(out_csv, rows)

if __name__ == "__main__":
    main()
