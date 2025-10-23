import os
import time
import psycopg2

def main():
    host = os.getenv("DB_HOST","db")
    port = int(os.getenv("DB_PORT","5432"))
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB")
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db)
            conn.close()
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("Postgres not available")

if __name__ == "__main__":
    main()
