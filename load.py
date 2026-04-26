import os
import json
import hashlib
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_PARAMS = {
    "dbname":   os.getenv("DB_NAME", "NYC"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "host":     os.getenv("DB_HOST", "db"),
    "port":     os.getenv("DB_PORT", "5432"),
}

csv_path   = Path("NewYork_transportations.csv")
state_path = Path("pipeline_state.json")


def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        print("Database connection successful.")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise


def compute_file_hash(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        hasher.update(f.read())
    return hasher.hexdigest()


def load_state() -> dict:
    if state_path.exists():
        return json.loads(state_path.read_text())
    return {}


def save_state(state: dict):
    state_path.write_text(json.dumps(state, indent=2))


def create_tables(cursor):
    """Create the 3-table schema if it doesn't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transport_types (
            transport_type_id SERIAL PRIMARY KEY,
            transport_type VARCHAR(20) UNIQUE
        );

        CREATE TABLE IF NOT EXISTS daily_ridership (
            id SERIAL PRIMARY KEY,
            date DATE,
            ridership NUMERIC(12, 2),
            transport_type VARCHAR(20) REFERENCES transport_types(transport_type),
            year INTEGER
        );

        CREATE TABLE IF NOT EXISTS yearly_ridership (
            id SERIAL PRIMARY KEY,
            year INTEGER,
            transport_type VARCHAR(20) REFERENCES transport_types(transport_type),
            total_ridership NUMERIC(15, 2)
        );
    """)

    # Unique indexes
    cursor.execute("""
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname='daily_ridership_date_transport_type_key') THEN
                CREATE UNIQUE INDEX daily_ridership_date_transport_type_key ON daily_ridership (date, transport_type);
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname='yearly_ridership_year_transport_type_key') THEN
                CREATE UNIQUE INDEX yearly_ridership_year_transport_type_key ON yearly_ridership (year, transport_type);
            END IF;
        END$$
    """)


def insert_transport_types(cursor, df):
    unique_types = df["transport_type"].dropna().unique().tolist()
    cursor.executemany(
        "INSERT INTO transport_types (transport_type) VALUES (%s) ON CONFLICT DO NOTHING;",
        [(t,) for t in unique_types],
    )
    print(f"Upserted {len(unique_types)} transport types.")


def insert_daily_ridership(cursor, rows, truncate=False):
    if truncate:
        cursor.execute("TRUNCATE TABLE daily_ridership CASCADE;")
        print("Cleared daily_ridership for full reload.")
    if not rows:
        return 0
    execute_values(cursor, """
        INSERT INTO daily_ridership (date, ridership, transport_type, year)
        VALUES %s ON CONFLICT DO NOTHING;
    """, rows, page_size=1000)
    return len(rows)


def refresh_yearly_aggregates(cursor, truncate=False):
    if truncate:
        cursor.execute("TRUNCATE TABLE yearly_ridership;")
        print("Cleared yearly_ridership for full reload.")
    cursor.execute("""
        INSERT INTO yearly_ridership (year, transport_type, total_ridership)
        SELECT year, transport_type, SUM(ridership)
        FROM daily_ridership
        GROUP BY year, transport_type
        ON CONFLICT (year, transport_type) DO UPDATE
          SET total_ridership = EXCLUDED.total_ridership;
    """)
    print("Yearly aggregates refreshed.")


def verify_database(cursor):
    cursor.execute("SELECT COUNT(*) FROM transport_types;")
    print(f"Transport Types: {cursor.fetchone()[0]} records")
    cursor.execute("SELECT COUNT(*) FROM daily_ridership;")
    print(f"Daily Ridership: {cursor.fetchone()[0]} records")
    cursor.execute("SELECT COUNT(*) FROM yearly_ridership;")
    print(f"Yearly Ridership: {cursor.fetchone()[0]} records")


def process():
    df = pd.read_csv(csv_path, parse_dates=["date"])
    df = df.rename(columns={c: c.strip() for c in df.columns})
    print(f"Loaded {len(df)} rows from {csv_path}")

    file_hash  = compute_file_hash(csv_path)
    state      = load_state()
    full_reload    = False
    rows_to_insert = df

    if not state:
        full_reload = True
        print("First run: full load.")
    elif file_hash != state.get("file_hash"):
        if len(df) > state.get("row_count", 0):
            rows_to_insert = df.iloc[state.get("row_count", 0):]
            print(f"Incremental load: {len(rows_to_insert)} new rows.")
        else:
            full_reload = True
            print("File changed — running full reload.")
    else:
        print("No changes detected. Skipping load.")
        return

    conn   = get_db_connection()
    cursor = conn.cursor()
    try:
        create_tables(cursor)
        insert_transport_types(cursor, df)

        daily_rows = [
            (row["date"], float(row["ridership"]), row["transport_type"], int(row["year"]))
            for _, row in rows_to_insert.iterrows()
        ]
        inserted = insert_daily_ridership(cursor, daily_rows, truncate=full_reload)
        refresh_yearly_aggregates(cursor, truncate=full_reload)

        print(f"Inserted {inserted} daily ridership rows.")
        conn.commit()
        save_state({"file_hash": file_hash, "row_count": len(df), "last_run": datetime.utcnow().isoformat()})
        verify_database(cursor)

    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    process()