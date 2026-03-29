import psycopg2
import pandas as pd

# Połączenie z bazą
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="flights_db",
    user="admin",
    password="admin123"
)
cur = conn.cursor()

# Tworzenie schematu bronze
cur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
cur.execute("""
    DROP TABLE IF EXISTS bronze.flights;
    CREATE TABLE bronze.flights (
        FL_DATE TEXT,
        AIRLINE TEXT,
        AIRLINE_DOT TEXT,
        AIRLINE_CODE TEXT,
        DOT_CODE TEXT,
        FL_NUMBER TEXT,
        ORIGIN TEXT,
        ORIGIN_CITY TEXT,
        DEST TEXT,
        DEST_CITY TEXT,
        CRS_DEP_TIME TEXT,
        DEP_TIME TEXT,
        DEP_DELAY TEXT,
        TAXI_OUT TEXT,
        WHEELS_OFF TEXT,
        WHEELS_ON TEXT,
        TAXI_IN TEXT,
        CRS_ARR_TIME TEXT,
        ARR_TIME TEXT,
        ARR_DELAY TEXT,
        CANCELLED TEXT,
        CANCELLATION_CODE TEXT,
        DIVERTED TEXT,
        CRS_ELAPSED_TIME TEXT,
        ELAPSED_TIME TEXT,
        AIR_TIME TEXT,
        DISTANCE TEXT,
        DELAY_DUE_CARRIER TEXT,
        DELAY_DUE_WEATHER TEXT,
        DELAY_DUE_NAS TEXT,
        DELAY_DUE_SECURITY TEXT,
        DELAY_DUE_LATE_AIRCRAFT TEXT
    );
""")
conn.commit()
print("Schemat bronze i tabela flights utworzone!")

# Ładowanie CSV partiami
print("⏳ Ładowanie danych...")
chunk_size = 50000
total = 0

for chunk in pd.read_csv("data/raw/flights_sample_3m.csv", chunksize=chunk_size, dtype=str):
    chunk = chunk.where(pd.notnull(chunk), None)
    rows = [tuple(row) for row in chunk.itertuples(index=False)]
    placeholders = ",".join(["%s"] * len(chunk.columns))
    cur.executemany(f"INSERT INTO bronze.flights VALUES ({placeholders})", rows)
    conn.commit()
    total += len(chunk)
    print(f"   Załadowano: {total:,} wierszy")

cur.close()
conn.close()
print(f"Gotowe! Łącznie: {total:,} wierszy w bronze.flights")