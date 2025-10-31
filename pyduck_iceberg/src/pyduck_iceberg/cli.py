import duckdb

S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"
NESSIE_ENDPOINT = "http://localhost:19120/iceberg"
WAREHOUSE = "s3://lake/warehouse"

def main():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
    con.execute("SET s3_url_style='path';")
    con.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{S3_SECRET_KEY}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute(f"""
    ATTACH '{WAREHOUSE}' AS nessie_catalog (
      TYPE iceberg,
      ENDPOINT '{NESSIE_ENDPOINT}',
      AUTHORIZATION_TYPE 'none'
    );
    """)
    con.sql("CREATE SCHEMA IF NOT EXISTS nessie_catalog.analytics;")
    con.sql("""
      CREATE TABLE IF NOT EXISTS nessie_catalog.analytics.users (
        id BIGINT, country VARCHAR, ts TIMESTAMP
      ) USING iceberg;
    """)
    con.sql("INSERT INTO nessie_catalog.analytics.users VALUES (1,'DE',now()),(2,'SE',now());")
    print(con.sql("SELECT * FROM nessie_catalog.analytics.users").df())
    snaps = con.sql("""
      SELECT snapshot_id, committed_at
      FROM iceberg_snapshots(nessie_catalog.analytics.users)
      ORDER BY committed_at
    """).df()
    print(snaps)
    snap_id = int(snaps["snapshot_id"].iloc[0])
    print(con.sql(f"""
      SELECT * FROM nessie_catalog.analytics.users AT (VERSION => {snap_id})
    """).df())

if __name__ == "__main__":
    main()

