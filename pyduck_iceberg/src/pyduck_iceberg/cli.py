import time
import json
import duckdb
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from urllib.parse import quote_plus, quote

S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"
NESSIE_ENDPOINT = "http://localhost:19120/iceberg"
WAREHOUSE = "local"  # named warehouse in Nessie

def wait_for_iceberg(timeout=60):
    url = f"{NESSIE_ENDPOINT}/v1/config?warehouse={quote_plus(WAREHOUSE)}"
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            with urlopen(url, timeout=2) as r:
                if r.status == 200:
                    return
        except Exception as e:
            last_err = e
        time.sleep(1)
    raise RuntimeError(f"Iceberg REST not ready: {last_err}")

def ensure_namespace(ns: str):
    prefix = quote(f"main|{WAREHOUSE}", safe="")
    # HEAD: exists?
    head = Request(f"{NESSIE_ENDPOINT}/v1/{prefix}/namespaces/{quote(ns, safe='')}", method="HEAD")
    try:
        with urlopen(head, timeout=5) as r:
            if 200 <= r.status < 300:
                return
    except HTTPError as e:
        if e.code != 404:
            raise
    # POST: create
    create = Request(
        f"{NESSIE_ENDPOINT}/v1/{prefix}/namespaces",
        data=json.dumps({"namespace": [ns]}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(create, timeout=10) as r:
        if r.status not in (200, 201):
            raise RuntimeError(f"Create namespace failed: HTTP {r.status}")

def main():
    wait_for_iceberg()

    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
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

    ensure_namespace("analytics")

    # Note: no 'USING iceberg' when creating in an Iceberg catalog
    con.sql("""
      CREATE TABLE IF NOT EXISTS nessie_catalog.analytics.users (
        id BIGINT, country VARCHAR, ts TIMESTAMP
      );
    """)
    con.sql("INSERT INTO nessie_catalog.analytics.users VALUES (1,'DE',now()),(2,'SE',now());")
    print(con.sql("SELECT * FROM nessie_catalog.analytics.users").df())

if __name__ == "__main__":
    main()

