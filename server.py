import os, urllib
from sqlalchemy import create_engine, text

server  = os.getenv("MSSQL_SERVER")        # e.g. MSSQLSERVER01  (or MSSQLSERVER01,1433)
database= os.getenv("MSSQL_DATABASE")      # e.g. KCC
driver  = os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")

odbc = (
    f"DRIVER={{{driver}}};"      # ← keep the curly braces around the driver name
    f"SERVER=tcp:{server};"
    f"DATABASE={database};"
    "Trusted_Connection=Yes;"    # ← Windows Auth (no SQL password)
    "Encrypt=Yes;"               # ← fine for Driver 18
    "TrustServerCertificate=Yes;"# ← flip to No if you have proper TLS certs
)

engine = create_engine(
    "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc),
    fast_executemany=True,
    future=True,
)

# quick smoke test
with engine.begin() as conn:
    print(conn.execute(text(
        "SELECT SUSER_SNAME() AS win_user, DB_NAME() AS db, SERVERPROPERTY('InstanceName') AS inst"
    )).fetchone())
