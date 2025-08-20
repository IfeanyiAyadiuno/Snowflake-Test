import os
from dotenv import load_dotenv
import snowflake.connector as sf

load_dotenv()

USER = os.getenv("SNOW_USER")
PWD = os.getenv("SNOW_PASSWORD")
ACCOUNT = os.getenv("SNOW_ACCOUNT")
WAREHOUSE = os.getenv("SNOW_WAREHOUSE")
DATABASE = os.getenv("SNOW_DATABASE")
SCHEMA = os.getenv("SNOW_SCHEMA")

def main():
    conn = sf.connect(
        user=USER,
        password=PWD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        role="RETEAMROLE",   # ✅ force this role
        # if SSO/Duo required:
        # authenticator="externalbrowser",
    )

    cur = conn.cursor()

    # show current context
    cur.execute("select * from PACIFICCANBRIAM_PV30.PACIFICCANBRIAM_PV30_DBO.PVT_PVUNITMETERORIFICE")
    print("Session:", cur.fetchone())

    # simple test query
    cur.execute("SELECT CURRENT_VERSION()")
    print("Snowflake version:", cur.fetchone()[0])

    cur.close()
    conn.close()
    print("✅ Connected and closed cleanly.")

if __name__ == "__main__":
    main()
