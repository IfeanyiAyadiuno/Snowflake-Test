import os
from dotenv import load_dotenv
import snowflake.connector as sf
import pandas as pd  # for DataFrame

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
        role="RETEAMROLE",  # ✅ force this role
    )

    cur = conn.cursor()

    query = """
        SELECT METERID, DTTM, VOLENTERGAS, DURONOR
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.METERORIFICEENTRIES_V1
        LIMIT 20;
    """
    cur.execute(query)
    rows = cur.fetchall()
    columns = [col[0] for col in cur.description]

    # show with pandas DataFrame
    df = pd.DataFrame(rows, columns=columns)

    # print column headers
    print("Columns:", df.columns.to_list())
    # print first few rows
    print("Sample rows:")
    print(df.head(10))  # change number to see more

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
