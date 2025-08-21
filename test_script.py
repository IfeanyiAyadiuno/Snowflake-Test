import os
from dotenv import load_dotenv
import snowflake.connector as sf
import pandas as pd

load_dotenv()

USER = os.getenv("SNOW_USER")
PWD = os.getenv("SNOW_PASSWORD")
ACCOUNT = os.getenv("SNOW_ACCOUNT")
WAREHOUSE = os.getenv("SNOW_WAREHOUSE")
DATABASE = os.getenv("SNOW_DATABASE", "PACIFICCANBRIAM_PV30")
SCHEMA = os.getenv("SNOW_SCHEMA", "UNITSMETRIC")
ROLE = "RETEAMROLE"
NEWEST_N = 10

# Same sources as the Excel pulls
QUERIES = {
    "meter_orifice_entries": f"""
        SELECT METERID, DTTM::TIMESTAMP_NTZ AS DTTM, VOLENTERGAS, DURONOR
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.METERORIFICEENTRIES_V1
        ORDER BY DTTM DESC
        LIMIT {NEWEST_N};
    """,
    "unit_comp_params": f"""
        SELECT IDRECPARENT, DTTM::TIMESTAMP_NTZ AS DTTM, PRESTUB, PRESCAS, SZCHOKE
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.PVUNITCOMPPARAM
        ORDER BY DTTM DESC
        LIMIT {NEWEST_N};
    """,
    "well_ratios": f"""
        SELECT UNITID, DTTM::TIMESTAMP_NTZ AS DTTM, CGR, WGR
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.WELLRATIOS_V1
        ORDER BY DTTM DESC
        LIMIT {NEWEST_N};
    """,
    "meter_orifice_ecf": f"""
        SELECT METERID, DTTM::TIMESTAMP_NTZ AS DTTM, EFFLUENTCORRFACT
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.METERORIFICEECF_V1
        ORDER BY DTTM DESC
        LIMIT {NEWEST_N};
    """,
    "well_daily_gathered": f"""
        SELECT UNITID, DTTM::TIMESTAMP_NTZ AS DTTM, RATEHCLIQ, RATEGAS
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.WELLDAILYGATHERED_V1
        ORDER BY DTTM DESC
        LIMIT {NEWEST_N};
    """,
    "unit_alloc_monthday": f"""
        SELECT IDRECCOMP, DTTM::TIMESTAMP_NTZ AS DTTM,
               VOLNEWPRODALLOCGAS, VOLNEWPRODALLOCCOND,
               VOLPRODALLOCWATER, VOLNEWPRODALLOCNGL,
               VolProdGathGas, VOLPRODGATHHCLIQ
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.PVUNITALLOCMONTHDAY
        ORDER BY DTTM DESC
        LIMIT {NEWEST_N};
    """,
    "comp_gath_monthday_calc": f"""
        SELECT IDRECCOMP, DTTM::TIMESTAMP_NTZ AS DTTM, VOLWATER
        FROM PACIFICCANBRIAM_PV30.PACIFICCANBRIAM_PV30_DBO.PVT_PVUNITCOMPGATHMONTHDAYCALC
        ORDER BY DTTM DESC
        LIMIT {NEWEST_N};
    """,
}

def main():
    conn = sf.connect(
        user=USER, password=PWD, account=ACCOUNT,
        warehouse=WAREHOUSE, database=DATABASE, schema=SCHEMA, role=ROLE
    )
    cur = conn.cursor()
    try:
        for name, sql in QUERIES.items():
            print(f"\n===== {name} (newest {NEWEST_N}) =====")
            try:
                cur.execute(sql)
                rows = cur.fetchall()
                cols = [c[0] for c in cur.description]
                df = pd.DataFrame(rows, columns=cols)
                if df.empty:
                    print("(no rows returned)")
                else:
                    # show full 10 rows without truncation
                    print(df.to_string(index=False))
            except Exception as e:
                print(f"[ERROR] {name}: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()