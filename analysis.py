import pandas as pd
import sqlite3
import os

DB_NAME = "FinalDB.db"

FILES = {
    "CENSUS_DATA": "ChicagoCensusData.csv",
    "CHICAGO_PUBLIC_SCHOOLS": "ChicagoPublicSchools.csv",
    "CHICAGO_CRIME_DATA": "ChicagoCrimeData.csv"
}

def load_data_to_db():
    """Loads CSV files into a SQLite database."""
    print(f"\nConnecting to database: {DB_NAME}...\n")
    con = sqlite3.connect(DB_NAME)
    
    for table_name, file_name in FILES.items():
        if os.path.exists(file_name):
            print(f"Loading {file_name} into table {table_name}...")
            df = pd.read_csv(file_name)
            df.to_sql(table_name, con, if_exists='replace', index=False)
        else:
            print(f"WARNING: File {file_name} not found. Skipping.")

    
    print("\nData loading completed.\n")
    return con

def run_query(con, query, description):
    """Executes an SQL query and prints results using Pandas."""
    print(f"\n{description}\n" + "-" * 50)
    try:
        result = pd.read_sql_query(query, con)
        if result.empty:
            print("No rows returned.")
        else:
            print(result)
    except Exception as e:
        print(f"Error executing query: {e}")
    print("=" * 50)

def main():
    con = None
    try:
        con = load_data_to_db()

        queries = [
            ("Total Crimes Recorded",
             "SELECT COUNT(*) as TOTAL_CRIMES FROM CHICAGO_CRIME_DATA;"),

            ("Community Areas with Per Capita Income < 11k",
             "SELECT COMMUNITY_AREA_NAME, COMMUNITY_AREA_NUMBER "
             "FROM CENSUS_DATA WHERE PER_CAPITA_INCOME < 11000;"),

            ("Crimes Involving Minors/Juveniles", """
                SELECT CASE_NUMBER, PRIMARY_TYPE, DESCRIPTION
                FROM CHICAGO_CRIME_DATA 
                WHERE PRIMARY_TYPE LIKE '%MINOR%' 
                OR PRIMARY_TYPE LIKE '%JUVENILE%' 
                OR DESCRIPTION LIKE '%MINOR%' 
                OR DESCRIPTION LIKE '%JUVENILE%';
            """),

            ("Kidnapping Involving Children", """
                SELECT CASE_NUMBER, PRIMARY_TYPE, DESCRIPTION
                FROM CHICAGO_CRIME_DATA 
                WHERE PRIMARY_TYPE LIKE '%KIDNAPPING%' 
                AND DESCRIPTION LIKE '%CHILD%';
            """),

            ("Crimes Occurring in Schools", """
                SELECT DISTINCT DESCRIPTION, LOCATION_DESCRIPTION 
                FROM CHICAGO_CRIME_DATA 
                WHERE LOCATION_DESCRIPTION LIKE '%SCHOOL%';
            """),

            ("Average Safety Score by School Type", """
                SELECT "Elementary, Middle, or High School" AS School_Type,
                       AVG(SAFETY_SCORE) AS AVERAGE_SAFETY_SCORE
                FROM CHICAGO_PUBLIC_SCHOOLS 
                WHERE Safety_Score IS NOT NULL 
                GROUP BY "Elementary, Middle, or High School";
            """),

            ("Community Area with Highest Hardship Index", """
                SELECT COMMUNITY_AREA_NAME
                FROM CENSUS_DATA 
                WHERE HARDSHIP_INDEX = (SELECT MAX(HARDSHIP_INDEX) FROM CENSUS_DATA);
            """),

            ("Community Area with Most Crimes", """
                SELECT COMMUNITY_AREA_NAME
                FROM CENSUS_DATA 
                WHERE COMMUNITY_AREA_NUMBER = (
                    SELECT COMMUNITY_AREA_NUMBER 
                    FROM CHICAGO_CRIME_DATA 
                    WHERE COMMUNITY_AREA_NUMBER IS NOT NULL 
                    GROUP BY COMMUNITY_AREA_NUMBER 
                    ORDER BY COUNT(*) DESC 
                    LIMIT 1
                );
            """)
        ]

        for desc, query in queries:
            run_query(con, query, desc)

    finally:
        if con:
            con.close()
            print("\nAnalysis finished. Database connection closed.\n")

if __name__ == "__main__":
    main()
