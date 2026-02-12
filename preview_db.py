import sqlite3
import pandas as pd
conn = sqlite3.connect("database.db")  
tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
for table_name in tables['name']:
    print(f"{table_name} (first 10 rows)")
    df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 10;", conn)
    print(df, "\n")
conn.close()
