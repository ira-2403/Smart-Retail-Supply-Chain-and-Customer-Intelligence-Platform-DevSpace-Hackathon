import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATABASE_PATH = BASE_DIR / "database.db"

def verify():
    if not DATABASE_PATH.exists():
        print(f"Database not found at {DATABASE_PATH}")
        return

    conn = sqlite3.connect(str(DATABASE_PATH))
    cursor = conn.cursor()
    
    tables = ['retail_transactions', 'warehouse_inventory', 'products']
    
    for table in tables:
        try:
            count = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"{table}: {count} rows")
            
            # Show a sample
            if count > 0:
                sample = cursor.execute(f"SELECT * FROM {table} LIMIT 1").fetchone()
                print(f"  Sample: {sample}")
        except Exception as e:
            print(f"Error checking {table}: {e}")
            
    conn.close()

if __name__ == "__main__":
    verify()
