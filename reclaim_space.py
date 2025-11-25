import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")

def reclaim_space():
    try:
        conn = psycopg2.connect(DB_URL)
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        
        print("--- Reclaiming Space ---")
        
        # 1. Create New Table
        print("1. Creating 'headlines_new'...")
        cur.execute("CREATE TABLE headlines_new (LIKE headlines INCLUDING ALL);")
        
        # 2. Copy Data
        print("2. Copying data (this might take a moment)...")
        cur.execute("INSERT INTO headlines_new SELECT * FROM headlines;")
        rows = cur.rowcount
        print(f"   Copied {rows} rows.")
        
        # 3. Drop Old Table
        print("3. Dropping old 'headlines' table...")
        cur.execute("DROP TABLE headlines;")
        
        # 4. Rename New Table
        print("4. Renaming 'headlines_new' to 'headlines'...")
        cur.execute("ALTER TABLE headlines_new RENAME TO headlines;")
        
        # 5. Check Size
        print("5. Checking new size...")
        cur.execute("SELECT pg_size_pretty(pg_total_relation_size('headlines'));")
        size = cur.fetchone()[0]
        print(f"New Table Size: {size}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    reclaim_space()
