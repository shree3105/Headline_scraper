import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")

def prune_aggressive():
    try:
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()
        
        print("--- Aggressive Pruning ---")
        
        # 1. Delete Non-Crisis Years (2006, 2007, 2016-2019)
        print("Deleting non-crisis years (2006, 2007, 2016-2019)...")
        cur.execute("""
            DELETE FROM headlines 
            WHERE EXTRACT(YEAR FROM published) IN (2006, 2007, 2016, 2017, 2018, 2019);
        """)
        print(f"Deleted {cur.rowcount} rows.")
        
        # 2. Prune 2009 (Delete 50%)
        print("Pruning 50% of 2009...")
        cur.execute("""
            DELETE FROM headlines 
            WHERE EXTRACT(YEAR FROM published) = 2009 
            AND random() < 0.5;
        """)
        print(f"Deleted {cur.rowcount} rows.")

        # 3. Prune 2008 (Delete 50% of remaining)
        print("Pruning 50% of 2008...")
        cur.execute("""
            DELETE FROM headlines 
            WHERE EXTRACT(YEAR FROM published) = 2008 
            AND random() < 0.5;
        """)
        print(f"Deleted {cur.rowcount} rows.")
        
        conn.commit()
        
        # 4. Check Stats
        cur.execute("SELECT COUNT(*) FROM headlines;")
        count = cur.fetchone()[0]
        print(f"\nNew Total Row Count: {count}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    prune_aggressive()
