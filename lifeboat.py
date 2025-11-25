import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")
BACKUP_FILE = "headlines_backup.csv"

def lifeboat():
    try:
        conn = psycopg2.connect(DB_URL)
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        
        print("--- CSV Lifeboat Operation ---")
        
        # 1. Export to CSV
        print("1. Exporting data to local CSV...")
        with open(BACKUP_FILE, 'w', encoding='utf-8') as f:
            cur.copy_expert("COPY headlines TO STDIN WITH (FORMAT CSV)", f)
        print("   Export complete.")
        
        # 2. Drop Table
        print("2. Dropping 'headlines' table...")
        cur.execute("DROP TABLE headlines;")
        print("   Table dropped.")
        
        # 3. Create Table
        print("3. Recreating 'headlines' table...")
        cur.execute("""
            CREATE TABLE headlines (
                link TEXT PRIMARY KEY,
                title TEXT,
                description TEXT,
                source TEXT,
                published TIMESTAMP,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        print("   Table created.")
        
        # 4. Import from CSV
        print("4. Restoring data from CSV...")
        with open(BACKUP_FILE, 'r', encoding='utf-8') as f:
            cur.copy_expert("COPY headlines FROM STDIN WITH (FORMAT CSV)", f)
        print("   Restore complete.")
        
        # 5. Check Size
        print("5. Checking new size...")
        cur.execute("SELECT pg_size_pretty(pg_total_relation_size('headlines'));")
        size = cur.fetchone()[0]
        print(f"New Table Size: {size}")
        
        cur.close()
        conn.close()
        
        # Cleanup
        if os.path.exists(BACKUP_FILE):
            os.remove(BACKUP_FILE)
            print("Backup file deleted.")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    lifeboat()
