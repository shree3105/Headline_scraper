import feedparser
import psycopg2
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load .env for local testing
load_dotenv()

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURATION ---
# The "Google Proxy" trick allows us to get headlines from paywalled sites
GOOGLE_NEWS_BASE = "https://news.google.com/rss/search?q="

FEEDS = [
    # 1. Public Financial Feeds (Reliable)
    "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",   # WSJ Markets
    "http://feeds.reuters.com/reuters/businessNews",     # Reuters Business
    "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664", # CNBC Finance
    
    # 2. The "Proxy" Feeds (Bloomberg & FT via Google News)
    # We search for "site:bloomberg.com" restricted to the "Markets" topic
    f"{GOOGLE_NEWS_BASE}site:bloomberg.com+intitle:markets&hl=en-US&gl=US&ceid=US:en",
    f"{GOOGLE_NEWS_BASE}site:ft.com+intitle:markets&hl=en-US&gl=US&ceid=US:en",
    
    # 3. BNN Bloomberg (Canadian affiliate, free official RSS)
    "https://www.bnnbloomberg.ca/content/bnnbloomberg/en/news.rss"
]

def clean_text(text):
    """Truncate to save DB space (Neon Free Tier limit)"""
    if not text: return ""
    # Remove HTML tags if any
    import re
    clean = re.sub('<[^<]+?>', '', text)
    return clean[:300]  # Hard limit 300 chars for description

def run_scraper():
    # 1. Connect to Neon
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL environment variable is missing!")
    
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    # 2. Ensure Table Exists (Idempotent)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS headlines (
            link TEXT PRIMARY KEY,
            title TEXT,
            description TEXT,
            source TEXT,
            published TIMESTAMP,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()

    total_new = 0
    
    # 3. Loop Through Feeds
    for url in FEEDS:
        try:
            logging.info(f"Checking: {url}...")
            feed = feedparser.parse(url)
            logging.info(f"  > Found {len(feed.entries)} entries.")
            
            feed_new_count = 0
            for entry in feed.entries:
                # 1. URL Cleaning (Strip query params)
                link = entry.link.split('?')[0]
                
                raw_title = entry.title
                
                # 2. Stop Phrases (Discard entirely)
                stop_phrases = [
                    "Video:", "Listen:", "Podcast:", "3-Minute MLIV", 
                    "Research and Markets", "Net Asset Value", "summary", 
                    "historical prices", "Profile and Biography", "Director Declaration", 
                    "Inv Trust", "Company Announcement", "Amundi", "OTC Markets"
                ]
                if any(p.lower() in raw_title.lower() for p in stop_phrases):
                    logging.info(f"    - Discarded (Stop Phrase): {raw_title[:50]}...")
                    continue
                
                # Specific check for "Watch" at start (to avoid "Fed Watch" false positives)
                if raw_title.lower().startswith("watch ") or "watch:" in raw_title.lower():
                    logging.info(f"    - Discarded (Watch): {raw_title[:50]}...")
                    continue
                
                # 3. Source Extraction & Title Cleaning
                # Google News often formats titles as "Headline Text - Source Name"
                source = "Google News" # Default fallback
                title = raw_title
                
                # Check for source suffixes
                import re
                # Match " - Source" at the end of the string
                match = re.search(r'(.*?) - (Bloomberg|Financial Times|Reuters|WSJ|CNBC|The Wall Street Journal)$', raw_title)
                if match:
                    title = match.group(1) # The headline part
                    source_str = match.group(2)
                    
                    # Clean " - Company Announcement" if present
                    title = title.replace(" - Company Announcement", "")
                    
                    # Normalize Source Names
                    if "Bloomberg" in source_str: source = "Bloomberg"
                    elif "Financial Times" in source_str: source = "Financial Times"
                    elif "Reuters" in source_str: source = "Reuters"
                    elif "WSJ" in source_str or "Wall Street" in source_str: source = "WSJ"
                    elif "CNBC" in source_str: source = "CNBC"
                else:
                    # Fallback to URL detection if title didn't have the suffix
                    if "wsj.com" in link: source = "WSJ"
                    elif "reuters.com" in link: source = "Reuters"
                    elif "cnbc.com" in link: source = "CNBC"
                    elif "bloomberg" in link: source = "Bloomberg"
                    elif "ft.com" in link: source = "Financial Times"

                # 4. Description Logic (Save space)
                raw_desc = clean_text(entry.get('summary', entry.get('description', '')))
                # If description is just the title (common in RSS), drop it
                if raw_desc.strip() == raw_title.strip() or raw_desc.strip() == title.strip():
                    desc = ""
                else:
                    desc = raw_desc

                # --- FILTER: NONSENSE (Existing) ---
                if len(title) < 15: 
                    logging.info(f"    - Discarded (Too Short): {title[:50]}...")
                    continue
                
                skip_keywords = ["market talk", "morning bid", "evening bid", "breakingviews", "roundup", "factbox"]
                if any(k in title.lower() for k in skip_keywords): 
                    logging.info(f"    - Discarded (Keyword): {title[:50]}...")
                    continue

                # 5. Insert
                try:
                    cur.execute("""
                        INSERT INTO headlines (link, title, description, source, published)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (link) DO NOTHING;
                    """, (link, title, desc, source, entry.get("published", datetime.now())))
                    
                    if cur.rowcount > 0:
                        total_new += 1
                        feed_new_count += 1
                        logging.info(f"    + Added: {title[:50]}...")
                except Exception as e:
                    conn.rollback()
                    logging.error(f"Row Error: {e}")

            conn.commit()
            logging.info(f"  > Saved {feed_new_count} new from this feed.")
            
        except Exception as e:
            logging.error(f"Feed Failed {url}: {e}")

    cur.close()
    conn.close()
    logging.info(f"✅ Job Complete. Added {total_new} new headlines.")

if __name__ == "__main__":
    run_scraper()
