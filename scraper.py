import os
import psycopg2
from gnews import GNews
from datetime import datetime
from dotenv import load_dotenv
import logging
import time

# Load Environment
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

NEON_DB_URL = os.getenv("DATABASE_URL")

def get_neon_connection():
    return psycopg2.connect(NEON_DB_URL)

def run_collector():
    logging.info("Initializing GNews Collector (Production Mode)...")
    # Fetch last 24 hours for hourly runs (overlap ensures safety)
    google_news = GNews(language='en', country='US', period='1d', max_results=50)
    
    # Premium Sources
    sources = [
        "Bloomberg", "Reuters", "CNBC", "Financial Times", 
        "Wall Street Journal", "MarketWatch", "Yahoo Finance", "Forbes"
    ]
    
    # Topics to widen the net (Multi-Query Strategy) - Removed Crypto/Tech
    topics = ["Markets", "Economy", "Energy", "Central Banks", "Real Estate"]
    
    all_headlines = []
    
    for source in sources:
        for topic in topics:
            logging.info(f"Fetching '{topic}' from {source}...")
            
            # Construct Query: "Markets site:bloomberg.com"
            query = f"{topic} site:{source.lower().replace(' ', '')}.com"
            
            try:
                news = google_news.get_news(query)
                
                for article in news:
                    try:
                        # GNews date parsing
                        pub_date = datetime.strptime(article['published date'], "%a, %d %b %Y %H:%M:%S %Z")
                    except:
                        pub_date = datetime.now()
                        
                    all_headlines.append({
                        'title': article['title'],
                        'link': article['url'],
                        'published': pub_date,
                        'source': source,
                        'topic_tag': topic
                    })
                
                # Be nice to Google
                time.sleep(1) 
                
            except Exception as e:
                logging.error(f"Failed to fetch {topic} from {source}: {e}")

    logging.info(f"Collected {len(all_headlines)} raw headlines.")
    
    # Filter Noise & Deduplicate
    noise_keywords = [
        "Movie", "Box Office", "Review", "Best of", "Shopping", "Deals", 
        "Celebrity", "Sport", "Horoscope", "Gift Guide", "The 10", "Top 10", 
        "How to", "Explained", "What to Know"
    ]
    
    # 1. Load Existing Titles from DB (Last 24h) to prevent "Semantic Duplicates"
    conn = get_neon_connection()
    cur = conn.cursor()
    cur.execute("SELECT title FROM headlines WHERE published >= NOW() - INTERVAL '24 hours'")
    existing_titles = {row[0].lower().strip() for row in cur.fetchall()}
    cur.close()
    conn.close()
    
    seen_titles = existing_titles # Initialize with DB history
    unique_headlines = []
    
    for h in all_headlines:
        title_clean = h['title'].lower().strip()
        # 1. Check Noise
        if any(k.lower() in title_clean for k in noise_keywords):
            continue
        # 2. Check Duplicate (against current batch AND db history)
        if title_clean in seen_titles:
            continue
            
        seen_titles.add(title_clean)
        unique_headlines.append(h)
        
    logging.info(f"Filtered out {len(all_headlines) - len(unique_headlines)} noisy/duplicate headlines.")
    
    if not unique_headlines:
        logging.warning("No headlines left after filtering!")
        return

    conn = get_neon_connection()
    cur = conn.cursor()
    
    logging.info("Saving to Neon Database...")
    inserted = 0
    for h in unique_headlines:
        try:
            # Use ON CONFLICT DO NOTHING to avoid duplicates
            cur.execute("""
                INSERT INTO headlines (title, link, published, source, scraped_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (link) DO NOTHING
            """, (h['title'], h['link'], h['published'], h['source']))
            if cur.rowcount > 0:
                inserted += 1
        except Exception as e:
            logging.error(f"Error inserting {h['title']}: {e}")
            conn.rollback()
            continue
            
    conn.commit()
    conn.close()
    logging.info(f"Successfully seeded {inserted} new headlines.")

if __name__ == "__main__":
    run_collector()
