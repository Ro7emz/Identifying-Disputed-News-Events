import requests
from bs4 import BeautifulSoup
import sqlite3
import re
import time
import random
from datetime import datetime

DB_PATH = "/home/israel-iran-war/Desktop/DisputedNews/news.db"
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
})

def get_connection():
    return sqlite3.connect(DB_PATH)

# ==========================================
# לוגיקת תגובות משופרת (Ynet)
# ==========================================
def fetch_ynet_comments(article_url, article_id_in_db):
    try:
        # חילוץ מזהה כתבה מהלינק של Ynet
        art_id = article_url.split('/')[-1].split('#')[0]
        if not art_id.startswith('art'): 
            # לפעמים ה-ID הוא רק מספרים בסוף
            art_id = re.findall(r'\d+', art_id)[-1]
        
        api_url = f"https://www.ynet.co.il/bin/en/public/v1/talkbacks/get/{art_id}"
        res = session.get(api_url, timeout=10)
        
        if res.status_code == 200:
            data = res.json()
            comments = data.get('data', {}).get('talkbacks', [])
            conn = get_connection()
            for cb in comments:
                conn.execute("""
                    INSERT OR IGNORE INTO Comments 
                    (news_article_id, author_name, comment_title, comment_text, likes, dislikes, source_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (article_id_in_db, cb.get('user'), cb.get('title'), cb.get('text'), 
                      cb.get('up', 0), cb.get('down', 0), article_url))
            conn.commit()
            conn.close()
            return len(comments)
    except Exception as e:
        print(f"      [!] שגיאת Ynet Comments: {e}")
    return 0

# ==========================================
# לוגיקת תגובות משופרת (Mako)
# ==========================================
def fetch_mako_comments(article_url, article_id_in_db):
    try:
        # ב-Mako ה-ID נמצא ב-URL אחרי "Article-"
        match = re.search(r"Article-([a-f0-9]+)", article_url)
        if not match: return 0
        mako_id = match.group(1)
        
        api_url = f"https://www.mako.co.il/web-services/PostGetArticleTalkbacks.ashx?articleId={mako_id}&page=1"
        res = session.get(api_url, headers={"Referer": article_url}, timeout=10)
        
        if res.status_code == 200 and res.text:
            soup = BeautifulSoup(res.text, "html.parser")
            items = soup.select(".talkback_item, .comment_body")
            conn = get_connection()
            for item in items:
                author = item.select_one(".talkback_author, .author_name")
                content = item.select_one(".talkback_content, .comment_text")
                if content:
                    conn.execute("""
                        INSERT OR IGNORE INTO Comments (news_article_id, author_name, comment_text, source_url)
                        VALUES (?, ?, ?, ?)
                    """, (article_id_in_db, author.get_text(strip=True) if author else "אנונימי", 
                          content.get_text(strip=True), article_url))
            conn.commit()
            conn.close()
            return len(items)
    except Exception as e:
        print(f"      [!] שגיאת Mako Comments: {e}")
    return 0

# ==========================================
# פונקציית העיבוד המרכזית
# ==========================================
def process_article(url, outlet_id, title, cat_name):
    conn = get_connection()
    cur = conn.cursor()
    
    # בדיקה אם הכתבה קיימת
    cur.execute("SELECT news_article_id FROM News_articles WHERE link = ?", (url,))
    row = cur.fetchone()
    
    if row:
        article_id = row[0]
        # נבדוק אם יש תגובות ב-DB
        cur.execute("SELECT COUNT(*) FROM Comments WHERE news_article_id = ?", (article_id,))
        count_in_db = cur.fetchone()[0]
        conn.close()
        
        if count_in_db == 0:
            # אם אין תגובות, ננסה לאסוף עכשיו (השלמה רטרואקטיבית)
            c_count = fetch_ynet_comments(url, article_id) if outlet_id == 3 else fetch_mako_comments(url, article_id)
            if c_count > 0:
                print(f"    💬 נוספו {c_count} תגובות לכתבה קיימת.")
        return

    # שמירת כתבה חדשה (כאן הוספתי את הקוד המקורי שלך כדי לא להרוס)
    try:
        res = session.get(url, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")
        # חילוץ תוכן בסיסי
        p_tags = [p.get_text(strip=True) for p in soup.find_all("p") if len(p.get_text()) > 30]
        text = "\n\n".join(p_tags)
        
        cur.execute("""
            INSERT INTO News_articles (news_outlet_id, date, title, link, text, type, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (outlet_id, datetime.now().strftime("%Y-%m-%d"), title, url, text, cat_name, datetime.now().strftime("%d/%m/%Y %H:%M")))
        
        new_id = cur.lastrowid
        conn.commit()
        conn.close()
        print(f"  [V] נשמר: {title[:40]}...")
        
        # איסוף תגובות מיד לאחר השמירה
        c_count = fetch_ynet_comments(url, new_id) if outlet_id == 3 else fetch_mako_comments(url, new_id)
        if c_count > 0:
            print(f"    💬 נאספו {c_count} תגובות.")
            
    except Exception as e:
        print(f"  [X] שגיאה בשמירה: {e}")

# ==========================================
# הסטרטר (Main)
# ==========================================
if __name__ == "__main__":
    print(f"🚀 סריקה התחילה: {datetime.now().strftime('%H:%M:%S')}")
    
    # דוגמה ל-Mako (כדי לבדוק תגובות)
    try:
        r = session.get("https://www.mako.co.il/news-military", timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        links = soup.find_all("a", href=True)
        for a in links:
            href = a['href']
            if "Article-" in href:
                full_url = "https://www.mako.co.il" + href if href.startswith("/") else href
                process_article(full_url, 1, a.get_text(strip=True) or "כתבה", "ביטחון")
                time.sleep(random.uniform(3, 5))
    except Exception as e:
        print(f"שגיאה כללית: {e}")

    print("🏁 סיימנו.")
