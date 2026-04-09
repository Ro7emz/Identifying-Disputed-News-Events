import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import time
import random
import re

# הגדרות נתיבים
DB_PATH = "/home/israel-iran-war/Desktop/DisputedNews/news.db"

def get_now_formatted():
    return datetime.now().strftime("%d/%m/%Y %H:%M")

def fetch_mako_comments(article_soup, news_article_id, article_url):
    """
    מחלץ תגובות מ-N12. 
    שיטה חדשה: מחפשים את ה-ArticleId בתוך ה-JSON הפנימי של הדף.
    """
    try:
        # חילוץ ה-ID מה-URL (השיטה הכי אמינה ב-Mako)
        match = re.search(r"Article-([a-f0-9]+|[0-9]+)", article_url)
        if not match:
            print(f"    ⚠️ לא נמצא מזהה כתבה בקישור: {article_url}")
            return
        
        article_id = match.group(1)
        # הכתובת העדכנית של שרת התגובות
        api_url = f"https://www.mako.co.il/web-services/PostGetArticleTalkbacks.ashx?articleId={article_id}&page=1"
        
        # קריטי: Mako דורשים לדעת מאיפה באת (Referer)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": article_url,
            "X-Requested-With": "XMLHttpRequest"
        }
        
        res = requests.get(api_url, headers=headers, timeout=10)
        if res.status_code != 200 or not res.text:
            print(f"    ⚠️ שרת התגובות החזיר תשובה ריקה (Status: {res.status_code})")
            return

        c_soup = BeautifulSoup(res.text, "html.parser")
        # ב-Mako התגובות נמצאות בתוך div עם קלאס talkback_item או בתוך טבלה
        comment_elements = c_soup.find_all(class_=re.compile("talkback_item|comment_body"))
        
        if not comment_elements:
            print(f"    ℹ️ לא נמצאו תגובות גלויות לכתבה הזו ב-API.")
            return

        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        count = 0
        
        for el in comment_elements:
            # חילוץ טקסט ושם (הסלקטורים משתנים מעט בין גרסאות האתר)
            author = el.select_one(".talkback_author, .author_name")
            text = el.select_one(".talkback_content, .comment_text")
            
            if text:
                cur.execute("""
                    INSERT OR IGNORE INTO Comments 
                    (news_article_id, author_name, comment_text, source_url)
                    VALUES (?, ?, ?, ?)
                """, (news_article_id, 
                      author.get_text(strip=True) if author else "אנונימי", 
                      text.get_text(strip=True), 
                      article_url))
                if cur.rowcount > 0: count += 1
        
        conn.commit()
        conn.close()
        print(f"    ✅ הצלחה! נשמרו {count} תגובות חדשות ל-DB.")
        
    except Exception as e:
        print(f"    ❌ שגיאה טכנית באיסוף תגובות: {e}")

# ... שאר הפונקציות של הסקרייפר (scrape_mako_article וכו') נשארות זהות ...
# רק תוודאי שבתוך insert_article_to_db את קוראת ל-fetch_mako_comments בסוף.
