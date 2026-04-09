import requests
from bs4 import BeautifulSoup, Tag
import sqlite3
from datetime import datetime
import time
import random
import re
from urllib.parse import urljoin

# ==========================================
# הגדרות וחיבורים
# ==========================================
DB_PATH = "/home/israel-iran-war/Desktop/DisputedNews/news.db"

def get_connection():
    return sqlite3.connect(DB_PATH)

def get_now_formatted():
    return datetime.now().strftime("%d/%m/%Y %H:%M")

# שימוש ב-Session אחד לכל הריצה - מדמה דפדפן שנשאר פתוח
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7"
})

# ==========================================
# פונקציות איסוף תגובות
# ==========================================

def fetch_ynet_comments(article_url, news_article_id):
    try:
        art_id = article_url.split('/')[-1].split('#')[0]
        api_url = f"https://www.ynet.co.il/bin/en/public/v1/talkbacks/get/{art_id}"
        res = session.get(api_url, timeout=10)
        if res.status_code == 200:
            data = res.json()
            comments = data.get('data', {}).get('talkbacks', [])
            conn = get_connection()
            for cb in comments:
                conn.execute("""
                    INSERT OR IGNORE INTO Comments 
                    (news_article_id, parent_comment_id, author_name, comment_title, comment_text, likes, dislikes, source_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (news_article_id, str(cb.get('parent')) if cb.get('parent') else None,
                      cb.get('user'), cb.get('title'), cb.get('text'), cb.get('up', 0), cb.get('down', 0), article_url))
            conn.commit()
            conn.close()
            return len(comments)
    except: return 0

def fetch_mako_comments(article_url, news_article_id):
    try:
        match = re.search(r"Article-([a-f0-9]+|[0-9]+)", article_url)
        if not match: return 0
        article_id = match.group(1)
        api_url = f"https://www.mako.co.il/web-services/PostGetArticleTalkbacks.ashx?articleId={article_id}&page=1"
        
        res = session.get(api_url, headers={"Referer": article_url, "X-Requested-With": "XMLHttpRequest"}, timeout=12)
        if res.status_code == 200:
            c_soup = BeautifulSoup(res.text, "html.parser")
            items = c_soup.select(".talkback_item, .comment_body")
            conn = get_connection()
            for item in items:
                author = item.select_one(".talkback_author, .author_name")
                content = item.select_one(".talkback_content, .comment_text")
                if content:
                    conn.execute("""
                        INSERT OR IGNORE INTO Comments (news_article_id, author_name, comment_text, source_url)
                        VALUES (?, ?, ?, ?)
                    """, (news_article_id, author.get_text(strip=True) if author else "אנונימי", 
                          content.get_text(strip=True), article_url))
            conn.commit()
            conn.close()
            return len(items)
    except: return 0

# ==========================================
# שמירה וניהול
# ==========================================

def process_and_save(details, outlet_id, cat_name):
    conn = get_connection()
    cursor = conn.cursor()
    
    # בדיקה אם הכתבה קיימת
    cursor.execute("SELECT news_article_id FROM News_articles WHERE link = ?", (details['link'],))
    row = cursor.fetchone()
    
    if row:
        article_id = row[0]
        # נבדוק אם יש תגובות, אם אין - ננסה להביא
        cursor.execute("SELECT COUNT(*) FROM Comments WHERE news_article_id = ?", (article_id,))
        if cursor.fetchone()[0] == 0:
            conn.close()
            count = fetch_ynet_comments(details['link'], article_id) if outlet_id == 3 else fetch_mako_comments(details['link'], article_id)
            if count: print(f"    💬 נוספו {count} תגובות לכתבה קיימת.")
        else: conn.close()
        return

    # שמירת כתבה חדשה
    scraped_at = get_now_formatted()
    cursor.execute("""
        INSERT INTO News_articles (news_outlet_id, date, title, description, type, link, text, author_name, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (outlet_id, details['date'], details['title'], details['description'], cat_name, 
          details['link'], details['text'], details['author'], scraped_at))
    
    article_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    print(f"  [V] נשמר: {details['title'][:40]}...")
    if outlet_id == 3: fetch_ynet_comments(details['link'], article_id)
    if outlet_id == 1: fetch_mako_comments(details['link'], article_id)

# ==========================================
# הרצה
# ==========================================

def main():
    print(f"🚀 סריקה מאסיבית התחילה: {get_now_formatted()}")

    # 1. YNET - נאסוף יותר כתבות
    print("\n--- [3] YNET ---")
    try:
        r = session.get("https://www.ynet.co.il/news/247", timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        links = list(set([urljoin("https://www.ynet.co.il", a['href']) for a in soup.find_all("a", href=True) if "/article/" in a['href']]))
        print(f"🔍 נמצאו {len(links)} כתבות פוטנציאליות ב-Ynet")
        for link in links[:25]: # ננסה 25 כתבות
            r_art = session.get(link, timeout=15)
            s_art = BeautifulSoup(r_art.text, "html.parser")
            t = s_art.select_one("h1.mainTitle, .mainTitle")
            if t:
                desc = s_art.select_one("h2.subTitle, .subTitle")
                paragraphs = [p.get_text(strip=True) for p in s_art.select(".article-body p") if len(p.get_text()) > 30]
                process_and_save({
                    "title": t.get_text(strip=True), "description": desc.get_text(strip=True) if desc else "",
                    "text": "\n\n".join(paragraphs), "link": link, "date": datetime.now().strftime("%Y-%m-%d"), "author": "Ynet"
                }, 3, "חדשות")
    except Exception as e: print(f"❌ שגיאה ב-Ynet: {e}")

    # 2. MAKO - נאסוף יותר דפים
    print("\n--- [1] MAKO ---")
    for page in range(1, 3): # סורק 2 דפים של כתבות (בערך 40 כתבות)
        print(f"📄 סורק דף {page}...")
        try:
            r = session.get(f"https://www.mako.co.il/news-military?page={page}", timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            links = list(set([urljoin("https://www.mako.co.il", a['href']) for a in soup.find_all("a", href=True) if "Article" in a['href']]))
            for link in links:
                r_art = session.get(link, timeout=15)
                s_art = BeautifulSoup(r_art.text, "html.parser")
                t = s_art.find("h1")
                if t:
                    desc = s_art.find("p", class_=lambda x: x and "ArticleSubtitle_root" in x) or s_art.find("h2")
                    p_tags = [p.get_text(strip=True) for p in s_art.find_all("p") if len(p.get_text()) > 30]
                    process_and_save({
                        "title": t.get_text(strip=True), "description": desc.get_text(strip=True) if desc else "",
                        "text": "\n\n".join(p_tags), "link": link, "date": datetime.now().strftime("%Y-%m-%d"), "author": "Mako"
                    }, 1, "ביטחון / צבא")
                    time.sleep(random.uniform(5, 8)) # המתנה הכרחית ב-Mako
        except Exception as e: print(f"❌ שגיאה ב-Mako: {e}")

    print("\n[!] סיום סריקה.")

if __name__ == "__main__":
    main()
