import requests
from bs4 import BeautifulSoup, Tag
import sqlite3
from datetime import datetime
import time
import random
import re
from urllib.parse import urljoin

# ==========================================
# הגדרות כלליות
# ==========================================
DB_PATH = "/home/israel-iran-war/Desktop/DisputedNews/news.db"

# Headers משופרים מאוד - נראים כמו דפדפן אמיתי לגמרי
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "max-age=0",
    "Sec-Ch-Ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Linux"',
    "Upgrade-Insecure-Requests": "1"
}

# ==========================================
# פונקציות עזר
# ==========================================

def get_connection():
    return sqlite3.connect(DB_PATH)

def get_now_formatted():
    return datetime.now().strftime("%d/%m/%Y %H:%M")

def clean_text(text):
    if not text: return ""
    return re.sub(r"\s+", " ", text).strip()

# ==========================================
# איסוף תגובות (Ynet + N12)
# ==========================================

def fetch_ynet_comments(article_url, news_article_id):
    try:
        art_id = article_url.split('/')[-1].split('#')[0]
        api_url = f"https://www.ynet.co.il/bin/en/public/v1/talkbacks/get/{art_id}"
        res = requests.get(api_url, headers=HEADERS, timeout=10)
        if res.status_code == 200:
            comments = res.json().get('data', {}).get('talkbacks', [])
            conn = get_connection()
            count = 0
            for cb in comments:
                conn.execute("""
                    INSERT OR IGNORE INTO Comments 
                    (news_article_id, parent_comment_id, author_name, comment_title, comment_text, likes, dislikes, source_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (news_article_id, str(cb.get('parent')) if cb.get('parent') else None,
                      cb.get('user'), cb.get('title'), cb.get('text'), cb.get('up', 0), cb.get('down', 0), article_url))
                count += 1
            conn.commit()
            conn.close()
            if count > 0: print(f"    [💬] Ynet: נאספו {count} תגובות.")
    except: pass

def fetch_mako_comments(article_url, news_article_id):
    try:
        match = re.search(r"Article-([a-f0-9]+|[0-9]+)", article_url)
        if not match: return
        article_id = match.group(1)
        api_url = f"https://www.mako.co.il/web-services/PostGetArticleTalkbacks.ashx?articleId={article_id}&page=1"
        
        h = HEADERS.copy()
        h["Referer"] = article_url
        h["X-Requested-With"] = "XMLHttpRequest"
        
        res = requests.get(api_url, headers=h, timeout=12)
        if res.status_code == 200 and res.text:
            c_soup = BeautifulSoup(res.text, "html.parser")
            items = c_soup.select(".talkback_item, .comment_body, [id^='comment_']")
            conn = get_connection()
            count = 0
            for item in items:
                author = item.select_one(".talkback_author, .author_name, .author")
                content = item.select_one(".talkback_content, .comment_text, .content")
                if content:
                    conn.execute("""
                        INSERT OR IGNORE INTO Comments (news_article_id, author_name, comment_text, source_url)
                        VALUES (?, ?, ?, ?)
                    """, (news_article_id, author.get_text(strip=True) if author else "אנונימי", 
                          content.get_text(strip=True), article_url))
                    count += 1
            conn.commit()
            conn.close()
            if count > 0: print(f"    [💬] N12: נאספו {count} תגובות.")
    except: pass

# ==========================================
# לוגיקת שמירה חכמה
# ==========================================

def save_and_fetch_comments(details, outlet_id, cat_name):
    conn = get_connection()
    cursor = conn.cursor()
    
    # בדיקה אם הכתבה קיימת
    cursor.execute("SELECT news_article_id FROM News_articles WHERE link = ?", (details['link'],))
    row = cursor.fetchone()
    
    if row:
        new_id = row[0]
        # חידוש: גם אם הכתבה קיימת, נבדוק אם יש לה תגובות. אם אין - ננסה לאסוף
        cursor.execute("SELECT COUNT(*) FROM Comments WHERE news_article_id = ?", (new_id,))
        if cursor.fetchone()[0] == 0:
            conn.close()
            if outlet_id == 3: fetch_ynet_comments(details['link'], new_id)
            if outlet_id == 1: fetch_mako_comments(details['link'], new_id)
        else:
            conn.close()
        return False
    
    # הכנסה של כתבה חדשה
    scraped_at = get_now_formatted()
    cursor.execute("""
        INSERT INTO News_articles (news_outlet_id, date, title, description, type, link, text, author_name, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (outlet_id, details['date'], details['title'], details['description'], cat_name, 
          details['link'], details['text'], details['author'], scraped_at))
    
    new_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    print(f"  [V] נשמר: {details['title'][:35]}...")
    if outlet_id == 3: fetch_ynet_comments(details['link'], new_id)
    if outlet_id == 1: fetch_mako_comments(details['link'], new_id)
    return True

# ... פונקציות ה-Scrape (Ynet, Mako, C14) מהגרסה הקודמת ...
# (השארתי את אותן פונקציות חילוץ תוכן)

def scrape_article_content(url, outlet_id):
    try:
        time.sleep(random.uniform(2, 4))
        res = requests.get(url, headers=HEADERS, timeout=15)
        if res.status_code != 200: return None
        soup = BeautifulSoup(res.text, "html.parser")
        data = {"title": "", "author": "מערכת", "text": "", "description": "", "date": datetime.now().strftime("%Y-%m-%d"), "link": url}

        if outlet_id == 3: # YNET
            t = soup.select_one("h1.mainTitle, .mainTitle")
            s = soup.select_one("h2.subTitle, .subTitle")
            p = [clean_text(el.get_text()) for el in soup.select(".text_editor_paragraph, .article-body p") if len(clean_text(el.get_text())) > 30]
            data.update({"title": clean_text(t.get_text()) if t else "", "description": clean_text(s.get_text()) if s else "", "text": "\n\n".join(p)})
        
        elif outlet_id == 1: # MAKO
            t = soup.find("h1")
            s = soup.find("p", class_=lambda x: x and "ArticleSubtitle_root" in x) or soup.find("h2")
            p = [clean_text(el.get_text()) for el in soup.find_all("p") if len(clean_text(el.get_text())) > 30]
            data.update({"title": clean_text(t.get_text()) if t else "", "description": clean_text(s.get_text()) if s else "", "text": "\n\n".join(p)})

        return data
    except: return None

def main():
    print(f"🚀 Master Scraper v2 - {get_now_formatted()}")
    
    # דוגמה ל-YNET
    print("\n--- [3] YNET ---")
    res = requests.get("https://www.ynet.co.il/news/247", headers=HEADERS)
    if res.status_code == 200:
        links = list(set([urljoin("https://www.ynet.co.il", a['href']) for a in BeautifulSoup(res.text, "html.parser").find_all("a", href=True) if "/article/" in a['href']]))
        for link in links[:10]:
            det = scrape_article_content(link, 3)
            if det: save_and_fetch_comments(det, 3, "חדשות")

    print("\n⌛ המתנה קלה למניעת חסימה...")
    time.sleep(15)

    # דוגמה ל-MAKO (כאן השדרוג)
    print("\n--- [1] N12 / MAKO ---")
    session = requests.Session() # שימוש ב-Session שומר על Cookies
    res = session.get("https://www.mako.co.il/news-military", headers=HEADERS)
    if res.status_code == 200:
        links = list(set([urljoin("https://www.mako.co.il", a['href']) for a in BeautifulSoup(res.text, "html.parser").find_all("a", href=True) if "Article" in a['href']]))
        for link in links[:10]:
            det = scrape_article_content(link, 1)
            if det: 
                save_and_fetch_comments(det, 1, "ביטחון / צבא")
                time.sleep(random.uniform(10, 15)) # המתנה ארוכה בין כתבות

    print("\n[!] בוצע.")

if __name__ == "__main__":
    main()
