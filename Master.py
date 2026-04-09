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
DB_PATH = "news.db"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
}

# מילות מפתח לסינון
KEYWORDS = ["צה\"ל", "צבא", "ביטחון", "בטחון", "מלחמה", "נתניהו", "חמאס", "חיזבאללה", "מדיני", "פוליטי", "כנסת", "עסקה", "חטופים", "איראן", "טראמפ"]

# ==========================================
# פונקציות ליבה
# ==========================================

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def clean_text(text):
    if not text: return ""
    return re.sub(r"\s+", " ", text).strip()

def get_now_formatted():
    """מחזירה את הזמן הנוכחי בפורמט: DD/MM/YYYY HH:MM"""
    return datetime.now().strftime("%d/%m/%Y %H:%M")

def fetch_ynet_comments(article_url, news_article_id):
    try:
        art_id = article_url.split('/')[-1].split('#')[0]
        api_url = f"https://www.ynet.co.il/bin/en/public/v1/talkbacks/get/{art_id}"
        res = requests.get(api_url, headers=HEADERS, timeout=10)
        if res.status_code != 200: return
        
        comments = res.json().get('data', {}).get('talkbacks', [])
        conn = get_connection()
        cursor = conn.cursor()
        count = 0
        for cb in comments:
            cursor.execute("""
                INSERT OR IGNORE INTO Comments 
                (news_article_id, parent_comment_id, author_name, comment_title, comment_text, likes, dislikes, source_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (news_article_id, 
                  str(cb.get('parent')) if cb.get('parent') else None,
                  cb.get('user'), 
                  cb.get('title'), 
                  cb.get('text'), 
                  cb.get('up', 0), 
                  cb.get('down', 0),
                  article_url))
            if cursor.rowcount > 0: count += 1
        conn.commit()
        conn.close()
        if count > 0: print(f"    [💬] נאספו {count} תגובות.")
    except Exception as e:
        print(f"    [!] שגיאה בתגובות: {e}")

def scrape_article_content(url, outlet_id):
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        if res.status_code != 200: return None
        soup = BeautifulSoup(res.text, "html.parser")
        
        # תאריך הכתבה עצמה (לא זמן הסריקה)
        data = {"title": "", "author": "מערכת", "text": "", "description": "", "date": datetime.now().strftime("%d/%m/%Y"), "link": url}

        if outlet_id == 3: # YNET
            t = soup.select_one("h1.mainTitle, .mainTitle")
            a = soup.select_one("a[rel='author'], span[itemprop='name']")
            s = soup.select_one("h2.subTitle, .subTitle")
            p = [clean_text(el.get_text()) for el in soup.select(".text_editor_paragraph, .article-body p") if len(clean_text(el.get_text())) > 30]
            data.update({"title": clean_text(t.get_text()) if t else "", "author": clean_text(a.get_text()) if a else "Ynet", 
                         "description": clean_text(s.get_text()) if s else "", "text": "\n\n".join(p)})
        
        elif outlet_id == 1: # MAKO
            t = soup.find("h1")
            a = soup.find("span", class_=lambda x: x and "AuthorSourceAndSponsor_name" in x)
            p = [clean_text(el.get_text()) for el in soup.find_all("p") if len(clean_text(el.get_text())) > 30]
            data.update({"title": clean_text(t.get_text()) if t else "", "author": clean_text(a.get_text()) if a else "Mako", "text": "\n\n".join(p)})

        elif outlet_id == 2: # C14
            t = soup.find("h1")
            author = "ערוץ 14"
            if t:
                for el in t.next_elements:
                    if isinstance(el, Tag):
                        txt = clean_text(el.get_text())
                        if txt and not re.search(r"\(\d{2}\.\d{2}\.\d{2}\)", txt) and not txt.isdigit():
                            author = txt; break
            p = [clean_text(el.get_text()) for el in soup.find_all("p") if len(clean_text(el.get_text())) > 30]
            data.update({"title": clean_text(t.get_text()) if t else "", "author": author, "text": "\n\n".join(p)})

        return data
    except: return None

def save_all_to_db(conn, details, outlet_id, cat_name):
    cursor = conn.cursor()
    cursor.execute("SELECT news_article_id FROM News_articles WHERE link = ?", (details['link'],))
    if cursor.fetchone(): return
    
    # כאן השתמשתי בפורמט החדש עבור scraped_at
    scraped_at_formatted = get_now_formatted()
    
    cursor.execute("""
        INSERT INTO News_articles (news_outlet_id, date, title, description, type, link, text, author_name, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (outlet_id, details['date'], details['title'], details['description'], cat_name, 
          details['link'], details['text'], details['author'], scraped_at_formatted))
    
    new_id = cursor.lastrowid
    
    cursor.execute("INSERT OR IGNORE INTO categories (category_name) VALUES (?)", (cat_name,))
    cat_id = conn.execute("SELECT category_id FROM categories WHERE category_name = ?", (cat_name,)).fetchone()[0]
    cursor.execute("INSERT INTO Article_Categories (news_article_id, category_id) VALUES (?, ?)", (new_id, cat_id))
    conn.commit()
    
    if outlet_id == 3: fetch_ynet_comments(details['link'], new_id)
    return True

# ==========================================
# לוגיקה ראשית
# ==========================================

def main():
    conn = get_connection()
    
    # 1. YNET
    print("\n--- [3] סורק YNET ---")
    res = requests.get("https://www.ynet.co.il/news/247", headers=HEADERS)
    if res.status_code == 200:
        soup = BeautifulSoup(res.text, "html.parser")
        for a in soup.find_all("a", href=True):
            link = urljoin("https://www.ynet.co.il", a['href'])
            if "/article/" in link and any(w.lower() in a.get_text().lower() for w in KEYWORDS):
                det = scrape_article_content(link, 3)
                if det and len(det['text']) > 150:
                    if save_all_to_db(conn, det, 3, "חדשות"):
                        print(f"  V נשמר: {det['title'][:40]}...")
                        time.sleep(random.uniform(1, 2))

    print("\n--- [1] סורק MAKO ---")
    res = requests.get("https://www.mako.co.il/news-military", headers=HEADERS)
    if res.status_code == 200:
        soup = BeautifulSoup(res.text, "html.parser")
        links = list(set([urljoin("https://www.mako.co.il", a['href']) for a in soup.find_all("a", href=True) if "Article" in a['href']]))
        for link in links[:15]:
            det = scrape_article_content(link, 1)
            if det and len(det['text']) > 150:
                if save_all_to_db(conn, det, 1, "צבא וביטחון"):
                    print(f"  V נשמר: {det['title'][:40]}...")
                    time.sleep(random.uniform(3, 5))

    print("\n--- [2] סורק ערוץ 14 ---")
    for cat, url in [("צבא וביטחון", "https://www.c14.co.il/archive/990"), ("פוליטי", "https://www.c14.co.il/archive/65839")]:
        res = requests.get(url, headers=HEADERS)
        if res.status_code == 200:
            soup = BeautifulSoup(res.text, "html.parser")
            links = list(set([urljoin("https://www.c14.co.il", a['href']) for a in soup.find_all("a", href=True) if "/article/" in a['href']]))
            for link in links[:15]:
                det = scrape_article_content(link, 2)
                if det and len(det['text']) > 150:
                    if save_all_to_db(conn, det, 2, cat):
                        print(f"  V נשמר: {det['title'][:40]}...")
                        time.sleep(random.uniform(3, 5))

    conn.close()
    print("\n[!] בוצע בהצלחה.")

if __name__ == "__main__":
    main()
