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
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
}

KEYWORDS = ["צה\"ל", "צבא", "ביטחון", "בטחון", "מלחמה", "נתניהו", "חמאס", "חיזבאללה", "מדיני", "פוליטי", "כנסת", "עסקה", "חטופים", "איראן", "טראמפ"]

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
        if res.status_code != 200: return
        
        comments = res.json().get('data', {}).get('talkbacks', [])
        conn = get_connection()
        cursor = conn.cursor()
        for cb in comments:
            cursor.execute("""
                INSERT OR IGNORE INTO Comments 
                (news_article_id, parent_comment_id, author_name, comment_title, comment_text, likes, dislikes, source_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (news_article_id, str(cb.get('parent')) if cb.get('parent') else None,
                  cb.get('user'), cb.get('title'), cb.get('text'), cb.get('up', 0), cb.get('down', 0), article_url))
        conn.commit()
        conn.close()
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
        
        res = requests.get(api_url, headers=h, timeout=10)
        c_soup = BeautifulSoup(res.text, "html.parser")
        for item in c_soup.select(".talkback_item, .comment_body"):
            author = item.select_one(".talkback_author, .author_name")
            content = item.select_one(".talkback_content, .comment_text")
            if content:
                conn = get_connection()
                conn.execute("""
                    INSERT OR IGNORE INTO Comments (news_article_id, author_name, comment_text, source_url)
                    VALUES (?, ?, ?, ?)
                """, (news_article_id, author.get_text(strip=True) if author else "אנונימי", content.get_text(strip=True), article_url))
                conn.commit()
                conn.close()
    except: pass

# ==========================================
# חילוץ תוכן כתבה
# ==========================================

def scrape_article_content(url, outlet_id):
    try:
        # זמן המתנה קטן לפני כל בקשת כתבה כדי להוריד עומס
        time.sleep(random.uniform(1, 3))
        res = requests.get(url, headers=HEADERS, timeout=15)
        if res.status_code != 200: 
            print(f"  ❌ שגיאה {res.status_code} בגישה לכתבה: {url}")
            return None
            
        soup = BeautifulSoup(res.text, "html.parser")
        data = {"title": "", "author": "מערכת", "text": "", "description": "", "date": datetime.now().strftime("%Y-%m-%d"), "link": url}

        if outlet_id == 3: # YNET
            t = soup.select_one("h1.mainTitle, .mainTitle")
            a = soup.select_one("a[rel='author'], span[itemprop='name']")
            s = soup.select_one("h2.subTitle, .subTitle") # תפיסת כותרת משנה ב-Ynet
            p = [clean_text(el.get_text()) for el in soup.select(".text_editor_paragraph, .article-body p") if len(clean_text(el.get_text())) > 30]
            data.update({
                "title": clean_text(t.get_text()) if t else "", 
                "author": clean_text(a.get_text()) if a else "Ynet", 
                "description": clean_text(s.get_text()) if s else "",
                "text": "\n\n".join(p)
            })
        
        elif outlet_id == 1: # MAKO
            t = soup.find("h1")
            # ניסיון תפיסת Description במבנה החדש של מאקו
            s = soup.find("p", class_=lambda x: x and "ArticleSubtitle_root" in x) or soup.find("h2")
            a = soup.find("span", class_=lambda x: x and "AuthorSourceAndSponsor_name" in x)
            p = [clean_text(el.get_text()) for el in soup.find_all("p") if len(clean_text(el.get_text())) > 30]
            data.update({
                "title": clean_text(t.get_text()) if t else "", 
                "author": clean_text(a.get_text()) if a else "Mako", 
                "description": clean_text(s.get_text()) if s else "",
                "text": "\n\n".join(p)
            })

        elif outlet_id == 2: # C14
            t = soup.find("h1")
            s = soup.select_one(".entry-excerpt, h2")
            author = "ערוץ 14"
            if t:
                for el in t.next_elements:
                    if isinstance(el, Tag):
                        txt = clean_text(el.get_text())
                        if txt and not re.search(r"\(\d{2}\.\d{2}\.\d{2}\)", txt) and not txt.isdigit():
                            author = txt; break
            p = [clean_text(el.get_text()) for el in soup.find_all("p") if len(clean_text(el.get_text())) > 30]
            data.update({
                "title": clean_text(t.get_text()) if t else "", 
                "author": author, 
                "description": clean_text(s.get_text()) if s else "",
                "text": "\n\n".join(p)
            })

        return data
    except Exception as e:
        print(f"  ❌ שגיאה בחילוץ תוכן: {e}")
        return None

def save_and_fetch_comments(details, outlet_id, cat_name):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT news_article_id FROM News_articles WHERE link = ?", (details['link'],))
    if cursor.fetchone(): 
        conn.close()
        return False
    
    scraped_at = get_now_formatted()
    cursor.execute("""
        INSERT INTO News_articles (news_outlet_id, date, title, description, type, link, text, author_name, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (outlet_id, details['date'], details['title'], details['description'], cat_name, 
          details['link'], details['text'], details['author'], scraped_at))
    
    new_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    print(f"  ✔️ נשמר: {details['title'][:40]}...")
    
    if outlet_id == 3: fetch_ynet_comments(details['link'], new_id)
    if outlet_id == 1: fetch_mako_comments(details['link'], new_id)
    return True

# ==========================================
# MAIN
# ==========================================

def main():
    print(f"🚀 מתחיל סריקה מאוחדת (מצב אנטי-חסימה)... {get_now_formatted()}")

    # 1. YNET
    print("\n--- [3] סורק YNET ---")
    try:
        res = requests.get("https://www.ynet.co.il/news/247", headers=HEADERS, timeout=15)
        if res.status_code == 200:
            soup = BeautifulSoup(res.text, "html.parser")
            links = [urljoin("https://www.ynet.co.il", a['href']) for a in soup.find_all("a", href=True) if "/article/" in a['href']]
            for link in list(set(links))[:10]:
                det = scrape_article_content(link, 3)
                if det: save_and_fetch_comments(det, 3, "חדשות")
    except: print("  ❌ שגיאה בגישה ל-Ynet")

    # המתנה בין אתרים
    print("\n⌛ ממתין כדי למנוע חסימות...")
    time.sleep(random.uniform(10, 20))

    # 2. MAKO (N12)
    print("\n--- [1] סורק MAKO ---")
    try:
        res = requests.get("https://www.mako.co.il/news-military", headers=HEADERS, timeout=15)
        if res.status_code == 200:
            soup = BeautifulSoup(res.text, "html.parser")
            links = [urljoin("https://www.mako.co.il", a['href']) for a in soup.find_all("a", href=True) if "Article" in a['href']]
            for link in list(set(links))[:10]:
                det = scrape_article_content(link, 1)
                if det: 
                    save_and_fetch_comments(det, 1, "ביטחון / צבא")
                    # המתנה ארוכה יותר בין כתבות ב-N12 (הם רגישים)
                    time.sleep(random.uniform(8, 15))
    except: print("  ❌ שגיאה בגישה ל-Mako")

    # 3. Channel 14
    print("\n--- [2] סורק ערוץ 14 ---")
    try:
        res = requests.get("https://www.c14.co.il/archive/990", headers=HEADERS, timeout=15)
        if res.status_code == 200:
            soup = BeautifulSoup(res.text, "html.parser")
            links = [urljoin("https://www.c14.co.il", a['href']) for a in soup.find_all("a", href=True) if "/article/" in a['href']]
            for link in list(set(links))[:10]:
                det = scrape_article_content(link, 2)
                if det: save_and_fetch_comments(det, 2, "צבא וביטחון")
    except: print("  ❌ שגיאה בגישה לערוץ 14")

    print("\n[!] הסריקה הסתיימה בהצלחה.")

if __name__ == "__main__":
    main()
