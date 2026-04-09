import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import time
import random
import re

# ======================================================
# הגדרות בסיס
# ======================================================
DB_PATH = "/home/israel-iran-war/Desktop/DisputedNews/news.db"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
}

# ======================================================
# פונקציות עזר (זמן וחיבור)
# ======================================================
def get_now_formatted():
    """פורמט נקי: DD/MM/YYYY HH:MM"""
    return datetime.now().strftime("%d/%m/%Y %H:%M")

def fetch_with_backoff(url, timeout=20, max_attempts=6):
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r is not None and r.status_code == 200:
                return r
        except Exception:
            r = None

        code = r.status_code if r is not None else "NETWORK_ERR"
        base = min(120, 5 * attempt * attempt)
        sleep_s = base + random.uniform(0, base / 2)
        print(f"⚠️ fetch failed ({code}) attempt {attempt}/{max_attempts}, sleep {sleep_s:.1f}s")
        time.sleep(sleep_s)
    return None

# ======================================================
# איסוף תגובות MAKO (N12)
# ======================================================
def fetch_mako_comments(article_soup, news_article_id, article_url):
    """מחלץ תגובות משרת ה-talkback של Mako"""
    try:
        # חילוץ מזהה כתבה פנימי מתוך ה-meta tags
        meta_img = article_soup.find("meta", attrs={"property": "og:image"})
        if not meta_img or "Article-" not in meta_img['content']:
            return
        
        mako_internal_id = re.search(r"Article-([0-9]+)", meta_img['content']).group(1)
        comments_api = f"https://www.mako.co.il/web-services/PostGetArticleTalkbacks.ashx?articleId={mako_internal_id}&page=1"
        
        res = requests.get(comments_api, headers=HEADERS, timeout=10)
        if res.status_code != 200: return
        
        c_soup = BeautifulSoup(res.text, "html.parser")
        comment_items = c_soup.select(".talkback_item") 
        
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        count = 0
        
        for item in comment_items:
            author = item.select_one(".talkback_author").get_text(strip=True) if item.select_one(".talkback_author") else "אנונימי"
            title = item.select_one(".talkback_title").get_text(strip=True) if item.select_one(".talkback_title") else ""
            content = item.select_one(".talkback_content").get_text(strip=True) if item.select_one(".talkback_content") else ""
            
            if not content: continue
            
            cur.execute("""
                INSERT OR IGNORE INTO Comments 
                (news_article_id, author_name, comment_title, comment_text, source_url)
                VALUES (?, ?, ?, ?, ?)
            """, (news_article_id, author, title, content, article_url))
            if cur.rowcount > 0: count += 1
            
        conn.commit()
        conn.close()
        if count > 0: print(f"    💬 נאספו {count} תגובות מ-N12")
    except Exception as e:
        print(f"    ❌ שגיאה באיסוף תגובות: {e}")

# ======================================================
# יצירת קטגוריה וטיפול בלינקים
# ======================================================
def get_or_create_category(category_name):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT category_id FROM categories WHERE category_name = ?", (category_name,))
    row = cur.fetchone()
    if row:
        category_id = row[0]
    else:
        cur.execute("INSERT INTO categories (category_name) VALUES (?)", (category_name,))
        category_id = cur.lastrowid
    conn.commit()
    conn.close()
    return category_id

def get_mako_category_links(category_url):
    r = fetch_with_backoff(category_url, timeout=15)
    if not r: return []
    soup = BeautifulSoup(r.text, "html.parser")
    links = set()
    for a in soup.find_all("a", href=True):
        href = a['href']
        if href.startswith("/"): href = "https://www.mako.co.il" + href
        if "mako.co.il" in href and "Article" in href:
            links.add(href)
    return list(links)

# ======================================================
# סריקת כתבה ושמירה
# ======================================================
def scrape_mako_article(url):
    r = fetch_with_backoff(url, timeout=20)
    if not r: return None
    soup = BeautifulSoup(r.text, "html.parser")

    title_tag = soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else None

    description = ""
    sub = soup.find("p", class_=lambda x: x and "ArticleSubtitle_root" in x)
    if sub: description = sub.get_text(strip=True)
    elif soup.find("h2"): description = soup.find("h2").get_text(strip=True)

    author = "N12"
    tag = soup.find("span", class_=lambda x: x and "AuthorSourceAndSponsor_name" in x)
    if tag: author = tag.get_text(strip=True)

    paragraphs = []
    for p in soup.find_all("p"):
        t = p.get_text(strip=True)
        if len(t) > 25 and "תגובה" not in t:
            paragraphs.append(t)

    return {
        "title": title,
        "description": description,
        "author": author,
        "date": datetime.now().strftime("%Y-%m-%d"),
        "text": "\n\n".join(paragraphs),
        "url": url,
        "soup": soup 
    }

def insert_article_to_db(data, outlet_id, category_id, category_name):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT news_article_id FROM News_articles WHERE link = ?", (data["url"],))
    if cur.fetchone():
        print("➡️ כבר קיים:", data["title"])
        conn.close()
        return

    scraped_at = get_now_formatted()
    cur.execute("""
        INSERT INTO News_articles (news_outlet_id, date, title, description, type, link, text, author_name, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (outlet_id, data["date"], data["title"], data["description"], category_name, data["url"], data["text"], data["author"], scraped_at))

    article_id = cur.lastrowid
    cur.execute("INSERT INTO Article_Categories (news_article_id, category_id) VALUES (?, ?)", (article_id, category_id))
    conn.commit()
    conn.close()
    
    print("✔️ הוכנסה כתבה:", data["title"])
    # קריאה לאיסוף תגובות
    fetch_mako_comments(data["soup"], article_id, data["url"])

# ======================================================
# הרצה
# ======================================================
def scrape_mako_all_pages(base_url, outlet_id):
    category_name = "ביטחון / צבא"
    category_id = get_or_create_category(category_name)
    page = 1

    while True:
        page_url = f"{base_url}?page={page}"
        print(f"\n📌 עמוד {page}: {page_url}")
        links = get_mako_category_links(page_url)
        
        if not links: break

        for url in links:
            data = scrape_mako_article(url)
            if data and data.get("title"):
                insert_article_to_db(data, outlet_id, category_id, category_name)
                time.sleep(random.uniform(5, 10))

        page += 1
        time.sleep(random.uniform(10, 20))

if __name__ == "__main__":
    scrape_mako_all_pages("https://www.mako.co.il/news-military", 1)
