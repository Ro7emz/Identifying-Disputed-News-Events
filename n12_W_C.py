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
# פונקציות עזר
# ======================================================
def get_now_formatted():
    """הפורמט שביקשת: DD/MM/YYYY HH:MM"""
    return datetime.now().strftime("%d/%m/%Y %H:%M")

def fetch_with_backoff(url, timeout=20, max_attempts=6):
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
        except Exception:
            r = None

        if r is not None and r.status_code == 200:
            return r

        code = r.status_code if r is not None else "NETWORK_ERR"
        base = min(120, 5 * attempt * attempt)
        sleep_s = base + random.uniform(0, base / 2)

        print(f"⚠️ fetch failed ({code}) attempt {attempt}/{max_attempts}, sleep {sleep_s:.1f}s")
        time.sleep(sleep_s)

    return None

# ======================================================
# איסוף תגובות MAKO (N12)
# ======================================================
def fetch_mako_comments(article_url, news_article_id):
    """מחלץ תגובות משרת ה-talkback של Mako"""
    try:
        # חילוץ ה-ID מה-URL
        match = re.search(r"Article-([a-f0-9]+|[0-9]+)", article_url)
        if not match: return
        
        article_id = match.group(1)
        api_url = f"https://www.mako.co.il/web-services/PostGetArticleTalkbacks.ashx?articleId={article_id}&page=1"
        
        # חובה להוסיף Referer כדי לא להיחסם
        headers = HEADERS.copy()
        headers["Referer"] = article_url
        headers["X-Requested-With"] = "XMLHttpRequest"
        
        res = requests.get(api_url, headers=headers, timeout=10)
        if res.status_code != 200 or not res.text: return
        
        c_soup = BeautifulSoup(res.text, "html.parser")
        comment_items = c_soup.select(".talkback_item, .comment_body") 
        
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        count = 0
        
        for item in comment_items:
            author = item.select_one(".talkback_author, .author_name, .author")
            content = item.select_one(".talkback_content, .comment_text, .content")
            
            if content:
                cur.execute("""
                    INSERT OR IGNORE INTO Comments 
                    (news_article_id, author_name, comment_text, source_url)
                    VALUES (?, ?, ?, ?)
                """, (news_article_id, 
                      author.get_text(strip=True) if author else "אנונימי",
                      content.get_text(strip=True), 
                      article_url))
                if cur.rowcount > 0: count += 1
            
        conn.commit()
        conn.close()
        if count > 0: print(f"    💬 נאספו {count} תגובות.")
    except Exception as e:
        print(f"    ❌ שגיאה בתגובות: {e}")

# ======================================================
# ניהול קטגוריות
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
        print(f"✔ נוצרה קטגוריה חדשה: {category_name} (ID={category_id})")
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
# סקרייפר כתבה MAKO
# ======================================================
def scrape_mako_article(url):
    r = fetch_with_backoff(url, timeout=20)
    if not r: return None
    soup = BeautifulSoup(r.text, "html.parser")

    title_tag = soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else None

    description = None
    sub_new = soup.find("p", class_=lambda x: x and "ArticleSubtitle_root" in x)
    if sub_new:
        description = sub_new.get_text(strip=True)
    else:
        h2_tag = soup.find("h2")
        if h2_tag: description = h2_tag.get_text(strip=True)

    author = None
    tag1 = soup.find("span", class_=lambda x: x and "AuthorSourceAndSponsor_name" in x)
    if tag1: author = tag1.get_text(strip=True)
    if not author:
        tag2 = soup.find("a", class_=lambda x: x and "AuthorSourceAndSponsor_clickableName" in x)
        if tag2: author = tag2.get_text(strip=True)

    date = None
    time_tag = soup.find("time")
    if time_tag and time_tag.get("datetime"):
        date = time_tag.get("datetime")

    paragraphs = []
    for p in soup.find_all("p"):
        t = p.get_text(strip=True)
        if len(t) > 25 and t not in paragraphs and "תגובה" not in t:
            paragraphs.append(t)

    full_text = "\n".join(paragraphs)

    return {
        "title": title,
        "description": description,
        "author": author,
        "date": date,
        "text": full_text,
        "url": url
    }

# ======================================================
# הכנסת כתבה למסד (כולל איסוף תגובות)
# ======================================================
def insert_article_to_db(data, outlet_id, category_id, category_name):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT news_article_id FROM News_articles
        WHERE link = ? OR (title = ? AND news_outlet_id = ?)
    """, (data["url"], data["title"], outlet_id))

    if cur.fetchone():
        print("➡️ כבר קיים במסד:", data["title"])
        conn.close()
        return

    scraped_at = get_now_formatted()

    cur.execute("""
        INSERT INTO News_articles
            (news_outlet_id, date, title, description, type, link, text, author_name, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        outlet_id,
        data["date"] or datetime.now().strftime("%Y-%m-%d"),
        data["title"],
        data["description"],
        category_name,
        data["url"],
        data["text"],
        data["author"],
        scraped_at
    ))

    article_id = cur.lastrowid
    cur.execute("INSERT INTO Article_Categories (news_article_id, category_id) VALUES (?, ?)", (article_id, category_id))
    conn.commit()
    conn.close()
    
    print("✔️ הוכנסה כתבה:", data["title"])
    
    # הפעלה של איסוף תגובות
    fetch_mako_comments(data["url"], article_id)

# ======================================================
# לוגיקת דפים
# ======================================================
def scrape_mako_all_pages(base_url, outlet_id):
    category_name = "ביטחון / צבא"
    category_id = get_or_create_category(category_name)
    page = 1

    while True:
        page_url = f"{base_url}?page={page}"
        print(f"\n📌 עמוד {page}: {page_url}")
        links = get_mako_category_links(page_url)
        
        if not links:
            print("🔚 אין עוד כתבות, עוצר.")
            break

        for url in links:
            print(f"📥 מעבד: {url}")
            data = scrape_mako_article(url)
            if data and data.get("title"):
                insert_article_to_db(data, outlet_id, category_id, category_name)
            
            time.sleep(random.uniform(5, 12))

        time.sleep(random.uniform(12, 30))
        page += 1

# ======================================================
# החלק שמפעיל את הכל - קריטי!
# ======================================================
if __name__ == "__main__":
    # MAKO outlet_id (1)
    MAKO_OUTLET_ID = 1
    
    # URL בסיס
    BASE_CATEGORY_URL = "https://www.mako.co.il/news-military"
    
    # קריאה לפונקציית ההרצה
    scrape_mako_all_pages(BASE_CATEGORY_URL, MAKO_OUTLET_ID)
