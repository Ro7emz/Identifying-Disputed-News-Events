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
    "Referer": "https://www.google.com/"
}
 
# ======================================================
# GET עם backoff חכם (כמו ב-C14)
# ======================================================
def fetch_with_backoff(url, timeout=20, max_attempts=6):
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
        except Exception:
            r = None
 
        if r is not None and r.status_code == 200:
            return r
 
        code = r.status_code if r is not None else "NETWORK_ERR"
        base = min(120, 5 * attempt * attempt)  # 5,20,45,80,120...
        sleep_s = base + random.uniform(0, base / 2)
 
        print(f"⚠️ fetch failed ({code}) attempt {attempt}/{max_attempts}, sleep {sleep_s:.1f}s")
        time.sleep(sleep_s)
 
    return None
 
 
# ======================================================
# יצירת קטגוריה אם לא קיימת
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
 
 
# ======================================================
# הבאת לינקים מדף קטגוריה (page יחיד)
# ======================================================
def get_mako_category_links(category_url):
    r = fetch_with_backoff(category_url, timeout=15)
    if not r:
        return []
 
    soup = BeautifulSoup(r.text, "html.parser")
    links = set()
 
    for a in soup.find_all("a"):
        href = a.get("href")
        if not href:
            continue
 
        # הופך לקישור מלא
        if href.startswith("/"):
            href = "https://www.mako.co.il" + href
 
        # תנאי זהות לכתבת מאקו
        if "mako.co.il" in href and "Article" in href:
            links.add(href)
 
    return list(links)
 
 
# ======================================================
# סקרייפר כתבה MAKO (כמו שהיה, בלי לשנות HTML)
# ======================================================
def scrape_mako_article(url):
    r = fetch_with_backoff(url, timeout=20)
    if not r:
        return None
 
    soup = BeautifulSoup(r.text, "html.parser")
 
    # ------ כותרת ------
    title_tag = soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else None
 
    # ------ TYPE אמיתי מהאתר (לא חובה, אבל נשמור אותו רק אם תרצי בעתיד) ------
    # כרגע type במסד יהיה category_name כמו שביקשת.
 
    # ------ description (subtitle) ------
    description = None
    sub_new = soup.find("p", class_=lambda x: x and "ArticleSubtitle_root" in x)
    if sub_new:
        description = sub_new.get_text(strip=True)
    else:
        h2_tag = soup.find("h2")
        if h2_tag:
            description = h2_tag.get_text(strip=True)
 
    # ------ מחבר (שם בלבד) ------
    author = None
    tag1 = soup.find("span", class_=lambda x: x and "AuthorSourceAndSponsor_name" in x)
    if tag1:
        author = tag1.get_text(strip=True)
 
    if not author:
        tag2 = soup.find("a", class_=lambda x: x and "AuthorSourceAndSponsor_clickableName" in x)
        if tag2:
            author = tag2.get_text(strip=True)
 
    # ------ תאריך ------
    date = None
    time_tag = soup.find("time")
    if time_tag and time_tag.get("datetime"):
        date = time_tag.get("datetime")
 
    # ------ טקסט מלא ------
    paragraphs = []
 
    # 1) פסקאות בתבנית החדשה
    for p in soup.find_all("p", class_=lambda x: x and "ArticleSubtitle_root" in x):
        t = p.get_text(strip=True)
        if len(t) > 25:
            paragraphs.append(t)
 
    # 2) פסקאות רגילות
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
# הכנסת כתבה למסד (author_name טקסט, type = category_name)
# ======================================================
def insert_article_to_db(data, outlet_id, category_id, category_name):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
 
    # כפילויות: לפי לינק או title+outlet
    cur.execute("""
        SELECT news_article_id
        FROM News_articles
        WHERE link = ?
           OR (title = ? AND news_outlet_id = ?)
    """, (data["url"], data["title"], outlet_id))
 
    if cur.fetchone():
        print("➡️ כבר קיים במסד:", data["title"])
        conn.close()
        return
 
    cur.execute("""
        INSERT INTO News_articles
            (news_outlet_id, date, title, description, type, link, text, author_name)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        outlet_id,
        data["date"] or datetime.now().strftime("%Y-%m-%d"),
        data["title"],
        data["description"],
        category_name,     # ✅ כמו C14: type = שם הקטגוריה
        data["url"],
        data["text"],
        data["author"]     # ✅ שם המחבר כטקסט
    ))
 
    article_id = cur.lastrowid
 
    cur.execute("""
        INSERT INTO Article_Categories (news_article_id, category_id)
        VALUES (?, ?)
    """, (article_id, category_id))
 
    conn.commit()
    conn.close()
    print("✔️ הוכנסה כתבה:", data["title"])
 
 
# ======================================================
# ריצה על כל העמודים של MAKO category (page=1..∞)
# ======================================================
def scrape_mako_all_pages(base_url, outlet_id):
    # ✅ אותו שם "type" כמו הסקרייפר האחרון שלך
    category_name = "ביטחון / צבא"
    category_id = get_or_create_category(category_name)
 
    page = 1
    seen_any = 0
 
    while True:
        # לפי מה ששלחת: https://www.mako.co.il/news-military?page=2
        page_url = f"{base_url}?page={page}"
 
        print("\n==============================")
        print(f"📌 עמוד {page}: {page_url}")
        print("==============================")
 
        links = get_mako_category_links(page_url)
        print(f"🔗 נמצאו {len(links)} כתבות בעמוד {page}")
 
        # אם אין לינקים, נניח שנגמר
        if not links:
            print("🔚 אין עוד כתבות, עוצר.")
            break
 
        # עוברים כתבה-כתבה
        for url in links:
            print("\n📥 מוריד כתבה:", url)
            data = scrape_mako_article(url)
 
            if data and data.get("title"):
                insert_article_to_db(data, outlet_id, category_id, category_name)
            else:
                print("❌ שגיאה בקריאת הכתבה:", url)
 
            # ✅ טיימר בין כתבות (כמו C14)
            time.sleep(random.uniform(5, 12))
 
        seen_any += len(links)
 
        # ✅ טיימר בין עמודים (כמו C14)
        time.sleep(random.uniform(12, 30))
        page += 1
 
 
# ======================================================
# main
# ======================================================
if __name__ == "__main__":
    # MAKO outlet_id לדוגמה 1 (תעדכני אם אצלך שונה)
    MAKO_OUTLET_ID = 1
 
    # URL בסיס בלי page (אנחנו מוסיפים ?page=)
    BASE_CATEGORY_URL = "https://www.mako.co.il/news-military"
 
    scrape_mako_all_pages(BASE_CATEGORY_URL, MAKO_OUTLET_ID)