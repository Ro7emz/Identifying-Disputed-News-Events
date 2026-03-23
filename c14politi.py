import requests
from bs4 import BeautifulSoup, Tag
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
# נרמול scraped_at (אחיד לכל הסקרייפרים)
# ======================================================
def normalize_scraped_at(dt=None):
    if dt is None:
        dt = datetime.now()
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ======================================================
# GET עם backoff חכם
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
        base = min(120, 5 * attempt * attempt)
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
 
    cur.execute(
        "SELECT category_id FROM categories WHERE category_name = ?",
        (category_name,)
    )
    row = cur.fetchone()
 
    if row:
        category_id = row[0]
    else:
        cur.execute(
            "INSERT INTO categories (category_name) VALUES (?)",
            (category_name,)
        )
        category_id = cur.lastrowid
        print(f"✔ נוצרה קטגוריה חדשה: {category_name} (ID={category_id})")
 
    conn.commit()
    conn.close()
    return category_id
 
# ======================================================
# הכנסת כתבה למסד
# ======================================================
def insert_article_to_db(data, outlet_id, category_id):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
 
    cur.execute(
        """
        SELECT news_article_id
        FROM News_articles
        WHERE link = ?
           OR (title = ? AND news_outlet_id = ?)
        """,
        (data["url"], data["title"], outlet_id)
    )
    if cur.fetchone():
        print("➡️ כבר קיים במסד:", data["title"])
        conn.close()
        return
 
    cur.execute(
        """
        INSERT INTO News_articles
            (news_outlet_id, date, title, description, type, link, text, author_name, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            outlet_id,
            data["date"],
            data["title"],
            data["description"],
            data["type"],
            data["url"],
            data["text"],
            data["author"],
            normalize_scraped_at()
        )
    )
 
    article_id = cur.lastrowid
 
    cur.execute(
        """
        INSERT INTO Article_Categories (news_article_id, category_id)
        VALUES (?, ?)
        """,
        (article_id, category_id)
    )
 
    conn.commit()
    conn.close()
    print("✔️ הוכנסה כתבה:", data["title"])


def get_article_id_by_link(link):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT news_article_id
        FROM News_articles
        WHERE link = ?
        LIMIT 1
        """,
        (link,)
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def get_or_insert_comment_id(
    cur,
    news_article_id,
    parent_comment_id,
    author_name,
    comment_title,
    comment_text,
    likes,
    dislikes,
    source_url
):
    # Duplicate check signature (no comment_external_id by requirement)
    if parent_comment_id is None:
        cur.execute(
            """
            SELECT comment_id
            FROM Comments
            WHERE news_article_id = ?
              AND parent_comment_id IS NULL
              AND author_name = ?
              AND comment_text = ?
              AND source_url = ?
            LIMIT 1
            """,
            (news_article_id, author_name, comment_text, source_url)
        )
    else:
        cur.execute(
            """
            SELECT comment_id
            FROM Comments
            WHERE news_article_id = ?
              AND parent_comment_id = ?
              AND author_name = ?
              AND comment_text = ?
              AND source_url = ?
            LIMIT 1
            """,
            (news_article_id, parent_comment_id, author_name, comment_text, source_url)
        )

    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        """
        INSERT INTO Comments
            (news_article_id, parent_comment_id, author_name, comment_title, comment_text, likes, dislikes, source_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            news_article_id,
            parent_comment_id,
            author_name,
            comment_title,
            comment_text,
            likes,
            dislikes,
            source_url
        )
    )
    return cur.lastrowid


def scrape_comments_from_c14(article_url, news_article_id):
    """
    C14 comments are available via JSON API (no need for JS rendering).
    """
    # article URL is like: https://www.c14.co.il/article/1504586
    m = re.search(r"/article/(\\d+)", article_url)
    if not m:
        print("❌ לא הצלחתי להוציא article_id מתוך:", article_url)
        return
    c14_article_id = int(m.group(1))

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    source_url = article_url

    base = "https://www.c14.co.il/wp-json/now14-api/v1/comments"
    per_page = 50
    offset = 0
    max_pages = 20  # safety bound
    pages_seen = 0

    try:
        def insert_tree(node, parent_db_id):
            author_name = (node.get("author") or "").strip()
            comment_text = (node.get("content") or "").strip()
            if not comment_text:
                return None
            likes = int(node.get("likes") or 0)
            dislikes = int(node.get("dislikes") or 0)
            comment_title = None

            db_comment_id = get_or_insert_comment_id(
                cur=cur,
                news_article_id=news_article_id,
                parent_comment_id=parent_db_id,
                author_name=author_name,
                comment_title=comment_title,
                comment_text=comment_text,
                likes=likes,
                dislikes=dislikes,
                source_url=source_url
            )

            for child in (node.get("subComments") or []):
                insert_tree(child, db_comment_id)
            return db_comment_id

        while pages_seen < max_pages:
            time.sleep(random.uniform(0.8, 1.6))
            r = fetch_with_backoff(
                f"{base}?article_id={c14_article_id}&per_page={per_page}&offset={offset}",
                timeout=30,
                max_attempts=6
            )
            if not r:
                break

            try:
                items = r.json()
            except Exception:
                print("❌ תגובות C14 לא בפורמט JSON:", article_url)
                break

            if not items:
                break

            for node in items:
                insert_tree(node, None)

            offset += per_page
            pages_seen += 1

        conn.commit()
        print(f"✔️ הוכנסו תגובות C14 (אולי חלקיות) ל-article_id={news_article_id}")
    except Exception as e:
        print("❌ שגיאה בעת scraping תגובות C14:", e)
    finally:
        conn.close()
 
# ======================================================
# ריצה על כל ארכיון ערוץ 14 – עד שאין עוד עמודים
# ======================================================
def scrape_c14_category(base_url, outlet_id):
    category_name = "פוליטי"
    category_id = get_or_create_category(category_name)
 
    page = 1
    while True:
        if page == 1:
            url = base_url
        else:
            url = base_url.rstrip("/") + f"/page/{page}"
 
        print("\n==============================")
        print(f"📌 עמוד {page}: {url}")
        print("==============================")
 
        r = fetch_with_backoff(url, timeout=15)
        if not r:
            print("❌ לא הצלחתי להביא את העמוד, עוצר.")
            break
 
        soup = BeautifulSoup(r.text, "html.parser")
        links = set()
 
        for a in soup.find_all("a"):
            href = a.get("href")
            if not href:
                continue
            if href.startswith("/"):
                href = "https://www.c14.co.il" + href
            if "https://www.c14.co.il/article/" in href:
                links.add(href)
 
        links = list(links)
        print(f"🔗 נמצאו {len(links)} כתבות")
 
        if not links:
            print("🔚 אין עוד כתבות, עוצר.")
            break
 
        for article_url in links:
            rr = fetch_with_backoff(article_url, timeout=20)
            if not rr:
                print("❌ דילוג על כתבה:", article_url)
                continue
 
            soup_a = BeautifulSoup(rr.text, "html.parser")
 
            # כותרת
            title_tag = soup_a.find("h1")
            title = title_tag.get_text(strip=True) if title_tag else None
            if not title:
                continue
 
            # תיאור
            description = None
            h2 = soup_a.find("h2")
            if h2:
                description = h2.get_text(strip=True)
 
            # מחבר
            author = None
            if title_tag:
                for el in title_tag.next_elements:
                    if isinstance(el, Tag):
                        t = el.get_text(strip=True)
                        if not t:
                            continue
                        if re.search(r"\(\d{2}\.\d{2}\.\d{2}\)", t):
                            continue
                        if t.isdigit():
                            continue
                        author = t
                        break
 
            # תאריך
            date_iso = None
            meta_time = soup_a.find("meta", attrs={"property": "article:published_time"})
            if meta_time and meta_time.get("content"):
                date_iso = meta_time["content"][:10]
 
            if not date_iso:
                m = re.search(r"\((\d{2})\.(\d{2})\.(\d{2})\)", soup_a.get_text())
                if m:
                    d, mth, y2 = m.groups()
                    date_iso = f"{2000 + int(y2):04d}-{mth}-{d}"
 
            if not date_iso:
                date_iso = datetime.now().strftime("%Y-%m-%d")
 
            # טקסט מלא
            paragraphs = []
            for p in soup_a.find_all("p"):
                t = p.get_text(strip=True)
                if len(t) > 25 and "הצטרפו למועדון" not in t and "תגובה" not in t:
                    paragraphs.append(t)
 
            data = {
                "title": title,
                "description": description,
                "author": author,
                "date": date_iso,
                "text": "\n".join(paragraphs),
                "url": article_url,
                "type": category_name
            }
 
            insert_article_to_db(data, outlet_id, category_id)
 
            # ------------------------------
            # תגובות (Comments)
            # ------------------------------
            article_id = get_article_id_by_link(article_url)
            if article_id:
                try:
                    scrape_comments_from_c14(article_url, article_id)
                except Exception as e:
                    print("❌ דילוג על תגובות בגלל שגיאה:", e)

            # השהייה בין כתבות
            time.sleep(random.uniform(5, 12))
 
        # השהייה בין עמודים
        time.sleep(random.uniform(12, 30))
        page += 1
 
# ======================================================
# main
# ======================================================
if __name__ == "__main__":
    CHANNEL14_OUTLET_ID = 2  # לעדכן לפי הטבלה News_Outlets
    scrape_c14_category(
        base_url="https://www.c14.co.il/archive/65839",
        outlet_id=CHANNEL14_OUTLET_ID
    )
