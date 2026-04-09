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
# GET JSON עם backoff חכם
# ======================================================
def fetch_json_with_backoff(url, timeout=20, max_attempts=6):
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
        except Exception:
            r = None

        if r is not None and r.status_code == 200:
            try:
                return r.json()
            except Exception:
                print(f"❌ התשובה אינה JSON: {url}")
                return None

        code = r.status_code if r is not None else "NETWORK_ERR"
        base = min(120, 5 * attempt * attempt)
        sleep_s = base + random.uniform(0, base / 2)

        print(f"⚠️ JSON fetch failed ({code}) attempt {attempt}/{max_attempts}, sleep {sleep_s:.1f}s")
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
# הבאת article_id לפי לינק
# ======================================================
def get_article_id_by_link(article_url):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        SELECT news_article_id
        FROM News_articles
        WHERE link = ?
        """,
        (article_url,)
    )
    row = cur.fetchone()
    conn.close()

    return row[0] if row else None

# ======================================================
# חילוץ article_id של C14 מתוך URL
# ======================================================
def extract_c14_article_id(article_url):
    m = re.search(r"/article/(\d+)", article_url)
    return m.group(1) if m else None
 
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
    row = cur.fetchone()
    if row:
        print("➡️ כבר קיים במסד:", data["title"])
        conn.close()
        return row[0]

    scraped_at = datetime.now().strftime("%d/%m/%Y %H:%M")
 
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
            scraped_at
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
    return article_id

# ======================================================
# הכנסת תגובה למסד
# ======================================================
def insert_comment_to_db(
    news_article_id,
    author_name,
    comment_text,
    likes,
    dislikes,
    source_url,
    comment_title=None,
    parent_comment_id=None
):
    if not comment_text or not str(comment_text).strip():
        return

    author_name = author_name.strip() if isinstance(author_name, str) else author_name
    comment_text = str(comment_text).strip()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # מניעת כפילויות בסיסית
    cur.execute(
        """
        SELECT comment_id
        FROM Comments
        WHERE news_article_id = ?
          AND IFNULL(author_name, '') = IFNULL(?, '')
          AND comment_text = ?
          AND IFNULL(parent_comment_id, -1) = IFNULL(?, -1)
        """,
        (news_article_id, author_name, comment_text, parent_comment_id)
    )
    if cur.fetchone():
        conn.close()
        print("➡️ תגובה כבר קיימת במסד")
        return

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

    conn.commit()
    conn.close()
    print("💬 נשמרה תגובה:", (comment_text[:60] + "...") if len(comment_text) > 60 else comment_text)

# ======================================================
# חילוץ אינט בטוח
# ======================================================
def safe_int(value):
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    text = str(value).strip()
    m = re.search(r"\d+", text)
    return int(m.group()) if m else 0

# ======================================================
# ניסיון חילוץ שדות מתגובה מה-API
# ======================================================
def extract_comment_fields(comment_obj):
    author_name = (
        comment_obj.get("author_name")
        or comment_obj.get("author")
        or comment_obj.get("name")
        or comment_obj.get("username")
        or comment_obj.get("user_name")
    )

    comment_title = (
        comment_obj.get("comment_title")
        or comment_obj.get("title")
    )

    comment_text = (
        comment_obj.get("comment_text")
        or comment_obj.get("content")
        or comment_obj.get("text")
        or comment_obj.get("comment")
        or comment_obj.get("body")
        or comment_obj.get("message")
    )

    likes = safe_int(
        comment_obj.get("likes")
        or comment_obj.get("like_count")
        or comment_obj.get("upvotes")
        or comment_obj.get("upvote_count")
    )

    dislikes = safe_int(
        comment_obj.get("dislikes")
        or comment_obj.get("dislike_count")
        or comment_obj.get("downvotes")
        or comment_obj.get("downvote_count")
    )

    return author_name, comment_title, comment_text, likes, dislikes

# ======================================================
# סקרייפר תגובות C14 דרך API
# ======================================================
def scrape_c14_comments_api(c14_article_id, article_url, news_article_id):
    print(f"🔎 מביא תגובות דרך API לכתבה {c14_article_id}")

    offset = 0
    total_saved_or_checked = 0

    while True:
        api_url = f"https://www.c14.co.il/comments?article_id={c14_article_id}&offset={offset}"
        print("🌐", api_url)

        data = fetch_json_with_backoff(api_url, timeout=15)
        if data is None:
            print("❌ לא התקבל JSON תקין מה-API של התגובות")
            break

        # לפעמים API מחזיר dict ולא list
        if isinstance(data, dict):
            if isinstance(data.get("comments"), list):
                comments_batch = data["comments"]
            elif isinstance(data.get("data"), list):
                comments_batch = data["data"]
            elif isinstance(data.get("results"), list):
                comments_batch = data["results"]
            else:
                # אם אין מערך ברור של תגובות
                print("ℹ️ מבנה JSON לא מוכר:", list(data.keys()))
                break
        elif isinstance(data, list):
            comments_batch = data
        else:
            print("ℹ️ מבנה תשובה לא צפוי")
            break

        if not comments_batch:
            print("🔚 אין עוד תגובות")
            break

        for comment_obj in comments_batch:
            try:
                if not isinstance(comment_obj, dict):
                    continue

                author_name, comment_title, comment_text, likes, dislikes = extract_comment_fields(comment_obj)

                insert_comment_to_db(
                    news_article_id=news_article_id,
                    author_name=author_name,
                    comment_text=comment_text,
                    likes=likes,
                    dislikes=dislikes,
                    source_url=article_url,
                    comment_title=comment_title,
                    parent_comment_id=None
                )

                total_saved_or_checked += 1

            except Exception as e:
                print("❌ שגיאה בעיבוד תגובה:", e)

        # אם חזר פחות ממה שציפינו, כנראה נגמר
        batch_size = len(comments_batch)
        if batch_size == 0:
            break

        offset += batch_size
        time.sleep(random.uniform(1, 2))

    print(f"✅ הסתיים ניסיון שמירת תגובות. נשמרו/נבדקו: {total_saved_or_checked}")
 
# ======================================================
# ריצה על כל ארכיון ערוץ 14 – עד שאין עוד עמודים
# ======================================================
def scrape_c14_category(base_url, outlet_id):
    category_name = "צבא וביטחון"
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
 
            article_db_id = insert_article_to_db(data, outlet_id, category_id)

            if not article_db_id:
                article_db_id = get_article_id_by_link(article_url)

            c14_article_id = extract_c14_article_id(article_url)

            if article_db_id and c14_article_id:
                scrape_c14_comments_api(c14_article_id, article_url, article_db_id)
            else:
                print("❌ לא הצלחתי למצוא article_id בשביל התגובות")
 
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
        base_url="https://www.c14.co.il/archive/990",
        outlet_id=CHANNEL14_OUTLET_ID
    )
