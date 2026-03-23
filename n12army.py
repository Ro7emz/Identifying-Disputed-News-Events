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
# נרמול scraped_at (אחיד לכל הסקרייפרים)
# ======================================================
def normalize_scraped_at(dt=None):
    if dt is None:
        dt = datetime.now()
    return dt.strftime("%Y-%m-%d %H:%M:%S")

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
            (news_outlet_id, date, title, description, type, link, text, author_name, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        outlet_id,
        data["date"] or datetime.now().strftime("%Y-%m-%d"),
        data["title"],
        data["description"],
        category_name,     # ✅ כמו C14: type = שם הקטגוריה
        data["url"],
        data["text"],
        data["author"],     # ✅ שם המחבר כטקסט
        normalize_scraped_at()
    ))
 
    article_id = cur.lastrowid
 
    cur.execute("""
        INSERT INTO Article_Categories (news_article_id, category_id)
        VALUES (?, ?)
    """, (article_id, category_id))
 
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


def get_n12_talkbacks_url(article_url):
    """
    Build tabletapp url from the mako article url, then extract the nTalkbacksPage url.
    """
    tablet_url = article_url
    tablet_url = tablet_url.replace("https://www.mako.co.il", "https://tabletapp.mako.co.il")
    tablet_url = tablet_url.replace("http://www.mako.co.il", "https://tabletapp.mako.co.il")

    r = fetch_with_backoff(tablet_url, timeout=20)
    if not r:
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a"):
        href = a.get("href") or ""
        if "nTalkbacksPage" in href:
            if href.startswith("/"):
                href = "https://tabletapp.mako.co.il" + href
            return href
    return None


def scrape_comments_from_n12(article_url, news_article_id):
    """
    Best-effort scraping from tabletapp nTalkbacksPage.
    If the server returns template placeholders (JS-rendered), we skip.
    """
    talkbacks_url = get_n12_talkbacks_url(article_url)
    if not talkbacks_url:
        print("⚠️ לא נמצא nTalkbacksPage עבור:", article_url)
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    source_url = article_url

    try:
        # polite delay
        time.sleep(random.uniform(0.8, 1.6))
        r = fetch_with_backoff(talkbacks_url, timeout=25, max_attempts=6)
        if not r:
            print("⚠️ דילוג תגובות N12 (לא הצלחתי להביא talkbacks):", talkbacks_url)
            return

        html = r.text
        # JS-rendered fallback detection (template tokens usually remain)
        if "{post_body}" in html or "post_body" in html or "{post_replies}" in html:
            print("⚠️ דילוג תגובות N12: נראה שתגובות נטענות דינמית (תבניות נשארו ב-HTML).")
            return

        soup = BeautifulSoup(html, "html.parser")

        # Heuristic parsing: attempt to parse comment rows from tables.
        # If nested/replies metadata exists via data-* attributes, we will map it.
        comment_nodes = []
        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue
            author = tds[0].get_text(" ", strip=True)
            title = tds[1].get_text(" ", strip=True) if tds[1] else ""
            body = tds[2].get_text(" ", strip=True)
            if not body:
                continue
            # Skip obvious non-comment/template lines
            if "ביטול" in body and len(body) < 50:
                continue

            parent_db_id = None

            # Optional nested mapping using data-* / id / inputs
            post_key = None
            parent_key = None
            try:
                post_key = tr.get("data-post-index") or tr.get("data-postid") or None
                parent_key = tr.get("data-parent-index") or tr.get("data-parentid") or None
            except Exception:
                pass

            comment_nodes.append({
                "post_key": post_key,
                "parent_key": parent_key,
                "author": author,
                "title": title,
                "body": body
            })

        if not comment_nodes:
            print("⚠️ דילוג תגובות N12: לא נמצאו תגובות ב-HTML שאפשר לפרש.")
            return

        # If we have keys, map parent->db id for nesting; otherwise all top-level.
        id_by_post_key = {}

        for node in comment_nodes:
            author_name = (node["author"] or "").strip()
            comment_text = (node["body"] or "").strip()
            if not comment_text:
                continue
            comment_title = node["title"].strip() if node["title"] else None

            parent_comment_db_id = None
            if node["parent_key"]:
                parent_comment_db_id = id_by_post_key.get(node["parent_key"])

            db_comment_id = get_or_insert_comment_id(
                cur=cur,
                news_article_id=news_article_id,
                parent_comment_id=parent_comment_db_id,
                author_name=author_name,
                comment_title=comment_title,
                comment_text=comment_text,
                likes=0,
                dislikes=0,
                source_url=source_url
            )

            if node["post_key"]:
                id_by_post_key[node["post_key"]] = db_comment_id

        conn.commit()
        print(f"✔️ הוכנסו תגובות N12 (אולי חלקיות) ל-article_id={news_article_id}")
    except Exception as e:
        print("❌ שגיאה בעת scraping תגובות N12:", e)
    finally:
        conn.close()
 
 
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
                # ------------------------------
                # תגובות (Comments)
                # ------------------------------
                try:
                    article_id = get_article_id_by_link(data["url"])
                    if article_id:
                        scrape_comments_from_n12(data["url"], article_id)
                except Exception as e:
                    print("❌ דילוג על תגובות N12 בגלל שגיאה:", e)
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
