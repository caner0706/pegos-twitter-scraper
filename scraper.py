# =====================================================
# Pegos X(Twitter) Scraper - Max Scroll + Full JSON Format
# - Keyword search (no top/live required) + optional live pass
# - Scroll as much as possible (adaptive stop)
# - Collect tweets + comments
# - Fetch follower/following for tweet owner + comment owners (with cache)
# - Save JSON (NOT JSONL): run + latest + daily + all
# =====================================================

import os
import time
import random
import json
from datetime import datetime, timezone
from urllib.parse import quote_plus

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


# ------------------------ helpers ------------------------
def utc_now():
    return datetime.now(timezone.utc)

def safe_int(val: str) -> int:
    if not val:
        return 0
    v = str(val).replace(",", "").replace("·", "").strip()
    try:
        # K/M/B variants
        if v.endswith("K"):
            return int(float(v[:-1]) * 1_000)
        if v.endswith("B"):  # sometimes used for thousand in some locales
            return int(float(v[:-1]) * 1_000)
        if v.endswith("M") or v.endswith("Mn"):
            return int(float(v[:-1]) * 1_000_000)
        return int(float(v))
    except:
        return 0

def find_view_node(article):
    v = article.find(attrs={"data-testid": ["viewCount", "views"]})
    if v: return v
    v = article.find("span", attrs={"aria-label": lambda s: s and "views" in s.lower()})
    if v: return v
    v = article.find("div", attrs={"aria-label": lambda s: s and "views" in s.lower()})
    return v

def extract_tweet_url(article):
    try:
        ttag = article.find("time")
        if ttag:
            a = ttag.find_parent("a")
            if a:
                href = a.get("href", "")
                if href and "/status/" in href:
                    return f"https://x.com{href}" if href.startswith("/") else href
        for a in article.find_all("a", href=True):
            href = a.get("href", "")
            if "/status/" in href:
                return f"https://x.com{href}" if href.startswith("/") else href
    except:
        pass
    return None

def extract_user_basic(article):
    """
    username + display_name (best effort) from tweet/article.
    """
    out = {"username": None, "display_name": None}
    try:
        user_elem = article.find(attrs={"data-testid": "User-Name"})
        if user_elem:
            # username
            for link in user_elem.find_all("a", href=True):
                href = link.get("href", "")
                if href.startswith("/") and "/status/" not in href and not href.startswith("//"):
                    parts = href.strip("/").split("/")
                    if parts and parts[0] and not parts[0].startswith("i/"):
                        out["username"] = parts[0]
                        break
            # display name
            # first visible span text usually includes display name (may be imperfect)
            span = user_elem.find("span")
            if span:
                out["display_name"] = span.get_text(strip=True) or None
    except:
        pass
    return out

def read_json(path: str):
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except:
        return []

def write_json(path: str, rows: list):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

def dedupe(rows: list):
    """
    Dedupe by (tweet_url,time) else (tweet,time,username)
    """
    seen = set()
    out = []
    for r in rows:
        tweet_url = (r.get("tweet_url") or "").strip()
        t = (r.get("time") or "").strip()
        tweet = (r.get("tweet") or "").strip()
        username = (r.get("username") or "").strip()
        key = (tweet_url, t) if tweet_url else (tweet, t, username)
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


# ------------------------ ENV (no hardcoded tokens) ------------------------
AUTH_TOKEN = os.getenv("AUTH_TOKEN")
CT0 = os.getenv("CT0")

if (not AUTH_TOKEN) or (not CT0):
    try:
        from dotenv import load_dotenv
        load_dotenv()
        AUTH_TOKEN = os.getenv("AUTH_TOKEN") or AUTH_TOKEN
        CT0 = os.getenv("CT0") or CT0
    except ImportError:
        pass

if not AUTH_TOKEN or not CT0:
    raise RuntimeError("❌ AUTH_TOKEN / CT0 eksik. GitHub Secrets veya .env ile ver.")

# Volume knobs
SCROLLS_MAX = int(os.getenv("SCROLLS_MAX", "250"))              # hard cap
SCROLL_STEP_PX = int(os.getenv("SCROLL_STEP_PX", "1400"))
STOP_AFTER_NO_NEW = int(os.getenv("STOP_AFTER_NO_NEW", "14"))   # N scrolls with no new tweets => stop
PAGE_WAIT = float(os.getenv("PAGE_WAIT", "6.0"))
SCROLL_WAIT_MIN = float(os.getenv("SCROLL_WAIT_MIN", "1.8"))
SCROLL_WAIT_MAX = float(os.getenv("SCROLL_WAIT_MAX", "3.1"))

# Data enrichment knobs (YOU asked for real follower/following)
FETCH_PROFILE = os.getenv("FETCH_PROFILE", "1").lower() in ("1", "true", "yes")
FETCH_COMMENTS = os.getenv("FETCH_COMMENTS", "1").lower() in ("1", "true", "yes")
MAX_COMMENTS_PER_TWEET = int(os.getenv("MAX_COMMENTS_PER_TWEET", "10"))

# Optional: do both passes (default search + live)
ENABLE_LIVE_PASS = os.getenv("ENABLE_LIVE_PASS", "1").lower() in ("1", "true", "yes")

# Keywords
KEYWORDS = os.getenv("KEYWORDS", "bitcoin,").split(",")
KEYWORDS = [k.strip() for k in KEYWORDS if k.strip()]

# ------------------------ paths ------------------------
TODAY = utc_now().strftime("%Y-%m-%d")
RUN_STAMP = utc_now().strftime("%Y%m%dT%H%M%SZ")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

OUT_DIR = os.path.join(SCRIPT_DIR, "data", TODAY)
RUNS_DIR = os.path.join(OUT_DIR, "runs")
ALL_DIR = os.path.join(SCRIPT_DIR, "data", "all")

RUN_JSON = os.path.join(RUNS_DIR, f"{RUN_STAMP}.json")
LATEST_JSON = os.path.join(OUT_DIR, "latest.json")
DAILY_JSON = os.path.join(OUT_DIR, "daily.json")
ALL_JSON = os.path.join(ALL_DIR, "all.json")

os.makedirs(RUNS_DIR, exist_ok=True)
os.makedirs(ALL_DIR, exist_ok=True)


# ------------------------ browser ------------------------
def make_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def login_with_cookies(driver):
    driver.get("https://x.com")
    time.sleep(3)
    driver.add_cookie({"name": "auth_token", "value": AUTH_TOKEN, "domain": ".x.com"})
    driver.add_cookie({"name": "ct0", "value": CT0, "domain": ".x.com"})
    driver.refresh()
    time.sleep(5)


# ------------------------ profile fetch (cached) ------------------------
_profile_cache = {}  # username -> (followers, following)

def fetch_profile_counts(driver, username: str):
    """
    Real follower/following from profile page. Cached to reduce repeats.
    """
    if not username:
        return (0, 0)
    if username in _profile_cache:
        return _profile_cache[username]

    followers = 0
    following = 0
    try:
        url = f"https://x.com/{username}"
        driver.get(url)
        time.sleep(2.5)

        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Following
        following_elem = soup.find("a", href=lambda x: x and x.endswith("/following"))
        if following_elem:
            txt = following_elem.get_text(" ", strip=True)
            # e.g. "123 Following"
            following = safe_int(txt.split()[0] if txt else "0")

        # Followers
        follower_elem = soup.find("a", href=lambda x: x and x.endswith("/followers"))
        if follower_elem:
            txt = follower_elem.get_text(" ", strip=True)
            followers = safe_int(txt.split()[0] if txt else "0")

    except:
        pass

    _profile_cache[username] = (followers, following)
    return (followers, following)


# ------------------------ comments extraction ------------------------
def extract_comments(driver, tweet_url: str, max_comments: int):
    """
    Returns list of comment dicts in YOUR required fields.
    """
    out = []
    if not tweet_url:
        return out

    try:
        driver.get(tweet_url)
        time.sleep(3.5)

        # load more replies
        for _ in range(4):
            driver.execute_script("window.scrollBy(0, 1000);")
            time.sleep(random.uniform(1.2, 1.8))

        soup = BeautifulSoup(driver.page_source, "html.parser")
        articles = soup.find_all("article")

        # first article is usually main tweet; skip it
        for art in articles[1:]:
            if len(out) >= max_comments:
                break
            try:
                txt = art.find(attrs={"data-testid": "tweetText"})
                if not txt:
                    continue
                comment_text = txt.get_text(" ", strip=True)
                if len(comment_text) < 2:
                    continue

                # comment username
                cu = extract_user_basic(art)
                comment_username = cu.get("username")

                # counts
                reply = art.find(attrs={"data-testid": ["reply", "conversation"]})
                rt = art.find(attrs={"data-testid": ["retweet", "repost"]})
                like = art.find(attrs={"data-testid": ["like", "favorite"]})
                view = find_view_node(art)

                comment_reply = safe_int(reply.get_text(strip=True) if reply else "0")
                comment_retweet = safe_int(rt.get_text(strip=True) if rt else "0")
                comment_like = safe_int(like.get_text(strip=True) if like else "0")
                comment_view = safe_int(view.get_text(strip=True) if view else "0")

                # follower/following for comment owner
                c_followers = 0
                c_following = 0
                if FETCH_PROFILE and comment_username:
                    c_followers, c_following = fetch_profile_counts(driver, comment_username)

                out.append({
                    "comment_text": comment_text,
                    "comment_username": comment_username,
                    "comment_like": comment_like,
                    "comment_retweet": comment_retweet,
                    "comment_reply": comment_reply,
                    "comment_view": comment_view,
                    "comment_follower_count": c_followers,
                    "comment_following_count": c_following
                })

            except:
                continue

    except:
        return out

    return out


# ------------------------ scraping search (max scroll) ------------------------
def scrape_keyword(driver, keyword: str, live_pass: bool):
    """
    - Search keyword
    - Scroll and collect as much as possible
    - Return list in YOUR required top-level format
    """
    suffix = "&f=live" if live_pass else ""
    q = keyword
    url = f"https://x.com/search?q={quote_plus(q)}&src=typed_query{suffix}"

    print(f"\n🔎 keyword={keyword} pass={'live' if live_pass else 'default'}")
    driver.get(url)
    time.sleep(PAGE_WAIT)

    rows = []
    seen_local = set()
    no_new_streak = 0
    last_total = 0

    for i in range(SCROLLS_MAX):
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        articles = soup.find_all("article")

        added = 0

        for art in articles:
            try:
                text_tag = art.find(attrs={"data-testid": "tweetText"})
                if not text_tag:
                    continue
                text = text_tag.get_text(" ", strip=True)
                if len(text) < 5:
                    continue

                ttag = art.find("time")
                tstr = ttag["datetime"] if ttag and ttag.has_attr("datetime") else None

                tweet_url = extract_tweet_url(art)
                key = (tweet_url, tstr) if tweet_url else (text, tstr)

                if key in seen_local:
                    continue
                seen_local.add(key)

                reply = art.find(attrs={"data-testid": ["reply", "conversation"]})
                rt = art.find(attrs={"data-testid": ["retweet", "repost"]})
                like = art.find(attrs={"data-testid": ["like", "favorite"]})
                view = find_view_node(art)

                u = extract_user_basic(art)
                username = u.get("username")
                display_name = u.get("display_name")

                follower_count = 0
                following_count = 0
                if FETCH_PROFILE and username:
                    follower_count, following_count = fetch_profile_counts(driver, username)

                comments = []
                if FETCH_COMMENTS and tweet_url:
                    comments = extract_comments(driver, tweet_url, max_comments=MAX_COMMENTS_PER_TWEET)
                    # go back to search page to continue scrolling
                    driver.back()
                    time.sleep(2.2)

                rows.append({
                    "keyword": keyword,
                    "tweet": text,
                    "time": tstr,
                    "tweet_url": tweet_url,  # keep for debugging; you can remove later if you want
                    "comment": safe_int(reply.get_text(strip=True) if reply else "0"),
                    "retweet": safe_int(rt.get_text(strip=True) if rt else "0"),
                    "like": safe_int(like.get_text(strip=True) if like else "0"),
                    "see_count": safe_int(view.get_text(strip=True) if view else "0"),
                    "username": username,
                    "display_name": display_name,
                    "follower_count": follower_count,
                    "following_count": following_count,
                    "comments_count": len(comments),
                    "comments": comments
                })

                added += 1

                time.sleep(random.uniform(0.15, 0.45))

            except:
                continue

        # adaptive stopping
        if len(rows) == last_total:
            no_new_streak += 1
        else:
            no_new_streak = 0
            last_total = len(rows)

        print(f"  scroll={i+1}/{SCROLLS_MAX} total={len(rows)} added={added} no_new_streak={no_new_streak}")

        if no_new_streak >= STOP_AFTER_NO_NEW:
            print("  ⛔ stop early: no new tweets")
            break

        driver.execute_script(f"window.scrollBy(0, {SCROLL_STEP_PX});")
        time.sleep(random.uniform(SCROLL_WAIT_MIN, SCROLL_WAIT_MAX))

    return rows


# ------------------------ main ------------------------
def main():
    driver = make_driver()
    try:
        login_with_cookies(driver)
        print("✅ Login OK:", driver.current_url)

        collected = []

        for kw in KEYWORDS:
            # default search
            collected.extend(scrape_keyword(driver, kw, live_pass=False))
            # optional live search too (more recall)
            if ENABLE_LIVE_PASS:
                collected.extend(scrape_keyword(driver, kw, live_pass=True))

        # dedupe
        collected = dedupe(collected)

        # remove tweet_url if you do NOT want it in final data (your sample didn't include it)
        for r in collected:
            r.pop("tweet_url", None)

        # save run/latest/daily/all (JSON arrays)
        write_json(RUN_JSON, collected)
        write_json(LATEST_JSON, collected)

        daily_old = read_json(DAILY_JSON)
        daily_new = dedupe(daily_old + collected)
        write_json(DAILY_JSON, daily_new)

        all_old = read_json(ALL_JSON)
        all_new = dedupe(all_old + collected)
        write_json(ALL_JSON, all_new)

        print(f"✅ RUN   : {RUN_JSON}   ({len(collected)})")
        print(f"✅ LATEST: {LATEST_JSON} ({len(collected)})")
        print(f"✅ DAILY : {DAILY_JSON}  ({len(daily_new)})")
        print(f"✅ ALL   : {ALL_JSON}    ({len(all_new)})")

    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    main()
