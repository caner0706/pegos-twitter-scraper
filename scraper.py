# =====================================================
# Pegos X(Twitter) Scraper - FINAL (Exact JSON Format)
# - OUTPUT: JSON (array) -> run.json, latest.json, daily.json, all.json
# - Tweets: keyword, tweet, time, comment, retweet, like, see_count, username, display_name,
#          follower_count, following_count, comments_count, comments[]
# - Comments: comment_text, comment_username, comment_like, comment_retweet, comment_reply,
#            comment_view, comment_follower_count, comment_following_count
# - REQUIRE: AUTH_TOKEN + CT0 env vars (GitHub Secrets / .env)
# =====================================================

import os
import time
import random
import json
from datetime import datetime, timezone
from urllib.parse import quote_plus

from bs4 import BeautifulSoup
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# -------------------- helpers --------------------
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

def extract_user_info_from_article(article):
    """
    Best-effort:
      - username from /<user> links under User-Name block
      - display_name from span
    """
    out = {"username": None, "display_name": None}
    try:
        user_elem = article.find(attrs={"data-testid": "User-Name"})
        if user_elem:
            for link in user_elem.find_all("a", href=True):
                href = link.get("href", "")
                if href.startswith("/") and "/status/" not in href and not href.startswith("//"):
                    parts = href.strip("/").split("/")
                    if parts and parts[0] and not parts[0].startswith("i/"):
                        out["username"] = parts[0]
                        break
            # display name (best effort)
            span = user_elem.find("span")
            if span:
                out["display_name"] = span.get_text(strip=True)
    except:
        pass
    return out

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

def extract_tweet_id(tweet_url: str):
    if not tweet_url:
        return None
    try:
        parts = tweet_url.split("/status/")
        if len(parts) < 2:
            return None
        tid = parts[1].split("?")[0].split("/")[0]
        return tid or None
    except:
        return None

def read_json_array(path: str):
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except:
        return []

def write_json_array(path: str, rows: list):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

def dedupe_rows(rows: list):
    """
    Strong dedupe:
      - prefer tweet_id
      - else tweet_url+time
      - else tweet+time+username
    """
    seen = set()
    out = []
    for r in rows:
        tweet_id = (r.get("_tweet_id") or "").strip()
        tweet_url = (r.get("_tweet_url") or "").strip()
        t = (r.get("time") or "").strip()
        tweet = (r.get("tweet") or "").strip()
        username = (r.get("username") or "").strip()

        if tweet_id:
            key = ("id", tweet_id)
        elif tweet_url and t:
            key = ("url_time", tweet_url, t)
        else:
            key = ("text_time_user", tweet, t, username)

        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

# -------------------- ENV (NO hardcoded tokens) --------------------
AUTH_TOKEN = os.getenv("AUTH_TOKEN")
CT0 = os.getenv("CT0")

if (not AUTH_TOKEN or not CT0):
    try:
        from dotenv import load_dotenv
        load_dotenv()
        AUTH_TOKEN = os.getenv("AUTH_TOKEN") or AUTH_TOKEN
        CT0 = os.getenv("CT0") or CT0
    except ImportError:
        pass

if not AUTH_TOKEN or not CT0:
    raise RuntimeError("❌ AUTH_TOKEN / CT0 missing. Set as environment variables (GitHub Secrets) or .env.")

# optional HF upload (if you use it elsewhere)
HF_TOKEN = os.getenv("HF_TOKEN")
HF_REPO_ID = os.getenv("HF_REPO_ID")

# -------------------- knobs --------------------
KEYWORDS = ["bitcoin",]
MODES = ["", "&f=live"]  # default + live together

# Max data scroll behavior
SCROLLS_MAX = int(os.getenv("SCROLLS_MAX", "160"))
SCROLL_STEP_PX = int(os.getenv("SCROLL_STEP_PX", "1400"))
STOP_AFTER_NO_NEW = int(os.getenv("STOP_AFTER_NO_NEW", "12"))
PAGE_WAIT = float(os.getenv("PAGE_WAIT", "6"))
SCROLL_WAIT_MIN = float(os.getenv("SCROLL_WAIT_MIN", "1.8"))
SCROLL_WAIT_MAX = float(os.getenv("SCROLL_WAIT_MAX", "3.0"))

# Profile fetch (REAL follower/following)
PROFILE_RETRIES = int(os.getenv("PROFILE_RETRIES", "2"))
PROFILE_WAIT_MIN = float(os.getenv("PROFILE_WAIT_MIN", "1.4"))
PROFILE_WAIT_MAX = float(os.getenv("PROFILE_WAIT_MAX", "2.6"))

# Comment depth (this is expensive; but you asked "kesin")
MAX_COMMENTS_PER_TWEET = int(os.getenv("MAX_COMMENTS_PER_TWEET", "9"))
COMMENTS_SCROLL_PASSES = int(os.getenv("COMMENTS_SCROLL_PASSES", "3"))
COMMENTS_SCROLL_STEP_PX = int(os.getenv("COMMENTS_SCROLL_STEP_PX", "900"))
COMMENTS_WAIT_MIN = float(os.getenv("COMMENTS_WAIT_MIN", "1.2"))
COMMENTS_WAIT_MAX = float(os.getenv("COMMENTS_WAIT_MAX", "1.8"))

# -------------------- paths --------------------
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

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(RUNS_DIR, exist_ok=True)
os.makedirs(ALL_DIR, exist_ok=True)

# -------------------- driver --------------------
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

# -------------------- profile fetch (cached) --------------------
_profile_cache = {}

def fetch_profile_counts(driver, username: str):
    if not username:
        return 0, 0
    if username in _profile_cache:
        return _profile_cache[username]

    follower_count, following_count = 0, 0

    for _ in range(PROFILE_RETRIES + 1):
        try:
            driver.get(f"https://x.com/{username}")
            time.sleep(random.uniform(PROFILE_WAIT_MIN, PROFILE_WAIT_MAX))
            html = driver.page_source or ""
            if ("Something went wrong" in html) or ("Try again" in html) or (len(html) < 5000):
                time.sleep(random.uniform(2.0, 4.0))
                continue

            soup = BeautifulSoup(html, "html.parser")

            follower_elem = soup.find("a", href=lambda x: x and x.endswith("/followers"))
            if follower_elem:
                follower_text = follower_elem.get_text(" ", strip=True)
                follower_count = safe_int(follower_text.split()[0] if follower_text else "0")

            following_elem = soup.find("a", href=lambda x: x and x.endswith("/following"))
            if following_elem:
                following_text = following_elem.get_text(" ", strip=True)
                following_count = safe_int(following_text.split()[0] if following_text else "0")

            if follower_count or following_count:
                break

        except:
            time.sleep(random.uniform(2.0, 4.0))

    _profile_cache[username] = (follower_count, following_count)
    return follower_count, following_count

# -------------------- comments extraction (with metrics) --------------------
def extract_comment_username(comment_article):
    try:
        user_elem = comment_article.find(attrs={"data-testid": "User-Name"})
        if user_elem:
            for link in user_elem.find_all("a", href=True):
                href = link.get("href", "")
                if href.startswith("/") and "/status/" not in href and not href.startswith("//"):
                    parts = href.strip("/").split("/")
                    if parts and parts[0] and not parts[0].startswith("i/"):
                        return parts[0]
        # fallback
        a = comment_article.find("a", href=lambda x: x and x.startswith("/") and "/status/" not in x)
        if a:
            href = a.get("href", "")
            parts = href.strip("/").split("/")
            if parts and parts[0] and not parts[0].startswith("i/"):
                return parts[0]
    except:
        pass
    return None

def extract_comments(tweet_url, driver, max_comments=9):
    comments = []
    if not tweet_url:
        return comments

    try:
        driver.get(tweet_url)
        time.sleep(3)

        # Scroll to load replies
        for _ in range(COMMENTS_SCROLL_PASSES):
            driver.execute_script(f"window.scrollBy(0, {COMMENTS_SCROLL_STEP_PX});")
            time.sleep(random.uniform(COMMENTS_WAIT_MIN, COMMENTS_WAIT_MAX))

        soup = BeautifulSoup(driver.page_source, "html.parser")
        arts = soup.find_all("article")

        # First is main tweet. Others are replies.
        for art in arts[1:]:
            if len(comments) >= max_comments:
                break
            try:
                txt = art.find(attrs={"data-testid": "tweetText"})
                if not txt:
                    continue
                comment_text = txt.get_text(" ", strip=True)
                if len(comment_text) < 1:
                    continue

                comment_username = extract_comment_username(art)

                like = art.find(attrs={"data-testid": ["like", "favorite"]})
                rt = art.find(attrs={"data-testid": ["retweet", "repost"]})
                rp = art.find(attrs={"data-testid": ["reply", "conversation"]})
                vw = find_view_node(art)

                c_followers, c_following = fetch_profile_counts(driver, comment_username) if comment_username else (0, 0)

                comments.append({
                    "comment_text": comment_text,
                    "comment_username": comment_username,
                    "comment_like": safe_int(like.get_text(strip=True) if like else "0"),
                    "comment_retweet": safe_int(rt.get_text(strip=True) if rt else "0"),
                    "comment_reply": safe_int(rp.get_text(strip=True) if rp else "0"),
                    "comment_view": safe_int(vw.get_text(strip=True) if vw else "0"),
                    "comment_follower_count": c_followers,
                    "comment_following_count": c_following
                })
            except:
                continue

    except:
        pass

    return comments

# -------------------- scraping --------------------
def scrape_keyword_mode(driver, keyword: str, mode_suffix: str):
    """
    mode_suffix: "" (default) or "&f=live"
    """
    q = keyword
    url = f"https://x.com/search?q={quote_plus(q)}&src=typed_query{mode_suffix}"
    driver.get(url)
    time.sleep(PAGE_WAIT)

    out = []
    seen_local = set()

    no_new_streak = 0
    last_total = 0

    for i in range(SCROLLS_MAX):
        soup = BeautifulSoup(driver.page_source, "html.parser")
        articles = soup.find_all("article")
        added_this_round = 0

        for art in articles:
            try:
                text_tag = art.find(attrs={"data-testid": "tweetText"})
                if not text_tag:
                    continue
                text = text_tag.get_text(" ", strip=True)
                if len(text) < 2:
                    continue

                ttag = art.find("time")
                tstr = ttag["datetime"] if ttag and ttag.has_attr("datetime") else None

                tweet_url = extract_tweet_url(art)
                tweet_id = extract_tweet_id(tweet_url)

                key = ("id", tweet_id) if tweet_id else (tweet_url, tstr, text)
                if key in seen_local:
                    continue
                seen_local.add(key)

                reply = art.find(attrs={"data-testid": ["reply", "conversation"]})
                retw = art.find(attrs={"data-testid": ["retweet", "repost"]})
                like = art.find(attrs={"data-testid": ["like", "favorite"]})
                view = find_view_node(art)

                ui = extract_user_info_from_article(art)
                username = ui.get("username")
                display_name = ui.get("display_name")

                # REAL follower/following
                follower_count, following_count = fetch_profile_counts(driver, username) if username else (0, 0)

                # Comments (REAL + metrics)
                comments = extract_comments(tweet_url, driver, max_comments=MAX_COMMENTS_PER_TWEET) if tweet_url else []

                out.append({
                    "_tweet_id": tweet_id,
                    "_tweet_url": tweet_url,

                    "keyword": keyword,
                    "tweet": text,
                    "time": tstr,
                    "comment": safe_int(reply.get_text(strip=True) if reply else "0"),
                    "retweet": safe_int(retw.get_text(strip=True) if retw else "0"),
                    "like": safe_int(like.get_text(strip=True) if like else "0"),
                    "see_count": safe_int(view.get_text(strip=True) if view else "0"),
                    "username": username,
                    "display_name": display_name,
                    "follower_count": follower_count,
                    "following_count": following_count,
                    "comments_count": len(comments),
                    "comments": comments
                })

                added_this_round += 1
                time.sleep(random.uniform(0.4, 0.9))  # throttle

                # after visiting tweet_url (comments) we are not on search page anymore
                # go back to search results
                try:
                    driver.back()
                    time.sleep(random.uniform(1.2, 2.0))
                except:
                    pass

            except:
                continue

        if len(out) == last_total:
            no_new_streak += 1
        else:
            no_new_streak = 0
            last_total = len(out)

        if no_new_streak >= STOP_AFTER_NO_NEW:
            break

        driver.execute_script(f"window.scrollBy(0, {SCROLL_STEP_PX});")
        time.sleep(random.uniform(SCROLL_WAIT_MIN, SCROLL_WAIT_MAX))

    return out

def strip_internal_fields(rows: list):
    for r in rows:
        r.pop("_tweet_id", None)
        r.pop("_tweet_url", None)
    return rows

# -------------------- main --------------------
def main():
    driver = make_driver()
    try:
        login_with_cookies(driver)

        collected = []
        for kw in KEYWORDS:
            for mode_suffix in MODES:
                collected.extend(scrape_keyword_mode(driver, kw, mode_suffix))

        # dedupe + sort (optional)
        collected = dedupe_rows(collected)
        df = pd.DataFrame(collected)
        if not df.empty:
            sort_cols = [c for c in ["like", "retweet", "comment", "see_count"] if c in df.columns]
            if sort_cols:
                df.sort_values(by=sort_cols, ascending=False, inplace=True)
            rows = df.to_dict(orient="records")
        else:
            rows = []

        rows = strip_internal_fields(rows)

        # save run + latest
        write_json_array(RUN_JSON, rows)
        write_json_array(LATEST_JSON, rows)

        # daily append+dedupe
        daily_old = read_json_array(DAILY_JSON)
        daily_new = dedupe_rows(daily_old + rows)
        daily_new = strip_internal_fields(daily_new)
        write_json_array(DAILY_JSON, daily_new)

        # all append+dedupe
        all_old = read_json_array(ALL_JSON)
        all_new = dedupe_rows(all_old + rows)
        all_new = strip_internal_fields(all_new)
        write_json_array(ALL_JSON, all_new)

        print(f"✅ RUN   : {RUN_JSON}   ({len(rows)})")
        print(f"✅ LATEST: {LATEST_JSON} ({len(rows)})")
        print(f"✅ DAILY : {DAILY_JSON}  ({len(daily_new)})")
        print(f"✅ ALL   : {ALL_JSON}    ({len(all_new)})")

    finally:
        try:
            driver.quit()
        except:
            pass

if __name__ == "__main__":
    main()
