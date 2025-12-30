# =====================================================
# Pegos Twitter Scraper (Top + Live, robust counts, always-save)
# MIN CHANGES: token/env fix + JSONL save (+ optional HF upload)
# =====================================================
import os, time, random
import json
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# (Optional) HF upload
try:
    from huggingface_hub import HfApi
    HF_AVAILABLE = True
except Exception:
    HF_AVAILABLE = False


def safe_int(val: str):
    """Metin sayıları (3.5K, 1M, vb.) güvenli int'e çevirir."""
    if not val:
        return 0
    val = str(val).replace(',', '').replace('·', '').strip()
    try:
        if val.endswith('B'):
            return int(float(val[:-1]) * 1_000)
        if val.endswith('M') or val.endswith('Mn'):
            return int(float(val[:-1]) * 1_000_000)
        return int(float(val))
    except:
        return 0


def find_view_node(article):
    """Tweet view sayısını yakalamak için alternatif testler."""
    v = article.find(attrs={"data-testid": ["viewCount", "views"]})
    if v: return v
    v = article.find("span", attrs={"aria-label": lambda s: s and "views" in s.lower()})
    if v: return v
    v = article.find("div", attrs={"aria-label": lambda s: s and "views" in s.lower()})
    return v


def extract_user_info(article, driver=None, fetch_profile=False):
    """Tweet sahibinin bilgilerini çıkarır (kullanıcı adı, takipçi, takip edilen)."""
    user_info = {
        "username": None,
        "display_name": None,
        "follower_count": 0,
        "following_count": 0
    }

    try:
        user_link = article.find("a", href=lambda x: x and "/" in x and x.startswith("/"))
        if user_link:
            href = user_link.get("href", "")
            if href.startswith("/") and not href.startswith("//") and "/status/" not in href:
                parts = href.strip("/").split("/")
                if parts and parts[0] and not parts[0].startswith("i/"):
                    user_info["username"] = parts[0]

            display_name_elem = user_link.find("span")
            if display_name_elem:
                user_info["display_name"] = display_name_elem.get_text(strip=True)

        if not user_info["username"]:
            user_elem = article.find(attrs={"data-testid": "User-Name"})
            if user_elem:
                links = user_elem.find_all("a")
                for link in links:
                    href = link.get("href", "")
                    if href.startswith("/") and not href.startswith("//") and "/status/" not in href:
                        parts = href.strip("/").split("/")
                        if parts and parts[0] and not parts[0].startswith("i/"):
                            user_info["username"] = parts[0]
                            break

        if fetch_profile and driver and user_info["username"]:
            try:
                profile_url = f"https://x.com/{user_info['username']}"
                driver.get(profile_url)
                time.sleep(2)
                profile_html = driver.page_source
                profile_soup = BeautifulSoup(profile_html, "html.parser")

                follower_elem = profile_soup.find("a", href=lambda x: x and "/followers" in x)
                if follower_elem:
                    follower_text = follower_elem.get_text(strip=True)
                    user_info["follower_count"] = safe_int(follower_text.split()[0] if follower_text else "0")

                following_elem = profile_soup.find("a", href=lambda x: x and "/following" in x)
                if following_elem:
                    following_text = following_elem.get_text(strip=True)
                    user_info["following_count"] = safe_int(following_text.split()[0] if following_text else "0")
            except Exception:
                pass

    except Exception:
        pass

    return user_info


def extract_comments(tweet_url, driver, max_comments=10):
    """Tweet detay sayfasına gidip yorumları çeker."""
    comments = []

    if not tweet_url:
        return comments

    try:
        driver.get(tweet_url)
        time.sleep(3)

        for _ in range(3):
            driver.execute_script("window.scrollBy(0, 800);")
            time.sleep(1.5)

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        articles = soup.find_all("article")

        for art in articles[1:max_comments + 1]:
            try:
                comment_text_tag = art.find(attrs={"data-testid": "tweetText"})
                if not comment_text_tag:
                    continue

                comment_text = comment_text_tag.get_text(" ", strip=True)
                if len(comment_text) < 3:
                    continue

                comment_user_link = art.find("a", href=lambda x: x and "/" in x and x.startswith("/"))
                comment_username = None
                if comment_user_link:
                    href = comment_user_link.get("href", "")
                    if href.startswith("/") and not href.startswith("//") and "/status/" not in href:
                        parts = href.strip("/").split("/")
                        if parts and parts[0] and not parts[0].startswith("i/"):
                            comment_username = parts[0]

                comment_like = art.find(attrs={"data-testid": ["like", "favorite"]})
                comment_like_count = safe_int(comment_like.get_text(strip=True) if comment_like else "0")

                comment_retweet = art.find(attrs={"data-testid": ["retweet", "repost"]})
                comment_retweet_count = safe_int(comment_retweet.get_text(strip=True) if comment_retweet else "0")

                comment_time_tag = art.find("time")
                comment_time = comment_time_tag["datetime"] if comment_time_tag else None

                comments.append({
                    "comment_text": comment_text,
                    "comment_username": comment_username,
                    "comment_like": comment_like_count,
                    "comment_retweet": comment_retweet_count,
                    "comment_time": comment_time
                })

                if len(comments) >= max_comments:
                    break

            except Exception:
                continue

    except Exception:
        pass

    return comments


print("✅ Kütüphaneler ve fonksiyonlar yüklendi.")


# ======================= ENV & PATHS (MIN CHANGE: token güvenli) =======================
# Cookie'ler sadece ENV/.env'den gelsin (hardcode YOK)

AUTH_TOKEN = os.getenv("AUTH_TOKEN")
CT0 = os.getenv("CT0")

# .env (opsiyonel)
if (not AUTH_TOKEN or not CT0):
    try:
        from dotenv import load_dotenv
        load_dotenv()
        AUTH_TOKEN = os.getenv("AUTH_TOKEN") or AUTH_TOKEN
        CT0 = os.getenv("CT0") or CT0
    except ImportError:
        pass

if not AUTH_TOKEN or not CT0:
    raise RuntimeError("❌ AUTH_TOKEN veya CT0 yok. GitHub Secrets veya .env ile ver.")

# HF (opsiyonel)
HF_TOKEN = os.getenv("HF_TOKEN")
HF_REPO_ID = os.getenv("HF_REPO_ID")  # örn: caner0706/pegos-twitter-data

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")
RUN_STAMP = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Günlük klasör: data/YYYY-MM-DD
OUT_DIR = os.path.join(SCRIPT_DIR, "data", TODAY)

# Run dosyaları: data/YYYY-MM-DD/runs/TIMESTAMP.jsonl
RUNS_DIR = os.path.join(OUT_DIR, "runs")
RUN_JSONL = os.path.join(RUNS_DIR, f"{RUN_STAMP}.jsonl")

# Günlük birleşik: data/YYYY-MM-DD/daily.jsonl  (overwrite)
DAILY_JSONL = os.path.join(OUT_DIR, "daily.jsonl")

# Son gelen run: data/YYYY-MM-DD/latest.jsonl (overwrite)
LATEST_JSONL = os.path.join(OUT_DIR, "latest.jsonl")

# Tüm zamanlar birleşik: data/all/all.jsonl (overwrite)
ALL_DIR = os.path.join(SCRIPT_DIR, "data", "all")
ALL_JSONL = os.path.join(ALL_DIR, "all.jsonl")

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(RUNS_DIR, exist_ok=True)
os.makedirs(ALL_DIR, exist_ok=True)

print("📁 OUT_DIR:", OUT_DIR)
print("🕒 RUN_STAMP:", RUN_STAMP)


# ======================= JSONL HELPERS =======================
def _read_jsonl(path: str):
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows

def _write_jsonl(path: str, rows: list):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def _dedupe_rows(rows: list):
    seen = set()
    out = []
    for r in rows:
        tweet_url = (r.get("tweet_url") or "").strip()
        t = (r.get("time") or "").strip()
        tweet = (r.get("tweet") or "").strip()
        username = (r.get("username") or "").strip()

        if tweet_url:
            key = (tweet_url, t)
        else:
            key = (tweet, t, username)

        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


# ======================= BROWSER =======================
opts = Options()
opts.add_argument("--headless=new")
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-gpu")
opts.add_argument("--disable-dev-shm-usage")
opts.add_argument("--window-size=1920,1080")
opts.add_argument("--disable-blink-features=AutomationControlled")
opts.add_experimental_option("excludeSwitches", ["enable-automation"])
opts.add_experimental_option("useAutomationExtension", False)

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

driver.get("https://x.com")
time.sleep(3)
driver.add_cookie({"name": "auth_token", "value": AUTH_TOKEN, "domain": ".x.com"})
driver.add_cookie({"name": "ct0", "value": CT0, "domain": ".x.com"})
driver.refresh()
time.sleep(5)

print("✅ Login başarılı:", driver.current_url)


# ======================= SCRAPE (SAME LOGIC) =======================
KEYWORDS = ["bitcoin", "blockchain", "cryptocurrency"]
MODES = ["top", "live"]
tweetArr = []

for kw in KEYWORDS:
    for mode in MODES:
        print(f"\n🔎 {kw} | mode={mode}")
        driver.get(f"https://x.com/search?q={kw}&src=typed_query&f={mode}")
        time.sleep(6)

        seen = set()
        for _ in range(60):
            driver.execute_script("window.scrollBy(0, 1200);")
            time.sleep(random.uniform(2.0, 3.2))
            html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")

            for art in soup.find_all("article"):
                try:
                    text_tag = art.find(attrs={"data-testid": "tweetText"})
                    if not text_tag:
                        continue
                    text = text_tag.get_text(" ", strip=True)
                    if len(text) < 8:
                        continue

                    ttag = art.find("time")
                    tstr = ttag["datetime"] if ttag else None
                    key = (text, tstr)
                    if key in seen:
                        continue
                    seen.add(key)

                    tweet_url = None
                    time_link = ttag.find_parent("a") if ttag else None
                    if time_link:
                        href = time_link.get("href", "")
                        if href.startswith("/"):
                            tweet_url = f"https://x.com{href}"

                    if not tweet_url:
                        all_links = art.find_all("a", href=True)
                        for link in all_links:
                            href = link.get("href", "")
                            if "/status/" in href:
                                if href.startswith("/"):
                                    tweet_url = f"https://x.com{href}"
                                else:
                                    tweet_url = href
                                break

                    reply = art.find(attrs={"data-testid": ["reply", "conversation"]})
                    retw = art.find(attrs={"data-testid": ["retweet", "repost"]})
                    like = art.find(attrs={"data-testid": ["like", "favorite"]})
                    view = find_view_node(art)

                    user_info = extract_user_info(art, driver, fetch_profile=False)

                    comments_data = []
                    if tweet_url:
                        try:
                            comments = extract_comments(tweet_url, driver, max_comments=5)
                            comments_data = comments
                            driver.back()
                            time.sleep(2)
                        except Exception as e:
                            print(f"⚠️ Yorum çekme hatası: {e}")
                            try:
                                driver.back()
                                time.sleep(1)
                            except:
                                pass

                    tweet_data = {
                        "keyword": kw,
                        "tweet": text,
                        "time": tstr,
                        "tweet_url": tweet_url,
                        "comment": safe_int(reply.get_text(strip=True) if reply else "0"),
                        "retweet": safe_int(retw.get_text(strip=True) if retw else "0"),
                        "like": safe_int(like.get_text(strip=True) if like else "0"),
                        "see_count": safe_int(view.get_text(strip=True) if view else "0"),
                        "username": user_info["username"],
                        "display_name": user_info["display_name"],
                        "follower_count": user_info["follower_count"],
                        "following_count": user_info["following_count"],
                        "comments_count": len(comments_data),
                        "comments_data": comments_data  # <-- JSON olarak saklıyoruz (string değil)
                    }

                    tweetArr.append(tweet_data)
                    time.sleep(random.uniform(0.5, 1.0))

                except Exception as e:
                    print(f"⚠️ Tweet işleme hatası: {e}")
                    continue

        print(f"✅ {kw}/{mode}: {len(tweetArr)} tweet toplandı.")

driver.quit()
print(f"🟢 Toplam tweet sayısı: {len(tweetArr)}")


# ======================= SAVE (MIN CHANGE: CSV yerine JSONL) =======================
df = pd.DataFrame(tweetArr)

if not df.empty:
    df.drop_duplicates(subset=["tweet", "time"], inplace=True)
    sort_cols = [c for c in ["like", "retweet", "comment", "see_count"] if c in df.columns]
    if sort_cols:
        df.sort_values(by=sort_cols, ascending=False, inplace=True)
else:
    df = pd.DataFrame(columns=[
        "keyword", "tweet", "time", "tweet_url",
        "comment", "retweet", "like", "see_count",
        "username", "display_name", "follower_count", "following_count",
        "comments_count", "comments_data"
    ])

# DataFrame -> list[dict]
rows = df.to_dict(orient="records")

# 1) Bu run dosyası (ayrı)
_write_jsonl(RUN_JSONL, rows)

# 2) latest.jsonl (overwrite)
_write_jsonl(LATEST_JSONL, rows)

# 3) daily.jsonl (merge + dedupe + overwrite)
daily_old = _read_jsonl(DAILY_JSONL)
daily_new = _dedupe_rows(daily_old + rows)
_write_jsonl(DAILY_JSONL, daily_new)

# 4) all.jsonl (merge + dedupe + overwrite)
all_old = _read_jsonl(ALL_JSONL)
all_new = _dedupe_rows(all_old + rows)
_write_jsonl(ALL_JSONL, all_new)

print(f"💾 Kaydedildi (RUN): {RUN_JSONL} ({len(rows)} satır)")
print(f"💾 Kaydedildi (LATEST): {LATEST_JSONL} ({len(rows)} satır)")
print(f"💾 Kaydedildi (DAILY): {DAILY_JSONL} ({len(daily_new)} satır)")
print(f"💾 Kaydedildi (ALL): {ALL_JSONL} ({len(all_new)} satır)")

# 5) HF upload (opsiyonel)
if HF_TOKEN and HF_REPO_ID and HF_AVAILABLE:
    try:
        api = HfApi()
        # data/ klasörünün tamamını HF dataset repo'ya gönder
        api.upload_folder(
            folder_path=os.path.join(SCRIPT_DIR, "data"),
            repo_id=HF_REPO_ID,
            repo_type="dataset",
            token=HF_TOKEN,
            commit_message=f"Scrape {TODAY} {RUN_STAMP} | run={len(rows)} daily={len(daily_new)} all={len(all_new)}"
        )
        print("✅ HF upload tamamlandı.")
    except Exception as e:
        print(f"⚠️ HF upload hatası: {e}")
else:
    print("ℹ️ HF upload atlandı (HF_TOKEN/HF_REPO_ID yok veya huggingface_hub yok).")
