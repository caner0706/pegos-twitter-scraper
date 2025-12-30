# =====================================================
# Pegos Twitter Scraper (Top + Live, robust counts, always-save)
# + GitHub Actions schedule
# + Hugging Face dataset storage: runs/daily/all + logs
# =====================================================

import os, time, random, json
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timezone

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# HF sync helpers
from hf_sync import update_aggregates_and_save_locally, upload_folder


# ----------------------- Helpers -----------------------

def safe_int(val: str):
    """Metin sayıları (3.5K, 1M, vb.) güvenli int'e çevirir."""
    if not val:
        return 0
    val = str(val).replace(",", "").replace("·", "").strip()
    try:
        if val.endswith("B"):
            return int(float(val[:-1]) * 1_000)
        if val.endswith("M") or val.endswith("Mn"):
            return int(float(val[:-1]) * 1_000_000)
        return int(float(val))
    except Exception:
        return 0


def find_view_node(article):
    """Tweet view sayısını yakalamak için alternatif testler."""
    v = article.find(attrs={"data-testid": ["viewCount", "views"]})
    if v:
        return v
    v = article.find("span", attrs={"aria-label": lambda s: s and "views" in s.lower()})
    if v:
        return v
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
        # Kullanıcı adı ve display name
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

        # Alternatif: data-testid ile kullanıcı adı
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

        # Profil sayfasına gidip takipçi/following çek (opsiyonel, yavaş)
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

        # Scroll yaparak daha fazla yorum yükle
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


# ----------------------- Main -----------------------

def load_env_if_needed():
    # Opsiyonel: lokalde .env kullanacaksan
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass


def get_required_env(name: str) -> str:
    v = os.getenv(name)
    if v:
        return v
    raise RuntimeError(f"❌ {name} env bulunamadı. (GitHub Secrets / .env / export ile ver)")


def build_driver() -> webdriver.Chrome:
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


def main():
    print("✅ Başladı: Pegos Twitter Scraper")

    load_env_if_needed()

    # Cookie'ler sadece ENV/.env üzerinden
    AUTH_TOKEN = get_required_env("AUTH_TOKEN")
    CT0 = get_required_env("CT0")

    # HF opsiyonel (lokalde HF push istemiyorsan boş bırakabilirsin)
    HF_TOKEN = os.getenv("HF_TOKEN")
    HF_REPO_ID = os.getenv("HF_REPO_ID")

    # Output base: Actions'ta GITHUB_WORKSPACE, lokalde script dizini
    WORKSPACE = os.getenv("GITHUB_WORKSPACE", os.path.dirname(os.path.abspath(__file__)))
    OUT_BASE = os.path.join(WORKSPACE, "output")

    os.makedirs(OUT_BASE, exist_ok=True)
    print("📁 OUT_BASE:", OUT_BASE)

    driver = build_driver()

    # Cookie login
    driver.get("https://x.com")
    time.sleep(3)
    driver.add_cookie({"name": "auth_token", "value": AUTH_TOKEN, "domain": ".x.com"})
    driver.add_cookie({"name": "ct0", "value": CT0, "domain": ".x.com"})
    driver.refresh()
    time.sleep(5)
    print("✅ Login sonrası URL:", driver.current_url)

    # ---------------- SCRAPE ----------------
    KEYWORDS = ["bitcoin", "blockchain", "cryptocurrency"]
    MODES = ["top", "live"]
    tweetArr = []

    for kw in KEYWORDS:
        for mode in MODES:
            print(f"\n🔎 {kw} | mode={mode}")
            driver.get(f"https://x.com/search?q={kw}&src=typed_query&f={mode}")
            time.sleep(6)

            seen = set()

            # Not: Actions ortamında çok uzun scroll riskli olabilir.
            # İstersen bunu env ile yönetebilirsin.
            SCROLLS = int(os.getenv("SCROLLS", "60"))

            for _ in range(SCROLLS):
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

                        # Tweet URL
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
                                    tweet_url = f"https://x.com{href}" if href.startswith("/") else href
                                    break

                        reply = art.find(attrs={"data-testid": ["reply", "conversation"]})
                        retw = art.find(attrs={"data-testid": ["retweet", "repost"]})
                        like = art.find(attrs={"data-testid": ["like", "favorite"]})
                        view = find_view_node(art)

                        user_info = extract_user_info(art, driver, fetch_profile=False)

                        # Yorumlar (Actions'ta riskli/yavaş olabilir; ENV ile kontrol edilebilir)
                        comments_data = []
                        ENABLE_COMMENTS = os.getenv("ENABLE_COMMENTS", "1") == "1"
                        if ENABLE_COMMENTS and tweet_url:
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
                                except Exception:
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
                            "comments_data": json.dumps(comments_data, ensure_ascii=False) if comments_data else ""
                        }

                        tweetArr.append(tweet_data)
                        time.sleep(random.uniform(0.5, 1.0))

                    except Exception as e:
                        print(f"⚠️ Tweet işleme hatası: {e}")
                        continue

            print(f"✅ {kw}/{mode}: şu ana kadar toplam {len(tweetArr)} tweet.")

    driver.quit()
    print(f"🟢 Toplam tweet sayısı: {len(tweetArr)}")

    # ---------------- SAVE (run df) ----------------
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

    # Lokal dosya üret (runs/daily/all mantığı için)
    if HF_TOKEN and HF_REPO_ID:
        info = update_aggregates_and_save_locally(
            run_df=df,
            out_base=OUT_BASE,
            repo_id=HF_REPO_ID,
            token=HF_TOKEN
        )

        commit_msg = (
            f"Scrape {info['day']} {info['stamp']} | "
            f"run={info['run_rows']} daily={info['daily_rows']} all={info['all_rows']}"
        )

        upload_folder(
            local_folder=OUT_BASE,
            repo_id=HF_REPO_ID,
            token=HF_TOKEN,
            message=commit_msg
        )

        print("✅ HF push tamamlandı.")
        print(info)
    else:
        # HF yoksa sadece lokal "tek dosya" çıktı üret (debug için)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        local_dir = os.path.join(OUT_BASE, "local_debug", today)
        os.makedirs(local_dir, exist_ok=True)
        out_csv = os.path.join(local_dir, "pegos_output.csv")
        df.to_csv(out_csv, index=False, encoding="utf-8-sig")
        print(f"ℹ️ HF env yok. Lokal debug çıktısı: {out_csv} ({len(df)} satır)")


if __name__ == "__main__":
    main()
