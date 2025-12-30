import os
import pandas as pd
from datetime import datetime, timezone
from huggingface_hub import HfApi, hf_hub_download

EXPECTED_COLS = [
    "keyword", "tweet", "time", "tweet_url",
    "comment", "retweet", "like", "see_count",
    "username", "display_name", "follower_count", "following_count",
    "comments_count", "comments_data"
]

def utc_run_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def ensure_schema(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        df = pd.DataFrame(columns=EXPECTED_COLS)

    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = "" if c in ["keyword","tweet","time","tweet_url","username","display_name","comments_data"] else 0

    return df[EXPECTED_COLS]

def robust_dedupe(df: pd.DataFrame) -> pd.DataFrame:
    # Tweet URL varsa onunla; yoksa tweet+time+username
    if "tweet_url" in df.columns and df["tweet_url"].notna().any():
        df["__key"] = df["tweet_url"].fillna("") + "|" + df["time"].fillna("")
    else:
        df["__key"] = df["tweet"].fillna("") + "|" + df["time"].fillna("") + "|" + df["username"].fillna("")

    df = df.drop_duplicates(subset=["__key"]).drop(columns=["__key"])
    return df

def try_download_csv(repo_id: str, path_in_repo: str, token: str) -> pd.DataFrame:
    """
    HF dataset repo içinden CSV indirir. Yoksa boş DF döner.
    """
    try:
        local_path = hf_hub_download(
            repo_id=repo_id,
            repo_type="dataset",
            filename=path_in_repo,
            token=token
        )
        return pd.read_csv(local_path, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame(columns=EXPECTED_COLS)

def upload_folder(local_folder: str, repo_id: str, token: str, message: str):
    api = HfApi()
    api.upload_folder(
        folder_path=local_folder,
        repo_id=repo_id,
        repo_type="dataset",
        token=token,
        commit_message=message
    )

def update_aggregates_and_save_locally(run_df: pd.DataFrame, out_base: str, repo_id: str, token: str) -> dict:
    """
    Üretilen dosyalar:
      output/data/runs/YYYY-MM-DD/TIMESTAMP.csv
      output/data/daily/YYYY-MM-DD.csv
      output/data/all/all.csv
      output/data/meta/runs_log.csv
    """
    day = utc_day()
    stamp = utc_run_stamp()

    runs_dir = os.path.join(out_base, "data", "runs", day)
    daily_dir = os.path.join(out_base, "data", "daily")
    all_dir = os.path.join(out_base, "data", "all")
    meta_dir = os.path.join(out_base, "data", "meta")

    os.makedirs(runs_dir, exist_ok=True)
    os.makedirs(daily_dir, exist_ok=True)
    os.makedirs(all_dir, exist_ok=True)
    os.makedirs(meta_dir, exist_ok=True)

    run_df = ensure_schema(run_df)
    run_df = robust_dedupe(run_df)

    # 1) Her run ayrı dosya
    run_rel = os.path.join("data", "runs", day, f"{stamp}.csv").replace("\\", "/")
    run_path = os.path.join(out_base, run_rel)
    run_df.to_csv(run_path, index=False, encoding="utf-8-sig")

    # 2) Günlük aggregate
    daily_rel = os.path.join("data", "daily", f"{day}.csv").replace("\\", "/")
    daily_old = ensure_schema(try_download_csv(repo_id, daily_rel, token))
    daily_new = robust_dedupe(pd.concat([daily_old, run_df], ignore_index=True))
    daily_path = os.path.join(out_base, daily_rel)
    daily_new.to_csv(daily_path, index=False, encoding="utf-8-sig")

    # 3) Global aggregate
    all_rel = os.path.join("data", "all", "all.csv").replace("\\", "/")
    all_old = ensure_schema(try_download_csv(repo_id, all_rel, token))
    all_new = robust_dedupe(pd.concat([all_old, run_df], ignore_index=True))
    all_path = os.path.join(out_base, all_rel)
    all_new.to_csv(all_path, index=False, encoding="utf-8-sig")

    # 4) Run log
    log_rel = os.path.join("data", "meta", "runs_log.csv").replace("\\", "/")
    log_old = try_download_csv(repo_id, log_rel, token)

    log_row = pd.DataFrame([{
        "day": day,
        "run_stamp_utc": stamp,
        "run_rows": int(len(run_df)),
        "daily_rows": int(len(daily_new)),
        "all_rows": int(len(all_new)),
        "run_file": run_rel,
    }])

    log_new = pd.concat([log_old, log_row], ignore_index=True)
    log_path = os.path.join(out_base, log_rel)
    log_new.to_csv(log_path, index=False, encoding="utf-8-sig")

    return {
        "day": day,
        "stamp": stamp,
        "run_file": run_rel,
        "daily_file": daily_rel,
        "all_file": all_rel,
        "log_file": log_rel,
        "run_rows": len(run_df),
        "daily_rows": len(daily_new),
        "all_rows": len(all_new),
    }
