import json
import re
from pathlib import Path
from typing import List, Set
from urllib.parse import quote_plus
from datetime import datetime
import os

import requests
import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, Page

# 設定ファイル
SOURCES_FILE = Path("config/sources.json")
SEEN_URLS_FILE = Path("data/seen_urls.json")

# 使う RSS-Bridge インスタンス
RSS_BRIDGE_BASE = "https://rss-bridge.org/bridge01/"

# Google Sheets 設定（★ここを自分のシートに合わせて変えてください）
GOOGLE_SHEET_NAME = "gofile_links"  # スプレッドシートの「名前」
GOOGLE_SHEET_WORKSHEET = "シート1"   # ワークシート名（デフォルトは「シート1」）

# gofile の URLパターン
GOFILE_REGEX = re.compile(r"https://gofile\.io/d/[0-9A-Za-z]+")

# gofile が死んでいるときに body に含まれるパターン
GOFILE_DEAD_PATTERNS = [
    "This content does not exist",
    "The content you are looking for could not be found",
    "No items to display",
    "This content is password protected",
]


def load_sources() -> List[str]:
    """スクレイピング対象の Nitter URL を config/sources.json から読み込み"""
    if not SOURCES_FILE.exists():
        raise FileNotFoundError(f"{SOURCES_FILE} が存在しません。NitterのURLをここに保存してください。")
    with SOURCES_FILE.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("sources", [])


def load_seen_urls() -> Set[str]:
    """すでに処理済み (または死んでいた) gofile URL を読み込み"""
    if not SEEN_URLS_FILE.exists():
        SEEN_URLS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with SEEN_URLS_FILE.open("w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=2)
        return set()
    with SEEN_URLS_FILE.open("r", encoding="utf-8") as f:
        try:
            urls = json.load(f)
            return set(urls)
        except json.JSONDecodeError:
            return set()


def save_seen_urls(seen: Set[str]) -> None:
    """処理済み gofile URL を JSON に保存"""
    SEEN_URLS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with SEEN_URLS_FILE.open("w", encoding="utf-8") as f:
        json.dump(sorted(seen), f, ensure_ascii=False, indent=2)


def nitter_url_to_rss_url(nitter_url: str) -> str:
    """Nitter URL を RSS-Bridge detect アクションのURLに変換"""
    encoded = quote_plus(nitter_url)
    return f"{RSS_BRIDGE_BASE}?action=detect&format=Atom&url={encoded}"


def collect_gofile_urls_from_nitter_via_rss_bridge(nitter_url: str) -> Set[str]:
    """Nitter → RSS-Bridge → RSS から gofile を抜き出す"""
    rss_url = nitter_url_to_rss_url(nitter_url)
    print(f"  Nitter URL: {nitter_url}")
    print(f"  RSS-Bridge detect URL: {rss_url}")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    }

    try:
        resp = requests.get(rss_url, headers=headers, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"  Failed to fetch RSS via RSS-Bridge: {e}")
        return set()

    text = resp.text
    urls = set(GOFILE_REGEX.findall(text))
    print(f"  Found {len(urls)} gofile URLs in feed (via RSS-Bridge)")
    return urls


def is_gofile_alive(page: Page, url: str) -> bool:
    """gofile.io のリンクが生きているか判定"""
    try:
        page.goto(url, wait_until="networkidle", timeout=30000)
    except Exception:
        return False

    page.wait_for_timeout(3000)

    try:
        text = page.inner_text("body")
    except Exception:
        return False

    for pattern in GOFILE_DEAD_PATTERNS:
        if pattern in text:
            return False

    return True


# --- Google Sheets 関連 ---

def get_gspread_client():
    """環境変数 GOOGLE_SERVICE_ACCOUNT_JSON から gspread クライアントを生成"""
    raw_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw_json:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON が環境変数に設定されていません。")
    info = json.loads(raw_json)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc


def append_row_to_sheet(gc, gofile_url: str, source_nitter_url: str) -> None:
    """Googleスプレッドシートに1行追記"""
    sh = gc.open(GOOGLE_SHEET_NAME)
    ws = sh.worksheet(GOOGLE_SHEET_WORKSHEET)

    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    row = [now, gofile_url, source_nitter_url]
    ws.append_row(row, value_input_option="USER_ENTERED")


def main():
    sources = load_sources()
    seen_urls = load_seen_urls()

    print(f"Loaded {len(seen_urls)} seen URLs")

    # Google Sheets クライアント作成
    gc = get_gspread_client()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 720},
        )

        gofile_page = context.new_page()

        new_seen = False
        processed = 0

        for src in sources:
            print(f"Scraping source (Nitter): {src}")
            urls = collect_gofile_urls_from_nitter_via_rss_bridge(src)

            for url in sorted(urls):
                if url in seen_urls:
                    continue

                print(f"  Checking gofile URL: {url}")
                if not is_gofile_alive(gofile_page, url):
                    print("    -> Dead or password protected. Skipped.")
                    seen_urls.add(url)
                    new_seen = True
                    continue

                print("    -> Alive. Appending to Google Sheet...")
                try:
                    append_row_to_sheet(gc, url, src)
                    print("    -> Append done.")
                    seen_urls.add(url)
                    new_seen = True
                    processed += 1
                except Exception as e:
                    print(f"    -> Append failed: {e}")

        browser.close()

    if new_seen:
        save_seen_urls(seen_urls)
        print(f"Saved {len(seen_urls)} seen URLs")
    print(f"Processed {processed} new URLs in this run.")


if __name__ == "__main__":
    main()
