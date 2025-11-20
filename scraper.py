import json
import re
from pathlib import Path
from typing import List, Set
from urllib.parse import quote_plus

import requests
from playwright.sync_api import sync_playwright, Page

# 設定ファイル
SOURCES_FILE = Path("config/sources.json")
SEEN_URLS_FILE = Path("data/seen_urls.json")

# 使う RSS-Bridge インスタンス
# 必要に応じて他の公開インスタンスに変えてOK
RSS_BRIDGE_BASE = "https://rss-bridge.org/bridge01/"

# gofile の URLパターン
GOFILE_REGEX = re.compile(r"https://gofile\.io/d/[0-9A-Za-z]+")

# ページ内テキストにこのどれかが含まれていたらリンク切れとみなす
GOFILE_DEAD_PATTERNS = [
    "This content does not exist",
    "The content you are looking for could not be found",
    "No items to display",
    "This content is password protected",
]

OREVIDEO_URL = "https://orevideo.pythonanywhere.com/"


def load_sources() -> List[str]:
    """スクレイピング対象の Nitter URL を config/sources.json から読み込み"""
    if not SOURCES_FILE.exists():
        raise FileNotFoundError(f"{SOURCES_FILE} が存在しません。NitterのURLをここに保存してください。")
    with SOURCES_FILE.open("r", encoding="utf-8") as f:
        data = json.load(f)
    # 形式: { "sources": ["https://nitter.net/...", ...] }
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
    """
    Nitter の URL を RSS-Bridge の detect アクションに渡すためのURLに変換
    detect は ?url= に対して適切な bridge を選んで RSS を返してくれる
    https://rss-bridge.github.io/rss-bridge/For_Developers/Actions.html#detect
    """
    encoded = quote_plus(nitter_url)
    return f"{RSS_BRIDGE_BASE}?action=detect&format=Atom&url={encoded}"


def collect_gofile_urls_from_nitter_via_rss_bridge(nitter_url: str) -> Set[str]:
    """
    Nitter の URL を RSS-Bridge 経由で RSS にして、
    そのフィードの中から gofile.io/d/... URL を全部抜き出す
    """
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

    # detect アクションは 301 で display にリダイレクトする仕様なので、
    # requests は自動で追いかけて Atom/XML を返してくれるはず。
    text = resp.text
    urls = set(GOFILE_REGEX.findall(text))
    print(f"  Found {len(urls)} gofile URLs in feed (via RSS-Bridge)")
    return urls


def is_gofile_alive(page: Page, url: str) -> bool:
    """gofile.io のリンクが生きているか判定"""
    try:
        page.goto(url, wait_until="networkidle", timeout=30000)
    except Exception:
        # タイムアウトやネットワークエラーは一旦「生きてない」とみなす
        return False

    # JSが落ち着くまで少し待つ
    page.wait_for_timeout(3000)

    try:
        text = page.inner_text("body")
    except Exception:
        return False

    for pattern in GOFILE_DEAD_PATTERNS:
        if pattern in text:
            return False

    return True


def upload_to_orevideo(page: Page, gofile_url: str) -> None:
    """orevideo.pythonanywhere.com に対してURLを送信"""
    page.goto(OREVIDEO_URL, wait_until="domcontentloaded", timeout=30000)
    page.wait_for_timeout(2000)

    # input#url に値を入れる
    page.fill("input#url", gofile_url)

    # ボタン押下
    page.click("#submitBtn")

    # 処理待ち（必要に応じて延長）
    page.wait_for_timeout(5000)


def main():
    sources = load_sources()
    seen_urls = load_seen_urls()

    print(f"Loaded {len(seen_urls)} seen URLs")

    with sync_playwright() as p:
        # headless Chromium を起動（gofile / orevideo 用）
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 720},
        )

        # gofile確認 & orevideoアップロード用ページ
        gofile_page = context.new_page()
        ore_page = context.new_page()

        new_seen = False

        for src in sources:
            print(f"Scraping source (Nitter): {src}")
            urls = collect_gofile_urls_from_nitter_via_rss_bridge(src)

            for url in sorted(urls):
                if url in seen_urls:
                    # すでに処理済み
                    continue

                print(f"  Checking gofile URL: {url}")
                if not is_gofile_alive(gofile_page, url):
                    print("    -> Dead or password protected. Skipped.")
                    # 死んでいるものも再チェックしたくないならここで add
                    seen_urls.add(url)
                    new_seen = True
                    continue

                print("    -> Alive. Uploading to orevideo...")
                try:
                    upload_to_orevideo(ore_page, url)
                    print("    -> Upload done.")
                    seen_urls.add(url)
                    new_seen = True
                except Exception as e:
                    print(f"    -> Upload failed: {e}")

        browser.close()

    if new_seen:
        save_seen_urls(seen_urls)
        print(f"Saved {len(seen_urls)} seen URLs")
    else:
        print("No new URLs processed.")


if __name__ == "__main__":
    main()
