import json
import re
from pathlib import Path
from typing import List, Set
from urllib.parse import urlparse, parse_qs, unquote

from playwright.sync_api import sync_playwright, Page

# 設定ファイル
SOURCES_FILE = Path("config/sources.json")
SEEN_URLS_FILE = Path("data/seen_urls.json")

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


def extract_gofile_from_href(href: str) -> str | None:
    """
    href から gofile.io/d/... の実URLを取り出す。
    - 直接 https://gofile.io/d/XXX の場合
    - /external?url=https%3A%2F%2Fgofile.io%2Fd%2FXXX のような場合
    """
    if not href:
        return None

    # ケース1: そのまま gofile.io/d/xxx が入っている
    m = GOFILE_REGEX.search(href)
    if m:
        return m.group(0)

    # ケース2: ?url= にエンコードされているパターン
    try:
        parsed = urlparse(href)
        qs = parse_qs(parsed.query)
        if "url" in qs:
            for v in qs["url"]:
                decoded = unquote(v)
                m2 = GOFILE_REGEX.search(decoded)
                if m2:
                    return m2.group(0)
    except Exception:
        pass

    return None


def scroll_and_collect_gofile_urls(page: Page) -> Set[str]:
    """
    Nitterページで 'Load more' を押しつつ、gofile.io/d/... URL をすべて収集。
    HTMLを正規表現で舐めるのではなく、aタグのhrefを全部見る。
    """
    urls: Set[str] = set()

    while True:
        link_locators = page.locator("a")
        count = link_locators.count()
        print(f"    Scanning {count} links on this page...")
        for i in range(count):
            try:
                href = link_locators.nth(i).get_attribute("href")
            except Exception:
                continue
            gofile_url = extract_gofile_from_href(href or "")
            if gofile_url:
                urls.add(gofile_url)

        # Load more ボタンを探す
        load_more = page.locator("div.show-more a:has-text('Load more')")
        if load_more.count() == 0:
            break

        try:
            print("    Clicking 'Load more'...")
            load_more.first.click()
            # 読み込み待ち（適宜調整）
            page.wait_for_timeout(2000)
        except Exception:
            print("    Failed to click 'Load more' or no more pages.")
            break

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
        # headless Chromium を起動
        browser = p.chromium.launch(headless=True)

        # 👉 User-Agent を普通のChromeブラウザっぽく偽装
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 720},
        )

        # Nitterスクレイピング用ページ
        nitter_page = context.new_page()
        # gofile確認 & orevideoアップロード用ページ
        gofile_page = context.new_page()
        ore_page = gofile_page  # 同じタブを使い回す

        new_seen = False

        for src in sources:
            print(f"Scraping source: {src}")
            try:
                nitter_page.goto(src, wait_until="networkidle", timeout=30000)
            except Exception as e:
                print(f"  Failed to open {src}: {e}")
                continue

            # 最終URLとHTMLの一部をデバッグ出力
            print(f"  Final URL: {nitter_page.url}")
            nitter_page.wait_for_timeout(2000)

            try:
                html = nitter_page.content()
                snippet = html[:600].replace("\n", " ")
                print(f"  Page HTML snippet: {snippet}")
            except Exception as e:
                print(f"  Failed to get page content: {e}")

            urls = scroll_and_collect_gofile_urls(nitter_page)
            print(f"  Found {len(urls)} gofile URLs")

            for url in sorted(urls):  # 一応ソート（安定性のため）
                if url in seen_urls:
                    # すでに処理済み
                    continue

                print(f"  Checking gofile URL: {url}")
                if not is_gofile_alive(gofile_page, url):
                    print("    -> Dead or password protected. Skipped.")
                    # 死んでいるものも再チェック不要ならここで追加
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
