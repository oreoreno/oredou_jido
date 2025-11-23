import json
import re
from pathlib import Path
from typing import List, Set, Optional
from urllib.parse import quote_plus, urlparse
from datetime import datetime
import os
import time

import requests
import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, Page

# ------------------------------------------
#  設定ファイル / 保存用ファイル
# ------------------------------------------

SOURCES_FILE = Path("config/sources.json")
SEEN_URLS_FILE = Path("data/seen_urls.json")

# ------------------------------------------
#  Nitter ミラー（全自動ローテーション）
# ------------------------------------------
NITTER_MIRRORS = [
    "https://nitter.poast.org",
    "https://nitter.cz",
    "https://nitter.rawbit.ninja",
    "https://nitter.esmailelbob.xyz",
    "https://nitter.d420.de",
    "https://nitter.privacydev.net",
    "https://nitter.lucabettini.dev",
    "https://nitter.altgrau.de",
    "https://nitter.soopy.moe",
    "https://nitter.salastil.com",
    "https://nitter.lunar.icu",
    "https://nitter.qwik.space",
    "https://nitter.uni-sonia.com",
    "https://nitter.vxempire.xyz",
    "https://nitter.slipfox.xyz",
]

# ------------------------------------------
#  RSS-Bridge ミラー
# ------------------------------------------
RSS_BRIDGE_MIRRORS = [
    "https://rss-bridge.org/bridge01/",
    "https://rss-bridge.org/bridge02/",
    "https://rss-bridge.org/bridge03/",
    "https://rss-bridge.org/bridge04/",
    "https://bridge.suumitsu.eu/",
    "https://rss-bridge.bb8.fun/",
]

# ------------------------------------------
#  Google Sheets 設定
# ------------------------------------------
GOOGLE_SHEET_NAME = "gofile_links"
GOOGLE_SHEET_WORKSHEET = "シート1"

# ------------------------------------------
#  gofile.io 判定
# ------------------------------------------
GOFILE_REGEX = re.compile(r"https://gofile\.io/d/[0-9A-Za-z]+")

GOFILE_DEAD_PATTERNS = [
    "This content does not exist",
    "The content you are looking for could not be found",
    "No items to display",
    "This content is password protected",
]

GOFILE_BLOCK_PATTERN = (
    "refreshAppdataAccountsAndSync getAccountActive Failed to fetch"
)

MAX_GOFILE_CHECKS_PER_RUN = 40
MAX_NITTER_PAGES = 50


# ------------------------------------------
# ユーティリティ
# ------------------------------------------
def load_sources() -> List[str]:
    if not SOURCES_FILE.exists():
        raise FileNotFoundError(f"{SOURCES_FILE} が存在しません")
    with SOURCES_FILE.open("r", encoding="utf-8") as f:
        return json.load(f).get("sources", [])

def load_seen_urls() -> Set[str]:
    if not SEEN_URLS_FILE.exists():
        SEEN_URLS_FILE.parent.mkdir(parents=True, exist_ok=True)
        SEEN_URLS_FILE.write_text("[]", encoding="utf-8")
        return set()
    try:
        return set(json.loads(SEEN_URLS_FILE.read_text(encoding="utf-8")))
    except:
        return set()

def save_seen_urls(urls: Set[str]):
    SEEN_URLS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with SEEN_URLS_FILE.open("w", encoding="utf-8") as f:
        json.dump(sorted(urls), f, ensure_ascii=False, indent=2)


# ------------------------------------------
#  Nitter タイムライン or 検索 URL 作成
# ------------------------------------------
def build_nitter_target_url(mirror: str, nitter_url: str) -> str:
    parsed = urlparse(nitter_url)
    segments = [s for s in parsed.path.split("/") if s]

    # /search?... → gofile検索
    if segments and segments[0].lower() == "search":
        return f"{mirror}/search?f=tweets&q={quote_plus('gofile.io/d/')}"
    # アカウント → タイムライン
    if segments:
        return f"{mirror}/{segments[0]}"
    # フォールバック
    return f"{mirror}/search?f=tweets&q={quote_plus('gofile.io/d/')}"


# ------------------------------------------
#  RSS-Bridge 取得
# ------------------------------------------
def build_rss_url(base: str, nitter_url: str) -> str:
    return f"{base}?action=detect&format=Atom&url={quote_plus(nitter_url)}"

def collect_gofile_via_rss_bridge(nitter_url: str) -> Set[str]:
    print(f"  [RSS] Source: {nitter_url}")
    headers = {"User-Agent": "Mozilla/5.0 Chrome/122 Safari/537.36"}

    for base in RSS_BRIDGE_MIRRORS:
        rss_url = build_rss_url(base, nitter_url)
        print(f"    [RSS] Try mirror: {base}")
        try:
            r = requests.get(rss_url, headers=headers, timeout=20)
            r.raise_for_status()
        except Exception as e:
            print(f"      [RSS] Failed: {e}")
            continue
        urls = set(GOFILE_REGEX.findall(r.text))
        print(f"      [RSS] Success! found {len(urls)} URLs")
        return urls
    print("    [RSS] All mirrors failed.")
    return set()


# ------------------------------------------
#  Nitter タイムラインを Load more で収集
# ------------------------------------------
def collect_gofile_via_nitter(page: Page, nitter_url: str) -> Set[str]:
    print(f"  [NITTER] Source: {nitter_url}")

    for mirror in NITTER_MIRRORS:
        target = build_nitter_target_url(mirror, nitter_url)
        print(f"    [NITTER] Try mirror: {mirror}")
        print(f"      URL: {target}")
        try:
            page.goto(target, wait_until="networkidle", timeout=30000)
        except Exception as e:
            print(f"      -> mirror dead ({e})")
            continue

        urls = set(GOFILE_REGEX.findall(page.content()))
        print(f"      Page 1: {len(urls)} URLs")
        time.sleep(2)

        for i in range(2, MAX_NITTER_PAGES + 1):
            btn = page.locator("div.show-more a, a:has-text('Load more')")
            if btn.count() == 0:
                break
            try:
                btn.first.click()
                time.sleep(2)
            except:
                break
            html = page.content()
            before = len(urls)
            urls |= set(GOFILE_REGEX.findall(html))
            print(f"      Page {i}: total {len(urls)} URLs (added {len(urls)-before})")

        print(f"      -> mirror success, collected total {len(urls)} URLs")
        return urls

    print("    [NITTER] All mirrors failed.")
    return set()


# ------------------------------------------
# gofile 状態チェック
# ------------------------------------------
def check_gofile_status(page: Page, url: str) -> str:
    try:
        page.goto(url, wait_until="networkidle", timeout=25000)
    except:
        return "dead"
    page.wait_for_timeout(2000)
    body = page.inner_text("body")

    if GOFILE_BLOCK_PATTERN in body:
        return "blocked"
    for p in GOFILE_DEAD_PATTERNS:
        if p in body:
            return "dead"
    return "alive"


# ------------------------------------------
# Google Sheets 書き込み（必ず A/B/C）
# ------------------------------------------
def get_gspread_client():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    info = json.loads(raw)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def append_row_to_sheet(gc, gofile_url: str, source_nitter_url: str):
    sh = gc.open(GOOGLE_SHEET_NAME)
    ws = sh.worksheet(GOOGLE_SHEET_WORKSHEET)
    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    row = [now, gofile_url, source_nitter_url]
    print("[APPEND ROW]", row)
    cur = len(ws.get_all_values())
    ws.update(f"A{cur+1}:C{cur+1}", [row], value_input_option="USER_ENTERED")


# ------------------------------------------
# メイン
# ------------------------------------------
def main():
    sources = load_sources()
    seen = load_seen_urls()
    print(f"Loaded {len(seen)} seen URLs")
    gc = get_gspread_client()

    with sync_playwright() as p:
        ctx = p.chromium.launch(headless=True).new_context()
        page_n = ctx.new_page()
        page_g = ctx.new_page()

        processed = 0
        checks = 0
        blocked = False
        update_seen = False

        for src in sources:
            print(f"\nScraping source: {src}")

            rss_urls = collect_gofile_via_rss_bridge(src)
            nitter_urls = collect_gofile_via_nitter(page_n, src)
            collected = rss_urls | nitter_urls

            print(f"  Total collected: {len(collected)}")

            for url in sorted(collected):
                if url in seen:
                    continue
                if checks >= MAX_GOFILE_CHECKS_PER_RUN:
                    break

                print(f"  Checking gofile: {url}")
                status = check_gofile_status(page_g, url)
                checks += 1

                if status == "blocked":
                    blocked = True
                    break
                if status == "dead":
                    seen.add(url)
                    update_seen = True
                    continue

                append_row_to_sheet(gc, url, src)
                seen.add(url)
                update_seen = True
                processed += 1

            if blocked or checks >= MAX_GOFILE_CHECKS_PER_RUN:
                break

    if update_seen:
        save_seen_urls(seen)

    print(f"Processed {processed} new URLs in this run.")
    print(f"Total gofile checks in this run: {checks}")
    if blocked:
        print("Run ended early due to block detection.")


if __name__ == "__main__":
    main()