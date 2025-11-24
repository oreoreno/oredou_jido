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
# 設定ファイル / 保存用ファイル
# ------------------------------------------

SOURCES_FILE = Path("config/sources.json")
SEEN_URLS_FILE = Path("data/seen_urls.json")

# ------------------------------------------
# Nitter ミラー
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
# RSS-Bridge ミラー
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
# Google Sheets 設定
# ------------------------------------------

GOOGLE_SHEET_NAME = "gofile_links"
GOOGLE_SHEET_WORKSHEET = "シート1"

# ------------------------------------------
# gofile 検索用
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
#  ユーティリティ
# ------------------------------------------

def load_sources() -> List[str]:
    if not SOURCES_FILE.exists():
        raise FileNotFoundError("config/sources.json がありません")
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


def extract_account(url: str) -> Optional[str]:
    parsed = urlparse(url)
    segments = [s for s in parsed.path.split("/") if s]
    if not segments:
        return None
    if segments[0].lower() == "search":
        return None
    return segments[0]


# ------------------------------------------
# RSS-Bridge で抽出
# ------------------------------------------

def build_rss_url(base: str, nitter_url: str) -> str:
    return f"{base}?action=detect&format=Atom&url={quote_plus(nitter_url)}"


def collect_via_rss_bridge(nitter_url: str) -> Set[str]:
    print(f"  [RSS] {nitter_url}")
    headers = {"User-Agent": "Mozilla/5.0"}

    for base in RSS_BRIDGE_MIRRORS:
        print(f"    Try: {base}")
        try:
            r = requests.get(build_rss_url(base, nitter_url), headers=headers, timeout=20)
            r.raise_for_status()
        except Exception as e:
            print(f"      Failed: {e}")
            continue

        urls = set(GOFILE_REGEX.findall(r.text))
        print(f"      Success → {len(urls)} URLs")
        return urls

    return set()


# ------------------------------------------
# Nitter (Playwright)
# ------------------------------------------

def build_search_url(mirror: str, nitter_url: str) -> str:
    account = extract_account(nitter_url)
    if account:
        return f"{mirror}/{account}"  # /<user> へ直接
    return f"{mirror}/search?f=tweets&q=gofile.io/d/"


def collect_via_nitter(page: Page, nitter_url: str) -> Set[str]:
    print(f"  [NITTER] {nitter_url}")

    for mirror in NITTER_MIRRORS:
        target = build_search_url(mirror, nitter_url)
        print(f"    Try mirror: {mirror}")
        try:
            page.goto(target, wait_until="domcontentloaded", timeout=30000)
        except:
            print("      mirror dead")
            continue

        time.sleep(2)

        found = set(GOFILE_REGEX.findall(page.content()))
        print(f"      Page1: {len(found)}")

        for _ in range(MAX_NITTER_PAGES):
            btn = page.locator("a:has-text('Load more')")
            if btn.count() == 0:
                break
            try:
                btn.first.click()
                time.sleep(2)
            except:
                break
            found |= set(GOFILE_REGEX.findall(page.content()))

        print(f"      success → total {len(found)} URLs")
        return found

    print("    All Nitter mirrors failed.")
    return set()


# ------------------------------------------
# mobile.twitter (requests) ← NEW
# ------------------------------------------

def collect_via_mobile(src: str) -> Set[str]:
    account = extract_account(src)
    if not account:
        return set()

    url = f"https://mobile.twitter.com/{account}"
    print(f"  [MOBILE] {url}")

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print(f"    mobile request fail: {e}")
        return set()

    urls = set(GOFILE_REGEX.findall(r.text))
    print(f"    mobile success → {len(urls)} URLs")
    return urls


# ------------------------------------------
# gofile 状態確認
# ------------------------------------------

def check_gofile_status(page: Page, url: str) -> str:
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=25000)
    except:
        return "dead"

    time.sleep(1)
    try:
        body = page.inner_text("body")
    except:
        return "dead"

    if GOFILE_BLOCK_PATTERN in body:
        return "blocked"
    for p in GOFILE_DEAD_PATTERNS:
        if p in body:
            return "dead"
    return "alive"


# ------------------------------------------
# Google Sheets
# ------------------------------------------

def get_gspread_client():
    info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    return gspread.authorize(Credentials.from_service_account_info(info, scopes=scopes))


def append_row(gc, gofile_url: str, source_url: str):
    sh = gc.open(GOOGLE_SHEET_NAME)
    ws = sh.worksheet(GOOGLE_SHEET_WORKSHEET)

    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    row = [now, gofile_url, source_url]

    print("[APPEND]", row)

    current_rows = len(ws.get_all_values())
    next_row = current_rows + 1

    ws.update(f"A{next_row}:C{next_row}", [row], value_input_option="USER_ENTERED")


# ------------------------------------------
# Main
# ------------------------------------------

def main():
    sources = load_sources()
    seen = load_seen_urls()
    gc = get_gspread_client()

    processed = 0
    checks = 0
    blocked = False
    new_seen_added = False

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0",
            viewport={"width": 1280, "height": 720},
        )
        page_nitter = ctx.new_page()
        page_gofile = ctx.new_page()

        for src in sources:
            print(f"\n=== Scraping: {src}")

            urls = set()
            urls |= collect_via_rss_bridge(src)
            urls |= collect_via_nitter(page_nitter, src)
            urls |= collect_via_mobile(src)

            print(f"  total collected: {len(urls)}")

            for url in sorted(urls):
                if url in seen:
                    continue

                if checks >= MAX_GOFILE_CHECKS_PER_RUN:
                    blocked = False
                    break

                status = check_gofile_status(page_gofile, url)
                checks += 1

                if status == "blocked":
                    blocked = True
                    break
                if status == "dead":
                    seen.add(url)
                    new_seen_added = True
                    continue

                append_row(gc, url, src)
                processed += 1
                seen.add(url)
                new_seen_added = True

            if blocked or checks >= MAX_GOFILE_CHECKS_PER_RUN:
                break

        browser.close()

    if new_seen_added:
        save_seen_urls(seen)

    print(f"Processed {processed} URLs | gofile checks: {checks}")
    if blocked:
        print("⚠️ blocked by gofile — run ended early")


if __name__ == "__main__":
    main()