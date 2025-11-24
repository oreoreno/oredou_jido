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
#  Nitter ミラー（フォールバック順）
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
GOFILE_BLOCK_PATTERN = "refreshAppdataAccountsAndSync getAccountActive Failed to fetch"

MAX_GOFILE_CHECKS_PER_RUN = 40
MAX_NITTER_PAGES = 60

# ------------------------------------------
#  Utility
# ------------------------------------------

def load_sources() -> List[str]:
    return json.loads(SOURCES_FILE.read_text())["sources"]

def load_seen_urls() -> Set[str]:
    if not SEEN_URLS_FILE.exists():
        return set()
    try:
        return set(json.loads(SEEN_URLS_FILE.read_text()))
    except:
        return set()

def save_seen_urls(urls: Set[str]):
    SEEN_URLS_FILE.parent.mkdir(parents=True, exist_ok=True)
    SEEN_URLS_FILE.write_text(json.dumps(sorted(urls), indent=2, ensure_ascii=False))

def extract_account(url: str) -> Optional[str]:
    segments = [s for s in urlparse(url).path.split("/") if s]
    if not segments or segments[0].lower() == "search":
        return None
    return segments[0]

# ------------------------------------------
#  RSS Bridge
# ------------------------------------------

def rss_collect(src: str) -> Set[str]:
    print(f"  [RSS] {src}")
    for base in RSS_BRIDGE_MIRRORS:
        print(f"    Try: {base}")
        encoded = quote_plus(src)
        rss_url = f"{base}?action=detect&format=Atom&url={encoded}"
        try:
            r = requests.get(rss_url, timeout=20)
            r.raise_for_status()
            found = set(GOFILE_REGEX.findall(r.text))
            print(f"      Success → {len(found)} URLs")
            return found
        except Exception as e:
            print(f"      Failed: {e}")
    return set()

# ------------------------------------------
#  Nitter
# ------------------------------------------

def collect_nitter(page: Page, src: str) -> Set[str]:
    print(f"  [NITTER] {src}")
    account = extract_account(src)

    for mirror in NITTER_MIRRORS:
        search = f"@{account}" if account else "gofile.io/d/"
        url = f"{mirror}/search?f=tweets&q={quote_plus(search)}"
        print(f"    Try mirror: {mirror}")

        try:
            page.goto(url, wait_until="networkidle", timeout=30000)
        except Exception:
            continue

        found = set(GOFILE_REGEX.findall(page.content()))
        print(f"      Page1: {len(found)}")

        for _ in range(MAX_NITTER_PAGES):
            btn = page.locator("a:has-text('Load more')")
            if btn.count() == 0:
                break
            try:
                btn.first.click()
                time.sleep(2)
                found |= set(GOFILE_REGEX.findall(page.content()))
            except Exception:
                break

        print(f"      success → total {len(found)} URLs")
        return found
    return set()

# ------------------------------------------
#  Mobile Twitter fallback
# ------------------------------------------

def collect_mobile(page: Page, src: str) -> Set[str]:
    account = extract_account(src)
    if not account:
        return set()

    url = f"https://mobile.twitter.com/{account}"
    print(f"  [MOBILE] {url}")
    try:
        page.goto(url, wait_until="networkidle", timeout=30000)
    except Exception as e:
        print(f"    [MOBILE] load error: {e}")
        return set()

    return set(GOFILE_REGEX.findall(page.content()))

# ------------------------------------------
#  gofile status
# ------------------------------------------

def check_gofile(page: Page, url: str) -> str:
    try:
        page.goto(url, wait_until="networkidle", timeout=25000)
    except Exception:
        return "dead"

    body = page.inner_text("body")
    if GOFILE_BLOCK_PATTERN in body:
        return "blocked"
    for p in GOFILE_DEAD_PATTERNS:
        if p in body:
            return "dead"
    return "alive"

# ------------------------------------------
#  Google Sheets append (列ズレ完全修正版)
# ------------------------------------------

def get_sheet():
    info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds).open(GOOGLE_SHEET_NAME).worksheet(GOOGLE_SHEET_WORKSHEET)

def append_row(gc_sheet, gofile, src):
    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    row = [now, gofile, src]

    values = gc_sheet.get_all_values()
    next_row = len(values) + 1

    cell_range = "A" + str(next_row) + ":C" + str(next_row)   # ← f-string禁止で安全
    print("[APPEND]", row)

    gc_sheet.update(cell_range, [row], value_input_option="USER_ENTERED")

# ------------------------------------------
#  Main
# ------------------------------------------

def main():
    sources = load_sources()
    seen = load_seen_urls()

    sheet = get_sheet()

    processed = 0
    checks = 0
    new_seen = False

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent="Mozilla/5.0 Chrome/123")
        page_n = ctx.new_page()
        page_g = ctx.new_page()
        page_m = ctx.new_page()

        for src in sources:
            print(f"\n=== Scraping: {src}")

            collected = set()
            collected |= rss_collect(src)
            collected |= collect_nitter(page_n, src)
            collected |= collect_mobile(page_m, src)

            print(f"  total collected: {len(collected)}")

            for url in sorted(collected):
                if url in seen:
                    continue
                if checks >= MAX_GOFILE_CHECKS_PER_RUN:
                    browser.close()
                    save_seen_urls(seen)
                    print(f"Processed {processed} URLs | gofile checks: {checks}")
                    return

                stat = check_gofile(page_g, url)
                checks += 1

                if stat == "blocked":
                    print("⚠️ gofile blocked — ending run")
                    browser.close()
                    save_seen_urls(seen)
                    return
                if stat == "dead":
                    seen.add(url)
                    new_seen = True
                    continue

                append_row(sheet, url, src)
                processed += 1
                seen.add(url)
                new_seen = True

        browser.close()

    if new_seen:
        save_seen_urls(seen)

    print(f"Processed {processed} URLs | gofile checks: {checks}")


if __name__ == "__main__":
    main()