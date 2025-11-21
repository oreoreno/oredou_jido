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

# 1回の Run でチェックする最大 gofile 件数
MAX_GOFILE_CHECKS_PER_RUN = 40

# Nitter の Load more 上限
MAX_NITTER_PAGES = 50


# ------------------------------------------
#  共通ユーティリティ
# ------------------------------------------

def load_sources() -> List[str]:
    """スクレイピング対象の Nitter URL 読み込み"""
    if not SOURCES_FILE.exists():
        raise FileNotFoundError(
            f"{SOURCES_FILE} が存在しません。sources.json を配置してください。"
        )
    with SOURCES_FILE.open("r", encoding="utf-8") as f:
        return json.load(f).get("sources", [])


def load_seen_urls() -> Set[str]:
    """すでに処理済みの gofile URL 読み込み"""
    if not SEEN_URLS_FILE.exists():
        SEEN_URLS_FILE.parent.mkdir(parents=True, exist_ok=True)
        SEEN_URLS_FILE.write_text("[]", encoding="utf-8")
        return set()

    try:
        return set(json.loads(SEEN_URLS_FILE.read_text(encoding="utf-8")))
    except json.JSONDecodeError:
        return set()


def save_seen_urls(urls: Set[str]):
    """seen URLs 保存"""
    SEEN_URLS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with SEEN_URLS_FILE.open("w", encoding="utf-8") as f:
        json.dump(sorted(urls), f, ensure_ascii=False, indent=2)


# ------------------------------------------
#  Nitter URL からアカウント抽出
# ------------------------------------------

def extract_account_from_nitter_url(nitter_url: str) -> Optional[str]:
    """
    https://nitter.net/tyui33601530 → tyui33601530
    https://nitter.net/search?... → None
    """
    parsed = urlparse(nitter_url)
    segments = [s for s in parsed.path.split("/") if s]

    if not segments:
        return None
    if segments[0].lower() == "search":
        return None
    return segments[0]


# ------------------------------------------
#  RSS-Bridge（全ミラー）で gofile 抽出
# ------------------------------------------

def build_rss_url(base: str, nitter_url: str) -> str:
    encoded = quote_plus(nitter_url)
    return f"{base}?action=detect&format=Atom&url={encoded}"


def collect_gofile_via_rss_bridge(nitter_url: str) -> Set[str]:
    """RSS-Bridge のミラーを順番に試し、成功した1つから gofile を抽出"""
    print(f"  [RSS] Source: {nitter_url}")

    headers = {
        "User-Agent": "Mozilla/5.0 Chrome/122 Safari/537.36"
    }

    for base in RSS_BRIDGE_MIRRORS:
        rss_url = build_rss_url(base, nitter_url)
        print(f"    [RSS] Try mirror: {base}")

        try:
            r = requests.get(rss_url, headers=headers, timeout=20)
            r.raise_for_status()
        except Exception as e:
            print(f"      [RSS] Failed: {e}")
            continue

        text = r.text
        urls = set(GOFILE_REGEX.findall(text))

        print(f"      [RSS] Success! found {len(urls)} URLs")
        return urls  # 成功したミラーのみ採用

    print("    [RSS] All mirrors failed.")
    return set()


# ------------------------------------------
#  Nitter ミラーで検索 → gofile 抽出
# ------------------------------------------

def build_search_url(mirror: str, nitter_url: str) -> str:
    """
    - アカウントURLなら: @username で検索
    - /search?... の場合: gofile.io/d/ で全体検索
    """
    account = extract_account_from_nitter_url(nitter_url)

    if account:
        q = f"@{account}"
    else:
        q = "gofile.io/d/"

    return f"{mirror}/search?f=tweets&q={quote_plus(q)}"


def collect_gofile_via_nitter(page: Page, nitter_url: str) -> Set[str]:
    """
    Nitter ミラーを1個ずつ試す。
    どこか1個でも動いたら、そのミラーで Load more を MAX_NITTER_PAGES 回まで押して回収。
    """
    print(f"  [NITTER] Source: {nitter_url}")

    for mirror in NITTER_MIRRORS:
        search_url = build_search_url(mirror, nitter_url)
        print(f"    [NITTER] Try mirror: {mirror}")
        print(f"      URL: {search_url}")

        try:
            page.goto(search_url, wait_until="networkidle", timeout=30000)
        except Exception as e:
            print(f"      -> mirror dead ({e})")
            continue

        time.sleep(2)

        found: Set[str] = set()

        # 1ページ目
        html = page.content()
        found |= set(GOFILE_REGEX.findall(html))
        print(f"      Page 1: {len(found)} URLs")

        # Load More を MAX_NITTER_PAGES 回まで繰り返す
        for i in range(2, MAX_NITTER_PAGES + 1):
            load_btn = page.locator("a:has-text('Load more')")
            if load_btn.count() == 0:
                print("      -> no more 'Load more' button, stop.")
                break

            try:
                load_btn.first.click()
                time.sleep(2)
            except Exception as e:
                print(f"      -> failed to click Load more: {e}")
                break

            html = page.content()
            new_urls = set(GOFILE_REGEX.findall(html))
            before = len(found)
            found |= new_urls
            print(f"      Page {i}: total {len(found)} URLs (added {len(found) - before})")

        print(f"      -> mirror success, collected total {len(found)} URLs")
        return found

    print("    [NITTER] All mirrors failed.")
    return set()


# ------------------------------------------
# gofile 死活 & ブロック判定
# ------------------------------------------

def check_gofile_status(page: Page, url: str) -> str:
    """
    gofile.io のリンク状態を判定する

    戻り値:
      - alive   : OK
      - dead    : 削除/パスワード/表示不可
      - blocked : IPブロック挙動
    """

    try:
        page.goto(url, wait_until="networkidle", timeout=25000)
    except Exception as e:
        print(f"    [GOFILE] load error: {e}")
        return "dead"

    page.wait_for_timeout(2000)

    try:
        body = page.inner_text("body")
    except Exception as e:
        print(f"    [GOFILE] body read error: {e}")
        return "dead"

    # ブロック判定
    if GOFILE_BLOCK_PATTERN in body:
        print("    [GOFILE] Block pattern detected")
        return "blocked"

    # 死亡判定
    for pattern in GOFILE_DEAD_PATTERNS:
        if pattern in body:
            print(f"    [GOFILE] Dead pattern detected: {pattern}")
            return "dead"

    return "alive"


# ------------------------------------------
# Google Sheets への追記（A/B/C 列に固定）
# ------------------------------------------

def get_gspread_client():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError("環境変数 GOOGLE_SERVICE_ACCOUNT_JSON が未設定です")

    info = json.loads(raw)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def append_row_to_sheet(gc, gofile_url: str, source_nitter_url: str):
    """
    必ず A=timestamp / B=gofile URL / C=元Nitter URL の順に書く。
    A〜C の範囲を明示して指定する。
    """
    sh = gc.open(GOOGLE_SHEET_NAME)
    ws = sh.worksheet(GOOGLE_SHEET_WORKSHEET)

    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    row = [now, gofile_url, source_nitter_url]

    # 送っている内容をログに出す（GitHub Actions のログで確認用）
    print("[APPEND ROW]", row)

    # 既存行数を数えて、次の行番号を決める
    current_rows = len(ws.get_all_values())
    next_row = current_rows + 1

    # A〜C 列だけを更新
    ws.update(
        f"A{next_row}:C{next_row}",
        [row],
        value_input_option="USER_ENTERED",
    )


# ------------------------------------------
# メイン処理
# ------------------------------------------

def main():
    sources = load_sources()
    seen_urls = load_seen_urls()
    print(f"Loaded {len(seen_urls)} seen URLs")

    gc = get_gspread_client()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 720},
        )

        nitter_page = context.new_page()
        gofile_page = context.new_page()

        processed = 0
        checks_done = 0
        blocked_detected = False
        new_seen = False

        for src in sources:
            print(f"\nScraping source: {src}")

            # 1) RSS-Bridge 経由
            rss_urls = collect_gofile_via_rss_bridge(src)

            # 2) Nitter ミラー経由
            nitter_urls = collect_gofile_via_nitter(nitter_page, src)

            collected: Set[str] = set()
            collected |= rss_urls
            collected |= nitter_urls

            print(f"  Total collected URLs (RSS + Nitter): {len(collected)}")

            # 3) gofile の生死チェック & シート書き込み
            for url in sorted(collected):

                if url in seen_urls:
                    continue

                if checks_done >= MAX_GOFILE_CHECKS_PER_RUN:
                    print("Reached max per-run limit, stopping.")
                    blocked_detected = False
                    break

                print(f"  Checking gofile: {url}")
                status = check_gofile_status(gofile_page, url)
                checks_done += 1

                if status == "blocked":
                    print("  BLOCK detected — ending run immediately.")
                    blocked_detected = True
                    break

                if status == "dead":
                    print("  -> dead, skipping.")
                    seen_urls.add(url)
                    new_seen = True
                    continue

                print("  -> alive, writing to sheet...")
                try:
                    append_row_to_sheet(gc, url, src)
                    processed += 1
                    seen_urls.add(url)
                    new_seen = True
                except Exception as e:
                    print(f"  -> append failed: {e}")

            if blocked_detected or checks_done >= MAX_GOFILE_CHECKS_PER_RUN:
                break

        browser.close()

    if new_seen:
        save_seen_urls(seen_urls)

    print(f"Processed {processed} new URLs in this run.")
    print(f"Total gofile checks in this run: {checks_done}")
    if blocked_detected:
        print("Run ended early due to block detection.")


if __name__ == "__main__":
    main()