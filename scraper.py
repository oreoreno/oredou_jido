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

# 🔹 使う RSS-Bridge インスタンスをできるだけたくさん用意しておく
#  上から順に試して、ダメなら次へフェイルオーバーする
RSS_BRIDGE_BASES = [
    "https://rss-bridge.org/bridge01/",
    "https://rss-bridge.bb8.fun/",
    "https://ololbu.ru/rss-bridge/",
    "https://tools.bheil.net/rss-bridge/",
    "https://bridge.suumitsu.eu/",
    "https://rss-bridge.ggc-project.de/",
    "https://rssbridge.projectsegfau.lt/",
    "https://rss.bloat.cat/",
]

# Google Sheets 設定（★自分のシート名に合わせて）
GOOGLE_SHEET_NAME = "gofile_links"  # スプレッドシートの名前
GOOGLE_SHEET_WORKSHEET = "シート1"   # タブ名

# gofile の URLパターン
GOFILE_REGEX = re.compile(r"https://gofile\.io/d/[0-9A-Za-z]+")

# gofile が死んでいるときの文言
GOFILE_DEAD_PATTERNS = [
    "This content does not exist",
    "The content you are looking for could not be found",
    "No items to display",
    "This content is password protected",
]

# gofile 側でブロックされてそうなときに出る文言
GOFILE_BLOCK_PATTERN = "refreshAppdataAccountsAndSync getAccountActive Failed to fetch"

# 1回の Run でチェックする gofile の最大件数
MAX_GOFILE_CHECKS_PER_RUN = 40


def load_sources() -> List[str]:
    """スクレイピング対象の Nitter URL を config/sources.json から読み込み"""
    if not SOURCES_FILE.exists():
        raise FileNotFoundError(f"{SOURCES_FILE} が存在しません。Nitter の URL をここに保存してください。")
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


def build_rss_url(base: str, nitter_url: str) -> str:
    """Nitter の URL を、指定した RSS-Bridge インスタンスの detect アクション URL に変換"""
    encoded = quote_plus(nitter_url)
    # base は末尾が / の想定（上のリストは全部そうしてある）
    return f"{base}?action=detect&format=Atom&url={encoded}"


def collect_gofile_urls_from_nitter_via_rss_bridge(nitter_url: str) -> Set[str]:
    """
    Nitter → (複数の RSS-Bridge を順番に試す) → RSS から gofile を抜き出す
    どれか1つのインスタンスからでも取れたら OK とする
    """
    print(f"  Nitter URL: {nitter_url}")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    }

    last_error = None

    for base in RSS_BRIDGE_BASES:
        rss_url = build_rss_url(base, nitter_url)
        print(f"  Trying RSS-Bridge: {base} -> {rss_url}")

        try:
            resp = requests.get(rss_url, headers=headers, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            print(f"    Failed on {base}: {e}")
            last_error = e
            # このインスタンスはダメだったので、次のインスタンスを試す
            continue

        text = resp.text
        urls = set(GOFILE_REGEX.findall(text))
        print(f"    Success on {base}: found {len(urls)} gofile URLs in feed (via RSS-Bridge)")
        return urls

    # どのインスタンスでもダメだった場合
    if last_error:
        print(f"  All RSS-Bridge instances failed for this source. Last error: {last_error}")
    else:
        print("  All RSS-Bridge instances failed for this source (unknown error).")
    return set()


def check_gofile_status(page: Page, url: str) -> str:
    """
    gofile.io のリンク状態を判定する

    戻り値:
      "alive"   : 生きている
      "dead"    : 死亡 or パス付きなど
      "blocked" : IP ブロックっぽい挙動を検出
    """
    try:
        page.goto(url, wait_until="networkidle", timeout=30000)
    except Exception as e:
        print(f"    -> Error loading page: {e}")
        return "dead"

    # JS が落ち着くまで少し待つ
    page.wait_for_timeout(3000)

    try:
        text = page.inner_text("body")
    except Exception as e:
        print(f"    -> Error reading body text: {e}")
        return "dead"

    # ブロックっぽい挙動の検出（最優先）
    if GOFILE_BLOCK_PATTERN in text:
        print("    -> Detected block pattern on gofile page!")
        return "blocked"

    # 通常の死活判定
    for pattern in GOFILE_DEAD_PATTERNS:
        if pattern in text:
            return "dead"

    return "alive"


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
        checks_done = 0
        blocked_detected = False

        for src in sources:
            print(f"Scraping source (Nitter): {src}")
            urls = collect_gofile_urls_from_nitter_via_rss_bridge(src)

            for url in sorted(urls):
                # すでに処理済みなら飛ばす
                if url in seen_urls:
                    continue

                # 一回の Run でのチェック上限
                if checks_done >= MAX_GOFILE_CHECKS_PER_RUN:
                    print(f"Reached max checks per run ({MAX_GOFILE_CHECKS_PER_RUN}). Stopping checks for this run.")
                    blocked_detected = False  # これは単なる上限なのでブロック扱いではない
                    break

                print(f"  Checking gofile URL: {url}")
                status = check_gofile_status(gofile_page, url)
                checks_done += 1

                if status == "blocked":
                    print("    -> Looks like gofile blocked us. Stopping this run immediately to be safe.")
                    blocked_detected = True
                    # このURLは seen にも入れず、次回以降に再チャレンジできるようにする
                    break

                if status == "dead":
                    print("    -> Dead or password protected. Skipped.")
                    # 死んでいるものも再チェックしたくないなら seen に入れる
                    seen_urls.add(url)
                    new_seen = True
                    continue

                # status == "alive"
                print("    -> Alive. Appending to Google Sheet...")
                try:
                    append_row_to_sheet(gc, url, src)
                    print("    -> Append done.")
                    seen_urls.add(url)
                    new_seen = True
                    processed += 1
                except Exception as e:
                    print(f"    -> Append failed: {e}")

            # 内側のループから抜ける理由が「ブロック検出」 or 「上限到達」の場合、
            # 外側のループも抜けて今回の Run を終了する
            if blocked_detected or checks_done >= MAX_GOFILE_CHECKS_PER_RUN:
                break

        browser.close()

    if new_seen:
        save_seen_urls(seen_urls)
        print(f"Saved {len(seen_urls)} seen URLs")
    print(f"Processed {processed} new URLs in this run.")
    print(f"Total gofile checks in this run: {checks_done}")
    if blocked_detected:
        print("Run ended early because a gofile block pattern was detected.")


if __name__ == "__main__":
    main()