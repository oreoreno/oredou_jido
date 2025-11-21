import json
import re
from pathlib import Path
from typing import List, Set, Optional
from urllib.parse import quote_plus, urlparse
from datetime import datetime
import os

import requests
import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, Page

# -------------------------
# 設定ファイル / 定数
# -------------------------

SOURCES_FILE = Path("config/sources.json")
SEEN_URLS_FILE = Path("data/seen_urls.json")

# 使う RSS-Bridge インスタンス（上から順に試す）
RSS_BRIDGE_BASES = [
    "https://rss-bridge.org/bridge01/",
]

# poast の Nitter ベースURL
NITTER_POAST_BASE = "https://nitter.poast.org"

# Nitter 検索で「Load more」をクリックする最大回数
MAX_NITTER_PAGES = 50  # 深く回収したい場合はここで調整

# Google Sheets 設定
GOOGLE_SHEET_NAME = "gofile_links"
GOOGLE_SHEET_WORKSHEET = "シート1"

# gofile URL パターン
#   - http/https あり・なし両方OK
#   - パスは /d/<英数> 固定
GOFILE_REGEX = re.compile(
    r"(?:https?://)?gofile\.io/d/[0-9A-Za-z]+"
)

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


# -------------------------
# 共通ユーティリティ
# -------------------------

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


def normalize_gofile_urls_from_text(text: str) -> Set[str]:
    """
    テキスト全体から gofile.io/d/... を抜き出して
    すべて "https://gofile.io/d/..." 形式にそろえて返す。
    JSON 由来の \"https:\/\/gofile.io\/d\/...\" にも対応するため
    まず \\/ を / に置き換える。
    """
    # JSON などでエスケープされたスラッシュを普通のスラッシュに戻す
    normalized_text = text.replace("\\/", "/")

    raw_urls = set(GOFILE_REGEX.findall(normalized_text))
    urls: Set[str] = set()

    for u in raw_urls:
        # GOFILE_REGEX は先頭にプロトコル無しも許しているので補正する
        if not u.startswith("http"):
            u = "https://" + u
        urls.add(u)

    return urls


# -------------------------
# RSS-Bridge 経由
# -------------------------

def build_rss_url(base: str, nitter_url: str) -> str:
    """Nitter の URL を、指定した RSS-Bridge インスタンスの detect アクション URL に変換"""
    encoded = quote_plus(nitter_url)
    return f"{base}?action=detect&format=Atom&url={encoded}"


def collect_gofile_urls_from_nitter_via_rss_bridge(nitter_url: str) -> Set[str]:
    """
    Nitter → RSS-Bridge → RSS から gofile を抜き出す
    どれか1つのインスタンスからでも取れたら OK とする
    """
    print(f"  [RSS] Nitter URL: {nitter_url}")

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
        print(f"  [RSS] Trying RSS-Bridge: {base} -> {rss_url}")

        try:
            resp = requests.get(rss_url, headers=headers, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            print(f"    [RSS] Failed on {base}: {e}")
            last_error = e
            continue

        # ★ここも normalize_gofile_urls_from_text を使う
        urls = normalize_gofile_urls_from_text(resp.text)
        print(f"    [RSS] Success on {base}: found {len(urls)} gofile URLs in feed")
        return urls

    if last_error:
        print(f"  [RSS] All RSS-Bridge instances failed for this source. Last error: {last_error}")
    else:
        print("  [RSS] All RSS-Bridge instances failed for this source (unknown error).")
    return set()


# -------------------------
# Nitter(poast) 直接スクレイピング
# -------------------------

def extract_account_from_nitter_url(nitter_url: str) -> Optional[str]:
    """
    https://nitter.net/tyui33601530 → tyui33601530
    https://nitter.net/search?f=tweets&q=... → None
    """
    parsed = urlparse(nitter_url)
    segments = [s for s in parsed.path.split("/") if s]
    if not segments:
        return None
    if segments[0].lower() == "search":
        return None
    # 先頭セグメントをアカウントIDとみなす
    return segments[0]


def build_poast_search_url(nitter_url: str) -> str:
    """
    poast 用の検索URLを作る。
    - アカウントURLなら: q="@account"
    - /search?... の場合は: q="gofile.io/d/" （全体検索）
    """
    parsed = urlparse(nitter_url)
    segments = [s for s in parsed.path.split("/") if s]

    # /search?... の場合は gofile.io/d/ で全体検索
    if segments and segments[0].lower() == "search":
        query = "gofile.io/d/"
    else:
        # プロフィールURLなら @ユーザー名 で検索
        account = extract_account_from_nitter_url(nitter_url)
        if account:
            query = f"@{account}"
        else:
            # 念のため fallback
            query = "gofile.io/d/"

    q_param = quote_plus(query)
    search_url = f"{NITTER_POAST_BASE}/search?f=tweets&q={q_param}&since=&until=&near="
    return search_url


def collect_gofile_urls_from_nitter_direct(page: Page, nitter_url: str) -> Set[str]:
    """
    Playwright で nitter.poast.org の Search を開いて、
    ページ内にある gofile.io/d/... を拾う。
    - プロフィールURLの場合: @ユーザー名で検索
    - /search?... の場合: gofile.io/d/ で全体検索
    - Load more を MAX_NITTER_PAGES 回までクリックして、過去分も回収
    """
    search_url = build_poast_search_url(nitter_url)
    print(f"  [POAST] Search URL: {search_url}")

    urls: Set[str] = set()

    try:
        page.goto(search_url, wait_until="networkidle", timeout=30000)
    except Exception as e:
        print(f"    [POAST] Failed to load search page: {e}")
        return urls

    page.wait_for_timeout(3000)

    for page_index in range(MAX_NITTER_PAGES):
        try:
            html = page.content()
            found = normalize_gofile_urls_from_text(html)
            print(f"    [POAST] Page {page_index+1}: found {len(found)} gofile URLs (via regex in HTML)")
            urls |= found
        except Exception as e:
            print(f"    [POAST] Error while scanning page HTML: {e}")
            break

        # Load more を探してクリック
        try:
            load_more = page.locator("div.show-more a, a:has-text('Load more')")
            if load_more.count() == 0:
                print("    [POAST] No more 'Load more' button. Stop pagination.")
                break
            print("    [POAST] Clicking 'Load more'...")
            load_more.first.click()
            page.wait_for_timeout(3000)
        except Exception:
            print("    [POAST] Failed to click 'Load more' or no more pages.")
            break

    return urls


# -------------------------
# gofile 死活 & ブロック判定
# -------------------------

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

    page.wait_for_timeout(3000)

    try:
        text = page.inner_text("body")
    except Exception as e:
        print(f"    -> Error reading body text: {e}")
        return "dead"

    if GOFILE_BLOCK_PATTERN in text:
        print("    -> Detected block pattern on gofile page!")
        return "blocked"

    for pattern in GOFILE_DEAD_PATTERNS:
        if pattern in text:
            return "dead"

    return "alive"


# -------------------------
# Google Sheets 関連
# -------------------------

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
    """Googleスプレッドシートに1行追記（必ず A〜C 列に固定）"""
    sh = gc.open(GOOGLE_SHEET_NAME)
    ws = sh.worksheet(GOOGLE_SHEET_WORKSHEET)

    # 現在のシートの行数（ヘッダー行も含む）
    current_values = ws.get_all_values()
    next_row = len(current_values) + 1  # 次に書く行番号

    # A列: timestamp, B列: gofile URL, C列: 元のNitter URL
    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    row_values = [now, gofile_url, source_nitter_url]

    ws.update(
        f"A{next_row}:C{next_row}",
        [row_values],
        value_input_option="USER_ENTERED"
    )


# -------------------------
# メイン処理
# -------------------------

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
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 720},
        )

        gofile_page = context.new_page()
        nitter_page = context.new_page()

        new_seen = False
        processed = 0
        checks_done = 0
        blocked_detected = False

        for src in sources:
            print(f"Scraping source (Nitter): {src}")

            # ① RSS-Bridge 経由で gofile を集める
            rss_urls = collect_gofile_urls_from_nitter_via_rss_bridge(src)

            # ② nitter.poast.org の Search 経由で gofile を集める
            direct_urls = collect_gofile_urls_from_nitter_direct(nitter_page, src)

            # ③ 合体（set なので自動で重複除去）
            urls = set()
            urls |= rss_urls
            urls |= direct_urls

            print(f"  -> Total collected URLs for this source: {len(urls)}")

            for url in sorted(urls):
                if url in seen_urls:
                    continue

                if checks_done >= MAX_GOFILE_CHECKS_PER_RUN:
                    print(f"Reached max checks per run ({MAX_GOFILE_CHECKS_PER_RUN}). Stopping checks for this run.")
                    blocked_detected = False
                    break

                print(f"  Checking gofile URL: {url}")
                status = check_gofile_status(gofile_page, url)
                checks_done += 1

                if status == "blocked":
                    print("    -> Looks like gofile blocked us. Stopping this run immediately to be safe.")
                    blocked_detected = True
                    break

                if status == "dead":
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