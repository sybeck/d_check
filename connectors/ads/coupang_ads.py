import os
import time
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeoutError

load_dotenv()
KST = timezone(timedelta(hours=9))

LOGIN_URL = (
    "https://xauth.coupang.com/auth/realms/seller/protocol/openid-connect/auth"
    "?response_type=code&client_id=wing"
    "&redirect_uri=https%3A%2F%2Fwing.coupang.com%2Fsso%2Flogin%3FreturnUrl%3D%252F"
    "&state=6d110f1c-c6e7-44f3-9f48-f5947e4803df&login=true&scope=openid"
)

TARGET_GRIDCELLS = [
    "260122_뉴턴젤리 수정 삭제",
    "수동_튼살크림_1029 수정 삭제",
]


def must_env(key: str) -> str:
    v = os.getenv(key)
    if not v:
        raise RuntimeError(f"{key} 환경변수가 필요합니다. .env를 확인하세요.")
    return v


def kst_yesterday_ymd() -> str:
    return (datetime.now(KST).date() - timedelta(days=1)).strftime("%Y-%m-%d")


def save_debug(page, prefix: str) -> None:
    os.makedirs("debug", exist_ok=True)
    page.screenshot(path=f"debug/{prefix}.png", full_page=True)
    with open(f"debug/{prefix}.html", "w", encoding="utf-8") as f:
        f.write(page.content())


def wait_soft(page, ms: int = 200) -> None:
    # ✅ 대기시간을 줄였고, SPA에서 networkidle이 오래 걸릴 수 있어 timeout 짧게
    try:
        page.wait_for_load_state("networkidle", timeout=5_000)
    except PwTimeoutError:
        pass
    if ms > 0:
        time.sleep(ms / 1000)


def login_coupang(page) -> None:
    user = must_env("COUPANG_ID")
    pw = must_env("COUPANG_PW")

    page.goto(LOGIN_URL, wait_until="domcontentloaded")
    wait_soft(page, 200)

    scopes = [page] + list(page.frames)
    submitted = False

    for scope in scopes:
        try:
            id_loc = scope.locator(
                "input[type='email'], input[name='username'], input#username, input[name='id'], input[type='text']"
            )
            pw_loc = scope.locator("input[type='password'], input[name='password'], input#password")

            if id_loc.count() == 0 or pw_loc.count() == 0:
                continue

            id_loc.first.fill(user)
            pw_loc.first.fill(pw)

            btn = scope.locator(
                "button:has-text('로그인'), button:has-text('Login'), button[type='submit'], input[type='submit']"
            )
            if btn.count() > 0:
                btn.first.click()
            else:
                pw_loc.first.press("Enter")

            submitted = True
            break
        except Exception:
            continue

    if not submitted:
        save_debug(page, "coupang_login_form_not_found")
        raise RuntimeError("로그인 폼/버튼을 찾지 못했습니다. debug/coupang_login_form_not_found.* 확인")

    wait_soft(page, 400)


def go_to_ad_center(page) -> None:
    # 2) 클릭해서 페이지 이동
    link = page.get_by_role("link", name="광고센터")
    link.wait_for(timeout=30_000)
    link.click()

    # 광고센터 화면이 로드되었다는 신호(앵커)로 tabs-contents를 기다림
    page.locator(".tabs-contents").wait_for(state="visible", timeout=60_000)
    wait_soft(page, 150)


def set_yesterday(page) -> None:
    """
    ✅ 'get_by_role("button", name="어제")'가 안 잡히는 케이스 대응:
    - role/button에 의존하지 않고 text 기반으로 클릭
    - '어제'가 여러 개면 첫 번째를 누름
    """
    page.locator(".tabs-contents").wait_for(state="visible", timeout=60_000)

    loc = page.get_by_text("어제", exact=True)
    if loc.count() == 0:
        # exact가 안 잡히면 partial로 한번 더
        loc = page.get_by_text("어제")

    if loc.count() == 0:
        save_debug(page, "coupang_ad_yesterday_not_found")
        raise RuntimeError("어제 텍스트를 찾지 못했습니다. debug/coupang_ad_yesterday_not_found.* 확인")

    loc.first.click(timeout=20_000)
    wait_soft(page, 200)


def extract_row_text_by_gridcell(page, gridcell_name: str) -> Dict[str, object]:
    cell = page.get_by_role("gridcell", name=gridcell_name).first
    cell.wait_for(state="visible", timeout=60_000)

    row = cell.locator("xpath=ancestor::*[@role='row'][1]")
    if row.count() == 0:
        row = cell.locator("xpath=ancestor::div[1]")

    cells = row.get_by_role("gridcell")
    n = cells.count()
    cell_texts: List[str] = []
    for i in range(n):
        t = cells.nth(i).inner_text(timeout=10_000).strip()
        cell_texts.append(t)

    return {
        "key": gridcell_name,
        "row_text": row.inner_text(timeout=10_000).strip(),
        "cells": cell_texts,
    }


def main():
    headless = os.getenv("HEADLESS", "false").lower() == "true"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()

        try:
            # 1) 로그인
            login_coupang(page)

            # 2) 광고센터 이동
            go_to_ad_center(page)

            # 3) 어제로 기준 변경 (텍스트 기반)
            set_yesterday(page)

            # 4) 두 개 행 읽기
            rows = {}
            for name in TARGET_GRIDCELLS:
                rows[name] = extract_row_text_by_gridcell(page, name)

            out = {
                "status": "ok",
                "date": kst_yesterday_ymd(),
                "url": page.url,
                "rows": rows,
            }

            print(json.dumps(out, ensure_ascii=False, indent=2))

        except Exception as e:
            save_debug(page, "coupang_ad_fail")
            raise RuntimeError(f"실패: {e} (debug/coupang_ad_fail.* 저장됨)") from e
        finally:
            context.close()
            browser.close()


if __name__ == "__main__":
    main()
