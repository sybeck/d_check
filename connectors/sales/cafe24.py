# connectors/sales/cafe24.py
import os
import re
import time
import json
import argparse
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeoutError

load_dotenv()

KST = timezone(timedelta(hours=9))


# ---------------------------
# Helpers
# ---------------------------
def must_env(key: str) -> str:
    v = os.getenv(key)
    if not v:
        raise RuntimeError(f"{key} 환경변수가 필요합니다. .env를 확인하세요.")
    return v


def must_env_profile(profile: str, suffix: str) -> str:
    """
    profile: burdenzero | brainology
    suffix: ADMIN_URL / ADMIN_ID / ADMIN_PW / GRID_SELECTOR / POST_LOGIN_WAIT_MS ...
    env key example:
      CAFE24_BURDENZERO_ADMIN_URL
      CAFE24_BRAINOLOGY_ADMIN_URL
    """
    p = profile.strip().upper()
    key = f"CAFE24_{p}_{suffix}"
    return must_env(key)


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def save_debug(page, prefix: str = "fail") -> None:
    os.makedirs("debug", exist_ok=True)
    page.screenshot(path=f"debug/{prefix}.png", full_page=True)
    with open(f"debug/{prefix}.html", "w", encoding="utf-8") as f:
        f.write(page.content())


# ---------------------------
# Cafe24 flow
# ---------------------------
def login_cafe24(page, profile: str) -> None:
    url = must_env_profile(profile, "ADMIN_URL")
    user = must_env_profile(profile, "ADMIN_ID")
    pw = must_env_profile(profile, "ADMIN_PW")

    page.goto(url, wait_until="domcontentloaded")

    scopes = [page] + list(page.frames)
    submitted = False

    for scope in scopes:
        try:
            id_loc = scope.locator(
                "input[name='id'], input#id, input[name='mall_id'], input[type='text'], input[placeholder*='아이디'], input[placeholder*='ID']"
            )
            pw_loc = scope.locator(
                "input[name='passwd'], input#passwd, input[name='password'], input[type='password'], input[placeholder*='비밀번호'], input[placeholder*='Password']"
            )

            if id_loc.count() == 0 or pw_loc.count() == 0:
                continue

            id_loc.first.fill(user)
            pw_loc.first.fill(pw)

            btn = scope.locator(
                "button:has-text('로그인'), [role='button']:has-text('로그인'), input[value*='로그인']"
            )
            if btn.count() > 0:
                btn.first.click()
                submitted = True
                break

            pw_loc.first.press("Enter")
            submitted = True
            break

        except Exception:
            continue

    if not submitted:
        save_debug(page, f"{profile}_login_form_not_found")
        raise RuntimeError(
            f"[{profile}] 로그인 폼/버튼을 찾지 못했습니다. debug/{profile}_login_form_not_found.* 확인"
        )


def wait_after_login(page, profile: str) -> None:
    try:
        page.wait_for_load_state("networkidle", timeout=30_000)
    except PwTimeoutError:
        pass

    # profile별 대기시간 지원 (없으면 기본 1500ms)
    wait_ms_key = f"CAFE24_{profile.strip().upper()}_POST_LOGIN_WAIT_MS"
    wait_ms = int(os.getenv(wait_ms_key, os.getenv("CAFE24_POST_LOGIN_WAIT_MS", "1500")))
    if wait_ms > 0:
        time.sleep(wait_ms / 1000)


def scrape_cell_3_3_text(page, profile: str) -> str:
    grid_selector = must_env_profile(profile, "GRID_SELECTOR")

    page.wait_for_selector(grid_selector, timeout=30_000)
    grid = page.locator(grid_selector).first

    # role 기반 우선
    try:
        row3 = grid.get_by_role("row").nth(2)
        cell = row3.get_by_role("cell").nth(2)
        text = normalize_text(cell.inner_text())
        if text:
            return text
    except Exception:
        pass

    # table 기반 fallback
    row3 = grid.locator("tr").nth(2)
    cell = row3.locator("td,th").nth(2)
    text = normalize_text(cell.inner_text())
    return text


def parse_sales_and_orders(raw: str) -> tuple[int, int]:
    """
    예: '688,100 원 19건' -> (688100, 19)
    """
    raw = normalize_text(raw)

    m_sales = re.search(r"([\d,]+)\s*원", raw)
    m_orders = re.search(r"(\d+)\s*건", raw)

    if not m_sales or not m_orders:
        raise ValueError(f"텍스트에서 매출/구매수를 파싱하지 못했습니다: {raw}")

    sales = int(m_sales.group(1).replace(",", ""))
    orders = int(m_orders.group(1))
    return sales, orders


# ---------------------------
# Public API for runner
# ---------------------------
def get_daily_metrics(profile: str, target_date=None) -> dict:
    """
    Returns:
      {
        "status": "ok",
        "date": "YYYY-MM-DD",
        "sales": int,
        "orders": int,
        "raw": str,
        "source": "cafe24",
        "profile": profile
      }
    NOTE: 실제 화면이 '어제' 행을 기준으로 고정되어있다면 target_date는 기록용으로만 사용.
    """
    if target_date is None:
        target_date = (datetime.now(KST) - timedelta(days=1)).date()

    headless = os.getenv("HEADLESS", "false").lower() == "true"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()

        try:
            login_cafe24(page, profile=profile)
            wait_after_login(page, profile=profile)

            # 운영 안정화되면 아래 디버그는 꺼도 됨 (필요하면 env로 끄기)
            if os.getenv("CAFE24_DEBUG", "false").lower() == "true":
                os.makedirs("debug", exist_ok=True)
                page.screenshot(path=f"debug/{profile}_after_login.png", full_page=True)
                with open(f"debug/{profile}_after_login.html", "w", encoding="utf-8") as f:
                    f.write(page.content())

            raw = scrape_cell_3_3_text(page, profile=profile)
            sales, orders = parse_sales_and_orders(raw)

            return {
                "status": "ok",
                "date": target_date.isoformat(),
                "sales": int(sales),
                "orders": int(orders),
                "raw": raw,
                "source": "cafe24",
                "profile": profile,
            }

        except Exception as e:
            save_debug(page, f"{profile}_fail")
            raise RuntimeError(
                f"[{profile}] 실패: {e} (debug/{profile}_fail.png, debug/{profile}_fail.html 저장됨)"
            ) from e
        finally:
            context.close()
            browser.close()


# ---------------------------
# CLI
# ---------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=False, default="burdenzero", choices=["burdenzero", "brainology"])
    ap.add_argument("--date", required=False, help="YYYY-MM-DD (기록용). 기본: 전날(KST)")
    args = ap.parse_args()

    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = (datetime.now(KST) - timedelta(days=1)).date()

    result = get_daily_metrics(profile=args.profile, target_date=target_date)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
