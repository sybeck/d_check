import os
import re
import json
import ast
import subprocess
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

KST = timezone(timedelta(hours=9))
LOGGER = logging.getLogger("daily_sales")

SPREADSHEET_ID = "1DeSRVN4pWf6rnp1v_FeePUYe1ngjwyq_znXZUzl_kbM"
SHEET_BURDENZERO = "부담제로"
SHEET_BRAINOLOGY = "뉴턴젤리"

# ✅ 해결 1: Playwright temp 경로를 안정적으로 고정
SAFE_TEMP_DIR = r"C:\Temp"


@dataclass
class DailyMetrics:
    sales: int
    orders: int


def yday_kst_date():
    return (datetime.now(KST) - timedelta(days=1)).date()


# ---------------------------
# Google Sheets
# ---------------------------
def gspread_client_from_service_account() -> gspread.Client:
    sa_path = os.getenv("GOOGLE_SA_JSON")
    if not sa_path or not os.path.exists(sa_path):
        raise RuntimeError(
            "GOOGLE_SA_JSON 환경변수에 서비스계정 JSON 경로가 필요합니다. "
            "예) GOOGLE_SA_JSON=C:\\keys\\moncgroup-gsheet-sa.json"
        )

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    return gspread.authorize(creds)


def find_or_create_row_by_date(ws: gspread.Worksheet, date_str: str) -> int:
    col_a = ws.col_values(1)
    for idx, val in enumerate(col_a, start=1):
        if (val or "").strip() == date_str:
            return idx
    ws.append_row([date_str], value_input_option="USER_ENTERED")
    return len(col_a) + 1


def write_metrics_row(
    ws: gspread.Worksheet,
    row: int,
    cafe24: Optional[DailyMetrics],
    coupang: Optional[DailyMetrics],
    naver: Optional[DailyMetrics],
) -> None:
    # B~G
    values = [
        cafe24.sales if cafe24 else "",
        cafe24.orders if cafe24 else "",
        coupang.sales if coupang else "",
        coupang.orders if coupang else "",
        naver.sales if naver else "",
        naver.orders if naver else "",
    ]
    ws.update(f"B{row}:G{row}", [values], value_input_option="USER_ENTERED")


# ---------------------------
# Subprocess JSON parsing
# ---------------------------
def _extract_last_object(text: str) -> Dict[str, Any]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        raise RuntimeError("stdout이 비어있습니다.")

    for ln in reversed(lines):
        if ln.startswith("{") and ln.endswith("}"):
            try:
                return json.loads(ln)
            except Exception:
                pass
            try:
                return ast.literal_eval(ln)
            except Exception:
                pass

        if "{" in ln and "}" in ln:
            m = re.search(r"(\{.*\})", ln)
            if m:
                chunk = m.group(1)
                try:
                    return json.loads(chunk)
                except Exception:
                    pass
                try:
                    return ast.literal_eval(chunk)
                except Exception:
                    pass

    raise RuntimeError("stdout에서 파싱 가능한 JSON/dict 객체를 찾지 못했습니다.")


def run_script(script_path: str, args: List[str]) -> Dict[str, Any]:
    """
    ✅ 해결 1 적용 (원본 구조 유지 + 최소 변경)
    - 서브프로세스에 TEMP/TMP를 C:\\Temp로 강제 전달 → Playwright mkdtemp ENOENT 방지
    - 서브프로세스에 UTF-8 강제 → UnicodeEncodeError(charmap) 방지
    """
    # TEMP 폴더가 없으면 파이썬에서 생성 (CMD/스케줄러에서도 안전)
    os.makedirs(SAFE_TEMP_DIR, exist_ok=True)

    env = os.environ.copy()

    # 1) Playwright artifacts temp 안정화
    env["TEMP"] = SAFE_TEMP_DIR
    env["TMP"] = SAFE_TEMP_DIR

    # 2) Windows 콘솔 인코딩 이슈 방지 (서브프로세스 파이썬 출력 UTF-8 강제)
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"

    cmd = ["python", script_path] + args
    LOGGER.info("RUN: %s", " ".join(cmd))

    p = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,  # ✅ 핵심: 서브프로세스에 환경변수 전달
    )

    if p.returncode != 0:
        raise RuntimeError(
            f"스크립트 실패: {script_path}\nSTDOUT:\n{p.stdout}\nSTDERR:\n{p.stderr}"
        )
    return _extract_last_object(p.stdout)


# ---------------------------
# Normalize channel payloads
# ---------------------------
def metrics_from_simple(payload: Dict[str, Any]) -> DailyMetrics:
    return DailyMetrics(int(payload.get("sales", 0) or 0), int(payload.get("orders", 0) or 0))


def metrics_from_coupang(payload: Dict[str, Any]) -> Dict[str, DailyMetrics]:
    """
    쿠팡 payload는 coupang.py 수정본 기준:
      payload["mapped"]["burdenzero"] / ["brainology"] / ["ppadi"]
    """
    mapped = payload.get("mapped") or {}
    bz = mapped.get("burdenzero") or {}
    br = mapped.get("brainology") or {}

    return {
        "burdenzero": DailyMetrics(int(bz.get("sales", 0) or 0), int(bz.get("orders", 0) or 0)),
        "brainology": DailyMetrics(int(br.get("sales", 0) or 0), int(br.get("orders", 0) or 0)),
    }


def main():
    load_dotenv()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    target_date = yday_kst_date()
    date_str = target_date.isoformat()
    LOGGER.info("Target date (KST): %s", date_str)

    # paths
    cafe24_path = os.path.join("connectors", "sales", "cafe24.py")
    coupang_path = os.path.join("connectors", "sales", "coupang.py")
    naver_path = os.path.join("connectors", "sales", "naver.py")

    # 1) Cafe24 two accounts
    cafe_bz_payload = run_script(cafe24_path, ["--profile", "burdenzero", "--date", date_str])
    cafe_br_payload = run_script(cafe24_path, ["--profile", "brainology", "--date", date_str])

    cafe_bz = metrics_from_simple(cafe_bz_payload)
    cafe_br = metrics_from_simple(cafe_br_payload)

    # 2) Coupang mixed -> mapped already
    coupang_payload = run_script(coupang_path, ["--date", date_str, "--json"])
    coupang = metrics_from_coupang(coupang_payload)
    coupang_bz = coupang["burdenzero"]
    coupang_br = coupang["brainology"]

    # 3) Naver (burdenzero only)
    naver_payload = run_script(naver_path, ["--date", date_str, "--json"])
    naver_bz = metrics_from_simple(naver_payload)

    # 4) Write to Google Sheets
    gc = gspread_client_from_service_account()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_bz = sh.worksheet(SHEET_BURDENZERO)
    ws_br = sh.worksheet(SHEET_BRAINOLOGY)

    row_bz = find_or_create_row_by_date(ws_bz, date_str)
    row_br = find_or_create_row_by_date(ws_br, date_str)

    # 부담제로: B~G 모두
    write_metrics_row(ws_bz, row_bz, cafe24=cafe_bz, coupang=coupang_bz, naver=naver_bz)
    # 뉴턴젤리: 카페24/쿠팡만, 네이버는 없음
    write_metrics_row(ws_br, row_br, cafe24=cafe_br, coupang=coupang_br, naver=None)

    LOGGER.info("✅ Done.")
    LOGGER.info("[부담제로] cafe24=%s coupang=%s naver=%s", cafe_bz, coupang_bz, naver_bz)
    LOGGER.info("[뉴턴젤리] cafe24=%s coupang=%s", cafe_br, coupang_br)


if __name__ == "__main__":
    main()
