#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
liaufa Open API v2 – download campaign instances for ALL connected LinkedIn accounts.

According to your "Copy as cURL", the working request uses custom headers:
- key: <...>
- secret: <...>
and also includes:
- authorization: Basic ...
- csrftoken cookie + x-csrftoken header

This script reproduces that reliably:
1) Loads EXPANDI_KEY + EXPANDI_SECRET from .env (REQUIRED)
2) Creates a requests.Session()
3) GETs /open-swagger/ to obtain csrftoken cookie (if required)
4) Calls:
   - GET https://api.liaufa.com/api/v1/open-api/v2/li_accounts/
   - GET https://api.liaufa.com/api/v1/open-api/v2/li_accounts/{id}/campaign_instances/
5) Saves JSON next to this file.

Install:
  pip install requests python-dotenv
"""

import os
import json
import time
import base64
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv


# ============================================================
# Fixed base + paths
# ============================================================

BASE_URL = "https://api.liaufa.com/api/v1/open-api/v2"
ACCOUNTS_PATH = "/li_accounts/"
CAMPAIGNS_PATH_TEMPLATE = "/li_accounts/{id}/campaign_instances/"

SWAGGER_URL = "https://api.liaufa.com/open-swagger/"  # used to obtain csrftoken cookie if needed

DEFAULT_PARAMS = {"page": 1, "limit": 100}
SLEEP_BETWEEN_ACCOUNTS_SEC = 0.2
RATE_LIMIT_SLEEP_SEC = 5



# ============================================================
# ENV loading
# ============================================================

def load_env():
    # Lokálně můžeš mít .env vedle skriptu.
    # Na GitHub Actions poběží secrets jako env vars, takže .env není potřeba.
    script_env = Path(__file__).with_name(".env")
    if script_env.exists():
        load_dotenv(dotenv_path=script_env)
    else:
        # načte případné env vars (v GitHubu už budou nastavené)
        load_dotenv()

load_env()


EXPANDI_KEY = (os.getenv("EXPANDI_KEY") or "").strip()          # from -H 'key: ...'
EXPANDI_SECRET = (os.getenv("EXPANDI_SECRET") or "").strip()    # from -H 'secret: ...'

# Optional (only if your working cURL includes "authorization: Basic ...")
EXPANDI_USERNAME = (os.getenv("EXPANDI_USERNAME") or "").strip()
EXPANDI_PASSWORD = (os.getenv("EXPANDI_PASSWORD") or "").strip()


# ============================================================
# Helpers
# ============================================================

def is_2xx(code: int) -> bool:
    return 200 <= code < 300

def safe_body(resp: requests.Response) -> str:
    try:
        return json.dumps(resp.json(), ensure_ascii=False)
    except Exception:
        return (resp.text or "")[:2000]

def extract_list_and_next(data):
    if isinstance(data, list):
        return data, None
    items = data.get("results") or data.get("items") or data.get("data") or []
    next_url = data.get("next") or data.get("nextPage") or data.get("next_page")
    return items, next_url

def pick_account_id(acc: dict) -> str | None:
    for key in ("id", "pk", "_id", "uuid", "account_id", "li_account_id"):
        if acc.get(key) is not None:
            return str(acc[key])
    return None

def pick_account_label(acc: dict) -> tuple[str, str]:
    name = acc.get("name") or acc.get("fullName") or acc.get("email") or acc.get("username") or ""
    linkedin = acc.get("linkedinUrl") or acc.get("linkedin_url") or acc.get("public_profile_url") or ""
    return name, linkedin

def build_basic_auth_value(username: str, password: str) -> str:
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


# ============================================================
# Session + headers that mimic the working cURL
# ============================================================

def build_base_headers(csrftoken: str | None) -> dict:
    """
    Builds headers similar to your cURL:
      - accept: application/json
      - key: ...
      - secret: ...
      - referer: open-swagger
      - x-csrftoken: ...
      - authorization: Basic ... (optional)
    """
    if not EXPANDI_KEY or not EXPANDI_SECRET:
        raise SystemExit(
            "❌ Chybí EXPANDI_KEY nebo EXPANDI_SECRET v .env.\n\n"
            "Z cURL zkopíruj hodnoty z:\n"
            "  -H 'key: ...'\n"
            "  -H 'secret: ...'\n"
            "a ulož do .env jako:\n"
            "  EXPANDI_KEY=...\n"
            "  EXPANDI_SECRET=...\n"
        )

    headers = {
        "accept": "application/json",
        "key": EXPANDI_KEY,
        "secret": EXPANDI_SECRET,
        "referer": SWAGGER_URL,
    }

    # CSRF header if we have token
    if csrftoken:
        headers["x-csrftoken"] = csrftoken

    # Optional Basic auth if your working request uses it
    if EXPANDI_USERNAME and EXPANDI_PASSWORD:
        headers["authorization"] = build_basic_auth_value(EXPANDI_USERNAME, EXPANDI_PASSWORD)

    return headers


def get_csrftoken(session: requests.Session) -> str | None:
    """
    Visits swagger page to obtain csrftoken cookie if server sets it.
    """
    try:
        r = session.get(SWAGGER_URL, timeout=60)
    except requests.RequestException:
        return None

    # If server sets csrftoken cookie, requests will store it
    token = session.cookies.get("csrftoken")
    return token


# ============================================================
# HTTP wrappers
# ============================================================

def session_get_json(session: requests.Session, url: str, headers: dict, params: dict | None = None):
    r = session.get(url, headers=headers, params=params, timeout=60)

    if r.status_code == 429:
        time.sleep(RATE_LIMIT_SLEEP_SEC)
        return session_get_json(session, url, headers, params)

    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} for {r.url}\nResponse body: {safe_body(r)}")

    return r.json()


# ============================================================
# API calls
# ============================================================

def test_li_accounts(session: requests.Session, headers: dict) -> tuple[bool, int, str]:
    url = urljoin(BASE_URL + "/", ACCOUNTS_PATH.lstrip("/"))
    r = session.get(url, headers=headers, params=DEFAULT_PARAMS, timeout=60)
    return is_2xx(r.status_code), r.status_code, safe_body(r)

def fetch_all_accounts(session: requests.Session, headers: dict) -> list[dict]:
    url = urljoin(BASE_URL + "/", ACCOUNTS_PATH.lstrip("/"))
    data = session_get_json(session, url, headers=headers, params=DEFAULT_PARAMS)
    accounts, next_url = extract_list_and_next(data)

    while next_url:
        next_full = next_url if next_url.startswith("http") else urljoin(BASE_URL + "/", next_url.lstrip("/"))
        data = session_get_json(session, next_full, headers=headers, params=None)
        chunk, next_url = extract_list_and_next(data)
        accounts.extend(chunk)

    return accounts

def fetch_campaigns_for_account(session: requests.Session, headers: dict, account_id: str) -> list[dict]:
    path = CAMPAIGNS_PATH_TEMPLATE.format(id=account_id)
    url = urljoin(BASE_URL + "/", path.lstrip("/"))

    all_campaigns: list[dict] = []
    page = 1
    limit = DEFAULT_PARAMS.get("limit", 100)

    while True:
        params = {"page": page, "limit": limit}
        data = session_get_json(session, url, headers=headers, params=params)
        campaigns, next_url = extract_list_and_next(data)
        all_campaigns.extend(campaigns)

        print(f"    page={page} campaigns={len(campaigns)} total={len(all_campaigns)}")

        if next_url:
            url = next_url if next_url.startswith("http") else urljoin(BASE_URL + "/", next_url.lstrip("/"))
            page += 1
            continue

        if len(campaigns) < limit:
            break

        page += 1

    return all_campaigns


# ============================================================
# MAIN
# ============================================================

def main():
    session = requests.Session()

    # Step 1: obtain csrftoken (if server uses it)
    csrftoken = get_csrftoken(session)
    headers = build_base_headers(csrftoken)

    print(f"🔐 Testing auth against: {BASE_URL}{ACCOUNTS_PATH}")
    print(f"   Using key prefix: {EXPANDI_KEY[:6]}... len={len(EXPANDI_KEY)}")
    print(f"   Using secret: {'SET' if bool(EXPANDI_SECRET) else 'MISSING'}")
    print(f"   csrftoken: {'SET' if bool(csrftoken) else 'NOT SET'}")
    print(f"   authorization header: {'SET' if 'authorization' in headers else 'NOT SET'}")

    ok, status, body = test_li_accounts(session, headers)
    print(f"➡️ test /li_accounts/ status={status} body={body}")

    if not ok:
        raise SystemExit(
            "❌ Autorizace stále neprošla.\n\n"
            "Nejčastější důvody:\n"
            "1) EXPANDI_SECRET je špatně (nebo chybí)\n"
            "2) Je potřeba ještě jiný header (někdy 'workspace', 'project', apod.)\n"
            "3) Swagger request používá ještě další cookie (např. sessionid), nejen csrftoken\n\n"
            "Tip: v Network zkopíruj celý 'Cookie:' header z requestu /li_accounts/ a pošli mi ho (bez citlivých částí klidně).\n"
        )

    # Step 2: fetch accounts + campaigns
    accounts = fetch_all_accounts(session, headers)
    print(f"\n👤 Found LinkedIn accounts: {len(accounts)}")

    all_rows = []
    for i, acc in enumerate(accounts, 1):
        account_id = pick_account_id(acc)
        if not account_id:
            print(f"\n[{i}/{len(accounts)}] ⚠️ Skipping account (no id/pk/uuid/account_id...)")
            continue

        name, linkedin = pick_account_label(acc)
        print(f"\n[{i}/{len(accounts)}] 📥 Downloading campaign instances for: {name} (id={account_id})")

        campaigns = fetch_campaigns_for_account(session, headers, account_id)
        for c in campaigns:
            all_rows.append({
                "account_id": account_id,
                "account_name": name,
                "account_linkedin": linkedin,
                "campaign": c,
            })

        time.sleep(SLEEP_BETWEEN_ACCOUNTS_SEC)

    # ✅ Always write to outputs/ and overwrite the previous file
    out_dir = Path("outputs")
    out_dir.mkdir(exist_ok=True)

    out_file = out_dir / "all_campaigns.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(all_rows, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Done. Saved: {out_file}")
    print(f"📦 Total rows (campaign instances): {len(all_rows)}")


if __name__ == "__main__":
    main()
