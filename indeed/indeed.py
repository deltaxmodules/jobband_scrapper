#!/usr/bin/env python3
"""
Indeed via RapidAPI (jobs-search-api) com output JSON e incremental simples.
"""

import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

HOST = "jobs-search-api.p.rapidapi.com"
URL = f"https://{HOST}/getjobs"


def load_env_key() -> str:
    load_dotenv()
    key = (
        os.getenv("RAPIDAPI_KEY")
        or os.getenv("X_RAPIDAPI_KEY")
        or os.getenv("x_rapidapi_key")
        or ""
    ).strip()
    if not key:
        raise RuntimeError("Defina RAPIDAPI_KEY no .env antes de executar.")
    return key


def load_json(path: str) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []
    return data if isinstance(data, list) else []


def save_json(path: str, rows: list[dict]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def load_state(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def save_state(path: str, payload: dict) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def response_jobs(payload: object) -> list[dict]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for k in ("jobs", "data", "result", "results"):
            value = payload.get(k)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
    return []


def normalize(row: dict) -> dict:
    return {
        "title": row.get("job_title") or row.get("title") or "N/A",
        "company": row.get("employer_name") or row.get("company") or row.get("company_name") or "N/A",
        "location": row.get("job_city") or row.get("location") or "N/A",
        "posting_date": row.get("job_posted_at_datetime_utc") or row.get("date_posted") or None,
        "salary": row.get("job_salary") or row.get("salary") or "",
        "description": row.get("job_description") or row.get("description") or None,
        "url": row.get("job_apply_link") or row.get("job_url") or row.get("url") or "",
        "source": row.get("job_publisher") or row.get("site") or "indeed_rapidapi",
    }


def key_for(row: dict) -> str:
    url = str(row.get("url") or "").strip()
    if url:
        return url
    return "|".join(
        [
            str(row.get("title") or "").strip().lower(),
            str(row.get("company") or "").strip().lower(),
            str(row.get("location") or "").strip().lower(),
        ]
    )


def merge_by_key(current: list[dict], fresh: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}
    max_id = 0
    for row in current:
        rid = row.get("id")
        if isinstance(rid, int) and rid > max_id:
            max_id = rid
        k = key_for(row)
        if k:
            merged[k] = row
    for row in fresh:
        k = key_for(row)
        if not k:
            continue
        old = merged.get(k)
        if old and isinstance(old.get("id"), int):
            row["id"] = old["id"]
        else:
            max_id += 1
            row["id"] = max_id
        merged[k] = row
    rows = list(merged.values())
    rows.sort(key=lambda x: int(x.get("id")) if isinstance(x.get("id"), int) else 0, reverse=True)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="RapidAPI jobs-search-api scraper (Genève)")
    parser.add_argument("--search-term", type=str, default="jobs")
    parser.add_argument("--location", type=str, default="Geneva")
    parser.add_argument("--country-indeed", type=str, default="Switzerland")
    parser.add_argument("--days", type=int, default=30, help="Filtro de idade em dias (hours_old = days*24)")
    parser.add_argument("--results-wanted", type=int, default=200)
    parser.add_argument("--distance", type=int, default=30)
    parser.add_argument("--job-type", type=str, default="")
    parser.add_argument("--is-remote", action="store_true")
    parser.add_argument("--site-name", type=str, default="indeed", help="Lista separada por virgulas")
    parser.add_argument("--timeout", type=int, default=45)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument(
        "--output-json",
        type=str,
        default="data/indeed/professions.json",
    )
    args = parser.parse_args()

    api_key = load_env_key()
    output = args.output_json
    state_file = str(Path(output).with_name(f"{Path(output).stem}.state.json"))

    existing = load_json(output)
    state = load_state(state_file)
    known = set(state.get("seen_keys", [])) if isinstance(state.get("seen_keys", []), list) else set()
    known.update(key_for(r) for r in existing if key_for(r))

    site_names = [x.strip() for x in args.site_name.split(",") if x.strip()]
    payload = {
        "search_term": args.search_term,
        "location": args.location,
        "country_indeed": args.country_indeed,
        "results_wanted": args.results_wanted,
        "site_name": site_names or ["indeed"],
        "distance": args.distance,
        "job_type": args.job_type,
        "is_remote": bool(args.is_remote),
        "linkedin_fetch_description": False,
        "hours_old": max(args.days, 0) * 24,
    }
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": HOST,
        "Content-Type": "application/json",
    }

    response = None
    for attempt in range(1, max(1, args.retries) + 1):
        try:
            response = requests.post(URL, json=payload, headers=headers, timeout=args.timeout)
            break
        except requests.RequestException as exc:
            if attempt >= max(1, args.retries):
                raise RuntimeError(f"Falha de rede apos {attempt} tentativas: {exc}") from exc
            wait_s = attempt * 2
            print(f"[WARN] tentativa {attempt}/{args.retries} falhou: {exc}. retry em {wait_s}s")
            time.sleep(wait_s)

    if response is None:
        raise RuntimeError("Falha inesperada sem resposta da API.")
    if response.status_code != 200:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text[:500]}")

    raw = response.json()
    jobs = [normalize(r) for r in response_jobs(raw)]
    print(f"[FETCH] API retornou {len(jobs)} vagas")

    fresh = []
    for row in jobs:
        k = key_for(row)
        if not k or k in known:
            continue
        fresh.append(row)
        known.add(k)
    print(f"[NEW] Novas vagas: {len(fresh)}")

    merged = merge_by_key(existing, fresh)
    save_json(output, merged)
    save_state(
        state_file,
        {
            "last_run_at": datetime.now().isoformat(timespec="seconds"),
            "seen_keys": sorted(known),
            "output_json": output,
            "search_term": args.search_term,
            "location": args.location,
            "days": args.days,
            "site_name": site_names or ["indeed"],
        },
    )
    print(f"[SAVE] JSON: {output} ({len(merged)} vagas)")
    print(f"[SAVE] STATE: {state_file} ({len(known)} keys)")


if __name__ == "__main__":
    main()
