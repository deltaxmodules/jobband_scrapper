#!/usr/bin/env python3
"""
Scraper de vagas Adzuna com incremental automatico.
"""

import argparse
import builtins
import csv
import json
import os
import re
import time
from datetime import date, datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

BASE_URL = "https://api.adzuna.com/v1/api/jobs"
MAX_API_PAGES = 50
DEFAULT_COUNTRY = "ch"
DEFAULT_LOCATION = "Geneva"
DEFAULT_TERM = "jobs"


def print(*args, **kwargs):  # type: ignore[override]
    kwargs.setdefault("flush", True)
    return builtins.print(*args, **kwargs)


def get_creds() -> tuple[str, str]:
    app_id = os.getenv("ADZUNA_APP_ID") or os.getenv("APP_ID") or os.getenv("app_id")
    app_key = os.getenv("ADZUNA_APP_KEY") or os.getenv("APP_KEY") or os.getenv("app_key")
    if not app_id or not app_key:
        raise RuntimeError("Defina ADZUNA_APP_ID/ADZUNA_APP_KEY (ou APP_ID/APP_KEY) no .env")
    return app_id, app_key


def fetch_page(
    app_id: str,
    app_key: str,
    country: str,
    page: int,
    what: str,
    where: str,
    results_per_page: int,
    max_days_old: int | None,
    retries: int = 4,
) -> dict:
    params: dict[str, str | int] = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": results_per_page,
        "what": what,
        "where": where,
    }
    if max_days_old is not None:
        params["max_days_old"] = max_days_old

    url = f"{BASE_URL}/{country}/search/{page}"
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}: {r.text[:200]}", response=r)
            r.raise_for_status()
            payload = r.json()
            return payload if isinstance(payload, dict) else {"results": [], "count": 0}
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError):
            if attempt == retries:
                raise
            wait_s = 1.5 * attempt
            print(f"[WARN] falha API page={page} attempt={attempt}/{retries} -> retry em {wait_s:.1f}s")
            time.sleep(wait_s)
    return {"results": [], "count": 0}


def normalize_job(job: dict) -> dict:
    title = str(job.get("title") or "").strip() or "N/A"

    company_obj = job.get("company") if isinstance(job.get("company"), dict) else {}
    company = str(company_obj.get("display_name") or "").strip() or "N/A"

    loc_obj = job.get("location") if isinstance(job.get("location"), dict) else {}
    location = str(loc_obj.get("display_name") or "").strip() or "N/A"

    posting_date = str(job.get("created") or "").strip()[:10] or None

    url = str(job.get("redirect_url") or "").strip()
    if not url:
        url = str(job.get("adref") or "").strip()

    description = str(job.get("description") or "").strip() or None
    if description:
        description = description[:2500]

    uid = str(job.get("id") or "").strip()

    return {
        "title": title,
        "company": company,
        "location": location,
        "posting_date": posting_date,
        "url": url,
        "description": description,
        "salary_min": job.get("salary_min"),
        "salary_max": job.get("salary_max"),
        "contract_time": job.get("contract_time"),
        "contract_type": job.get("contract_type"),
        "external_id": uid,
        "source": "adzuna",
    }


def within_days(posting_date: str | None, max_days: int | None) -> bool:
    if max_days is None:
        return True
    if not posting_date:
        return False
    try:
        day = datetime.fromisoformat(posting_date).date()
    except ValueError:
        return False
    age = (date.today() - day).days
    return 0 <= age <= max_days


def save_json(path: str, rows: list[dict]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def save_csv(path: str, rows: list[dict]) -> None:
    fields = [
        "id",
        "source",
        "title",
        "company",
        "location",
        "posting_date",
        "salary_min",
        "salary_max",
        "contract_time",
        "contract_type",
        "url",
        "description",
    ]
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in fields})


def load_json_jobs(path: str) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    return [x for x in data if isinstance(x, dict)]


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
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def merge_jobs_by_url(current: list[dict], fresh: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}
    max_id = 0

    for row in current:
        raw_id = row.get("id")
        if isinstance(raw_id, int) and raw_id > max_id:
            max_id = raw_id

    for row in current:
        url = str(row.get("url") or "").strip()
        if not url:
            continue
        merged[url] = row

    for row in fresh:
        url = str(row.get("url") or "").strip()
        if not url:
            continue
        existing = merged.get(url)
        if existing and isinstance(existing.get("id"), int):
            row["id"] = existing["id"]
        else:
            max_id += 1
            row["id"] = max_id
        merged[url] = row

    for row in merged.values():
        row.setdefault("source", "adzuna")
        if not isinstance(row.get("id"), int):
            max_id += 1
            row["id"] = max_id

    out = list(merged.values())
    out.sort(key=lambda r: int(r.get("id")) if isinstance(r.get("id"), int) else 0, reverse=True)
    return out


def main() -> None:
    load_dotenv(Path(".env"))

    parser = argparse.ArgumentParser(description="Scraper de vagas no Adzuna")
    parser.add_argument("--country", type=str, default=DEFAULT_COUNTRY, help="Pais Adzuna (default: ch)")
    parser.add_argument("--location", type=str, default=DEFAULT_LOCATION, help="Localizacao (default: Geneva)")
    parser.add_argument("--term", type=str, default=DEFAULT_TERM, help="Termo de pesquisa (default: jobs)")
    parser.add_argument("--max-pages", type=int, default=0, help="Maximo de paginas (0 = automatico)")
    parser.add_argument("--max-jobs", type=int, default=0, help="Maximo de vagas novas (0 = sem limite)")
    parser.add_argument("--days", type=int, default=30, help="Filtra vagas dos ultimos N dias (0 = sem filtro)")
    parser.add_argument("--delay", type=float, default=0.2, help="Espera entre pedidos em segundos")
    parser.add_argument(
        "--stop-after-seen",
        type=int,
        default=120,
        help="Para apos N links ja conhecidos em sequencia (0 desativa)",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default="",
        help="Ficheiro JSON de saida (default: data/adzuna/professions.json)",
    )
    parser.add_argument("--save-csv", action="store_true", help="Tambem salva CSV")
    args = parser.parse_args()

    output_arg = args.output_json.strip()
    if not output_arg:
        output_path = Path("data") / "adzuna" / "professions.json"
    else:
        candidate = Path(output_arg)
        if candidate.exists() and candidate.is_dir():
            output_path = candidate / "professions.json"
        elif output_arg.endswith("/") or candidate.suffix == "":
            output_path = candidate / "professions.json"
        else:
            output_path = candidate
    output_json = str(output_path)
    auto_state_file = str(output_path.with_name(f"{output_path.stem}.state.json"))

    app_id, app_key = get_creds()

    existing_jobs = load_json_jobs(output_json)
    known_urls = {str(j.get("url") or "").strip() for j in existing_jobs if str(j.get("url") or "").strip()}
    auto_state = load_state(auto_state_file)
    state_urls = auto_state.get("seen_urls") if isinstance(auto_state, dict) else []
    if isinstance(state_urls, list):
        known_urls.update(str(x).strip() for x in state_urls if str(x).strip())

    if known_urls:
        print(
            f"[INCREMENTAL] Execucao automatica: {len(known_urls)} URLs conhecidas "
            f"(dados + estado: {auto_state_file})"
        )

    print(
        f"[SEARCH] Fonte: adzuna | country={args.country} | location={args.location} | term='{args.term}' | days={args.days}"
    )
    if args.max_pages and args.max_pages > 0:
        print(f"[PAGES] A processar (manual): ate {args.max_pages}")
    else:
        print("[PAGES] A processar: modo dinamico")

    page = 1
    max_pages = args.max_pages if args.max_pages and args.max_pages > 0 else MAX_API_PAGES
    max_days = args.days if args.days and args.days > 0 else None

    fresh_jobs: list[dict] = []
    all_page_urls: list[str] = []
    known_streak = 0
    total_est: int | None = None

    while page <= max_pages:
        payload = fetch_page(
            app_id=app_id,
            app_key=app_key,
            country=args.country,
            page=page,
            what=args.term,
            where=args.location,
            results_per_page=50,
            max_days_old=args.days if args.days > 0 else None,
        )

        results = payload.get("results") if isinstance(payload, dict) else []
        if not isinstance(results, list) or not results:
            print(f"  Pagina {page}: 0 registos (fim)")
            break

        if total_est is None:
            count = payload.get("count") if isinstance(payload, dict) else None
            if isinstance(count, int):
                total_est = count

        print(f"  Pagina {page}: {len(results)} registos")
        for raw in results:
            if not isinstance(raw, dict):
                continue
            row = normalize_job(raw)
            url = str(row.get("url") or "").strip()
            if not url:
                continue
            all_page_urls.append(url)

            if args.stop_after_seen > 0:
                if url in known_urls:
                    known_streak += 1
                    if known_streak >= args.stop_after_seen:
                        print(f"  Paragem incremental: {known_streak} links conhecidos em sequencia")
                        break
                else:
                    known_streak = 0

            if url in known_urls:
                continue
            if not within_days(row.get("posting_date"), max_days):
                continue

            fresh_jobs.append(row)
            if args.max_jobs and args.max_jobs > 0 and len(fresh_jobs) >= args.max_jobs:
                break

        if args.stop_after_seen > 0 and known_streak >= args.stop_after_seen:
            break
        if args.max_jobs and args.max_jobs > 0 and len(fresh_jobs) >= args.max_jobs:
            print(f"  Limite aplicado em detalhe: {len(fresh_jobs)}")
            break
        if total_est is not None and len(set(all_page_urls)) >= total_est:
            break

        page += 1
        if args.delay > 0:
            time.sleep(args.delay)

    print(f"[LINKS] Links unicos: {len(set(all_page_urls))}")
    print(f"[NEW] Novos links para detalhe: {len(fresh_jobs)}")

    merged = merge_jobs_by_url(existing_jobs, fresh_jobs)
    filtered = [row for row in merged if within_days(row.get("posting_date"), max_days)]

    seen_now = set(known_urls) | set(all_page_urls) | {
        str(j.get("url") or "").strip() for j in merged if str(j.get("url") or "").strip()
    }
    save_state(
        auto_state_file,
        {
            "last_run_at": datetime.now().isoformat(timespec="seconds"),
            "seen_urls": sorted(seen_now),
            "output_json": output_json,
            "country": args.country,
            "location": args.location,
            "term": args.term,
        },
    )
    print(f"[SAVE] Estado incremental salvo: {auto_state_file} ({len(seen_now)} URLs)")

    save_json(output_json, filtered)
    print(f"[SAVE] JSON salvo: {output_json} ({len(filtered)} vagas)")

    if args.save_csv:
        csv_name = re.sub(r"\.json$", ".csv", output_json, flags=re.IGNORECASE)
        if csv_name == output_json:
            csv_name = f"{output_json}.csv"
        save_csv(csv_name, filtered)
        print(f"[SAVE] CSV salvo: {csv_name}")


if __name__ == "__main__":
    main()
