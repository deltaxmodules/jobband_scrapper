#!/usr/bin/env python3
"""
Scraper para catálogo local de vagas do JobUp (search pages + páginas de detalhe).
"""

import argparse
import builtins
import csv
import json
import re
import time
from datetime import date, datetime
from html import unescape
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.jobup.ch/fr/emplois/"
DETAIL_HOST = "www.jobup.ch"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}


def print(*args, **kwargs):  # type: ignore[override]
    kwargs.setdefault("flush", True)
    return builtins.print(*args, **kwargs)


def build_search_url(location: str, term: str = "", page: int = 1) -> str:
    params = {"location": location}
    if term.strip():
        params["term"] = term.strip()
    if page > 1:
        params["page"] = str(page)
    return f"{BASE_URL}?{urlencode(params)}"


def set_page_param(url: str, page: int) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if page > 1:
        query["page"] = str(page)
    else:
        query.pop("page", None)
    return urlunparse(parsed._replace(query=urlencode(query)))


def extract_total_pages(html: str) -> int | None:
    for pattern in (
        r'"totalPages"\s*:\s*(\d+)',
        r'"total_pages"\s*:\s*(\d+)',
    ):
        match = re.search(pattern, html)
        if match:
            return max(1, int(match.group(1)))
    return None


def _is_job_detail_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    return parsed.netloc.lower().endswith("jobup.ch") and "/emplois/detail/" in parsed.path


def extract_detail_links(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = soup.select("a[href]")
    out: list[str] = []
    seen: set[str] = set()

    for link in links:
        href = (link.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(f"https://{DETAIL_HOST}", href).split("#")[0]
        if not _is_job_detail_url(full):
            continue
        if full in seen:
            continue
        seen.add(full)
        out.append(full)
    return out


def _clean_text(value: str | None, limit: int = 800) -> str | None:
    if not value:
        return None
    value = unescape(value)
    value = re.sub(r"\s+", " ", value).strip()
    if not value:
        return None
    return value[:limit]


def _collect_ldjson_nodes(obj: object) -> list[dict]:
    out: list[dict] = []
    if isinstance(obj, dict):
        out.append(obj)
        for key in ("@graph", "graph", "itemListElement"):
            child = obj.get(key)
            if isinstance(child, list):
                for it in child:
                    out.extend(_collect_ldjson_nodes(it))
            elif isinstance(child, dict):
                out.extend(_collect_ldjson_nodes(child))
    elif isinstance(obj, list):
        for it in obj:
            out.extend(_collect_ldjson_nodes(it))
    return out


def _extract_from_ldjson(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    scripts = soup.select('script[type="application/ld+json"]')
    for script in scripts:
        raw = (script.string or script.get_text() or "").strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        for node in _collect_ldjson_nodes(payload):
            if str(node.get("@type") or "").lower() != "jobposting":
                continue
            company = None
            org = node.get("hiringOrganization")
            if isinstance(org, dict):
                company = org.get("name")

            location = None
            loc = node.get("jobLocation")
            if isinstance(loc, dict):
                addr = loc.get("address")
                if isinstance(addr, dict):
                    location = addr.get("addressLocality") or addr.get("addressRegion")

            return {
                "title": _clean_text(node.get("title"), 180),
                "company": _clean_text(company, 140),
                "location": _clean_text(location, 120),
                "description": _clean_text(node.get("description"), 1200),
                "posting_date": _clean_text(node.get("datePosted"), 32),
            }
    return {}


def _extract_meta(html: str, prop: str) -> str | None:
    match = re.search(
        rf'<meta[^>]+property=["\']{re.escape(prop)}["\'][^>]+content=["\']([^"\']+)["\']',
        html,
        flags=re.IGNORECASE,
    )
    return match.group(1).strip() if match else None


def fetch_detail(session: requests.Session, url: str, timeout: int = 30) -> dict:
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    html = response.text

    ld = _extract_from_ldjson(html)
    title = ld.get("title") or _clean_text(_extract_meta(html, "og:title"), 180)
    description = ld.get("description") or _clean_text(_extract_meta(html, "og:description"), 1200)

    posting_date = (ld.get("posting_date") or "").strip()
    if posting_date and "T" in posting_date:
        posting_date = posting_date.split("T", 1)[0]
    posting_date = posting_date or None

    return {
        "title": title,
        "company": ld.get("company"),
        "location": ld.get("location"),
        "description": description,
        "posting_date": posting_date,
        "url": url,
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


def save_json(path: str, jobs: list[dict]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)


def save_csv(path: str, jobs: list[dict]) -> None:
    fields = ["id", "title", "company", "location", "posting_date", "url", "description"]
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for job in jobs:
            writer.writerow({k: job.get(k) for k in fields})


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

    for job in current:
        raw_id = job.get("id")
        if isinstance(raw_id, int) and raw_id > max_id:
            max_id = raw_id
    for job in current:
        url = str(job.get("url") or "").strip()
        if not url:
            continue
        merged[url] = job
    for job in fresh:
        url = str(job.get("url") or "").strip()
        if not url:
            continue
        existing = merged.get(url)
        if existing and isinstance(existing.get("id"), int):
            job["id"] = existing["id"]
        else:
            max_id += 1
            job["id"] = max_id
        merged[url] = job

    # Garante id para registos antigos que possam não ter id.
    for job in merged.values():
        if not isinstance(job.get("id"), int):
            max_id += 1
            job["id"] = max_id
    out = list(merged.values())

    out.sort(key=lambda job: int(job.get("id")) if isinstance(job.get("id"), int) else 0, reverse=True)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Scraper de vagas no jobup.ch")
    parser.add_argument("--url", type=str, default="", help="URL completa de pesquisa (opcional)")
    parser.add_argument("--location", type=str, default="Genève", help="Localização (quando --url não é dado)")
    parser.add_argument("--term", type=str, default="", help="Termo de pesquisa (opcional)")
    parser.add_argument("--max-pages", type=int, default=0, help="Máximo de páginas de pesquisa (0 = automático)")
    parser.add_argument("--max-jobs", type=int, default=0, help="Máximo de vagas em detalhe (0 = sem limite)")
    parser.add_argument("--days", type=int, default=30, help="Filtra vagas dos últimos N dias (0 = sem filtro)")
    parser.add_argument("--delay", type=float, default=0.3, help="Espera entre pedidos em segundos")
    parser.add_argument("--incremental", action="store_true", help="(Legacy) Mantido por compatibilidade")
    parser.add_argument("--state-file", type=str, default="state.json", help="Ficheiro de estado incremental")
    parser.add_argument(
        "--master-file",
        type=str,
        default="jobup_jobs_master.json",
        help="Base mestre de vagas no modo incremental",
    )
    parser.add_argument(
        "--stop-after-seen",
        type=int,
        default=200,
        help="No incremental, para após N links já conhecidos em sequência (0 desativa)",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default="",
        help="Ficheiro JSON de saída (default: data/jobup/professions.json)",
    )
    parser.add_argument("--save-csv", action="store_true", help="Também salva CSV")
    args = parser.parse_args()

    output_json = args.output_json.strip() or str(Path("data") / "jobup" / "professions.json")
    output_path = Path(output_json)
    auto_state_file = str(output_path.with_name(f"{output_path.stem}.state.json"))
    existing_jobs = load_json_jobs(output_json)
    known_urls = {str(j.get("url") or "").strip() for j in existing_jobs if str(j.get("url") or "").strip()}
    auto_state = load_state(auto_state_file)
    auto_state_urls = auto_state.get("seen_urls") if isinstance(auto_state, dict) else []
    if isinstance(auto_state_urls, list):
        known_urls.update(str(u).strip() for u in auto_state_urls if str(u).strip())
    if known_urls:
        print(
            f"[INCREMENTAL] Execução automática: {len(known_urls)} URLs conhecidas "
            f"(dados + estado: {auto_state_file})"
        )

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    page1_url = args.url.strip() or build_search_url(location=args.location, term=args.term, page=1)
    print(f"[SEARCH] Página base: {page1_url}")
    try:
        resp = session.get(page1_url, timeout=30)
        if resp.status_code in (403, 429):
            print(f"[WARN] Bloqueio HTTP {resp.status_code} na página base. Seguindo sem novos links nesta execução.")
            html = ""
        elif resp.status_code >= 400:
            print(f"[WARN] HTTP {resp.status_code} na página base. Seguindo sem novos links nesta execução.")
            html = ""
        else:
            html = resp.text
    except requests.RequestException as exc:
        print(f"[WARN] Falha ao carregar página base ({exc}). Seguindo sem novos links nesta execução.")
        html = ""

    total_pages = extract_total_pages(html)
    if total_pages is not None:
        if args.max_pages and args.max_pages > 0:
            total_pages = min(total_pages, args.max_pages)
        print(f"[PAGES] A processar (detetado): {total_pages}")
    else:
        if args.max_pages and args.max_pages > 0:
            print(f"[PAGES] A processar (manual): até {args.max_pages}")
        else:
            print("[PAGES] A processar: modo dinâmico")

    links: list[str] = []
    seen_links: set[str] = set()
    known_urls_state: set[str] = set()
    master_jobs: list[dict] = []
    known_streak = 0

    if args.incremental:
        master_jobs = load_json_jobs(args.master_file)
        known_urls_state = {str(j.get("url") or "").strip() for j in master_jobs if str(j.get("url") or "").strip()}
        state = load_state(args.state_file)
        state_urls = state.get("seen_urls") or []
        if isinstance(state_urls, list):
            known_urls_state.update(str(x).strip() for x in state_urls if str(x).strip())
        print(f"  Incremental legacy: {len(known_urls_state)} links já conhecidos")

    combined_known = set(known_urls) | set(known_urls_state)

    def fetch_search_page(url: str) -> tuple[str | None, bool]:
        try:
            resp = session.get(url, timeout=30)
        except requests.RequestException as exc:
            print(f"  [WARN] Falha ao carregar página de pesquisa: {url} ({exc})")
            return None, True

        if resp.status_code in (403, 429):
            print(f"  [WARN] Bloqueio HTTP {resp.status_code} na pesquisa: {url}. Paragem segura da paginação.")
            return None, True
        if resp.status_code >= 400:
            print(f"  [WARN] HTTP {resp.status_code} na pesquisa: {url}. Paragem segura da paginação.")
            return None, True

        return resp.text, False

    page1_links = extract_detail_links(html) if html else []
    print(f"  Página 1: {len(page1_links)} links")
    for link in page1_links:
        if link not in seen_links:
            seen_links.add(link)
            links.append(link)

    if total_pages is not None:
        page_iter = range(2, total_pages + 1)
        for page in page_iter:
            url = set_page_param(page1_url, page)
            page_html, should_stop = fetch_search_page(url)
            if should_stop:
                break
            if not page_html:
                break
            page_links = extract_detail_links(page_html)
            print(f"  Página {page}: {len(page_links)} links")
            for link in page_links:
                if link in seen_links:
                    continue
                seen_links.add(link)
                links.append(link)
                if args.stop_after_seen > 0:
                    if link in combined_known:
                        known_streak += 1
                        if known_streak >= args.stop_after_seen:
                            print(f"  Paragem incremental: {known_streak} links conhecidos em sequência")
                            page_links = []
                            break
                    else:
                        known_streak = 0
            if args.stop_after_seen > 0 and known_streak >= args.stop_after_seen:
                break
            if args.delay > 0:
                time.sleep(args.delay)
    else:
        max_dynamic_pages = args.max_pages if args.max_pages and args.max_pages > 0 else 200
        for page in range(2, max_dynamic_pages + 1):
            url = set_page_param(page1_url, page)
            page_html, should_stop = fetch_search_page(url)
            if should_stop:
                break
            if not page_html:
                break
            page_links = extract_detail_links(page_html)
            if not page_links:
                print(f"  Página {page}: 0 links (fim)")
                break
            print(f"  Página {page}: {len(page_links)} links")
            for link in page_links:
                if link in seen_links:
                    continue
                seen_links.add(link)
                links.append(link)
                if args.stop_after_seen > 0:
                    if link in combined_known:
                        known_streak += 1
                        if known_streak >= args.stop_after_seen:
                            print(f"  Paragem incremental: {known_streak} links conhecidos em sequência")
                            page_links = []
                            break
                    else:
                        known_streak = 0
            if args.stop_after_seen > 0 and known_streak >= args.stop_after_seen:
                break
            if args.delay > 0:
                time.sleep(args.delay)

    unique_links = links
    print(f"[LINKS] Links únicos: {len(unique_links)}")

    # Incremental automático: só processa links novos face ao ficheiro de saída atual.
    detail_links = [u for u in unique_links if u not in known_urls]
    print(f"[NEW] Novos links para detalhe: {len(detail_links)}")
    if args.max_jobs and args.max_jobs > 0:
        detail_links = detail_links[: args.max_jobs]
        print(f"  Limite aplicado em detalhe: {len(detail_links)}")

    fetched_jobs: list[dict] = []
    max_days = args.days if args.days and args.days > 0 else None

    for idx, link in enumerate(detail_links, start=1):
        try:
            job = fetch_detail(session, link)
        except Exception:
            continue
        fetched_jobs.append(job)
        if idx % 25 == 0:
            print(f"  Detalhes processados: {idx}/{len(detail_links)}")
        if args.delay > 0:
            time.sleep(args.delay)

    if args.incremental:
        merged_master = merge_jobs_by_url(master_jobs, fetched_jobs)
        save_json(args.master_file, merged_master)
        save_state(
            args.state_file,
            {
                "last_run_at": datetime.now().isoformat(timespec="seconds"),
                "seen_urls": [str(j.get("url") or "").strip() for j in merged_master if str(j.get("url") or "").strip()],
                "master_file": args.master_file,
            },
        )
        base_jobs = merge_jobs_by_url(existing_jobs, merged_master)
        print(f"[SAVE] Master salvo: {args.master_file} ({len(merged_master)} vagas)")
    else:
        base_jobs = merge_jobs_by_url(existing_jobs, fetched_jobs)

    # Guarda estado de URLs vistas para acelerar futuras execuções
    # mesmo quando output_json está filtrado por dias.
    seen_now = set(known_urls) | set(unique_links) | {
        str(j.get("url") or "").strip() for j in base_jobs if str(j.get("url") or "").strip()
    }
    save_state(
        auto_state_file,
        {
            "last_run_at": datetime.now().isoformat(timespec="seconds"),
            "seen_urls": sorted(seen_now),
            "output_json": output_json,
        },
    )
    print(f"[SAVE] Estado incremental salvo: {auto_state_file} ({len(seen_now)} URLs)")

    filtered_jobs = [job for job in base_jobs if within_days(job.get("posting_date"), max_days)]
    save_json(output_json, filtered_jobs)
    print(f"[SAVE] JSON salvo: {output_json} ({len(filtered_jobs)} vagas)")

    if args.save_csv:
        csv_name = re.sub(r"\.json$", ".csv", output_json, flags=re.IGNORECASE)
        if csv_name == output_json:
            csv_name = f"{output_json}.csv"
        save_csv(csv_name, filtered_jobs)
        print(f"[SAVE] CSV salvo: {csv_name}")


if __name__ == "__main__":
    main()
