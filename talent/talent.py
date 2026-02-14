#!/usr/bin/env python3
"""
Scraper de vagas Talent.com (Genève) com incremental automático.
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

BASE_URL = "https://ch.talent.com/fr/jobs"
DETAIL_HOST = "ch.talent.com"
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
    params = {"k": term.strip(), "l": location}
    if page > 1:
        params["p"] = str(page)
    return f"{BASE_URL}?{urlencode(params)}"


def set_page_param(url: str, page: int) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if page > 1:
        query["p"] = str(page)
    else:
        query.pop("p", None)
    return urlunparse(parsed._replace(query=urlencode(query)))


def _extract_total_pages(html: str) -> int | None:
    # fallback por links de paginação
    max_page = 1
    for m in re.findall(r"[?&]p=(\d+)", html):
        try:
            max_page = max(max_page, int(m))
        except Exception:
            continue
    return max_page if max_page > 1 else None


def _is_job_detail_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    return (
        parsed.netloc.lower().endswith("talent.com")
        and parsed.path.endswith("/view")
        and "id=" in parsed.query
    )


FR_HINTS = {
    "avec", "pour", "poste", "vous", "nous", "experience", "expérience", "equipe", "équipe",
    "mission", "competences", "compétences", "formation", "travail", "emploi", "profil",
    "francais", "français", "responsable", "gestion", "assurer", "recherche", "candidat",
}
EN_HINTS = {"with", "for", "you", "team", "experience", "job", "position", "skills", "work", "english", "required"}
DE_HINTS = {"mit", "für", "sie", "erfahrung", "stelle", "aufgaben", "kenntnisse", "arbeit", "deutsch"}
IT_HINTS = {"con", "per", "lavoro", "posizione", "esperienza", "richiesto", "competenze", "squadra"}


def _is_allowed_language(title: str | None, description: str | None) -> bool:
    text = f"{title or ''} {description or ''}".lower()
    tokens = set(re.findall(r"[a-zàâçéèêëîïôûùüÿñæœ]{3,}", text))
    fr = len(tokens & FR_HINTS)
    en = len(tokens & EN_HINTS)
    de = len(tokens & DE_HINTS)
    it = len(tokens & IT_HINTS)

    # Genève pode ter vagas em francês e inglês; exclui quando alemão/italiano dominam.
    return (fr >= 2 or en >= 2) and max(fr, en) >= max(de, it)


def extract_detail_links(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    out: list[str] = []
    seen: set[str] = set()

    for link in soup.select("a[href]"):
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


def _clean_text(value: str | None, limit: int = 1600) -> str | None:
    if not value:
        return None
    value = unescape(value)
    value = re.sub(r"\s+", " ", value).strip()
    if not value:
        return None
    return value[:limit]


def _collect_nodes(obj: object) -> list[dict]:
    out: list[dict] = []
    if isinstance(obj, dict):
        out.append(obj)
        for v in obj.values():
            out.extend(_collect_nodes(v))
    elif isinstance(obj, list):
        for it in obj:
            out.extend(_collect_nodes(it))
    return out


def _extract_jobposting_ldjson(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.select('script[type="application/ld+json"]'):
        raw = (script.string or script.get_text() or "").strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue

        for node in _collect_nodes(payload):
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

            posting_date = str(node.get("datePosted") or "").strip() or None
            if posting_date and "T" in posting_date:
                posting_date = posting_date.split("T", 1)[0]

            return {
                "title": _clean_text(node.get("title"), 180),
                "company": _clean_text(company, 160),
                "location": _clean_text(location, 120),
                "description": _clean_text(node.get("description"), 3500),
                "posting_date": posting_date,
            }
    return {}


def fetch_detail(session: requests.Session, url: str, timeout: int = 30) -> dict:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    html = r.text

    ld = _extract_jobposting_ldjson(html)

    return {
        "title": ld.get("title"),
        "company": ld.get("company"),
        "location": ld.get("location"),
        "description": ld.get("description"),
        "posting_date": ld.get("posting_date"),
        "url": url,
        "source": "talent",
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


def save_csv(path: str, jobs: list[dict]) -> None:
    fields = ["id", "source", "title", "company", "location", "posting_date", "url", "description"]
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


def merge_jobs_by_url(current: list[dict], fresh: list[dict], filter_lang: bool = True) -> list[dict]:
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
        if filter_lang and not _is_allowed_language(row.get("title"), row.get("description")):
            continue
        merged[url] = row

    for row in fresh:
        url = str(row.get("url") or "").strip()
        if not url:
            continue
        if filter_lang and not _is_allowed_language(row.get("title"), row.get("description")):
            continue
        existing = merged.get(url)
        if existing and isinstance(existing.get("id"), int):
            row["id"] = existing["id"]
        else:
            max_id += 1
            row["id"] = max_id
        merged[url] = row

    for row in merged.values():
        row.setdefault("source", "talent")
        if not isinstance(row.get("id"), int):
            max_id += 1
            row["id"] = max_id

    out = list(merged.values())
    out.sort(key=lambda r: int(r.get("id")) if isinstance(r.get("id"), int) else 0, reverse=True)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Scraper de vagas no Talent.com")
    parser.add_argument("--url", type=str, default="", help="URL completa de pesquisa (opcional)")
    parser.add_argument("--location", type=str, default="Genève", help="Localização (quando --url não é dado)")
    parser.add_argument("--term", type=str, default="", help="Termo de pesquisa (opcional)")
    parser.add_argument("--max-pages", type=int, default=0, help="Máximo de páginas de pesquisa (0 = automático)")
    parser.add_argument("--max-jobs", type=int, default=0, help="Máximo de vagas em detalhe (0 = sem limite)")
    parser.add_argument("--days", type=int, default=30, help="Filtra vagas dos últimos N dias (0 = sem filtro)")
    parser.add_argument("--delay", type=float, default=0.2, help="Espera entre pedidos em segundos")
    parser.add_argument(
        "--stop-after-seen",
        type=int,
        default=120,
        help="Para após N links já conhecidos em sequência (0 desativa)",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default="",
        help="Ficheiro JSON de saída (default: data/talent/professions.json)",
    )
    parser.add_argument(
        "--allow-non-french",
        action="store_true",
        help="Não filtra idioma (por omissão, mantém apenas vagas em francês/inglês).",
    )
    parser.add_argument("--save-csv", action="store_true", help="Também salva CSV")
    args = parser.parse_args()

    output_json = args.output_json.strip() or str(Path("data") / "talent" / "professions.json")
    output_path = Path(output_json)
    auto_state_file = str(output_path.with_name(f"{output_path.stem}.state.json"))

    existing_jobs = load_json_jobs(output_json)
    known_urls = {str(j.get("url") or "").strip() for j in existing_jobs if str(j.get("url") or "").strip()}
    auto_state = load_state(auto_state_file)
    state_urls = auto_state.get("seen_urls") if isinstance(auto_state, dict) else []
    if isinstance(state_urls, list):
        known_urls.update(str(x).strip() for x in state_urls if str(x).strip())

    if known_urls:
        print(
            f"[INCREMENTAL] Execução automática: {len(known_urls)} URLs conhecidas "
            f"(dados + estado: {auto_state_file})"
        )

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    page1_url = args.url.strip() or build_search_url(location=args.location, term=args.term, page=1)
    print(f"[SEARCH] Página base: {page1_url}")

    r = session.get(page1_url, timeout=30)
    r.raise_for_status()
    html = r.text

    # Talent frequentemente só expõe "p=2" na primeira página.
    # Para não perder resultados, usamos varrimento dinâmico por defeito.
    total_pages = args.max_pages if args.max_pages and args.max_pages > 0 else None
    if total_pages:
        print(f"[PAGES] A processar (manual): até {total_pages}")
    else:
        print("[PAGES] A processar: modo dinâmico")

    links: list[str] = []
    seen_links: set[str] = set()
    known_streak = 0

    page1_links = extract_detail_links(html)
    print(f"  Página 1: {len(page1_links)} links")
    for link in page1_links:
        if link not in seen_links:
            seen_links.add(link)
            links.append(link)

    max_dyn = total_pages if total_pages else 200
    page_iter = range(2, max_dyn + 1)

    for page in page_iter:
        page_url = set_page_param(page1_url, page)
        pr = session.get(page_url, timeout=30)
        pr.raise_for_status()
        page_links = extract_detail_links(pr.text)

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
                if link in known_urls:
                    known_streak += 1
                    if known_streak >= args.stop_after_seen:
                        print(f"  Paragem incremental: {known_streak} links conhecidos em sequência")
                        break
                else:
                    known_streak = 0

        if args.stop_after_seen > 0 and known_streak >= args.stop_after_seen:
            break

        if args.delay > 0:
            time.sleep(args.delay)

    print(f"[LINKS] Links únicos: {len(links)}")

    detail_links = [u for u in links if u not in known_urls]
    print(f"[NEW] Novos links para detalhe: {len(detail_links)}")

    if args.max_jobs and args.max_jobs > 0:
        detail_links = detail_links[: args.max_jobs]
        print(f"  Limite aplicado em detalhe: {len(detail_links)}")

    fresh_jobs: list[dict] = []
    max_days = args.days if args.days and args.days > 0 else None

    for idx, link in enumerate(detail_links, start=1):
        try:
            job = fetch_detail(session, link)
        except Exception:
            continue

        if not within_days(job.get("posting_date"), max_days):
            continue

        fresh_jobs.append(job)
        if idx % 25 == 0:
            print(f"  Detalhes processados: {idx}/{len(detail_links)}")
        if args.delay > 0:
            time.sleep(args.delay)

    filter_lang = not args.allow_non_french
    merged = merge_jobs_by_url(existing_jobs, fresh_jobs, filter_lang=filter_lang)

    seen_now = set(known_urls) | set(links) | {
        str(j.get("url") or "").strip() for j in merged if str(j.get("url") or "").strip()
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

    save_json(output_json, merged)
    print(f"[SAVE] JSON salvo: {output_json} ({len(merged)} vagas)")

    if args.save_csv:
        csv_name = re.sub(r"\.json$", ".csv", output_json, flags=re.IGNORECASE)
        if csv_name == output_json:
            csv_name = f"{output_json}.csv"
        save_csv(csv_name, merged)
        print(f"[SAVE] CSV salvo: {csv_name}")


if __name__ == "__main__":
    main()
