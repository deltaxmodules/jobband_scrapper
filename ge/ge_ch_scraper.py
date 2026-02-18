#!/usr/bin/env python3
"""
Scraper de vagas ge.ch (Etat de Geneve) com incremental automatico.
"""

import argparse
import builtins
import csv
import json
import re
import time
from datetime import date, datetime
from email.utils import parsedate_to_datetime
from html import unescape
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.ge.ch"
LIST_URL = f"{BASE_URL}/offres-emploi-etat-geneve/liste-offres"
RSS_URL = f"{BASE_URL}/rss/offres-emploi-etat-geneve"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-CH,fr;q=0.9,en;q=0.8",
}


def print(*args, **kwargs):  # type: ignore[override]
    kwargs.setdefault("flush", True)
    return builtins.print(*args, **kwargs)


def build_search_url(domaine: int | None = None, page: int = 1) -> str:
    params: dict[str, str] = {}
    if domaine is not None:
        params["field_domaine_activite_target_id"] = str(domaine)
    if page > 1:
        # Drupal usa pagina 0-indexed internamente: page=1 eh a segunda pagina.
        params["page"] = str(page - 1)
    return f"{LIST_URL}?{urlencode(params)}" if params else LIST_URL


def set_page_param(url: str, page: int) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if page > 1:
        query["page"] = str(page - 1)
    else:
        query.pop("page", None)
    return urlunparse(parsed._replace(query=urlencode(query)))


def _is_job_detail_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    return bool(
        parsed.netloc.lower().endswith("ge.ch")
        and re.search(r"/offres-emploi-etat-geneve/liste-offres/\d+$", parsed.path)
    )


def _clean_text(value: str | None, limit: int = 1600) -> str | None:
    if not value:
        return None
    value = unescape(value)
    value = re.sub(r"\s+", " ", value).strip()
    if not value:
        return None
    return value[:limit]


def _parse_li_card(li: BeautifulSoup) -> dict:
    title = ""
    departement = None
    taux = None
    remuneration = None

    link = li.find("a", href=True)
    if link:
        title = _clean_text(link.get_text(" ", strip=True), 240) or ""

    dept_links = li.find_all("a", href=re.compile(r"/(organisation|justice\.ge\.ch)"))
    if dept_links:
        departement = _clean_text(dept_links[0].get_text(" ", strip=True), 160)

    li_text = li.get_text(separator=" ", strip=True)
    taux_match = re.search(
        r"Taux d.?activit[eé]\s*([\d\s%a-zA-Z.,\-]+?)(?:R[ée]mun[ée]ration|Classe|$)",
        li_text,
        flags=re.IGNORECASE,
    )
    if taux_match:
        taux = _clean_text(taux_match.group(1), 120)

    classe_match = re.search(r"classe\s*(\d+)", li_text, flags=re.IGNORECASE)
    if classe_match:
        remuneration = f"classe {classe_match.group(1)}"
    else:
        rem_match = re.search(r"R[ée]mun[ée]ration\s*([^|]+)", li_text, flags=re.IGNORECASE)
        if rem_match:
            remuneration = _clean_text(rem_match.group(1), 120)

    return {
        "title": title or None,
        "departement": departement,
        "taux": taux,
        "remuneration": remuneration,
    }


def extract_detail_links(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    out: list[dict] = []
    seen: set[str] = set()
    pattern = re.compile(r"/offres-emploi-etat-geneve/liste-offres/\d+$")

    for li in soup.select("li"):
        link = li.find("a", href=pattern)
        if not link:
            continue
        href = (link.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(BASE_URL, href).split("#")[0]
        if not _is_job_detail_url(full):
            continue
        if full in seen:
            continue
        seen.add(full)
        base = _parse_li_card(li)
        base["url"] = full
        out.append(base)
    return out


def _parse_fr_date(raw: str | None) -> str | None:
    if not raw:
        return None
    raw = raw.strip()
    for fmt in ("%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def fetch_rss_map(session: requests.Session, timeout: int = 30) -> dict[str, dict]:
    out: dict[str, dict] = {}
    r = session.get(RSS_URL, timeout=timeout)
    r.raise_for_status()
    root = ET.fromstring(r.text)

    for item in root.findall(".//item"):
        link = (item.findtext("link") or "").strip()
        if not link:
            continue
        pub_date = (item.findtext("pubDate") or "").strip()
        desc_html = (item.findtext("description") or "").strip()
        title = (item.findtext("title") or "").strip()

        posting_date = None
        if pub_date:
            try:
                posting_date = parsedate_to_datetime(pub_date).date().isoformat()
            except Exception:
                posting_date = None

        desc_text = _clean_text(BeautifulSoup(desc_html, "html.parser").get_text(" ", strip=True), 1200)
        out[link] = {
            "posting_date": posting_date,
            "summary": desc_text,
            "title": _clean_text(title, 240),
        }
    return out


def fetch_detail(session: requests.Session, url: str, timeout: int = 30) -> dict:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    main_content = soup.find("main") or soup.find("article") or soup
    text = main_content.get_text(separator=" ", strip=True)

    title = None
    h1 = soup.find("h1")
    if h1:
        title = _clean_text(h1.get_text(" ", strip=True), 240)

    description = None
    paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
    long_paras = [p for p in paragraphs if len(p) > 80]
    if long_paras:
        description = _clean_text(long_paras[0], 2200)

    posting_date = None
    date_patterns = [
        r"publi[eé]\s*le[^\d]*(\d{1,2}[./]\d{1,2}[./]\d{2,4})",
        r"mise en ligne[^\d]*(\d{1,2}[./]\d{1,2}[./]\d{2,4})",
    ]
    for pat in date_patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            posting_date = _parse_fr_date(m.group(1))
            if posting_date:
                break

    date_limite = None
    deadline_patterns = [
        r"date limite[^\d]*(\d{1,2}[./]\d{1,2}[./]\d{2,4})",
        r"d[ée]lai de candidature[^\d]*(\d{1,2}[./]\d{1,2}[./]\d{2,4})",
        r"avant le[^\d]*(\d{1,2}[./]\d{1,2}[./]\d{2,4})",
    ]
    for pat in deadline_patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            date_limite = _parse_fr_date(m.group(1))
            if date_limite:
                break

    return {
        "title": title,
        "description": description,
        "posting_date": posting_date,
        "date_limite": date_limite,
        "url": url,
        "source": "ge",
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
    fields = [
        "id",
        "source",
        "title",
        "departement",
        "taux",
        "remuneration",
        "posting_date",
        "date_limite",
        "url",
        "description",
        "summary",
    ]
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in jobs:
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
        row.setdefault("source", "ge")
        if not isinstance(row.get("id"), int):
            max_id += 1
            row["id"] = max_id

    out = list(merged.values())
    out.sort(key=lambda r: int(r.get("id")) if isinstance(r.get("id"), int) else 0, reverse=True)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Scraper de vagas no ge.ch")
    parser.add_argument("--url", type=str, default="", help="URL completa de pesquisa (opcional)")
    parser.add_argument("--domaine", type=int, default=None, help="Filtro de dominio (ex: 19 = IT)")
    parser.add_argument("--max-pages", type=int, default=0, help="Maximo de paginas de pesquisa (0 = dinamico)")
    parser.add_argument("--max-jobs", type=int, default=0, help="Maximo de vagas em detalhe (0 = sem limite)")
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
        help="Ficheiro JSON de saida (default: data/ge/professions.json)",
    )
    parser.add_argument("--save-csv", action="store_true", help="Tambem salva CSV")
    args = parser.parse_args()

    output_json = args.output_json.strip() or str(Path("data") / "ge" / "professions.json")
    output_path = Path(output_json)
    auto_state_file = str(output_path.with_name(f"{output_path.stem}.state.json"))

    existing_jobs = load_json_jobs(output_json)
    known_urls = {str(j.get("url") or "").strip() for j in existing_jobs if str(j.get("url") or "").strip()}
    auto_state = load_state(auto_state_file)
    state_urls = auto_state.get("seen_urls") if isinstance(auto_state, dict) else []
    if isinstance(state_urls, list):
        known_urls.update(str(u).strip() for u in state_urls if str(u).strip())

    if known_urls:
        print(
            f"[INCREMENTAL] Execucao automatica: {len(known_urls)} URLs conhecidas "
            f"(dados + estado: {auto_state_file})"
        )

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    rss_map: dict[str, dict] = {}
    try:
        rss_map = fetch_rss_map(session)
        print(f"[RSS] Entradas carregadas: {len(rss_map)}")
    except Exception as exc:
        print(f"[WARN] RSS indisponivel ({exc}). A continuar sem RSS.")

    page1_url = args.url.strip() or build_search_url(domaine=args.domaine, page=1)
    print(f"[SEARCH] Pagina base: {page1_url}")

    try:
        r = session.get(page1_url, timeout=30)
        r.raise_for_status()
        page1_html = r.text
    except requests.RequestException as exc:
        print(f"[WARN] Falha ao carregar pagina base ({exc}). Seguindo sem novos links nesta execucao.")
        page1_html = ""

    page1_items = extract_detail_links(page1_html) if page1_html else []
    print(f"  Pagina 1: {len(page1_items)} links")

    links: list[dict] = []
    seen_links: set[str] = set()
    known_streak = 0

    for item in page1_items:
        url = str(item.get("url") or "").strip()
        if url and url not in seen_links:
            seen_links.add(url)
            links.append(item)

    max_dyn = args.max_pages if args.max_pages and args.max_pages > 0 else 200
    for page in range(2, max_dyn + 1):
        page_url = set_page_param(page1_url, page)
        try:
            pr = session.get(page_url, timeout=30)
        except requests.RequestException as exc:
            print(f"  [WARN] Falha ao carregar pagina {page} ({exc}). Paragem segura da paginacao.")
            break
        if pr.status_code >= 400:
            break
        items = extract_detail_links(pr.text)
        if not items:
            print(f"  Pagina {page}: 0 links (fim)")
            break

        print(f"  Pagina {page}: {len(items)} links")
        for item in items:
            url = str(item.get("url") or "").strip()
            if not url or url in seen_links:
                continue
            seen_links.add(url)
            links.append(item)

            if args.stop_after_seen > 0:
                if url in known_urls:
                    known_streak += 1
                    if known_streak >= args.stop_after_seen:
                        print(f"  Paragem incremental: {known_streak} links conhecidos em sequencia")
                        break
                else:
                    known_streak = 0

        if args.stop_after_seen > 0 and known_streak >= args.stop_after_seen:
            break

        if args.delay > 0:
            time.sleep(args.delay)

    print(f"[LINKS] Links unicos: {len(links)}")

    detail_items = [x for x in links if str(x.get("url") or "").strip() not in known_urls]
    print(f"[NEW] Novos links para detalhe: {len(detail_items)}")

    if args.max_jobs and args.max_jobs > 0:
        detail_items = detail_items[: args.max_jobs]
        print(f"  Limite aplicado em detalhe: {len(detail_items)}")

    fresh_jobs: list[dict] = []
    max_days = args.days if args.days and args.days > 0 else None

    for idx, item in enumerate(detail_items, start=1):
        url = str(item.get("url") or "").strip()
        if not url:
            continue
        try:
            detail = fetch_detail(session, url)
        except Exception:
            continue

        rss = rss_map.get(url, {})
        row = {
            "title": detail.get("title") or item.get("title") or rss.get("title"),
            "departement": item.get("departement"),
            "taux": item.get("taux"),
            "remuneration": item.get("remuneration"),
            "description": detail.get("description"),
            "summary": rss.get("summary"),
            "posting_date": detail.get("posting_date") or rss.get("posting_date"),
            "date_limite": detail.get("date_limite"),
            "url": url,
            "source": "ge",
        }
        if not within_days(row.get("posting_date"), max_days):
            continue

        fresh_jobs.append(row)
        if idx % 25 == 0:
            print(f"  Detalhes processados: {idx}/{len(detail_items)}")
        if args.delay > 0:
            time.sleep(args.delay)

    merged = merge_jobs_by_url(existing_jobs, fresh_jobs)
    filtered = [row for row in merged if within_days(row.get("posting_date"), max_days)]

    seen_now = set(known_urls) | {str(x.get("url") or "").strip() for x in links} | {
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
