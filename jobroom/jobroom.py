#!/usr/bin/env python3
"""
Scraper de vagas no job-room.ch (SECO/RAV) com incremental automatico.
"""

import argparse
import builtins
import csv
import json
import re
import time
from datetime import date, datetime
from pathlib import Path

import requests

BASE_URL = "https://www.job-room.ch"
SEARCH_ENDPOINT = "/jobadservice/api/jobAdvertisements/_search"
LANG_TO_NG = {
    "fr": "ZnI=",
    "de": "ZGU=",
    "it": "aXQ=",
    "en": "ZW4=",
}
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "fr-CH,fr;q=0.9,en;q=0.8",
    "Content-Type": "application/json",
    "Origin": "https://www.job-room.ch",
    "Referer": "https://www.job-room.ch/job-search",
}
LOCALITIES = {
    "GE": {"communalCodes": ["6621"], "geoPoint": {"lat": 46.222, "lon": 6.124}, "label": "Geneve"},
    "VD": {"communalCodes": ["5586"], "geoPoint": {"lat": 46.516, "lon": 6.632}, "label": "Lausanne"},
    "FR": {"communalCodes": ["2196"], "geoPoint": {"lat": 46.806, "lon": 7.162}, "label": "Fribourg"},
    "NE": {"communalCodes": ["6454"], "geoPoint": {"lat": 46.991, "lon": 6.931}, "label": "Neuchatel"},
    "VS": {"communalCodes": ["6155"], "geoPoint": {"lat": 46.233, "lon": 7.362}, "label": "Sion"},
    "BE": {"communalCodes": ["351"], "geoPoint": {"lat": 46.948, "lon": 7.447}, "label": "Berne"},
}

FR_HINTS = {
    "avec",
    "pour",
    "poste",
    "vous",
    "nous",
    "experience",
    "equipe",
    "mission",
    "competences",
    "formation",
    "travail",
    "emploi",
    "profil",
    "francais",
    "responsable",
    "gestion",
    "assurer",
    "recherche",
    "candidat",
}
EN_HINTS = {"with", "for", "you", "team", "experience", "job", "position", "skills", "work", "english"}
DE_HINTS = {"mit", "fur", "sie", "erfahrung", "stelle", "aufgaben", "kenntnisse", "arbeit", "deutsch"}
IT_HINTS = {"con", "per", "lavoro", "posizione", "esperienza", "richiesto", "competenze", "squadra"}


def print(*args, **kwargs):  # type: ignore[override]
    kwargs.setdefault("flush", True)
    return builtins.print(*args, **kwargs)


def _normalize_lang(lang: str) -> str:
    lang = (lang or "fr").strip().lower()
    return lang if lang in LANG_TO_NG else "fr"


def _date_iso(value: str | None) -> str | None:
    if not value:
        return None
    value = str(value).strip()
    return value[:10] if value else None


def _days_ago(posting_date: str | None) -> int | None:
    if not posting_date:
        return None
    try:
        d = datetime.fromisoformat(posting_date).date()
    except ValueError:
        return None
    return (date.today() - d).days


def _pick_description(descriptions: list[dict], preferred_lang: str) -> tuple[str | None, str | None, set[str]]:
    lang_codes = {str(d.get("languageIsoCode") or "").lower() for d in descriptions if isinstance(d, dict)}

    def pick(lang: str) -> tuple[str | None, str | None]:
        for d in descriptions:
            if not isinstance(d, dict):
                continue
            if str(d.get("languageIsoCode") or "").lower() == lang:
                title = str(d.get("title") or "").strip() or None
                desc = str(d.get("description") or "").strip() or None
                return title, desc
        return None, None

    title, desc = pick(preferred_lang)
    if not title and not desc:
        for alt in ("fr", "de", "it", "en"):
            title, desc = pick(alt)
            if title or desc:
                break

    if not title and descriptions and isinstance(descriptions[0], dict):
        title = str(descriptions[0].get("title") or "").strip() or None
    if not desc and descriptions and isinstance(descriptions[0], dict):
        desc = str(descriptions[0].get("description") or "").strip() or None

    return title, desc, lang_codes


def normalize(item: dict, preferred_lang: str = "fr") -> dict:
    ja = item.get("jobAdvertisement", item)
    if not isinstance(ja, dict):
        ja = {}
    jc = ja.get("jobContent") or {}
    if not isinstance(jc, dict):
        jc = {}

    descriptions = jc.get("jobDescriptions") or []
    if not isinstance(descriptions, list):
        descriptions = []
    title, desc, lang_codes = _pick_description(descriptions, preferred_lang)

    employer = jc.get("employer") or {}
    if not isinstance(employer, dict):
        employer = {}
    company = str(employer.get("name") or "").strip() or "N/A"

    location_obj = jc.get("location") or {}
    if not isinstance(location_obj, dict):
        location_obj = {}
    location = str(location_obj.get("city") or location_obj.get("communalName") or "").strip() or "N/A"
    postal_code = str(location_obj.get("zipCode") or "").strip()
    canton = str(location_obj.get("cantonCode") or "").strip()

    publication = ja.get("publication") or {}
    if not isinstance(publication, dict):
        publication = {}
    posting_date = _date_iso(publication.get("startDate") or ja.get("createdTime"))
    expiry_date = _date_iso(publication.get("endDate"))

    employment = jc.get("employment") or {}
    if not isinstance(employment, dict):
        employment = {}
    wl_min = employment.get("workloadPercentageMin")
    wl_max = employment.get("workloadPercentageMax")
    if isinstance(wl_min, (int, float)) and isinstance(wl_max, (int, float)):
        workload = f"{int(wl_min)}-{int(wl_max)}%"
    elif isinstance(wl_max, (int, float)):
        workload = f"{int(wl_max)}%"
    else:
        workload = ""

    perm = employment.get("permanent")
    if perm is True:
        contract_type = "permanent"
    elif perm is False:
        contract_type = "temporary"
    else:
        contract_type = ""

    external_id = str(ja.get("id") or ja.get("stellennummerEgov") or "").strip()
    external_url = str(jc.get("externalUrl") or "").strip()
    url = external_url or (f"https://www.job-room.ch/job-search/detail/{external_id}" if external_id else "")

    rav_exclusive = bool(ja.get("reportingObligation"))
    description = (str(desc or "")[:2500]).strip() or None

    return {
        "title": title or "N/A",
        "company": company,
        "location": location,
        "postal_code": postal_code,
        "canton": canton,
        "posting_date": posting_date,
        "expiry_date": expiry_date,
        "days_ago": _days_ago(posting_date),
        "workload": workload,
        "contract_type": contract_type,
        "rav_exclusive": rav_exclusive,
        "url": url,
        "external_id": external_id,
        "language_codes": sorted(lang_codes),
        "source": "jobroom",
        "description": description,
    }


def is_french_job(row: dict) -> bool:
    lang_codes = row.get("language_codes")
    if isinstance(lang_codes, list) and any(str(x).lower() == "fr" for x in lang_codes):
        return True

    text = f"{row.get('title') or ''} {row.get('description') or ''}".lower()
    tokens = set(re.findall(r"[a-zA-Z]{3,}", text))
    fr = len(tokens & FR_HINTS)
    en = len(tokens & EN_HINTS)
    de = len(tokens & DE_HINTS)
    it = len(tokens & IT_HINTS)
    return fr >= 2 and fr >= max(en, de, it)


def search_page(
    session: requests.Session,
    canton: str = "GE",
    days: int = 30,
    keyword: str = "",
    radius: int = 30,
    page: int = 0,
    size: int = 25,
    lang: str = "fr",
) -> tuple[list[dict], int | None]:
    loc = LOCALITIES.get(canton, LOCALITIES["GE"])
    body = {
        "workloadPercentageMin": 10,
        "workloadPercentageMax": 100,
        "permanent": None,
        "companyName": None,
        "onlineSince": max(days, 1),
        "displayRestricted": False,
        "professionCodes": [],
        "keywords": [keyword] if keyword.strip() else [],
        "communalCodes": loc["communalCodes"],
        "cantonCodes": [],
        "radiusSearchRequest": {
            "geoPoint": loc["geoPoint"],
            "distance": radius,
        },
    }
    params = {"sort": "date_desc", "_ng": LANG_TO_NG[_normalize_lang(lang)], "page": page, "size": size}

    r = session.post(BASE_URL + SEARCH_ENDPOINT, params=params, json=body, timeout=25)
    r.raise_for_status()
    data = r.json()

    if isinstance(data, dict):
        raw = data.get("content")
        if not isinstance(raw, list):
            raw = data.get("jobs")
        if not isinstance(raw, list):
            raw = data.get("results")
        if not isinstance(raw, list):
            raw = []
        total = data.get("totalElements")
        if not isinstance(total, int):
            total = data.get("total")
        if not isinstance(total, int):
            total = None
    elif isinstance(data, list):
        raw = data
        total = None
    else:
        raw = []
        total = None

    return [x for x in raw if isinstance(x, dict)], total


def within_days(posting_date: str | None, max_days: int | None) -> bool:
    if max_days is None:
        return True
    if not posting_date:
        return False
    try:
        d = datetime.fromisoformat(posting_date).date()
    except ValueError:
        return False
    age = (date.today() - d).days
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
        "canton",
        "posting_date",
        "expiry_date",
        "workload",
        "contract_type",
        "rav_exclusive",
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
        row.setdefault("source", "jobroom")
        if not isinstance(row.get("id"), int):
            max_id += 1
            row["id"] = max_id

    out = list(merged.values())
    out.sort(key=lambda r: int(r.get("id")) if isinstance(r.get("id"), int) else 0, reverse=True)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Scraper de vagas no job-room.ch")
    parser.add_argument("--canton", type=str, default="GE", choices=list(LOCALITIES.keys()), help="Cantao da pesquisa")
    parser.add_argument("--lang", type=str, default="fr", help="Idioma da API (_ng): fr/de/it/en")
    parser.add_argument("--keyword", type=str, default="", help="Palavra-chave")
    parser.add_argument("--radius", type=int, default=30, help="Raio (km)")
    parser.add_argument("--max-pages", type=int, default=0, help="Maximo de paginas (0 = dinamico)")
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
        help="Ficheiro JSON de saida (default: data/jobroom/professions.json)",
    )
    parser.add_argument(
        "--allow-non-french",
        action="store_true",
        help="Nao filtra idioma (por omissao, mantem apenas vagas francesas).",
    )
    parser.add_argument("--save-csv", action="store_true", help="Tambem salva CSV")
    args = parser.parse_args()

    output_arg = args.output_json.strip()
    if not output_arg:
        output_path = Path("data") / "jobroom" / "professions.json"
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

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    print(
        f"[SEARCH] Fonte: job-room.ch | cantao={args.canton} | lang={_normalize_lang(args.lang)} "
        f"| days={args.days} | radius={args.radius} | keyword='{args.keyword}'"
    )
    if args.max_pages and args.max_pages > 0:
        print(f"[PAGES] A processar (manual): ate {args.max_pages}")
    else:
        print("[PAGES] A processar: modo dinamico (sem limite fixo)")

    max_days = args.days if args.days and args.days > 0 else None
    filter_french = not args.allow_non_french

    fresh_jobs: list[dict] = []
    all_page_urls: list[str] = []
    seen_streak = 0
    total_api_known: int | None = None

    page = 0
    while True:
        if args.max_pages and args.max_pages > 0 and page >= args.max_pages:
            break
        try:
            raw_rows, total_api = search_page(
                session=session,
                canton=args.canton,
                days=args.days if args.days > 0 else 3650,
                keyword=args.keyword,
                radius=args.radius,
                page=page,
                size=25,
                lang=args.lang,
            )
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            print(f"  [WARN] HTTP {status} na pagina {page + 1}. Paragem segura.")
            break
        except requests.RequestException as exc:
            print(f"  [WARN] Falha de rede na pagina {page + 1}: {exc}")
            break

        if total_api_known is None and isinstance(total_api, int):
            total_api_known = total_api

        if not raw_rows:
            print(f"  Pagina {page + 1}: 0 registos (fim)")
            break

        print(f"  Pagina {page + 1}: {len(raw_rows)} registos")
        for raw in raw_rows:
            row = normalize(raw, preferred_lang=_normalize_lang(args.lang))
            url = str(row.get("url") or "").strip()
            if not url:
                continue
            all_page_urls.append(url)

            if args.stop_after_seen > 0:
                if url in known_urls:
                    seen_streak += 1
                    if seen_streak >= args.stop_after_seen:
                        print(f"  Paragem incremental: {seen_streak} links conhecidos em sequencia")
                        break
                else:
                    seen_streak = 0

            if url in known_urls:
                continue
            if filter_french and not is_french_job(row):
                continue
            if not within_days(row.get("posting_date"), max_days):
                continue

            fresh_jobs.append(row)
            if args.max_jobs and args.max_jobs > 0 and len(fresh_jobs) >= args.max_jobs:
                break

        if args.stop_after_seen > 0 and seen_streak >= args.stop_after_seen:
            break
        if args.max_jobs and args.max_jobs > 0 and len(fresh_jobs) >= args.max_jobs:
            print(f"  Limite aplicado em detalhe: {len(fresh_jobs)}")
            break
        if isinstance(total_api_known, int) and (page + 1) * 25 >= total_api_known:
            break
        if len(raw_rows) < 25:
            break
        if args.delay > 0:
            time.sleep(args.delay)
        page += 1

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
            "canton": args.canton,
            "lang": _normalize_lang(args.lang),
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
