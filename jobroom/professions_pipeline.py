#!/usr/bin/env python3
"""
Pipeline unico: adiciona `professions` em cada registo de vagas.
"""

import argparse
import json
import os
from datetime import date
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openai import OpenAI


SYSTEM_PROMPT = (
    "Tu classifies des offres d'emploi en professions. "
    "Utilise seulement le titre et la description fournis. "
    "Reponds strictement en JSON avec la structure: "
    '{"professions":["Profession 1","Profession 2"]}. '
    "Renvoie entre 1 et 5 professions maximum, en francais."
)


def find_today_json() -> Path:
    d = date.today()
    padded = f"{d:%d_%m_%Y}.json"
    unpadded = f"{d.day}_{d.month}_{d.year}.json"
    candidates = [
        Path(padded),
        Path("data") / "jobroom" / padded,
        Path("jobroom") / padded,
        Path(unpadded),
        Path("data") / "jobroom" / unpadded,
        Path("jobroom") / unpadded,
    ]
    for c in candidates:
        if c.exists():
            return c
    raise FileNotFoundError(
        f"Nao encontrei ficheiro do dia: {padded} (ou {unpadded}) na raiz/data/jobroom."
    )


def find_default_input_and_output(input_arg: str, output_arg: str) -> tuple[Path, Path]:
    if input_arg:
        in_path = Path(input_arg)
        if not in_path.exists():
            raise FileNotFoundError(f"Ficheiro nao encontrado: {in_path}")
        out_path = Path(output_arg) if output_arg else in_path
        return in_path, out_path

    fixed_candidates = [
        Path("professions.json"),
        Path("data") / "jobroom" / "professions.json",
        Path("jobroom") / "professions.json",
    ]
    for c in fixed_candidates:
        if c.exists():
            out_path = Path(output_arg) if output_arg else c
            return c, out_path

    source = find_today_json()
    default_out = Path("data") / "jobroom" / "professions.json"
    out_path = Path(output_arg) if output_arg else default_out
    return source, out_path


def clean_html_text(raw_html: str | None) -> str:
    if not raw_html:
        return ""
    text = BeautifulSoup(raw_html, "html.parser").get_text(" ", strip=True)
    return " ".join(text.split())


def ensure_openai_api_key() -> None:
    load_dotenv(Path(".env"))
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY nao encontrada no .env")


def classify_professions(client: OpenAI, model: str, title: str, description: str) -> list[str]:
    content = f"Titre: {title}\nDescription: {description[:4000]}"
    resp = client.chat.completions.create(
        model=model,
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": content},
        ],
    )
    raw = resp.choices[0].message.content or "{}"
    data: dict[str, Any] = json.loads(raw)
    values = data.get("professions", [])
    if not isinstance(values, list):
        return []

    out: list[str] = []
    seen = set()
    for v in values:
        if isinstance(v, str):
            t = " ".join(v.split()).strip()
            if t and t.lower() not in seen:
                seen.add(t.lower())
                out.append(t)
        if len(out) >= 5:
            break
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Adiciona professions em cada registo do JSON de vagas (pipeline unico)."
    )
    parser.add_argument(
        "--input",
        default="",
        help="JSON de entrada. Se omitido, usa data/jobroom/professions.json.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="JSON de saida. Se omitido, atualiza no proprio ficheiro.",
    )
    parser.add_argument("--model", default="gpt-4.1", help="Modelo OpenAI para classificacao.")
    parser.add_argument("--log-file", default="", help="Guarda logs em ficheiro (opcional).")
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=1,
        help="Guarda progresso no output a cada N registos (default: 1).",
    )
    parser.add_argument("--force-all", action="store_true", help="Reprocessa todos os registos.")
    parser.add_argument("--retry-errors", action="store_true", help="Tambem reprocessa registos com erro.")
    args = parser.parse_args()

    ensure_openai_api_key()
    in_path, out_path = find_default_input_and_output(args.input, args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    payload = json.loads(in_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise RuntimeError("O JSON de entrada deve ser uma lista de registos.")

    client = OpenAI()
    total = len(payload)
    log_path = Path(args.log_file) if args.log_file else None

    def write_log(message: str) -> None:
        print(message)
        if log_path:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with log_path.open("a", encoding="utf-8") as f:
                f.write(message + "\n")

    processed = 0
    skipped = 0
    for i, row in enumerate(payload, start=1):
        if not isinstance(row, dict):
            continue

        current_prof = row.get("professions")
        if isinstance(current_prof, list):
            has_prof = bool(current_prof)
        else:
            has_prof = bool(str(current_prof or "").strip())
        has_err = bool(str(row.get("professions_error") or "").strip())
        if not args.force_all:
            if has_prof:
                skipped += 1
                continue
            if has_err and not args.retry_errors:
                skipped += 1
                continue

        title = str(row.get("title") or "").strip()
        description = clean_html_text(row.get("description"))
        if not title and not description:
            row["professions"] = ""
            write_log(f"[{i}/{total}] professions=")
            processed += 1
            continue

        try:
            profs = classify_professions(client, args.model, title, description)
            row["professions"] = ", ".join(profs)
            row.pop("professions_error", None)
        except Exception as exc:
            row["professions"] = ""
            row["professions_error"] = f"{type(exc).__name__}: {exc}"

        write_log(f"[{i}/{total}] professions={row['professions']}")
        processed += 1
        if args.checkpoint_every > 0 and i % args.checkpoint_every == 0:
            out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(
        f"[DONE] Registos totais: {total} | processados: {processed} | ignorados: {skipped} | output: {out_path}"
    )


if __name__ == "__main__":
    main()
