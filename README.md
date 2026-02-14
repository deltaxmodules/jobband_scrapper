# local_scrappers

Ambiente isolado para scraping local (JobUp + Talent + classificação de profissões).

## 1) Setup

```bash
cd /Users/jataide/Desktop/local_scrappers
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

O ficheiro `.env` já está ligado ao projeto principal por symlink:
- `/Users/jataide/Desktop/local_scrappers/.env` -> `/Users/jataide/Desktop/projecto_cv/.env`

## 2) JobUp (incremental)

```bash
cd /Users/jataide/Desktop/local_scrappers
source .venv/bin/activate
python jobup/jobup.py --location "Genève" --days 30 --stop-after-seen 40
```

Output:
- `data/jobup/professions.json`
- `data/jobup/professions.state.json`

## 3) Talent (incremental)

```bash
cd /Users/jataide/Desktop/local_scrappers
source .venv/bin/activate
python talent/talent.py --location "Genève" --days 30 --stop-after-seen 120
```

Output:
- `data/talent/professions.json`
- `data/talent/professions.state.json`

## 4) Classificar profissões (OpenAI)

JobUp:

```bash
cd /Users/jataide/Desktop/local_scrappers
source .venv/bin/activate
python jobup/professions_pipeline.py --input data/jobup/professions.json --retry-errors
```

Talent:

```bash
cd /Users/jataide/Desktop/local_scrappers
source .venv/bin/activate
python talent/professions_pipeline.py --input data/talent/professions.json --retry-errors
```

Notas:
- O pipeline atualiza o mesmo ficheiro passado em `--input`.
- Registos já preenchidos em `professions` são ignorados (exceto `--force-all`).
