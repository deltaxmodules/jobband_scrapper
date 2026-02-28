# local_scrappers

Scrapers locais com modo incremental para:
- `jobup`
- `talent`
- `ge`
- `jobroom`
- `adzuna`
- `indeed` (RapidAPI)
- `linkedin` (RapidAPI)

Todos gravam em `data/<source>/professions.json` e mantêm estado em `data/<source>/professions.state.json`.

## Quick Start

```bash
cd /Users/jataide/Desktop/JOBBAND_MVP/local_scrappers
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Se usares classificação OpenAI, garante `OPENAI_API_KEY` no `.env`.
Para Indeed via RapidAPI, define também `RAPIDAPI_KEY` no `.env`.

## Run All (recomendado)

Executa scraping + classificação para todas as fontes configuradas:

```bash
cd /Users/jataide/Desktop/JOBBAND_MVP/local_scrappers
source .venv/bin/activate
./run_all.sh
```

Variações úteis:

```bash
# só scraping (sem classificação)
RUN_PIPELINES=0 ./run_all.sh

# janela de 15 dias
DAYS=15 ./run_all.sh

# não reprocessar registos com erro no pipeline
RETRY_ERRORS=0 ./run_all.sh
```

## Scraping Por Fonte

```bash
cd /Users/jataide/Desktop/JOBBAND_MVP/local_scrappers
source .venv/bin/activate
```

JobUp:

```bash
python jobup/jobup.py --location "Genève" --days 30 --stop-after-seen 40
```

Talent:

```bash
python talent/talent.py --location "Genève" --days 30 --stop-after-seen 120
```

GE:

```bash
python ge/ge_ch_scraper.py --days 30 --stop-after-seen 120
```

JobRoom:

```bash
python jobroom/jobroom.py --canton GE --lang fr --days 30 --stop-after-seen 120 --output-json data/jobroom/professions.json
```

Adzuna:

```bash
python adzuna/adzuna.py --country ch --location "Geneva" --term "jobs" --days 30 --stop-after-seen 120 --output-json data/adzuna/professions.json
```

Indeed:

```bash
python indeed/indeed.py --search-term "jobs" --location "Geneva" --country-indeed "Switzerland" --days 30 --results-wanted 1000 --site-name "indeed" --output-json data/indeed/professions.json
```

LinkedIn:

```bash
python linkedin/linkedin.py --search-term "jobs" --location "Geneva" --country-indeed "Switzerland" --days 30 --results-wanted 1000 --site-name "linkedin" --output-json data/linkedin/professions.json
```

## Classificação De Profissões (OpenAI)

```bash
cd /Users/jataide/Desktop/JOBBAND_MVP/local_scrappers
source .venv/bin/activate
```

JobUp:

```bash
python jobup/professions_pipeline.py --input data/jobup/professions.json --retry-errors
```

Talent:

```bash
python talent/professions_pipeline.py --input data/talent/professions.json --retry-errors
```

GE:

```bash
python ge/professions_pipeline.py --input data/ge/professions.json --retry-errors
```

JobRoom:

```bash
python jobroom/professions_pipeline.py --input data/jobroom/professions.json --retry-errors
```

Adzuna:

```bash
python adzuna/professions_pipeline.py --input data/adzuna/professions.json --retry-errors
```

Indeed:

```bash
python indeed/professions_pipeline.py --input data/indeed/professions.json --retry-errors
```

LinkedIn:

```bash
python linkedin/professions_pipeline.py --input data/linkedin/professions.json --retry-errors
```

Notas:
- O pipeline atualiza o mesmo ficheiro passado em `--input`.
- Registos já preenchidos em `professions` são ignorados (usa `--force-all` para reprocessar tudo).
