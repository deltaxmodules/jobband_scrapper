# scraper_ge_courses.py
from playwright.sync_api import sync_playwright
import json, time

API_RESPONSES = []

def intercept_response(response):
    url = response.url
    # Captura qualquer chamada JSON da API
    if response.headers.get("content-type", "").startswith("application/json"):
        try:
            data = response.json()
            print(f"[API] {response.status} {url}")
            API_RESPONSES.append({"url": url, "data": data})
        except:
            pass

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)  # headless=False para ver o browser
    page = browser.new_page()
    page.on("response", intercept_response)
    
    page.goto("https://ge.ch/socialcafcatalogue/explore", wait_until="networkidle")
    time.sleep(3)

    # Tenta scroll para carregar mais cursos
    for i in range(10):
        page.keyboard.press("End")
        time.sleep(1)

    # Guarda o HTML para análise
    with open("page.html", "w", encoding="utf-8") as f:
        f.write(page.content())

    browser.close()

# Salva todas as respostas da API
with open("api_responses.json", "w", encoding="utf-8") as f:
    json.dump(API_RESPONSES, f, indent=2, ensure_ascii=False)

print(f"\nCapturadas {len(API_RESPONSES)} respostas da API")
print("Ficheiros guardados: page.html e api_responses.json")