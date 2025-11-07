import os
import csv
import json
import time
import random
import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# ==============================================================================
# DEFINICIÓN DE RUTAS
# ==============================================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ZONES_FILE = os.path.join(BASE_DIR, "01_zones.json")
CATEGORIES_FILE = os.path.join(BASE_DIR, "02_category_uber.json")
DATA_DIR = os.path.join(BASE_DIR, '..', 'data')
JSON_FILE_OUTPUT = os.path.join(DATA_DIR, "restaurantes.json")
CSV_FILE_OUTPUT = os.path.join(DATA_DIR, "restaurantes.csv")
UBER_BASE_URL = "https.www.ubereats.com"
SELECTOR_TARJETA_RESTAURANTE = "a[data-testid='store-card']"


# ==============================================================================
# FUNCIONES DE CARGA DE DATOS
# ==============================================================================

def load_and_sort_zones(filepath):
    """Carga y ordena las zonas por prioridad."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            zones = json.load(f)
    except Exception as e:
        print(f"Error fatal: No se pudo cargar {filepath}: {e}")
        return []

    def sort_key(zone):
        last_scraped_date = zone.get('last scraped')
        if last_scraped_date == 'date' or not last_scraped_date:
            timestamp = 0 
        else:
            try:
                timestamp = datetime.datetime.fromisoformat(last_scraped_date).timestamp()
            except (ValueError, TypeError):
                timestamp = 0
        return (zone.get('scraped', 0), timestamp)

    zones.sort(key=sort_key)
    print(f"Se cargaron y ordenaron {len(zones)} zonas.")
    return zones

def load_categories(filepath):
    """Carga la lista de categorías de comida."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            categories = data.get("category_uber", [])
            if not categories:
                print(f"Advertencia: No se encontraron categorías en {filepath}")
            return categories
    except Exception as e:
        print(f"Error fatal: No se pudo cargar {filepath}: {e}")
        return []

# ==============================================================================
# FUNCIONES DE GUARDADO (Modo Append)
# ==============================================================================

def guardar_restaurantes_csv(nuevos_restaurantes, csv_filename):
    os.makedirs(os.path.dirname(csv_filename), exist_ok=True)
    
    csv_headers = [
        'name', 'service', 'latitude', 'longitude', 
        'address', 'zone', 'service_link', 'image'
    ]
    
    archivo_existe = os.path.isfile(csv_filename)
    try:
        with open(csv_filename, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=csv_headers, restval=None)
            if not archivo_existe:
                writer.writeheader()
            writer.writerows(nuevos_restaurantes)
        print(f"  [Guardado CSV] Se añadieron {len(nuevos_restaurantes)} nuevos restaurantes a '{csv_filename}'")
    except Exception as e:
        print(f"  [Error Guardado CSV]: {e}")

def guardar_restaurantes_jsonl(nuevos_restaurantes, jsonl_filename):
    os.makedirs(os.path.dirname(jsonl_filename), exist_ok=True)
    
    try:
        # Abrir en modo 'append'
        with open(jsonl_filename, 'a', encoding='utf-8') as f:
            for restaurante in nuevos_restaurantes:
                # Convertir el dict de Python a un string JSON
                json_line = json.dumps(restaurante, ensure_ascii=False)
                # Escribir esa línea en el archivo, seguida de un salto de línea
                f.write(json_line + '\n')
                
        print(f"  [Guardado JSONL] Se añadieron {len(nuevos_restaurantes)} nuevos restaurantes a '{jsonl_filename}'")
        
    except Exception as e:
        print(f"  [Error Guardado JSONL]: {e}")

def save_updated_zones(zones_data, filepath):
    """Guarda la lista de zonas con los contadores actualizados."""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(zones_data, f, indent=2, ensure_ascii=False)
        print(f"  [Guardado Zonas] Se actualizó el contador en {filepath}")
    except Exception as e:
        print(f"  [ERROR] No se pudo guardar el archivo de zonas actualizado: {e}")

# ==============================================================================
# FUNCIÓN PARA CREAR EL DRIVER
# ==============================================================================

def create_driver():
    """Configura e inicia una nueva instancia del driver de Chrome."""
    print("Iniciando nueva sesión de driver...")
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
    chrome_options = Options()
    # chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument(f'user-agent={USER_AGENT}')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        return driver
    except Exception as e:
        print(f"Error fatal al iniciar el driver: {e}")
        return None

# ==============================================================================
# FUNCIÓN DE SCRAPING
# ==============================================================================

def scrape_restaurants_from_url(driver, category_url, category_name, commune_name):
    """
    Navega a una URL de categoría y extrae los restaurantes.
    Reutiliza el mismo driver.
    """
    results = []
    try:
        print(f"  Navegando a categoría: {category_name}...")
        driver.get(category_url)
        
        WebDriverWait(driver, 15).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, SELECTOR_TARJETA_RESTAURANTE))
        )
        print("  Contenido cargado.")
        
        scroll_height = driver.execute_script("return document.body.scrollHeight")
        driver.execute_script(f"window.scrollTo(0, {scroll_height / 3});")
        time.sleep(random.uniform(0.6, 1.4))
        driver.execute_script(f"window.scrollTo(0, {scroll_height / 1.5});")
        time.sleep(random.uniform(1.5, 3.5))

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        restaurant_cards = soup.find_all("a", {"data-testid": "store-card"})
        print(f"  Se encontraron {len(restaurant_cards)} restaurantes.")

        for card in restaurant_cards:
            h3_tag = card.find("h3")
            href = card.get("href")

            if href and h3_tag:
                restaurant_url = UBER_BASE_URL + href
                restaurant_name = h3_tag.text.strip()
                
                results.append({
                    "name": restaurant_name,
                    "service": "Uber Eats",
                    "latitude": None,
                    "longitude": None,
                    "address": None,
                    "zone": commune_name,
                    "service_link": restaurant_url,
                    "image": None
                })
        
    except Exception as e:
        print(f"  [ERROR] Falló el scraping para {category_name}: {e}")
        with open("checkpoint_error.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
            print("  Se guardó 'checkpoint_error.html' para depuración.")
            
    return results

# ==============================================================================
# FUNCIÓN PRINCIPAL (El Controlador)
# ==============================================================================

def main():
    print("--- Iniciando Proceso de Scraping de Restaurantes ---")
    
    zones_to_scrape = load_and_sort_zones(ZONES_FILE)
    categories_to_scrape = load_categories(CATEGORIES_FILE)
    
    if not zones_to_scrape or not categories_to_scrape:
        print("Faltan Zonas o Categorías. Abortando.")
        return
    
    for zone_data in zones_to_scrape:
        commune_name = zone_data['commune_name']
        url_base = zone_data['url_base']
        
        # --- [CAMBIO] Chequeo de URL vacía ---
        if not url_base or url_base.strip() == "":
            print(f"\n--- Saltando Zona: {commune_name} (URL base está vacía) ---")
            continue # Pasa a la siguiente zona
        # --- [FIN DEL CAMBIO] ---
            
        print(f"\n--- Procesando Zona: {commune_name} (Contador: {zone_data.get('scraped', 0)}) ---")
        
        driver = None
        restaurants_scraped_this_zone = []
            
        try:
            driver = create_driver()
            if not driver:
                print(f"No se pudo iniciar el driver para {commune_name}. Saltando zona.")
                continue 
            
            for category_name in categories_to_scrape:
                scrape_url = f"{url_base}&scq={category_name}"
                
                restaurants_found = scrape_restaurants_from_url(
                    driver, scrape_url, category_name, commune_name
                )
                
                if restaurants_found:
                    restaurants_scraped_this_zone.extend(restaurants_found)
                
                time.sleep(random.uniform(5, 15))

            if restaurants_scraped_this_zone:
                print(f"  Guardando {len(restaurants_scraped_this_zone)} restaurantes de {commune_name}...")
                guardar_restaurantes_jsonl(restaurants_scraped_this_zone, JSON_FILE_OUTPUT)
                guardar_restaurantes_csv(restaurants_scraped_this_zone, CSV_FILE_OUTPUT)
                
                zone_data['scraped'] = zone_data.get('scraped', 0) + 1
                zone_data['last scraped'] = datetime.datetime.now().isoformat()
            else:
                print(f"  No se encontraron nuevos restaurantes en {commune_name}.")

        except Exception as e:
            print(f"  [ERROR FATAL DE ZONA] Un error inesperado ocurrió en {commune_name}: {e}")
        
        finally:
            if driver:
                print("Cerrando driver de la zona...")
                driver.quit()

            save_updated_zones(zones_to_scrape, ZONES_FILE)

            print("  Pausa larga entre comunas...")
            time.sleep(random.uniform(30, 60))

    print("\n--- Proceso de Scraping Terminado ---")


if __name__ == "__main__":
    main()