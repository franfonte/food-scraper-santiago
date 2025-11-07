import os
import csv
import json
import time
import random
import sys
import requests
from bs4 import BeautifulSoup

# ==============================================================================
# DEFINICIÓN DE RUTAS
# ==============================================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data')

# --- Archivo de Entrada (Tu nuevo archivo de links) ---
# Asume un CSV con una columna llamada 'service_link'
RESTAURANTES_LINKS_CSV = os.path.join(DATA_DIR, "service_links.csv")

# --- Archivos de Salida (en modo 'append') ---
# (Usamos .jsonl para un guardado 'append' eficiente)
PRODUCTOS_JSONL_OUTPUT = os.path.join(DATA_DIR, "productos_completo.jsonl") 
PRODUCTOS_CSV_OUTPUT = os.path.join(DATA_DIR, "productos.csv")
LINKS_CACHE_FILE = os.path.join(DATA_DIR, "scraped_menu_links.txt") # Cache de links scrapeados

# ==============================================================================
# ESTRATEGIA "HUMANA"
# ==============================================================================

# Lista de User-Agents reales para rotar
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/119.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
]

# ==============================================================================
# FUNCIÓN DE SCRAPING DE MENÚ
# ==============================================================================

def scrape_menu_restaurante(restaurant_url, headers):
    """
    Scrapea un restaurante usando requests y devuelve el JSON-LD completo.
    """
    if restaurant_url.startswith("/"):
        full_url = "https://www.ubereats.com" + restaurant_url
    else:
        full_url = restaurant_url
    
    try:
        response = requests.get(full_url, headers=headers, timeout=10)
        response.raise_for_status() 
        soup = BeautifulSoup(response.text, 'html.parser')
        
        script_tag = soup.find('script', type='application/ld+json')
        
        if not script_tag:
            print(f"  [Advertencia] No se encontró JSON-LD en {full_url}", file=sys.stderr)
            return None

        data = json.loads(script_tag.string)
        data['restaurant_url'] = full_url 
        return data

    except requests.exceptions.HTTPError as e:
        # Detectar si nos bloquearon
        if e.response.status_code in [403, 429, 503]:
            print(f"  [ERROR BLOQUEO] {e.response.status_code} en {full_url}. El servidor nos está bloqueando.")
            # Lanzar un error especial para que el 'main' lo atrape
            raise ConnectionRefusedError("Bloqueado por el servidor") 
        print(f"  [Error HTTP] {e} al scrapear {full_url}", file=sys.stderr)
        return None
    except requests.exceptions.RequestException as e:
        print(f"  [Error] {e} al scrapear {full_url}", file=sys.stderr)
        return None
    except json.JSONDecodeError:
        print(f"  [Error] No se pudo decodificar el JSON de {full_url}", file=sys.stderr)
        return None

# ==============================================================================
# FUNCIONES DE GUARDADO (Modo Append)
# ==============================================================================

def guardar_datos_csv(nuevos_datos_restaurantes, csv_filename):
    """
    Toma el JSON-LD de UN restaurante, lo aplana en productos,
    y los AÑADE al archivo CSV.
    """
    os.makedirs(os.path.dirname(csv_filename), exist_ok=True)
    
    # Columnas del CSV (para Supabase)
    csv_headers = [
        'name', 'description', 'price', 'store_name', 
        'category_name', 'category_uber', 'restaurante_url'
    ]

    productos_para_csv = []
    
    # 'nuevos_datos_restaurantes' es una lista con UN solo JSON-LD
    for data in nuevos_datos_restaurantes: 
        if not data: continue
            
        store_name = data.get('name', 'N/A')
        restaurante_url = data.get('restaurant_url', 'N/A')
        
        if 'hasMenu' in data and 'hasMenuSection' in data['hasMenu']:
            for section in data['hasMenu']['hasMenuSection']:
                category_name = section.get('name', 'N/A')
                
                if 'hasMenuItem' in section:
                    for item in section['hasMenuItem']:
                        fila_producto = {
                            'name': item.get('name', 'N/A'),
                            'description': item.get('description', ''),
                            'price': item.get('offers', {}).get('price', 'N/A'),
                            'store_name': store_name,
                            'category_name': category_name,
                            'category_uber': '',
                            'restaurante_url': restaurante_url
                        }
                        productos_para_csv.append(fila_producto)
    
    if not productos_para_csv:
        print("  No se encontraron productos en este JSON-LD.")
        return

    archivo_existe = os.path.isfile(csv_filename)
    try:
        with open(csv_filename, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=csv_headers, restval=None)
            if not archivo_existe:
                writer.writeheader()
            writer.writerows(productos_para_csv)
        print(f"  [Guardado CSV] Se añadieron {len(productos_para_csv)} nuevos productos a '{csv_filename}'")
    except Exception as e:
        print(f"  [Error Guardado CSV]: {e}", file=sys.stderr)

def guardar_datos_jsonl(nuevos_datos_restaurantes, jsonl_filename):
    """
    Toma la lista de NUEVOS JSONs de restaurantes y los AÑADE
    como nuevas líneas al archivo .jsonl (JSON Lines).
    """
    os.makedirs(os.path.dirname(jsonl_filename), exist_ok=True)
    try:
        # 'a' (append) es la clave de la eficiencia
        with open(jsonl_filename, 'a', encoding='utf-8') as f:
            for restaurante_data in nuevos_datos_restaurantes:
                if restaurante_data:
                    # Convertir el dict a un string JSON y escribirlo
                    json_line = json.dumps(restaurante_data, ensure_ascii=False)
                    f.write(json_line + '\n')
        print(f"  [Guardado JSONL] Se añadió {len(nuevos_datos_restaurantes)} menú a '{jsonl_filename}'")
    except Exception as e:
        print(f"  [Error Guardado JSONL]: {e}", file=sys.stderr)

# ==============================================================================
# FUNCIONES DE CARGA Y CACHE
# ==============================================================================

def load_restaurant_links(filepath):
    """Carga todos los links de restaurantes desde el CSV de links."""
    links = set()
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            # Asume que la columna se llama 'service_link'
            for row in reader:
                if row.get('service_link'):
                    links.add(row['service_link'])
        print(f"Se cargaron {len(links)} links de restaurantes únicos desde '{filepath}'")
        return list(links) # Convertir a lista para barajar
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo '{filepath}'")
        return []
    except Exception as e:
        print(f"Error al leer {filepath}: {e}. Asegúrate que tenga la columna 'service_link'.")
        return []

def load_scraped_links_cache(filepath):
    """Carga los links de menús que ya scrapeamos para no repetirlos."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    if not os.path.isfile(filepath):
        return set()
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            links = set(line.strip() for line in f if line.strip())
        print(f"Se cargaron {len(links)} links de menús ya scrapeados desde el cache.")
        return links
    except Exception as e:
        print(f"Error al cargar cache de links: {e}. Empezando con cache vacío.")
        return set()

def save_link_to_cache(link, filepath):
    """Añade un link al archivo cache en modo 'append'."""
    try:
        with open(filepath, 'a', encoding='utf-8') as f:
            f.write(link + '\n')
    except Exception as e:
        print(f"  [Error Cache] No se pudo guardar {link} en el cache: {e}")

# ==============================================================================
# FUNCIÓN PRINCIPAL (El Orquestador)
# ==============================================================================

def main():
    print("--- Iniciando Proceso de Scraping de Menús (Modo Humano) ---")
    
    # 1. Cargar datos
    all_links = load_restaurant_links(RESTAURANTES_LINKS_CSV)
    scraped_links = load_scraped_links_cache(LINKS_CACHE_FILE)
    
    # 2. Filtrar links que ya tenemos
    links_to_scrape = [link for link in all_links if link not in scraped_links]
    
    if not links_to_scrape:
        print("¡No hay links nuevos que scrapear! Todo está al día.")
        return

    # 3. Aleatoriedad (Shuffle)
    random.shuffle(links_to_scrape)
    print(f"Se van a scrapear {len(links_to_scrape)} menús nuevos (en orden aleatorio).")
    
    total_links = len(links_to_scrape)
    
    for i, link in enumerate(links_to_scrape):
        
        print(f"\n--- Procesando {i+1} de {total_links}: {link} ---")
        
        # 4. Ritmo (Pauses Aleatorias)
        sleep_time = random.uniform(2.5, 4.0) # Pausa entre 2.5 y 4 segundos
        print(f"Pausando por {sleep_time:.1f} segundos...")
        time.sleep(sleep_time)
        
        # 5. Cabeceras (Headers) Rotativas
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept-Language': 'es-CL,es;q=0.9',
            'Referer': 'https.www.ubereats.com/' # Simular que venimos de la home
        }
        
        try:
            # 6. Ejecutar el scraping
            restaurant_data = scrape_menu_restaurante(link, headers)
            
            if restaurant_data:
                # 7. Guardar incrementalmente
                guardar_datos_jsonl([restaurant_data], PRODUCTOS_JSONL_OUTPUT)
                guardar_datos_csv([restaurant_data], PRODUCTOS_CSV_OUTPUT)
                
                # 8. Marcar como scrapeado
                save_link_to_cache(link, LINKS_CACHE_FILE)
            
        except ConnectionRefusedError:
            # 9. Resistencia a Fallos (Back-off)
            print("[BLOQUEO DETECTADO] Pausa larga de 5 minutos...")
            time.sleep(300) # Esperar 5 minutos
            print("Continuando con el siguiente link (se reintentará este link en la próxima ejecución)...")
        except Exception as e:
            print(f"  [ERROR FATAL] Ocurrió un error inesperado con {link}: {e}")
            # Guardar el link fallido para reintentar luego
            save_link_to_cache(link + "_FAILED", LINKS_CACHE_FILE)

    print("\n--- Proceso de Scraping de Menús Terminado ---")

if __name__ == "__main__":
    main()