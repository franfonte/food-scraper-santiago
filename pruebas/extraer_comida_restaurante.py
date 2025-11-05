import requests
from bs4 import BeautifulSoup
import json
import csv
import sys
import os  # <-- Importamos 'os' para crear carpetas y verificar archivos
import time

def scrape_menu_restaurante(restaurant_url):
    """
    Scrapea un restaurante usando requests y devuelve el JSON-LD completo.
    'restaurant_url' debe ser el path (ej. /store/...) o la URL completa.
    """
    if restaurant_url.startswith("/"):
        full_url = "https://www.ubereats.com" + restaurant_url
    else:
        full_url = restaurant_url

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.0.0 Safari/537.36'
    }
    
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
        print(f"  [Error HTTP] {e} al scrapear {full_url}", file=sys.stderr)
        return None
    except requests.exceptions.RequestException as e:
        print(f"  [Error] {e} al scrapear {full_url}", file=sys.stderr)
        return None
    except json.JSONDecodeError:
        print(f"  [Error] No se pudo decodificar el JSON de {full_url}", file=sys.stderr)
        return None

def guardar_datos_csv(nuevos_datos_restaurantes, csv_filename):
    """
    Toma la lista de NUEVOS datos de restaurantes, los procesa
    y los AÑADE a un archivo CSV existente (o crea uno nuevo).
    """
    # Asegurarse que el directorio 'data' exista
    os.makedirs(os.path.dirname(csv_filename), exist_ok=True)
    
    print(f"\n--- Actualizando CSV de productos en '{csv_filename}' ---")
    
    csv_headers = [
        'name', 'description', 'price', 'store_name', 
        'category_name', 'category_uber', 'restaurante_url'
    ]

    # 1. Procesar los nuevos datos para obtener filas de productos
    productos_nuevos = []
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
                        productos_nuevos.append(fila_producto)

    if not productos_nuevos:
        print("No se encontraron nuevos productos para añadir al CSV.")
        return

    # 2. Verificar si el archivo ya existe para saber si escribir headers
    archivo_existe = os.path.isfile(csv_filename)

    # 3. Abrir el archivo en modo 'append' (a+)
    try:
        with open(csv_filename, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=csv_headers)
            
            # Si el archivo NO existía, escribir los encabezados
            if not archivo_existe:
                writer.writeheader()
                
            # Escribir las nuevas filas de productos
            writer.writerows(productos_nuevos)
    
        print(f"¡Éxito! Se añadieron {len(productos_nuevos)} nuevos productos a '{csv_filename}'")

    except Exception as e:
        print(f"Error al actualizar el CSV: {e}", file=sys.stderr)

def guardar_datos_json(nuevos_datos_restaurantes, json_filename):
    """
    Toma la lista de NUEVOS JSONs de restaurantes y los AÑADE
    a un archivo JSON existente (o crea uno nuevo).
    """
    # Asegurarse que el directorio 'data' exista
    os.makedirs(os.path.dirname(json_filename), exist_ok=True)
    
    print(f"\n--- Actualizando JSON gigante en '{json_filename}' ---")
    
    datos_existentes = []

    # 1. Intentar leer los datos existentes si el archivo ya existe
    if os.path.isfile(json_filename):
        try:
            with open(json_filename, 'r', encoding='utf-8') as f:
                datos_existentes = json.load(f)
                # Asegurarnos que es una lista
                if not isinstance(datos_existentes, list):
                    datos_existentes = []
        except json.JSONDecodeError:
            print(f"Advertencia: '{json_filename}' contenía JSON inválido. Se sobrescribirá.")
            datos_existentes = []
        except Exception as e:
            print(f"Error al leer JSON existente: {e}. Se sobrescribirá.", file=sys.stderr)
            datos_existentes = []

    # 2. Añadir los nuevos datos a la lista existente
    datos_existentes.extend(nuevos_datos_restaurantes)

    # 3. Guardar la lista combinada
    try:
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(datos_existentes, f, indent=2, ensure_ascii=False)
        print(f"Datos guardados. Total de restaurantes en '{json_filename}': {len(datos_existentes)}")
    except Exception as e:
        print(f"Error al guardar el JSON: {e}", file=sys.stderr)


# ==============================================================================
# BLOQUE DE PRUEBA (`if __name__ == "__main__":`)
# ==============================================================================
if __name__ == "__main__":
    print("--- Ejecutando 'scraper_utils.py' como script de prueba ---")
    
    # --- Definir los nombres de archivo dentro de la carpeta 'data' ---
    JSON_FILE = os.path.join("data", "uber_eats_data.json")
    CSV_FILE = os.path.join("data", "productos.csv")
    
    # URL de prueba
    URL_PRUEBA = "/store/de-pura-madre-rodeo/orlDF_6sWz-P3AmSW47N7Q?diningMode=DELIVERY&surfaceName="
    
    print(f"Probando scrape_menu_restaurante() con {URL_PRUEBA}...")
    start_time = time.time()
    datos_restaurante = scrape_menu_restaurante(URL_PRUEBA)
    end_time = time.time()
    
    if datos_restaurante:
        print(f"[ÉXITO] Se scrapeó '{datos_restaurante.get('name')}' en {end_time - start_time:.2f} segundos.")
        
        # Simular que es una lista de nuevos datos
        lista_de_datos_nuevos = [datos_restaurante]
        
        # Probar las funciones de guardado (ahora añadirán datos a los archivos)
        guardar_datos_json(lista_de_datos_nuevos, JSON_FILE)
        guardar_datos_csv(lista_de_datos_nuevos, CSV_FILE)
        
        print(f"\nPrueba completada. Revisa '{JSON_FILE}' y '{CSV_FILE}'.")
        print("Si ejecutas esto de nuevo, los datos se duplicarán (modo 'append').")
    else:
        print("[FALLO] La prueba de scrape_menu_restaurante() no devolvió datos.")