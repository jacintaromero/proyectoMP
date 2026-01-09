#extracción_productos_paralelizado
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import threading
from pandas.errors import EmptyDataError


# URL base
base_url = "https://conveniomarco2.mercadopublico.cl/alimentos2/alimentos"

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

session = requests.Session()
session.headers.update(headers)

# Definir archivo de salida

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
output_file = os.path.join(BASE_DIR, 'productos_convenio_marco_prueba.csv')

def get_total_products(url):
    """Extrae el total de productos de la página"""
    
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Buscar el span con clase "toolbar-number" que contiene el total
        toolbar_spans = soup.find_all('span', class_='toolbar-number')
        if len(toolbar_spans) >= 2:
            # El segundo span contiene el total (7459 en tu caso)
            return int(toolbar_spans[1].get_text().strip())
            
    except Exception as e:
        print(f"Error al obtener total de productos: {e}")
        return None
    
    return None


def scrape_products_page(page_num):
    """Extrae información de productos de una página específica"""
    url = f"{base_url}?p={page_num}&product_list_limit=25&product_list_mode=list&product_list_order=name"
    
    products_data = []
    
    try:
        print(f"Extrayendo página {page_num}...")
        response = session.get(url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Encontrar todos los productos con class="item product product-item"
        products = soup.find_all('li', class_='item product product-item')
        
        for product in products:
            try:
                # Nombre del producto - CORREGIDO: está en <a class="product-item-link">
                product_name_elem = product.find('a', class_='product-item-link')
                product_name = product_name_elem.get_text().strip() if product_name_elem else ''
                
                # Link del producto - también está en el mismo elemento
                product_link = product_name_elem.get('href', '') if product_name_elem else ''
                
                # Número de proveedores (desde div con class="sellers-count")
                sellers_elem = product.find('div', class_='sellers-count')
                num_providers = ''
                if sellers_elem:
                    sellers_text = sellers_elem.get_text().strip()
                    # Extraer solo el número (ej: "78 proveedores" -> "78")
                    match = re.search(r'(\d+)', sellers_text)
                    if match:
                        num_providers = match.group(1)
                
                # ID del producto (desde div con class="product-id-top")
                product_id_elem = product.find('div', class_='product-id-top')
                product_id = ''
                if product_id_elem:
                    id_text = product_id_elem.get_text().strip()
                    match = re.search(r'ID\s+(\d+)', id_text)
                    if match:
                        product_id = match.group(1)
                
                products_data.append({
                    'ID_Producto': product_id,
                    'Nombre_Producto': product_name,
                    'Numero_Proveedores': num_providers,
                    'Link_Producto': product_link,
                    'Pagina': page_num
                })
                
            except Exception as e:
                print(f"Error al procesar un producto en página {page_num}: {e}")
                continue
        
        return products_data
        
    except Exception as e:
        print(f"Error al procesar página {page_num}: {e}")
        return []


# SCRIPT PRINCIPAL
if __name__ == "__main__":

    # MEDIR TIEMPO DE EJECUCIÓN
    # COMIENZO TIEMPO
    start_time = time.time()

    MAX_PAGES_TEST = 999999  # Cambia este número para probar con más o menos páginas

    # Contador de registros de nuevos productos del catálogo (si aplica)
    total_nuevos = 0 

    # Paso 1: Obtener el total de productos desde la primera página
    print("=" * 60)
    print("EXTRACCIÓN DE PRODUCTOS - CONVENIO MARCO ALIMENTOS")
    print("=" * 60)
    print(f"\nExtrayendo páginas")
    print("\n[1/3] Obteniendo total de productos...")
    
    first_page_url = f"{base_url}?p=1&product_list_limit=25&product_list_mode=list&product_list_order=name"
    total_products = get_total_products(first_page_url)
    
    if total_products:
        print(f"✓ Total de productos encontrados: {total_products}")
        
        # Calcular total de páginas (total_products / 25, redondeado hacia arriba)
        total_pages = (total_products + 24) // 25
        print(f"✓ Total de páginas disponibles: {total_pages}")
        
        # Limitar a MAX_PAGES_TEST para prueba
        pages_to_extract = min(MAX_PAGES_TEST, total_pages)
        print(f"✓ Páginas a extraer en esta prueba: {pages_to_extract}")
        
        lock = threading.Lock()

        processed_ids = set()

        if os.path.exists(output_file):
            try:
                df_existente = pd.read_csv(output_file, usecols=["ID_Producto"])
                processed_ids = set(df_existente["ID_Producto"].astype(str))
                print(f"✓ IDs cargados desde CSV existente: {len(processed_ids)}")
            except EmptyDataError:
                print("⚠️ El CSV existe pero está vacío. Se procesará desde cero.")
            except ValueError:
                print("⚠️ El CSV no contiene la columna ID_Producto. Se procesará desde cero.")


        # Paso 2: Extraer información de las primeras páginas
        
        # PARTE NUEVA
        # PARÁMETROS DE PARALELIZACIÓN
        # prueba1
        MAX_WORKERS = 5  # 3–7 recomendado
        DELAY_BETWEEN_REQUESTS = 0.5  # segundos (suave)

        print(f"\n[2/3] Extrayendo información de {pages_to_extract} páginas (paralelo)...\n")


        all_new_products = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(scrape_products_page, page): page
                for page in range(1, pages_to_extract + 1)
            }

            for future in as_completed(futures):
                page = futures[future]
                try:
                    products = future.result()

                    # INICIO PARTE NUEVA DEDUPLICACIÓN
                    with lock:
                        for p in products:
                            pid = p["ID_Producto"]
                            if pid and pid not in processed_ids:
                                processed_ids.add(pid)
                                all_new_products.append(p)
                                total_nuevos += 1
                        
                    #FIN PARTE NUEVA DEDUPLICACIÓN

                    print(f"  ✓ Página {page}/{pages_to_extract} completada - {len(products)} productos")
                except Exception as e:
                    print(f"  ✗ Error en página {page}: {e}")

                time.sleep(DELAY_BETWEEN_REQUESTS)
        # FIN PARTE NUEVA
        if all_new_products:
            df_nuevos = pd.DataFrame(all_new_products)

            if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                df_nuevos.to_csv(
                    output_file,
                    mode="a",
                    header=False,
                    index=False,
                    encoding="utf-8-sig"
                )
            else:
                df_nuevos.to_csv(
                    output_file,
                    index=False,
                    encoding="utf-8-sig"
                )

            print(f"✓ Se agregaron {len(df_nuevos)} productos nuevos al CSV")
        else:
            print("✓ No se encontraron productos nuevos")


        # Paso 3: Crear DataFrame y guardar
        print(f"\n[3/3] Finalizando proceso...")
        print(f"✓ Datos guardados en '{output_file}'")

        #no borrar
        #print(f"\nPrimeros 10 productos extraídos:")
        #print(df[['ID_Producto', 'Nombre_Producto', 'Numero_Proveedores']].head(10))
        #fin no borrar

        #PARTE NUEVA (MEJORA: MOSTRAR RESUMEN)
        print("\n" + "=" * 60)
        print("RESUMEN DE EXTRACCIÓN")
        print("=" * 60)

        if os.path.exists(output_file):
            df_final = pd.read_csv(output_file)

            print(f"Total de productos guardados: {len(df_final)}")
            print(f"Productos únicos (por ID): {df_final['ID_Producto'].nunique()}")
            print(f"Páginas procesadas: {pages_to_extract} de {total_pages} disponibles")

            if total_nuevos == 0:
                print("✓ No se encontraron productos nuevos. El archivo ya estaba actualizado.")
            else:
                print(f"✓ Se agregaron {total_nuevos} productos nuevos al CSV.")
        else:
            print("✗ No se pudo generar el archivo de salida.")
        
        #FIN PARTE NUEVA


        end_time = time.time()
        print(f"\n⏱️ Tiempo total de ejecución: {end_time - start_time:.2f} segundos")
        
    else:
        print("✗ No se pudo obtener el total de productos. Verifica la URL o la conexión.")

