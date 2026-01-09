import requests 
import pandas
import json
import time
import unicodedata
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

start_time = time.time()

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

session = requests.Session()
session.headers.update(headers)

def extract_json_object_by_key(html_content, key_name):
    """
    Robustly extracts a JSON object from HTML by searching for a key (e.g., "jsonResult")
    and finding the matching closing brace.
    """
    idx_key = html_content.find(key_name)
    
    if idx_key == -1:
        idx_key = html_content.find(f'"{key_name}"')
    
    if idx_key == -1:
        return None

    # Find the first opening brace after the key
    idx_open_brace = html_content.find('{', idx_key)
    
    if idx_open_brace == -1:
        return None

    brace_count = 0
    idx_end_brace = -1
    # Limit search to avoid freezing on massive files
    search_limit = min(idx_open_brace + 500000, len(html_content))
    
    for i in range(idx_open_brace, search_limit):
        if html_content[i] == '{':
            brace_count += 1
        elif html_content[i] == '}':
            brace_count -= 1
            if brace_count == 0:
                idx_end_brace = i + 1
                break
    
    if idx_end_brace != -1:
        json_str = html_content[idx_open_brace:idx_end_brace]
        try:
            # Clean standard JS/JSON formatting issues
            json_str_clean = json_str.replace('\n', '').replace('\r', '').replace('\\"', '"')
            return json.loads(json_str_clean)
        except json.JSONDecodeError:
            try:
                # Fallback: Replace single quotes if valid JSON fails
                return json.loads(json_str.replace("'", '"'))
            except:
                pass
                
    return None

def extract_product_id(html_content):
    """
    Extracts the main Product ID, required to lookup offers in offerPrices.
    """
    match = re.search(r'"productId"\s*:\s*"(\d+)"', html_content)
    if match:
        return match.group(1)
    return None

def clean_column_name(region_name):
    """
    Standardizes region names for Excel columns.
    """
    nfkd_form = unicodedata.normalize('NFKD', region_name)
    cleaned = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    cleaned = cleaned.replace("Region de ", "").replace("Region del ", "")
    cleaned = cleaned.replace(' ', '_').replace('-', '_').replace('.', '')
    return f"Precio_{cleaned}"

def clean_price_value(value):
    """
    Converts price string (e.g., '15,167.00') to integer (15167).
    """
    if not value:
        return 0
    try:
        if isinstance(value, str):
            # Remove comma (thousands) and currency symbol. Keep dot for float conversion.
            clean_val = value.replace(',', '').replace('$', '').strip()
            if not clean_val:
                return 0
            return int(float(clean_val))
        else:
            return int(float(value))
    except (ValueError, TypeError):
        return 0

def get_minimum_price_by_region_with_offers(json_prices, offer_prices, product_id, region_names_map):
    """
    Calculates min price per region, prioritizing 'special_price' from offerPrices if available.
    """
    if not json_prices:
        return {}
    
    precios_finales = {}
    
    # jsonResult keys are Region IDs
    for region_id, providers in json_prices.items():
        # Get real region name
        nombre_region_real = region_names_map.get(region_id, f"Region_ID_{region_id}")
        
        if not isinstance(providers, dict):
            continue
            
        lista_precios = []
        
        for provider_id, data in providers.items():
            if not isinstance(data, dict):
                continue
                
            # 1. Get Standard Price from jsonResult
            price_raw = data.get('price', '0')
            price_final = clean_price_value(price_raw)
            
            # 2. Check for Special Price in offerPrices
            # Structure: offerPrices[ProviderID][ProductID][RegionID]['special_price']
            if offer_prices and product_id:
                try:
                    provider_offers = offer_prices.get(str(provider_id))
                    if provider_offers:
                        product_offers = provider_offers.get(str(product_id))
                        if product_offers:
                            region_offer = product_offers.get(str(region_id))
                            if region_offer:
                                special_price_raw = region_offer.get('special_price')
                                if special_price_raw: # If special price exists
                                    special_price = clean_price_value(special_price_raw)
                                    if special_price > 0:
                                        price_final = special_price
                except Exception:
                    pass # Fail silently and use standard price
            
            if price_final > 0:
                lista_precios.append(price_final)
        
        if lista_precios:
            precios_finales[nombre_region_real] = min(lista_precios)
            
    return precios_finales

#nueva función
def procesar_producto(row):
    producto_id_csv = row['ID_Producto']
    nombre = row['Nombre_Producto']
    link = row['Link_Producto']

    try:
        raw_prov = row.get('Numero_Proveedores', 0)
        if pandas.isna(raw_prov):
            num_providers = 0
        else:
            num_providers = int(float(raw_prov))
    except:
        num_providers = 0
    
    print(f"→ Procesando ID {producto_id_csv}")

    try:
        response = session.get(link, timeout=30)
        if response.status_code != 200:
            return None

        html = response.text

        region_names_map = extract_json_object_by_key(html, "region_names") \
            or extract_json_object_by_key(html, "regionMapping") \
            or {}

        product_id_internal = extract_product_id(html)

        json_prices = extract_json_object_by_key(html, "jsonResult")
        offer_prices = extract_json_object_by_key(html, "offerPrices")

        if not json_prices:
            return None

        precios_region = get_minimum_price_by_region_with_offers(
            json_prices, offer_prices, product_id_internal, region_names_map
        )

        if not precios_region:
            return None

        precio_global = int(min(precios_region.values()))
        mejor_region = min(precios_region, key=precios_region.get)

        row_data = {
            'ID_Producto': producto_id_csv,
            'Nombre_Producto': nombre,
            'Numero_Proveedores': num_providers,
            'Link_Producto': link,
            'Precio_Minimo_Global': precio_global,
            'Region_Mejor_Precio': mejor_region
        }

        for region, precio in precios_region.items():
            col_name = clean_column_name(region)
            row_data[col_name] = precio

        time.sleep(1)  # respeto al servidor
        return row_data

    except Exception:
        return None
#fin nueva función

def process_products_with_prices(csv_file, max_products=3):
    """
    Main processing function.
    """
    print("=" * 70)
    print("EXTRACCIÓN DE PRECIOS CON OFERTAS")
    print("=" * 70)
    try:
        df = pandas.read_csv(csv_file)
    except FileNotFoundError:
        print(f"✗ Error: No se encontró {csv_file}")
        return None
    
    # MODO PRUEBA
    df_test = df.head(max_products)
    print(f"✓ Procesando {len(df_test)} productos...\n")

    from concurrent.futures import ThreadPoolExecutor, as_completed

    MAX_WORKERS = 4  # seguro

    resultados = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(procesar_producto, row)
            for _, row in df_test.iterrows()
        ]
        j = 1
        for future in as_completed(futures):
            result = future.result()
            print(j)
            j += 1
            if result:
                resultados.append(result)
        
    if resultados:
        print(f"\n[4/4] Guardando {len(resultados)} productos...")
        df_final = pandas.DataFrame(resultados)
        
        # Sort columns
        cols_fijas = ['ID_Producto', 'Nombre_Producto', 'Numero_Proveedores', 'Link_Producto', 'Precio_Minimo_Global', 'Region_Mejor_Precio']
        cols_precios = sorted([c for c in df_final.columns if c not in cols_fijas])
        
        # Select valid columns
        final_cols = [c for c in cols_fijas if c in df_final.columns] + cols_precios
        df_final = df_final[final_cols]
        
        output_file = 'productos_precios_ofertas_v4.csv'
        df_final.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"✓ Listo: {output_file}")
        return df_final
    else:
        print("✗ No se generaron resultados.")
        return None

if __name__ == "__main__":
    print(__name__)
    archivo_entrada = 'productos_convenio_marco_prueba.csv'
    # Change max_products to process full list
    process_products_with_prices(archivo_entrada, max_products=999999)

    end_time = time.time()
    print(f"\n⏱ Tiempo total de ejecución: {end_time - start_time:.2f} segundos")
