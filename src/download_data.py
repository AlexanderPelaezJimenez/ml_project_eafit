import os
import requests
import json
import time

def descargar_todos_los_datos(carpeta_destino, nombre_archivo="saber11_raw.json"):
    if not os.path.exists(carpeta_destino):
        os.makedirs(carpeta_destino)

    ruta_completa = os.path.join(carpeta_destino, nombre_archivo)
    url_base = "https://www.datos.gov.co/resource/kgxf-xxbe.json"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/json"
    }

    limite = 50000
    offset = 0
    todos_los_datos = []

    print("🚀 Iniciando descarga masiva con sistema de reintentos (Anti-Caídas)...")

    while True:
        url_paginada = f"{url_base}?$limit={limite}&$offset={offset}"
        print(f"⏳ Descargando bloque (Filas {offset} a {offset + limite})...")
        
        exito_bloque = False
        
        # Sistema de reintentos: Intentará 5 veces descargar el mismo bloque si hay un fallo
        for intento in range(5):
            try:
                # El timeout evita que se quede colgado eternamente
                respuesta = requests.get(url_paginada, headers=headers, timeout=60)
                respuesta.raise_for_status()
                
                lote = respuesta.json()
                exito_bloque = True
                break  # ¡Funcionó! Rompemos el ciclo de reintentos
                
            except Exception as e:
                print(f"   ⚠️ Falla en la conexión (Intento {intento + 1}/5): {e}")
                print("   🔄 Esperando 5 segundos antes de reintentar...")
                time.sleep(5)  # Pausa antes de volver a intentar
                
        # Si después de 5 intentos no se logró, abortamos el ciclo principal
        if not exito_bloque:
            print(f"\n❌ Abortando después de 5 intentos fallidos en el bloque {offset}.")
            break
            
        # Si el lote llega vacío, significa que ya descargamos todos los datos que existen
        if not lote:
            print("✅ Descarga finalizada: Ya no hay más registros en el servidor.")
            break
            
        todos_los_datos.extend(lote)
        offset += limite
        time.sleep(1)  # Pausa de cortesía para no saturar la API

    print(f"\n💾 Guardando {len(todos_los_datos)} registros en tu disco duro...")
    try:
        with open(ruta_completa, 'w', encoding='utf-8') as archivo:
            json.dump(todos_los_datos, archivo, ensure_ascii=False)
        print(f"🎉 ¡Éxito total! Archivo maestro guardado en:\n   👉 {os.path.abspath(ruta_completa)}")
    except Exception as e:
         print(f"❌ Error al guardar el archivo: {e}")

# ==========================================
# EJECUCIÓN DIRECTA
# ==========================================
if __name__ == "__main__":
    carpeta_salida = os.path.join("data", "raw") 
    descargar_todos_los_datos(carpeta_salida)