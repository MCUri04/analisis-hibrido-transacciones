import multiprocessing as mp
import threading
import time

# =====================================================================
# CONFIGURACIÓN GLOBAL DEL DATASET (3,500,000 transacciones)
# =====================================================================
TRANSACCIONES_BASE = [100, 600, 250, 800, 50, 450, 900]
MULTIPLICADOR = 500_000
UMBRAL_FRAUDE = 500
NUM_NODOS = 3
NUM_HILOS = 4


def generar_dataset():
    """Genera el dataset masivo de 3.5 millones de transacciones."""
    return TRANSACCIONES_BASE * MULTIPLICADOR


# =====================================================================
# ESCENARIO 1: EJECUCIÓN SECUENCIAL PURA
# =====================================================================
def ejecutar_secuencial(datos):
    """Procesa todo el dataset en un solo proceso y un solo hilo."""
    total = 0
    for transaccion in datos:
        if transaccion <= UMBRAL_FRAUDE:
            total += transaccion
    return total


# =====================================================================
# ESCENARIO 2: EJECUCIÓN DISTRIBUIDA BÁSICA (SOLO PROCESOS)
# =====================================================================
def trabajador_solo_proceso(fragmento):
    """Cada nodo procesa su lote secuencialmente sin hilos internos."""
    return sum(v for v in fragmento if v <= UMBRAL_FRAUDE)


def ejecutar_distribuido(datos, num_nodos):
    """Patrón Scatter-Reduce usando únicamente multiprocessing."""
    lotes = dividir_en_partes_exactas(datos, num_nodos)
    with mp.Pool(processes=num_nodos) as cluster:
        subtotales = cluster.map(trabajador_solo_proceso, lotes)
    return sum(subtotales)


# =====================================================================
# ESCENARIO 3: ARQUITECTURA HÍBRIDA (PROCESOS + HILOS + LOCKS)
# =====================================================================
def dividir_en_partes_exactas(lista, n_partes):
    """
    Divide una lista en 'n_partes' sin perder elementos sobrantes
    por redondeo de división entera.
    """
    longitud = len(lista)
    tamano_base = longitud // n_partes
    partes = []
    for i in range(n_partes):
        inicio = i * tamano_base
        # El último fragmento absorbe el residuo hasta el final
        fin = longitud if i == n_partes - 1 else (i + 1) * tamano_base
        partes.append(lista[inicio:fin])
    return partes


def procesamiento_interno_nodo(fragmento_asignado):
    """
    NIVEL MICRO (Memoria Compartida):
    Cada nodo levanta múltiples hilos para procesar su fragmento
    y sincroniza la escritura del subtotal con un Lock.
    """
    subtotal_nodo = 0
    lock_nodo = threading.Lock()

    def hilo_trabajador(sub_fragmento):
        nonlocal subtotal_nodo
        # Cálculo local fuera del Lock para evitar cuellos de botella
        suma_local = sum(v for v in sub_fragmento if v <= UMBRAL_FRAUDE)

        # Región crítica mínima: solo se bloquea al acumular el resultado
        with lock_nodo:
            subtotal_nodo += suma_local

    sub_fragmentos = dividir_en_partes_exactas(fragmento_asignado, NUM_HILOS)
    hilos = []

    for sub in sub_fragmentos:
        hilo = threading.Thread(target=hilo_trabajador, args=(sub,))
        hilos.append(hilo)
        hilo.start()

    for hilo in hilos:
        hilo.join()

    return subtotal_nodo


def ejecutar_hibrido(datos, num_nodos):
    """
    NIVEL MACRO (Memoria Distribuida):
    Orquestador que ejecuta Scatter -> Procesamiento Híbrido -> Reduce.
    """
    # 1. SCATTER: Fragmentación exacta para los N nodos
    lotes = dividir_en_partes_exactas(datos, num_nodos)

    # 2. MAP DISTRIBUIDO: Envío por paso de mensajes a los nodos
    with mp.Pool(processes=num_nodos) as cluster:
        resultados_nodos = cluster.map(procesamiento_interno_nodo, lotes)

    # 3. REDUCE: Consolidación final en el Orquestador
    return sum(resultados_nodos)


# =====================================================================
# BENCHMARKING Y VALIDACIÓN DE CORRECTNESS
# =====================================================================
if __name__ == "__main__":
    print("Generando dataset de transacciones...")
    dataset = generar_dataset()
    print(f"Total de registros a procesar: {len(dataset):,}\n")

    # --- Prueba 1: Secuencial ---
    t0 = time.perf_counter()
    res_secuencial = ejecutar_secuencial(dataset)
    t_secuencial = time.perf_counter() - t0

    # --- Prueba 2: Distribuido Básico ---
    t0 = time.perf_counter()
    res_distribuido = ejecutar_distribuido(dataset, NUM_NODOS)
    t_distribuido = time.perf_counter() - t0

    # --- Prueba 3: Híbrido Óptimo ---
    t0 = time.perf_counter()
    res_hibrido = ejecutar_hibrido(dataset, NUM_NODOS)
    t_hibrido = time.perf_counter() - t0

    # Validación matemática (Correctness)
    assert res_secuencial == res_distribuido == res_hibrido, "¡Error! Los totales no coinciden."

    # Cálculo de Speedup (S = T_secuencial / T_paralelo)
    speedup_distribuido = t_secuencial / t_distribuido
    speedup_hibrido = t_secuencial / t_hibrido

    print("=== RESULTADOS DE BENCHMARKING ===")
    print(f"Resultado válido verificado: ${res_hibrido:,}\n")
    print(f"1. Secuencial puro     : {t_secuencial:.4f} s | Speedup: 1.00x")
    print(f"2. Distribuido (3 proc): {t_distribuido:.4f} s | Speedup: {speedup_distribuido:.2f}x")
    print(f"3. Híbrido (3p x 4h)   : {t_hibrido:.4f} s | Speedup: {speedup_hibrido:.2f}x")