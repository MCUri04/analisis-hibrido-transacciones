# Análisis Híbrido de Transacciones

**Materia:** Cómputo de Alto Rendimiento (E-CAR-3)  
**Unidad III:** Paralelización — Proyecto Integrador 
**Alumno;** Uriel Mota Casta;eda

---

## Descripción del Proyecto

Es como si tienes una caja con 3.5 millones de recibos bancarios y debes sumar todos los recibos válidos (menores o iguales a $500), ignorando los fraudulentos (mayores a $500).

Nivel Macro — Memoria Distribuida (Procesos): Es como contratar 3 sucursales independientes (3 procesos de Python separados que no comparten memoria entre sí). El jefe (Orquestador) reparte la caja de recibos en 3 paquetes iguales (Scatter), se los envía a cada sucursal y al final suma los 3 resultados (Reduce).

Nivel Micro — Memoria Compartida (Hilos): Dentro de cada sucursal hay 4 empleados (4 hilos) trabajando en la misma mesa y anotando el resultado en la misma libreta (subtotal_nodo). Como todos escriben en la misma libreta, usan un candado (Lock) para que no escriban encima del número del otro al mismo tiempo.

### Arquitectura Implementada

1. **Nivel Macro — Memoria Distribuida (`multiprocessing`):**
   * Un proceso **Orquestador** implementa el patrón **Scatter-Reduce**.
   * Divide el dataset global de manera exacta en **3 lotes independientes** (*Scatter*) y los envía mediante paso de mensajes a un clúster de 3 procesos aislados (`multiprocessing.Pool`).
   * Finalmente, consolida los subtotales devueltos por cada nodo (*Reduce*).

2. **Nivel Micro — Memoria Compartida (`threading` + `Lock`):**
   * Dentro de cada nodo distribuido se levantan **4 hilos de ejecución** (`threading.Thread`) que comparten la memoria local del proceso.
   * Para garantizar la integridad de los datos (*Correctness*) y evitar condiciones de carrera, cada hilo calcula su suma parcial de forma privada y utiliza exclusión mutua mediante `threading.Lock` únicamente al acumular su resultado en la variable compartida del nodo (`subtotal_nodo`), evitando cuellos de botella.

3. **Benchmarking y Métricas de Rendimiento:**
   * El programa mide y compara los tiempos de ejecución y el factor de aceleración ($\text{Speedup} = \frac{T_{\text{secuencial}}}{T_{\text{paralelo}}}$) en tres escenarios:
     1. **Secuencial puro:** Bucle `for` tradicional en un solo proceso y un solo hilo.
     2. **Distribuido básico:** Únicamente multiprocesamiento (3 procesos sin hilos internos).
     3. **Híbrido óptimo:** Arquitectura anidada completa (3 procesos $\times$ 4 hilos locales con `Lock`).

---
## Salida Esperada

Al finalizar la ejecución, el programa valida automáticamente mediante aserciones (`assert`) que los tres métodos obtengan exactamente el mismo resultado matemático antes de mostrar la tabla de tiempos:

```text
Generando dataset de transacciones...
Total de registros a procesar: 3,500,000

=== RESULTADOS DE BENCHMARKING ===
Resultado válido verificado: $425,000,000

1. Secuencial puro     : 0.1450 s | Speedup: 1.00x
2. Distribuido (3 proc): 0.0680 s | Speedup: 2.13x
3. Híbrido (3p x 4h)   : 0.0710 s | Speedup: 2.04x
```