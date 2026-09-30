# Tarea 3 — Beam avanzado

## Streaming de datos y sus aplicaciones

**Universidad Nacional de Asunción**  
**Maestría en Inteligencia Artificial y Análisis de Datos**  
**Alumna:** Rocío Belén Giménez Cuenca

---

## Objetivo

Implementar un pipeline de procesamiento de eventos con **Apache Beam** capaz de tolerar:

- eventos fuera de orden;
- eventos duplicados;
- eventos tardíos;
- reintentos de escritura.

El pipeline calcula el **total confirmado por comercio y por minuto**, utilizando tiempo de evento, ventanas fijas, estado por clave, timers, triggers e idempotencia.

---

## Descripción del problema

Los pagos pueden llegar al sistema en condiciones no ideales:

- un evento puede llegar después de otro que ocurrió más tarde;
- un productor puede reenviar el mismo `event_id`;
- algunos eventos pueden llegar con atraso;
- un sink puede recibir el mismo resultado más de una vez debido a reintentos.

El objetivo es producir resultados correctos aun bajo estas condiciones.

Cada evento contiene, entre otros campos:

```text
event_id
merchant_id
event_time
arrival_time
amount
status
```

Los estados posibles incluyen:

```text
CONFIRMED
PENDING
REJECTED
```

Solo los eventos con estado `CONFIRMED` participan en el cálculo de los totales.

---

# Decisiones de diseño

## 1. Tiempo de evento

Se utiliza el campo:

```text
event_time
```

como timestamp del dominio.

Esto significa que la ventana de un pago depende del momento real en que ocurrió y no del momento en que llegó al sistema.

Por ejemplo:

```text
event_time   = 13:00:42
arrival_time = 13:01:10
```

Aunque el evento llegue después de las 13:01, continúa perteneciendo a la ventana:

```text
[13:00:00, 13:01:00)
```

Esta decisión permite procesar correctamente eventos fuera de orden.

---

## 2. Ventanas

Se utilizan ventanas fijas de:

```text
60 segundos
```

Ejemplo:

```text
[13:00:00, 13:01:00)
[13:01:00, 13:02:00)
[13:02:00, 13:03:00)
```

Cada evento pertenece a una única ventana según su `event_time`.

La agregación se realiza por:

```text
merchant_id + ventana
```

El resultado contiene:

```text
merchant_id
window_start
window_end
total
```

---

## 3. Datos tardíos

La política permite:

```text
120 segundos de allowed lateness
```

Un evento que llega dentro de esa tolerancia todavía puede modificar el resultado de su ventana.

Si un evento llega después del cierre de su ventana, pero continúa dentro de la tolerancia:

```text
accepted = True
revision = True
```

Si el atraso supera el límite permitido:

```text
accepted = False
too_late = True
reason = "too_late"
```

El evento se conserva en la auditoría, pero no modifica el total.

---

## 4. Auditoría de eventos

La función `summarize_payments` genera una auditoría de cada evento procesado.

Cada fila contiene:

```text
event_id
merchant_id
delay_seconds
duplicate
too_late
accepted
revision
reason
```

Esto permite conocer por qué un evento fue aceptado o descartado.

Las razones posibles incluyen:

```text
accepted
duplicate
too_late
not_confirmed
```

---

## 5. Deduplicación

La deduplicación se realiza por:

```text
event_id
```

dentro de cada comercio.

El estado está aislado por:

```text
merchant_id
```

Por lo tanto, estos dos eventos son independientes:

```text
merchant_id = m-a
event_id    = shared
```

```text
merchant_id = m-b
event_id    = shared
```

aunque compartan el mismo `event_id`.

En Apache Beam se utiliza:

```text
SetStateSpec
```

para recordar los identificadores ya procesados.

---

## 6. Expiración del estado

Mantener todos los `event_id` indefinidamente provocaría un crecimiento continuo del estado.

Para evitarlo se utiliza un timer basado en tiempo de evento.

La expiración ocurre en:

```text
fin de ventana + allowed lateness
```

Con una ventana de 60 segundos y una lateness permitida de 120 segundos, el estado permanece disponible únicamente durante el periodo en que todavía podrían aceptarse correcciones.

Cuando vence el timer:

```text
seen_ids.clear()
```

se elimina el estado asociado.

---

## 7. Triggers

La política temporal utiliza tres tipos de disparadores.

### On-time

```text
AfterWatermark
```

El resultado principal se genera cuando el watermark supera el final de la ventana.

### Early

```text
AfterProcessingTime(10)
```

Permite producir una estimación temprana antes del cierre lógico de la ventana.

### Late

```text
AfterCount(1)
```

Cada evento tardío aceptado puede producir una revisión del resultado.

---

## 8. Modo de acumulación

Se utiliza:

```text
ACCUMULATING
```

Esto significa que cada nuevo pane contiene el resultado acumulado actualizado.

Ejemplo:

```text
Early   → total = 100
On-time → total = 150
Late    → total = 180
```

El pane tardío contiene el total completo corregido, no únicamente la diferencia.

---

# Idempotencia y reintentos

## 9. Clave de idempotencia

El sink utiliza una clave de idempotencia construida como:

```text
merchant_id|window_start
```

Ejemplo:

```text
m-a|2026-07-24T13:00:00+00:00
```

Esta clave representa un resultado lógico único.

---

## 10. Reintentos del sink

Se simulan dos tipos de escritura.

### Append-only

Operación:

```text
POST
```

Cada reintento genera una nueva fila.

Ejemplo:

```text
intento 1 → fila 1
intento 2 → fila 2
```

Esto puede producir duplicados visibles.

### Idempotente

Operación:

```text
UPSERT
```

Cada intento utiliza la misma clave lógica:

```text
merchant_id|window_start
```

Por lo tanto:

```text
intento 1 → UPSERT clave X
intento 2 → UPSERT clave X
```

El estado final contiene una única entidad.

La idempotencia no evita los reintentos; hace que repetir el mismo efecto lógico no altere el resultado final.

---

# Trade-offs

## Latencia vs. completitud

Una mayor `allowed lateness` permite incorporar más eventos tardíos y mejora la completitud del resultado.

Sin embargo, también implica:

- mantener estado durante más tiempo;
- utilizar más memoria;
- permitir revisiones tardías durante un periodo mayor.

Una lateness menor:

- reduce el costo;
- permite estabilizar antes los resultados;
- aumenta la posibilidad de descartar eventos tardíos válidos.

Para esta tarea se utilizan:

```text
120 segundos
```

como equilibrio entre completitud y costo.

---

## Acumulación vs. descarte

El modo `ACCUMULATING` facilita la interpretación de las revisiones, porque cada pane representa el estado completo actualizado.

La desventaja es que pueden emitirse varias versiones del mismo resultado lógico.

Por este motivo, la salida debe combinarse con una estrategia idempotente.

---

# Estructura del proyecto

```text
streaming-fpuna-clase6-tarea/
│
├── data/
│   └── payments.jsonl
│
├── tests/
│   ├── conftest.py
│   └── test_assignment.py
│
├── notebook.py
├── README.md
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── pyproject.toml
└── uv.lock
```

---

# Reproducibilidad

## Ejecución con Docker

### Construir la imagen

```bash
docker compose build
```

### Ejecutar las pruebas

```bash
docker compose run --rm notebook uv run pytest
```

Resultado obtenido:

```text
collected 13 items

tests/test_assignment.py ............. [100%]

13 passed in 5.06s
```

---

## Validación de estilo

Ejecutar:

```bash
docker compose run --rm notebook uv run ruff check notebook.py
```

Resultado:

```text
All checks passed!
```

---

## Validación de Marimo

Ejecutar:

```bash
docker compose run --rm notebook uv run marimo check --strict notebook.py
```

Resultado:

```text
Sin errores
```

---

## Ejecutar el notebook Marimo

También puede iniciarse el editor interactivo con:

```bash
docker compose up --build notebook
```

Luego abrir en el navegador:

```text
http://localhost:2718
```

---

# Ejecución con uv

Como alternativa a Docker:

```bash
uv sync --frozen
uv run marimo edit notebook.py
```

Ejecutar las pruebas:

```bash
uv run pytest
```

Validar estilo:

```bash
uv run ruff check notebook.py
```

Validar Marimo:

```bash
uv run marimo check --strict notebook.py
```

---

# Casos verificados por la suite de pruebas

La implementación valida correctamente los siguientes escenarios:

- un duplicado no modifica el total;
- dos comercios distintos no comparten estado;
- un evento fuera de orden utiliza su `event_time`;
- un evento tardío dentro de tolerancia produce una revisión;
- un evento demasiado tardío queda auditado;
- los pagos no confirmados no modifican el total;
- el pipeline agrega correctamente por comercio y ventana;
- la política usa ventanas fijas de 60 segundos;
- la política permite 120 segundos de lateness;
- los panes utilizan modo acumulativo;
- los reintentos idempotentes convergen a una sola entidad;
- un sink append-only materializa cada intento;
- el timer elimina el estado al expirar.

---

# Resultado final

La suite completa fue ejecutada satisfactoriamente:

```text
13 passed in 5.06s
```

La validación de estilo también fue satisfactoria:

```text
All checks passed!
```

La validación estricta del notebook Marimo finalizó sin errores.

Por lo tanto, la implementación cumple con los requisitos de:

- tiempo de evento;
- ventanas;
- eventos fuera de orden;
- datos tardíos;
- deduplicación;
- estado por clave;
- timers;
- triggers;
- panes acumulativos;
- idempotencia;
- reintentos;
- reproducibilidad.

---

# Conclusión

La solución implementada demuestra cómo Apache Beam permite manejar problemas frecuentes de los sistemas de streaming.

El uso de `event_time` garantiza que los eventos sean agregados según el momento real del dominio, mientras que las ventanas fijas permiten calcular totales por minuto.

La combinación de estado por clave y deduplicación evita contabilizar dos veces el mismo evento. Al mismo tiempo, el timer limita el crecimiento del estado.

La política de triggers y lateness permite trabajar con información fuera de orden y eventos tardíos, mientras que el modo acumulativo permite emitir resultados actualizados.

Finalmente, la utilización de una clave de idempotencia permite que los reintentos de escritura converjan hacia un único resultado observable.

La suite de pruebas provista confirma que estas garantías se cumplen correctamente.

---

## Autor

**Rocío Belén Giménez Cuenca**  
Maestría en Inteligencia Artificial y Análisis de Datos  
Universidad Nacional de Asunción