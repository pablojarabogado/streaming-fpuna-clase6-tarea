# Tarea 3 — Beam avanzado

Proyecto base autocontenido para la asignatura **Streaming de datos y sus
aplicaciones**. La tarea consiste en completar un pipeline de pagos con tiempo
de evento, ventanas, estado por clave y una salida idempotente.

El repositorio es deliberadamente un esqueleto: `notebook.py` contiene la
consigna, contratos y funciones sin implementación. No incluye la solución.

## Objetivo

Producir totales confirmados por comercio y minuto:

- usando `event_time`, no el tiempo de llegada;
- tolerando hasta 120 segundos de atraso;
- descartando estados distintos de `CONFIRMED`;
- deduplicando `event_id` dentro de cada comercio;
- conservando metadatos de ventana y pane;
- materializando la salida mediante una clave idempotente.

## Ejecutar con Docker

Desde este directorio:

```bash
docker compose up --build notebook
```

Abrir <http://localhost:2718>. Docker inicia Marimo en modo editor porque la
tarea requiere completar las celdas de código. Los cambios en `notebook.py` se
guardan en el directorio local.

El editor usa `--no-token` para simplificar el trabajo en `localhost`; no debe
exponerse directamente a una red pública.

## Ejecutar con uv

```bash
uv sync --frozen
uv run marimo edit notebook.py
```

## Trabajar con tests

```bash
uv run pytest
```

Los tests se entregan deliberadamente en rojo: las funciones del notebook
lanzan `NotImplementedError`. El objetivo es implementar las celdas hasta
obtener una suite completamente verde.

Los tests cargan las funciones directamente desde `notebook.py`; no hay que
copiar la solución a otro módulo.

Para validar además estilo y estructura:

```bash
uv run ruff check notebook.py
uv run marimo check --strict notebook.py
```

Dentro del contenedor también se puede ejecutar:

```bash
docker compose exec notebook uv run pytest
```

## Entrega

Entregar un repositorio propio que incluya:

- `notebook.py` con todas las funciones implementadas;
- evidencia de ejecución del pipeline;
- todas las pruebas provistas para desorden, duplicados, atraso y reintentos
  ejecutadas y aprobadas;
- un README breve con decisiones y trade-offs;
- instrucciones reproducibles con Docker o `uv`.

No modificar `data/payments.jsonl`; puede agregarse un conjunto de datos
adicional para las pruebas.

## Decisiones y Trade-offs:
## Decisiones de diseño
1. Tiempo de evento (parse_utc)
Se usa event_time como timestamp del dominio, no arrival_time.

Todos los timestamps se normalizan a UTC timezone-aware.

Se rechazan valores inválidos con ValueError en lugar de None para forzar al llamador a manejar el error.

Trade-off: ser estricto con los formatos evita datos silenciosamente incorrectos, pero obliga a que el dataset sea consistente. Si el dataset tuviera formatos mixtos, habría que agregar fallbacks.

2. Ventanas fijas de 60s (assign_fixed_window)
Se alinean al epoch UNIX (múltiplos exactos de 60s).

Se devuelven [start, end) para dejar claro que el fin es exclusivo.

Trade-off: alinear al epoch es simple y determinista, pero no respeta zonas horarias locales (por ejemplo, "minutos de pared" en Paraguay). Para esta tarea es suficiente y reproducible.

3. Lateness de 120s
Un evento con arrival_time > window_end + 120s se marca como too_late.

Un evento aceptado que llega después del cierre lógico pero dentro de la lateness se marca revision=True.

Trade-off: 120s cubre el caso del dataset, pero es un valor fijo. En producción convendría hacerlo configurable por comercio o por tipo de pago.

4. Deduplicación (DeduplicatePayments)
Se deduplica por (merchant_id, event_id).

El estado se maneja con SetStateSpec, expuesto por clave de comercio.

La clave se fuerza con un GroupByKey antes del ParDo stateful.

Trade-off: SetStateSpec es eficiente para cardinalidades bajas, pero crece indefinidamente si no se expira. Por eso se usa un TimerSpec de event time que limpia el estado al final de la ventana + lateness.

Alternativa descartada: usar un dict en memoria del worker. No funciona en runners distribuidos y no sobrevive a reintentos.

5. Trigger policy
Se compone con AfterWatermark(early=..., late=...).

early: disparo aproximado por processing time.

late: revisiones cuando llega un late aceptado.

AccumulationMode.ACCUMULATING para que cada pane contenga el total acumulado.

Trade-off: ACCUMULATING es más caro (recalcula el total completo en cada pane) pero es el modo que pide el enunciado y el más seguro para el consumidor downstream (no tiene que sumar panes). DISCARDING sería más liviano pero rompería el contrato de "total por ventana".

6. Idempotencia (make_idempotency_key + simulate_sink_retries)
Clave lógica: merchant_id|window_start.

Modo UPSERT: dict[key] = row → múltiples intentos dejan una fila.

Modo append-only: list.append(row) → múltiples intentos dejan N filas.

Trade-off: el sink idempotente requiere que el downstream soporte UPSERT (BigQuery, Postgres, etc.). Si el sink es append-only (Kafka, GCS), se necesita una capa de deduplicación posterior.

7. Separación "definiciones" vs "demos"
Las celdas de definición solo contienen def + return.

Las celdas de demo/prueba están al final, con nombres únicos (events_summary, events_pipeline, etc.) para evitar el warning de marimo.

Trade-off: más celdas y más verboso, pero el notebook queda legible y las pruebas se pueden borrar sin tocar la lógica.
