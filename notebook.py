import marimo

__generated_with = "0.24.2"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import datetime, timedelta, timezone
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms import window as beam_window
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.trigger import (
        AccumulationMode,
        AfterProcessingTime,
        AfterWatermark,
    )
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )
    import apache_beam as beam
    from apache_beam.transforms import trigger

    print("Beam:", beam.__version__)   
 

    return (
        AccumulationMode,
        AfterProcessingTime,
        AfterWatermark,
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        beam_window,
        datetime,
        mo,
        on_timer,
        timedelta,
        timezone,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado
    ...
    """)
    return


@app.cell
def _(datetime, timezone):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC."""
        if not isinstance(raw_value, str):
            raise TypeError(
                f"parse_utc espera str, recibió {type(raw_value).__name__}"
            )
        value = raw_value.strip()
        if not value:
            raise ValueError("parse_utc recibió una cadena vacía")
        # Reemplazar la 'Z' final por el offset UTC explícito
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(
                f"Timestamp inválido {raw_value!r}: {exc}"
            ) from exc
        if parsed.tzinfo is None:
            raise ValueError(
                f"Timestamp sin zona horaria: {raw_value!r}"
            )
        return parsed.astimezone(timezone.utc)

    return (parse_utc,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento...
    """)
    return


@app.cell
def _(datetime, timedelta):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija."""
        if size_seconds <= 0:
            raise ValueError("size_seconds debe ser positivo")

        epoch = datetime(1970, 1, 1, tzinfo=timestamp.tzinfo)
        delta = timestamp - epoch
        offset_seconds = int(delta.total_seconds())
        start_offset = (offset_seconds // size_seconds) * size_seconds
        window_start = epoch + timedelta(seconds=start_offset)
        window_end = window_start + timedelta(seconds=size_seconds)
        return window_start, window_end

    return (assign_fixed_window,)


@app.cell
def _(Any, Iterable, assign_fixed_window, datetime, parse_utc, timedelta):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento."""

        # Estructuras auxiliares
        totals_state: dict[tuple[str, datetime], float] = {}
        seen_ids: dict[str, set[str]] = {}
        # Para detectar revisions: registramos cuándo "cerró" cada ventana
        # (watermark = window_end + allowed_lateness)
        window_close: dict[tuple[str, datetime], datetime] = {}

        audit: list[dict[str, Any]] = []

        for event in events:
            event_id = event.get("event_id")
            merchant_id = event.get("merchant_id")

            try:
                event_time = parse_utc(event["event_time"])
                arrival_time = parse_utc(event["arrival_time"])
            except (KeyError, ValueError) as exc:
                audit.append(
                    {
                        "event_id": event_id,
                        "merchant_id": merchant_id,
                        "delay_seconds": None,
                        "duplicate": False,
                        "too_late": False,
                        "accepted": False,
                        "revision": False,
                        "reason": f"invalid_event: {exc}",
                    }
                )
                continue

            delay_seconds = int(
                (arrival_time - event_time).total_seconds()
            )

            window_start, window_end = assign_fixed_window(
                event_time, window_seconds
            )
            watermark = window_end + timedelta(
                seconds=allowed_lateness_seconds
            )
            # La ventana se considera "cerrada" cuando el arrival supera
            # window_end + lateness
            window_closed = arrival_time > watermark

            reason: str | None = None
            duplicate = False
            too_late = False
            accepted = False

            # Sólo CONFIRMED participa en el total, pero igual auditamos
            # todos los eventos.
            if event.get("status") != "CONFIRMED":
                reason = "not_confirmed"
            else:
                # Deduplicación por (merchant_id, event_id)
                merchant_seen = seen_ids.setdefault(merchant_id, set())
                if deduplicate and event_id in merchant_seen:
                    duplicate = True
                    reason = "duplicate"
                elif window_closed:
                    too_late = True
                    reason = "too_late"
                else:
                    accepted = True
                    merchant_seen.add(event_id)
                    key = (merchant_id, window_start)
                    totals_state[key] = totals_state.get(key, 0.0) + float(
                        event.get("amount", 0.0)
                    )
                    window_close[key] = watermark
                    reason = "accepted"

            revision = bool(
                accepted
                and window_start in (window_start,)
                and window_closed
                and not too_late
            )
            # Revision: un evento aceptado llegó después del cierre
            # lógico de la ventana (arrival > watermark), pero dentro de
            # lateness — es decir, sigue siendo aceptado y dispara una
            # revisión del pane. Según el enunciado: "un late aceptado
            # tiene accepted=True y revision=True".
            if accepted and window_closed:
                revision = True

            audit.append(
                {
                    "event_id": event_id,
                    "merchant_id": merchant_id,
                    "delay_seconds": delay_seconds,
                    "duplicate": duplicate,
                    "too_late": too_late,
                    "accepted": accepted,
                    "revision": revision,
                    "reason": reason,
                }
            )

        # Construir filas de totales
        totals: list[dict[str, Any]] = []
        for (merchant_id, window_start), total in totals_state.items():
            window_end = window_start + timedelta(seconds=window_seconds)
            totals.append(
                {
                    "merchant_id": merchant_id,
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "total": total,
                }
            )
        totals.sort(key=lambda r: (r["merchant_id"], r["window_start"]))

        return totals, audit

    return (summarize_payments,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista...
    """)
    return


@app.cell
def _(Any, beam, beam_window, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana."""

        def _to_kv(event: dict[str, Any]):
            ts = parse_utc(event["event_time"])
            return beam.transforms.window.TimestampedValue(
                (event["merchant_id"], float(event.get("amount", 0.0))),
                ts.timestamp(),
            )

        def _is_confirmed(event: dict[str, Any]) -> bool:
            return event.get("status") == "CONFIRMED"

        confirmed = (
            pipeline
            | "CreateEvents" >> beam.Create(events)
            | "FilterConfirmed" >> beam.Filter(_is_confirmed)
            | "ToTimestampedKV" >> beam.Map(_to_kv)
        )

        windowed = confirmed | "WindowInto" >> beam.WindowInto(
            beam_window.FixedWindows(window_seconds)
        )

        totals = windowed | "SumByMerchant" >> beam.CombinePerKey(sum)

        return totals

    return


@app.cell
def _(
    Any,
    SetStateSpec,
    StrUtf8Coder,
    TimeDomain,
    TimerSpec,
    beam,
    on_timer,
    timedelta,
):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio."""

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparición."""
            key, event = element
            event_id = event.get("event_id")
            if event_id is None:
                return
            current = set(seen_ids.read())
            if event_id in current:
                return  # duplicado: no emitir
            current.add(event_id)
            seen_ids.clear()
            for existing in current:
                seen_ids.add(existing)

            # Programar expiración al final de la ventana + 120s de lateness
            #expiry.set(window.end + timedelta(seconds=120))
            expiry.set(window.end + 120)

            yield element

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time."""
            seen_ids.clear()

    return


@app.cell
def _(AccumulationMode, AfterProcessingTime, AfterWatermark, Any, beam):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformación WindowInto para streaming."""
        
        trigger = AfterWatermark(
            early=AfterProcessingTime(delay=60),
            late=AfterProcessingTime(delay=60),
        )

        return beam.WindowInto(
            beam.window.FixedWindows(window_seconds),
            trigger=trigger,
            accumulation_mode=AccumulationMode.ACCUMULATING,
            allowed_lateness=allowed_lateness_seconds,
        )

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers...
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado lógico."""
        return f"{result['merchant_id']}|{result['window_start']}"

    return (make_idempotency_key,)


@app.cell
def _(Any, make_idempotency_key):
    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar `(materialized, audit)`."""
        append_sink: list[dict[str, Any]] = []
        upsert_sink: dict[str, dict[str, Any]] = {}
        audit: list[dict[str, Any]] = []

        for attempt in range(1, attempts + 1):
            for result in results:
                key = make_idempotency_key(result)
                if idempotent:
                    upsert_sink[key] = dict(result)
                else:
                    append_sink.append(dict(result))
                audit.append(
                    {
                        "attempt": attempt,
                        "idempotency_key": key,
                        "merchant_id": result["merchant_id"],
                        "window_start": result["window_start"],
                        "idempotent": idempotent,
                    }
                )

        if idempotent:
            materialized = list(upsert_sink.values())
        else:
            materialized = append_sink

        return materialized, audit

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos...
    """)
    return


@app.cell
def _(mo, summarize_payments):
    _events = [
        {
            "event_id": "e1",
            "merchant_id": "m1",
            "event_time": "2024-01-01T00:00:10Z",
            "arrival_time": "2024-01-01T00:00:11Z",
            "amount": 10.0,
            "status": "CONFIRMED",
        },
        {
            "event_id": "e1",  # duplicado
            "merchant_id": "m1",
            "event_time": "2024-01-01T00:00:10Z",
            "arrival_time": "2024-01-01T00:00:12Z",
            "amount": 10.0,
            "status": "CONFIRMED",
        },
        {
            "event_id": "e2",
            "merchant_id": "m1",
            "event_time": "2024-01-01T00:00:20Z",
            "arrival_time": "2024-01-01T00:00:21Z",
            "amount": 5.0,
            "status": "CONFIRMED",
        },
        {
            "event_id": "e3",
            "merchant_id": "m1",
            "event_time": "2024-01-01T00:00:30Z",
            "arrival_time": "2024-01-01T00:03:10Z",  # > 120s
            "amount": 100.0,
            "status": "CONFIRMED",
        },
    ]

    _totals, _audit = summarize_payments(_events)
    mo.md("### Totales")
    mo.ui.table(_totals)
    mo.md("### Auditoría")
    mo.ui.table(_audit)
    return

@app.cell
def _(parse_utc):
    # Casos válidos
    print("OK  ->", parse_utc("2024-01-01T00:00:10Z"))
    print("OK  ->", parse_utc("2024-01-01T00:00:10.500Z"))
    print("OK  ->", parse_utc("2024-01-01T03:00:00+03:00"))

    # Casos inválidos
    for bad in ["", "no-es-fecha", "2024-01-01T00:00:10"]:
        try:
            parse_utc(bad)
        except (ValueError, TypeError) as exc:
            print(f"ERROR esperado para {bad!r}: {exc}")
    return

@app.cell
def _(assign_fixed_window, parse_utc):
    for ts_str in [
        "2024-01-01T00:00:00Z",
        "2024-01-01T00:00:37Z",
        "2024-01-01T00:00:59Z",
        "2024-01-01T00:01:00Z",
        "2024-01-01T00:02:15Z",
    ]:
        ts = parse_utc(ts_str)
        start, end = assign_fixed_window(ts)
        print(f"{ts_str}  ->  [{start.isoformat()}, {end.isoformat()})")
    return

@app.cell
def _(beam, build_windowed_totals_pipeline):
    events_1 = [
        {"event_id": "e1", "merchant_id": "m1",
         "event_time": "2024-01-01T00:00:10Z",
         "amount": 10.0, "status": "CONFIRMED"},
        {"event_id": "e2", "merchant_id": "m1",
         "event_time": "2024-01-01T00:00:20Z",
         "amount": 5.0, "status": "CONFIRMED"},
        {"event_id": "e3", "merchant_id": "m1",
         "event_time": "2024-01-01T00:01:05Z",
         "amount": 3.0, "status": "CONFIRMED"},
        {"event_id": "e4", "merchant_id": "m2",
         "event_time": "2024-01-01T00:00:15Z",
         "amount": 7.0, "status": "CONFIRMED"},
        {"event_id": "e5", "merchant_id": "m2",
         "event_time": "2024-01-01T00:00:20Z",
         "amount": 1.0, "status": "REJECTED"},  # no debe aparecer
    ]

    with beam.Pipeline() as p1:
        totals = build_windowed_totals_pipeline(p1, events_1)
        (
            totals
            | "Format" >> beam.Map(
                lambda kv: f"{kv[0]}  total={kv[1]}"
            )
            | "Print" >> beam.Map(print)
        )
    return

@app.cell
def _(DeduplicatePayments, beam, beam_window):
    events_2 = [
        ("m1", {"event_id": "e1", "amount": 10}),
        ("m1", {"event_id": "e1", "amount": 10}),   # duplicado -> se cae
        ("m1", {"event_id": "e2", "amount": 5}),
        ("m2", {"event_id": "e1", "amount": 7}),    # otra clave -> pasa
        ("m2", {"event_id": "e1", "amount": 7}),    # duplicado en m2 -> se cae
    ]

    with beam.Pipeline() as p2:
        (
            p2
            | "Create" >> beam.Create(events_2)
            | "Window" >> beam.WindowInto(beam_window.FixedWindows(60))
            | "Dedup" >> beam.ParDo(DeduplicatePayments())
            | "Print" >> beam.Map(print)
        )
    return

@app.cell
def _(build_trigger_policy):
    wi = build_trigger_policy()
    print("WindowInto creado:", wi)
    print("  windowfn:", wi.windowing.windowfn)
    print("  triggerfn:", wi.windowing.triggerfn)
    print("  accumulation_mode:", wi.windowing.accumulation_mode)
    print("  allowed_lateness:", wi.windowing.allowed_lateness)
    return

@app.cell
def _(make_idempotency_key):
    samples = [
        {"merchant_id": "m1",
         "window_start": "2024-01-01T00:00:00+00:00"},
        {"merchant_id": "m2",
         "window_start": "2024-01-01T00:01:00+00:00"},
    ]
    for s in samples:
        print(s, "->", make_idempotency_key(s))
    return

@app.cell
def _(mo, simulate_sink_retries):
    results = [
        {"merchant_id": "m1",
         "window_start": "2024-01-01T00:00:00+00:00", "total": 10},
        {"merchant_id": "m1",
         "window_start": "2024-01-01T00:01:00+00:00", "total": 5},
        {"merchant_id": "m2",
         "window_start": "2024-01-01T00:00:00+00:00", "total": 7},
        {"merchant_id": "m2",
         "window_start": "2024-01-01T00:01:00+00:00", "total": 3},
    ]

    mat_upsert, aud_upsert = simulate_sink_retries(
        results, attempts=2, idempotent=True
    )
    mat_append, aud_append = simulate_sink_retries(
        results, attempts=2, idempotent=False
    )

    mo.vstack([
        mo.md(f"### UPSERT idempotente — materialized: {len(mat_upsert)} filas"),
        mo.ui.table(mat_upsert),
        mo.md(f"### Append-only — materialized: {len(mat_append)} filas"),
        mo.ui.table(mat_append),
        mo.md(f"### Auditoría (UPSERT): {len(aud_upsert)} intentos"),
        mo.ui.table(aud_upsert),
    ])
    return

if __name__ == "__main__":
    app.run()
