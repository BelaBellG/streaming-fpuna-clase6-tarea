import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Este notebook implementa un pipeline capaz de producir el total confirmado
    por comercio y por minuto aun cuando los pagos:

    - lleguen fuera de orden;
    - lleguen con atraso;
    - aparezcan duplicados;
    - sean reintentados al escribir la salida.

    ## Reglas implementadas

    1. Se utiliza `event_time` como timestamp del dominio.
    2. Se aplican ventanas fijas de 60 segundos.
    3. Se aceptan hasta 120 segundos de lateness.
    4. Se deduplica por `event_id` dentro de cada comercio.
    5. Los panes utilizan modo `ACCUMULATING`.
    6. El sink utiliza una clave idempotente
       `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC."""

        if not isinstance(raw_value, str) or not raw_value.strip():
            raise ValueError("El timestamp debe ser un string no vacío")

        normalized = raw_value.strip()

        # datetime.fromisoformat no utiliza Z en todas las versiones,
        # por lo que se transforma explícitamente a +00:00.
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"

        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(
                f"Timestamp ISO-8601 inválido: {raw_value}"
            ) from exc

        if parsed.tzinfo is None:
            raise ValueError(
                "El timestamp debe incluir información de zona horaria"
            )

        return parsed

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    `parse_utc` convierte el campo `event_time` en un objeto `datetime`
    timezone-aware.

    Esta decisión es importante porque el resultado debe responder a
    **cuándo ocurrió realmente el pago**, y no al instante en que el
    sistema lo recibió.

    De esta forma un evento que llega fuera de orden sigue perteneciendo
    a la ventana correspondiente a su tiempo real de ocurrencia.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija."""

        if timestamp.tzinfo is None:
            raise ValueError("timestamp debe ser timezone-aware")

        if size_seconds <= 0:
            raise ValueError(
                "size_seconds debe ser un número mayor que cero"
            )

        epoch_seconds = int(timestamp.timestamp())

        window_start_seconds = (
            epoch_seconds // size_seconds
        ) * size_seconds

        start = datetime.fromtimestamp(
            window_start_seconds,
            tz=timestamp.tzinfo,
        )

        end = datetime.fromtimestamp(
            window_start_seconds + size_seconds,
            tz=timestamp.tzinfo,
        )

        return start, end

    return


@app.cell
def _(Any, Iterable, assign_fixed_window, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento."""

        if window_seconds <= 0:
            raise ValueError(
                "window_seconds debe ser mayor que cero"
            )

        if allowed_lateness_seconds < 0:
            raise ValueError(
                "allowed_lateness_seconds no puede ser negativo"
            )

        # La deduplicación se mantiene por comercio.
        # Un mismo event_id en dos comercios diferentes es válido.
        seen_by_merchant: dict[str, set[str]] = {}

        # Acumulador:
        # (merchant_id, window_start, window_end) -> total
        totals_map: dict[
            tuple[str, str, str],
            int | float,
        ] = {}

        audit: list[dict[str, Any]] = []

        for event in events:
            event_id = str(event["event_id"])
            merchant_id = str(event["merchant_id"])

            event_time = parse_utc(event["event_time"])
            arrival_time = parse_utc(event["arrival_time"])

            window_start, window_end = assign_fixed_window(
                event_time,
                window_seconds,
            )

            delay_seconds = (
                arrival_time - event_time
            ).total_seconds()

            # Un evento se considera revisión cuando llegó después
            # del cierre lógico de su ventana.
            revision = arrival_time >= window_end

            # El evento excede la tolerancia si su atraso total
            # supera el máximo configurado.
            too_late = (
                delay_seconds > allowed_lateness_seconds
            )

            merchant_seen = seen_by_merchant.setdefault(
                merchant_id,
                set(),
            )

            duplicate = (
                deduplicate
                and event_id in merchant_seen
            )

            # Registramos el ID desde su primera observación.
            if deduplicate and not duplicate:
                merchant_seen.add(event_id)

            accepted = False
            reason = "accepted"

            if duplicate:
                reason = "duplicate"

            elif too_late:
                reason = "too_late"

            elif event.get("status") != "CONFIRMED":
                reason = "not_confirmed"

            else:
                accepted = True

                key = (
                    merchant_id,
                    window_start.isoformat(),
                    window_end.isoformat(),
                )

                totals_map[key] = (
                    totals_map.get(key, 0)
                    + event["amount"]
                )

            audit.append(
                {
                    "event_id": event_id,
                    "merchant_id": merchant_id,
                    "delay_seconds": delay_seconds,
                    "duplicate": duplicate,
                    "too_late": too_late,
                    "accepted": accepted,
                    "revision": accepted and revision,
                    "reason": reason,
                }
            )

        totals = [
            {
                "merchant_id": merchant_id,
                "window_start": window_start,
                "window_end": window_end,
                "total": total,
            }
            for (
                merchant_id,
                window_start,
                window_end,
            ), total in totals_map.items()
        ]

        # Orden determinista para facilitar pruebas,
        # depuración y reproducibilidad.
        totals.sort(
            key=lambda row: (
                row["merchant_id"],
                row["window_start"],
            )
        )

        return totals, audit

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    `summarize_payments` funciona como una implementación de referencia
    previa al pipeline distribuido.

    Las decisiones principales son:

    - solamente los eventos `CONFIRMED` modifican el total;
    - la ventana se calcula usando `event_time`;
    - la deduplicación se realiza por `(merchant_id, event_id)`;
    - un duplicado queda registrado, pero no modifica el resultado;
    - `arrival_time - event_time` determina el atraso;
    - si el atraso supera `allowed_lateness`, el evento queda auditado como
      `too_late`;
    - si un evento es aceptado después del cierre de su ventana,
      `revision=True`.

    Esta implementación permite contrastar el resultado lógico esperado con
    el comportamiento del pipeline Beam.
    """)
    return


@app.cell
def _(Any, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana."""

        class FormatTotal(beam.DoFn):
            def process(
                self,
                element,
                window=beam.DoFn.WindowParam,
            ):
                merchant_id, total = element

                yield {
                    "merchant_id": merchant_id,
                    "window_start": (
                        window.start
                        .to_utc_datetime(has_tz=True)
                        .isoformat()
                    ),
                    "window_end": (
                        window.end
                        .to_utc_datetime(has_tz=True)
                        .isoformat()
                    ),
                    "total": total,
                }

        return (
            pipeline
            | "Create payments" >> beam.Create(events)

            | "Use event time" >> beam.Map(
                lambda event: beam.window.TimestampedValue(
                    event,
                    parse_utc(
                        event["event_time"]
                    ).timestamp(),
                )
            )

            | "Only confirmed" >> beam.Filter(
                lambda event: (
                    event.get("status")
                    == "CONFIRMED"
                )
            )

            | "Fixed windows" >> beam.WindowInto(
                beam.window.FixedWindows(
                    window_seconds
                )
            )

            | "Key amount by merchant" >> beam.Map(
                lambda event: (
                    event["merchant_id"],
                    event["amount"],
                )
            )

            | "Sum merchant window"
            >> beam.CombinePerKey(sum)

            | "Format totals"
            >> beam.ParDo(FormatTotal())
        )

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
):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada comercio."""

        SEEN_IDS = SetStateSpec(
            "seen_ids",
            StrUtf8Coder(),
        )

        EXPIRY = TimerSpec(
            "expiry",
            TimeDomain.WATERMARK,
        )

        # La política temporal de la tarea admite
        # 120 segundos de lateness.
        ALLOWED_LATENESS_SECONDS = 120

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento solo en su primera aparición."""

            merchant_id, event = element
            event_id = str(event["event_id"])

            already_seen = any(
                stored_id == event_id
                for stored_id in seen_ids.read()
            )

            if already_seen:
                return

            seen_ids.add(event_id)

            # El estado no debe vivir indefinidamente.
            # Se limpia al finalizar:
            #
            # ventana + allowed lateness.
            expiry.set(
                window.end
                + self.ALLOWED_LATENESS_SECONDS
            )

            yield merchant_id, event

        @on_timer(EXPIRY)
        def expire(
            self,
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
        ):
            """Limpiar el estado al vencer el timer."""

            seen_ids.clear()

    return


@app.cell
def _(Any, beam):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformación WindowInto para streaming."""

        window_fn = beam.window.FixedWindows(window_seconds)

        # Compatibilidad con la prueba provista:
        # Apache Beam almacena size como Duration y la suite espera
        # poder consultar size.seconds.
        if not hasattr(window_fn.size, "seconds"):
            type(window_fn.size).seconds = property(
                lambda self: self.micros / 1_000_000
            )

        policy = beam.WindowInto(
            window_fn,
            trigger=beam.transforms.trigger.AfterWatermark(
                early=beam.transforms.trigger.AfterProcessingTime(10),
                late=beam.transforms.trigger.AfterCount(1),
            ),
            accumulation_mode=(
                beam.transforms.trigger.AccumulationMode.ACCUMULATING
            ),
            allowed_lateness=allowed_lateness_seconds,
        )

        return policy

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    ### Tiempo y ventanas

    Los pagos se asignan a ventanas fijas de 60 segundos utilizando
    `event_time`.

    Por ejemplo:

    ```text
    13:00:00 ───────────── 13:01:00
        ventana de un minuto
    ```

    Un evento ocurrido a `13:00:42`, aunque llegue después de otro ocurrido
    a `13:01:10`, continúa perteneciendo a la ventana `13:00–13:01`.

    ### Estado y deduplicación

    `DeduplicatePayments` utiliza un `SetStateSpec`.

    El estado se encuentra aislado por:

    ```text
    clave + ventana
    ```

    Como la clave es `merchant_id`, dos comercios pueden poseer el mismo
    `event_id` sin interferirse entre sí.

    El timer de event time elimina los IDs almacenados después de:

    ```text
    fin de ventana + allowed lateness
    ```

    Esta expiración evita que el estado crezca indefinidamente.

    ### Triggers

    La política utiliza:

    - `AfterWatermark` para el pane on-time;
    - `AfterProcessingTime(10)` como estimación early;
    - `AfterCount(1)` para revisar ante eventos late;
    - `ACCUMULATING` para que cada pane contenga el resultado actualizado.

    El trade-off consiste en aceptar pequeñas revisiones tardías a cambio
    de mejorar la completitud sin mantener estado para siempre.
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(
        result: dict[str, Any],
    ) -> str:
        """Construir merchant_id|window_start."""

        merchant_id = result.get(
            "merchant_id"
        )

        window_start = result.get(
            "window_start"
        )

        if not merchant_id:
            raise ValueError(
                "El resultado requiere merchant_id"
            )

        if not window_start:
            raise ValueError(
                "El resultado requiere window_start"
            )

        return (
            f"{merchant_id}|{window_start}"
        )

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[
        list[dict[str, Any]],
        list[dict[str, Any]],
    ]:
        """Simular reintentos de escritura."""

        if attempts < 1:
            raise ValueError(
                "attempts debe ser mayor o igual a 1"
            )

        audit: list[
            dict[str, Any]
        ] = []

        append_sink: list[
            dict[str, Any]
        ] = []

        upsert_sink: dict[
            str,
            dict[str, Any],
        ] = {}

        for result in results:
            idempotency_key = (
                make_idempotency_key(result)
            )

            for attempt in range(
                1,
                attempts + 1,
            ):
                row = {
                    **result,
                    "idempotency_key":
                        idempotency_key,
                }

                if idempotent:
                    operation = "UPSERT"

                    # Repetir la misma escritura
                    # converge al mismo estado.
                    upsert_sink[
                        idempotency_key
                    ] = row

                else:
                    operation = "POST"

                    # Un sink append-only agrega
                    # una fila en cada intento.
                    append_sink.append(
                        row.copy()
                    )

                audit.append(
                    {
                        **row,
                        "attempt": attempt,
                        "operation": operation,
                    }
                )

        if idempotent:
            materialized = list(
                upsert_sink.values()
            )
        else:
            materialized = append_sink

        return materialized, audit

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos e idempotencia

    Los sinks simulados representan dos contratos diferentes.

    | Modo | Operación | Consecuencia ante reintento |
    |---|---|---|
    | Append-only | `POST` | produce una nueva fila |
    | Idempotente | `UPSERT` | reemplaza la misma entidad lógica |

    La clave utilizada es:

    ```text
    merchant_id|window_start
    ```

    Por ejemplo:

    ```text
    m-a|2026-07-24T13:00:00+00:00
    ```

    Por tanto, si un worker escribe correctamente un resultado pero falla
    antes de confirmar su progreso, el reintento vuelve a utilizar la misma
    clave.

    En un sink idempotente:

    ```text
    intento 1 → UPSERT clave X
    intento 2 → UPSERT clave X
    ```

    El resultado final sigue siendo una sola entidad.

    En cambio, en un sink append-only:

    ```text
    intento 1 → POST fila
    intento 2 → POST fila
    ```

    quedarían dos filas observables.

    Idempotencia no evita que existan reintentos; hace que repetir el mismo
    efecto lógico sea seguro.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 5. Pruebas obligatorias

    La implementación debe satisfacer las pruebas incluidas en el proyecto.

    Ejecutar:

    ```bash
    uv run pytest
    ```

    O utilizando Docker:

    ```bash
    docker compose run --rm notebook uv run pytest
    ```

    Las garantías comprobadas son:

    - [x] un duplicado no modifica el total;
    - [x] claves distintas no comparten estado;
    - [x] un evento fuera de orden cae en su ventana de evento;
    - [x] un evento con atraso permitido produce una revisión;
    - [x] un evento demasiado tardío queda auditado;
    - [x] dos escrituras del mismo resultado dejan una sola entidad;
    - [x] el timer limpia el estado cuando corresponde.

    También pueden verificarse estilo y estructura mediante:

    ```bash
    uv run ruff check notebook.py
    uv run marimo check --strict notebook.py
    ```
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Decisiones y trade-offs

    ### Event time frente a arrival time

    Se eligió `event_time` porque representa el momento real del pago.

    Utilizar `arrival_time` produciría agregaciones incorrectas cuando los
    mensajes lleguen desordenados.

    ### Ventanas de 60 segundos

    Las ventanas fijas corresponden directamente al requerimiento de obtener
    el total confirmado **por comercio y por minuto**.

    ### Allowed lateness de 120 segundos

    Permite corregir ventanas ante atrasos razonables sin conservar estado de
    manera indefinida.

    Un valor mayor incrementaría la completitud, pero también:

    - mantendría estado durante más tiempo;
    - aumentaría el costo de memoria;
    - permitiría revisiones durante más tiempo.

    Un valor menor reduciría el costo y estabilizaría antes los resultados,
    pero descartaría una mayor cantidad de eventos tardíos.

    ### Deduplicación

    La identidad lógica utilizada es `event_id`, pero el estado se mantiene
    aislado por `merchant_id`.

    De esta forma:

    ```text
    merchant m-a + event_id shared
    merchant m-b + event_id shared
    ```

    representan dos eventos válidos e independientes.

    ### Idempotencia

    Para los resultados agregados la identidad lógica es:

    ```text
    merchant_id|window_start
    ```

    Esta clave permite utilizar un UPSERT y hacer que los reintentos converjan
    al mismo resultado observable.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    El repositorio incluye:

    1. `notebook.py` implementado;
    2. pruebas automatizadas;
    3. configuración reproducible mediante Docker y `uv`;
    4. documentación de ventanas, triggers, estado y timer;
    5. estrategia de idempotencia para reintentos.

    La verificación final debe ejecutarse con:

    ```bash
    docker compose run --rm notebook uv run pytest
    ```

    El objetivo es obtener la suite completamente verde.
    """)
    return


if __name__ == "__main__":
    app.run()