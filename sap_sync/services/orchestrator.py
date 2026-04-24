import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from decimal import Decimal

from django.conf import settings
from django.db.models import Q
from django.utils import timezone

# --- MODELOS DE CORE ---
from core.models import (
    AsientoAuditoria,
    ClasificacionGasto,
    DashboardConsolidado,
    SaldoBancario,
)

# --- MODELOS DE SAP_SYNC ---
from sap_sync.models import (
    Compensacion,
    CuentaConfiguracion,
    Partida,
    PartidaPosicion,
    PartidaPosicionFiltro,
    TasaBCV,
)
from sap_sync.services.mapper import GeneradorDinamicoSAP
from sap_sync.services.sap_client import (
    AMBIENTE_SAP,
    PASSWORD,
    USERNAME,
    SAPODataClient,
    SAPServiceURL,
    SAPTasaBCVClient,
    fecha_sap,
)
from sap_sync.utils.utils import (
    conciliar_cadena_zr_zp_facturas,
    procesar_comisiones_bancarias,
    procesar_ingresos_bancarios,
    procesar_transferencias_y_divisas,
)

logger = logging.getLogger(__name__)


def _fecha_a_anio_periodo(f: date) -> tuple[str, str]:
    mes = f.month
    if mes >= 5:
        periodo = mes - 4
        anio = f.year
    else:
        periodo = mes + 8
        anio = f.year - 1
    return str(anio), str(periodo).zfill(2)


def _chunked_list(lst: list, n: int):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def _obtener_estado_paso(log, errores_antes: int) -> str:
    log.refresh_from_db(fields=["errores_count"])
    return "PARCIAL" if log.errores_count > errores_antes else "EXITOSO"


def _ejecutar_batch_thread(
    client, entity_set, chunk, expand=None, use_filters=False, is_raw=False
):
    if is_raw:
        return client.execute_batch(
            entity_set=entity_set, raw_filters=chunk, expand=expand
        )
    else:
        return client.execute_batch(
            entity_set=entity_set, items=chunk, expand=expand, use_filters=use_filters
        )


# ⚡ NUEVA FUNCIÓN: Descubrimiento dinámico de cuentas Standalone
def descubrir_cuentas_standalone(cuentas_terminadas_en_0):
    """
    Deduce cuáles cuentas terminadas en '0' son Standalone verificando
    si NUNCA han existido cuentas transitorias (1-9) asociadas a su misma raíz.
    """
    standalone_detectadas = set()
    for cuenta_cero in cuentas_terminadas_en_0:
        raiz_cuenta = str(cuenta_cero)[:-1]
        posibles_transitorias = [f"{raiz_cuenta}{i}" for i in range(1, 10)]
        tiene_transitorias = PartidaPosicion.objects.filter(
            ractt__in=posibles_transitorias
        ).exists()

        if not tiene_transitorias:
            standalone_detectadas.add(cuenta_cero)

    return standalone_detectadas


def _procesar_y_guardar_en_paralelo_sap_batch(
    client,
    entity_set,
    chunks,
    db_callback,
    max_workers=4,
    expand=None,
    use_filters=False,
    is_raw=False,
    paso_log=None,
    log_obj=None,
    paso_id=None,
):
    resultados_callback = []
    client._ensure_metadata()
    total_chunks = len(chunks)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futuros = {
            executor.submit(
                _ejecutar_batch_thread,
                client,
                entity_set,
                chunk,
                expand,
                use_filters,
                is_raw,
            ): (i, chunk)
            for i, chunk in enumerate(chunks)
        }

        for procesados, futuro in enumerate(as_completed(futuros), 1):
            chunk_idx, chunk_data = futuros[futuro]

            if log_obj and procesados % 5 == 0:
                try:
                    log_obj.verificar_cancelacion()
                except InterruptedError:
                    for f in futuros:
                        f.cancel()
                    raise

            if log_obj and paso_id:
                log_obj.actualizar_progreso_paso(
                    paso_id,
                    f"Procesando en paralelo: {procesados}/{total_chunks} lotes de {entity_set}...",
                )

            try:
                registros, errores = futuro.result()

                if errores and log_obj and paso_log:
                    for err in errores:
                        contexto = {
                            "lote_idx": chunk_idx,
                            "contenido_muestra": chunk_data[:3]
                            if isinstance(chunk_data, list)
                            else chunk_data,
                        }
                        log_obj.registrar_error(
                            paso_log,
                            f"Error devuelto por SAP en lote {chunk_idx}: {err}",
                            contexto=contexto,
                        )

                if registros:
                    res = db_callback(registros)
                    if res is not None:
                        resultados_callback.append(res)

            except Exception as exc:
                if log_obj and paso_log:
                    contexto = {
                        "lote_idx": chunk_idx,
                        "tipo_error": type(exc).__name__,
                        "contenido_muestra": chunk_data[:3]
                        if isinstance(chunk_data, list)
                        else chunk_data,
                    }
                    log_obj.registrar_error(
                        paso_log,
                        f"Excepción crítica (Timeout/Red) en lote {chunk_idx}: {exc}",
                        contexto=contexto,
                    )

    return resultados_callback


def _bulk_upsert_filtros(registros_pos: list):
    if not registros_pos:
        return

    mapper = GeneradorDinamicoSAP("PartidaPosicionFiltro")
    campos_update = [
        regla.campo_django
        for regla in mapper.reglas
        if regla.campo_django not in ("bukrs", "docnr", "ryear", "docln")
    ]

    for chunk in _chunked_list(registros_pos, settings.DB_BATCH_SIZE_LARGE):
        bukrs_set = {p.get("Bukrs", "") for p in chunk}
        docnr_set = {p.get("Docnr", "") for p in chunk}
        ryear_set = {p.get("Ryear", "") for p in chunk}
        docln_set = {p.get("Docln", "") for p in chunk}

        candidatos = PartidaPosicionFiltro.objects.filter(
            bukrs__in=bukrs_set,
            docnr__in=docnr_set,
            ryear__in=ryear_set,
            docln__in=docln_set,
        )
        existentes = {(p.bukrs, p.docnr, p.ryear, p.docln): p for p in candidatos}

        to_create, to_update = [], []

        for pos in chunk:
            datos_kwargs = mapper.construir_kwargs(pos)
            llave = (
                datos_kwargs.get("bukrs", ""),
                datos_kwargs.get("docnr", ""),
                datos_kwargs.get("ryear", ""),
                datos_kwargs.get("docln", ""),
            )

            if llave in existentes:
                obj = existentes[llave]
                hay_cambios = False
                for campo, nuevo_valor in datos_kwargs.items():
                    if getattr(obj, campo) != nuevo_valor:
                        setattr(obj, campo, nuevo_valor)
                        hay_cambios = True
                if hay_cambios:
                    to_update.append(obj)
            else:
                obj = PartidaPosicionFiltro(**datos_kwargs)
                to_create.append(obj)
                existentes[llave] = obj

        if to_create:
            PartidaPosicionFiltro.objects.bulk_create(
                to_create, batch_size=settings.DB_BATCH_SIZE_LARGE
            )
        if to_update:
            PartidaPosicionFiltro.objects.bulk_update(
                to_update, campos_update, batch_size=settings.DB_BATCH_SIZE_LARGE
            )


def _guardar_posiciones_bulk(posiciones_raw: list):
    if not posiciones_raw:
        return

    mapper = GeneradorDinamicoSAP("PartidaPosicion")
    llaves_pk = ("bukrs", "docnr", "ryear", "docln")
    campos_update = [
        r.campo_django for r in mapper.reglas if r.campo_django not in llaves_pk
    ]

    for chunk in _chunked_list(posiciones_raw, settings.DB_BATCH_SIZE_LARGE):
        bukrs_set = {p.get("Bukrs", "") for p in chunk}
        docnr_set = {p.get("Docnr", "") for p in chunk}
        ryear_set = {p.get("Ryear", "") for p in chunk}
        docln_set = {p.get("Docln", "") for p in chunk}

        candidatos = PartidaPosicion.objects.filter(
            bukrs__in=bukrs_set,
            docnr__in=docnr_set,
            ryear__in=ryear_set,
            docln__in=docln_set,
        )
        existentes = {(p.bukrs, p.docnr, p.ryear, p.docln): p for p in candidatos}

        to_create, to_update = [], []

        for pos in chunk:
            partida_obj = pos.get("_partida_ref")
            if not partida_obj or not partida_obj.pk:
                continue

            datos_kwargs = mapper.construir_kwargs(pos)

            llave = (
                datos_kwargs.get("bukrs", ""),
                datos_kwargs.get("docnr", ""),
                datos_kwargs.get("ryear", ""),
                datos_kwargs.get("docln", ""),
            )

            if llave in existentes:
                obj = existentes[llave]
                hay_cambios = False

                for campo in campos_update:
                    nuevo_valor = datos_kwargs.get(campo)
                    if getattr(obj, campo) != nuevo_valor:
                        setattr(obj, campo, nuevo_valor)
                        hay_cambios = True

                if hay_cambios or obj.partida_id != partida_obj.pk:  # type: ignore
                    obj.partida = partida_obj
                    to_update.append(obj)
            else:
                obj = PartidaPosicion(partida=partida_obj, **datos_kwargs)
                to_create.append(obj)
                existentes[llave] = obj

        if to_create:
            PartidaPosicion.objects.bulk_create(
                to_create, batch_size=settings.DB_BATCH_SIZE_LARGE
            )
        if to_update:
            PartidaPosicion.objects.bulk_update(
                to_update,
                ["partida"] + campos_update,
                batch_size=settings.DB_BATCH_SIZE_LARGE,
            )


def _guardar_partidas_desde_sap(registros_sap: list) -> tuple[int, int]:
    if not registros_sap:
        return 0, 0
    total_creadas = total_actualizadas = 0

    mapper = GeneradorDinamicoSAP("Partida")
    campos_update = [
        regla.campo_django
        for regla in mapper.reglas
        if regla.campo_django not in ("bukrs", "belnr", "gjahr")
    ]

    for chunk in _chunked_list(registros_sap, settings.DB_BATCH_SIZE_MEDIUM):
        bukrs_set = {d.get("Bukrs", "") for d in chunk}
        belnr_set = {d.get("Belnr", "") for d in chunk}
        gjahr_set = {d.get("Gjahr", "") for d in chunk}

        candidatos = Partida.objects.filter(
            bukrs__in=bukrs_set, belnr__in=belnr_set, gjahr__in=gjahr_set
        )
        existentes = {(p.bukrs, p.belnr, p.gjahr): p for p in candidatos}

        to_create, to_update, posiciones_raw_list = [], [], []

        for doc in chunk:
            datos_kwargs = mapper.construir_kwargs(doc)
            llave = (
                datos_kwargs.get("bukrs", ""),
                datos_kwargs.get("belnr", ""),
                datos_kwargs.get("gjahr", ""),
            )

            if llave in existentes:
                p = existentes[llave]
                hay_cambios = False
                for campo, nuevo_valor in datos_kwargs.items():
                    if getattr(p, campo) != nuevo_valor:
                        setattr(p, campo, nuevo_valor)
                        hay_cambios = True
                if hay_cambios:
                    to_update.append(p)
            else:
                p = Partida(**datos_kwargs)
                to_create.append(p)
                existentes[llave] = p

        if to_create:
            Partida.objects.bulk_create(
                to_create, batch_size=settings.DB_BATCH_SIZE_MEDIUM
            )
            total_creadas += len(to_create)
            nuevos = Partida.objects.filter(
                bukrs__in=bukrs_set, belnr__in=belnr_set, gjahr__in=gjahr_set
            )
            for n in nuevos:
                existentes[(n.bukrs, n.belnr, n.gjahr)] = n

        if to_update:
            Partida.objects.bulk_update(
                to_update, campos_update, batch_size=settings.DB_BATCH_SIZE_MEDIUM
            )
            total_actualizadas += len(to_update)

        for doc in chunk:
            llave_padre = (
                doc.get("Bukrs", ""),
                doc.get("Belnr", ""),
                doc.get("Gjahr", ""),
            )
            partida_obj = existentes.get(llave_padre)
            if partida_obj:
                for pos in doc.get("toPosiciones", {}).get("results", []):
                    pos["_partida_ref"] = partida_obj
                    posiciones_raw_list.append(pos)

        _guardar_posiciones_bulk(posiciones_raw_list)

    return total_creadas, total_actualizadas


class SAPSyncOrchestrator:
    """Orquesta el flujo completo de sincronización de SAP, separando la lógica del framework de colas."""

    def __init__(self, log_instance):
        self.log = log_instance

    def ejecutar_sync_completa(self, fecha_inicio: date, fecha_fin: date, anio: str):
        self.log.verificar_cancelacion()
        self.log.refresh_from_db(fields=["errores_count"])
        err_antes = self.log.errores_count
        self.log.registrar_inicio_paso("paso1", "Saldos Bancarios")
        sc, sa = self._paso1_saldos_bancarios(anio)
        self.log.saldos_creados += sc
        self.log.saldos_actualizados += sa
        self.log.save(update_fields=["saldos_creados", "saldos_actualizados"])
        self.log.registrar_fin_paso(
            "paso1",
            {"creados": sc, "actualizados": sa},
            estado=_obtener_estado_paso(self.log, err_antes),
        )

        self.log.verificar_cancelacion()
        self.log.refresh_from_db(fields=["errores_count"])
        err_antes = self.log.errores_count
        self.log.registrar_inicio_paso("paso2", "Derivación de Cuentas HKONT")
        cuentas_derivadas = self._paso2_derivar_hkont()
        self.log.registrar_fin_paso(
            "paso2",
            {"cuentas_obtenidas": len(cuentas_derivadas)},
            estado=_obtener_estado_paso(self.log, err_antes),
        )

        self.log.verificar_cancelacion()
        self.log.refresh_from_db(fields=["errores_count"])
        err_antes = self.log.errores_count
        self.log.registrar_inicio_paso("paso3", "Partidas por Rango de Fechas")
        pc, pa = self._paso3_partidas_por_fechas(
            fecha_inicio, fecha_fin, cuentas_derivadas
        )
        self.log.partidas_creadas += pc
        self.log.partidas_actualizadas += pa
        self.log.save(update_fields=["partidas_creadas", "partidas_actualizadas"])
        self.log.registrar_fin_paso(
            "paso3",
            {"creadas": pc, "actualizadas": pa},
            estado=_obtener_estado_paso(self.log, err_antes),
        )

        self.log.verificar_cancelacion()
        self.log.refresh_from_db(fields=["errores_count"])
        err_antes = self.log.errores_count
        self.log.registrar_inicio_paso("paso4", "Extracción de Rangos AUGBL")
        datos_augbl = self._paso4_rangos_augbl(fecha_inicio, fecha_fin)
        total_identificados = len(datos_augbl[0]) + len(datos_augbl[1])
        self.log.registrar_fin_paso(
            "paso4",
            {"grupos_identificados": total_identificados},
            estado=_obtener_estado_paso(self.log, err_antes),
        )

        self.log.verificar_cancelacion()
        self.log.refresh_from_db(fields=["errores_count"])
        err_antes = self.log.errores_count
        self.log.registrar_inicio_paso("paso5", "Sincronización de Compensaciones")
        n_comp = self._paso5_compensaciones(datos_augbl)
        self.log.compensaciones_proc += n_comp
        self.log.save(update_fields=["compensaciones_proc"])
        self.log.registrar_fin_paso(
            "paso5",
            {"procesadas": n_comp},
            estado=_obtener_estado_paso(self.log, err_antes),
        )

        self.log.verificar_cancelacion()
        self.log.refresh_from_db(fields=["errores_count"])
        err_antes = self.log.errores_count
        self.log.registrar_inicio_paso("paso6", "Partidas complementarias por BELNR")
        pc2, pa2 = self._paso6_partidas_por_belnr()
        self.log.partidas_creadas += pc2
        self.log.partidas_actualizadas += pa2
        self.log.save(update_fields=["partidas_creadas", "partidas_actualizadas"])
        self.log.registrar_fin_paso(
            "paso6",
            {"creadas": pc2, "actualizadas": pa2},
            estado=_obtener_estado_paso(self.log, err_antes),
        )

        self.log.verificar_cancelacion()
        self.log.refresh_from_db(fields=["errores_count"])
        err_antes = self.log.errores_count
        self.log.registrar_inicio_paso("paso7", "Actualización de Tasas BCV")
        n_tasas = self._paso7_tasas_bcv(fecha_inicio, fecha_fin)
        self.log.tasas_procesadas += n_tasas
        self.log.save(update_fields=["tasas_procesadas"])
        self.log.registrar_fin_paso(
            "paso7",
            {"nuevas_procesadas": n_tasas},
            estado=_obtener_estado_paso(self.log, err_antes),
        )

        self.log.verificar_cancelacion()
        self.paso8_calculo_disponibilidad(fecha_inicio, fecha_fin)

        self.log.refresh_from_db(fields=["errores_count"])
        estado_final = "PARCIAL" if self.log.errores_count > 0 else "EXITOSO"
        self.log.marcar_finalizado(estado_final)

    def ejecutar_reintento(self, fecha_inicio: date, fecha_fin: date, anio: str):
        progreso = self.log.progreso_detalle or {}

        def needs_run(paso_key):
            if paso_key not in progreso:
                return True
            return progreso[paso_key].get("estado") != "EXITOSO"

        if needs_run("paso1"):
            self.log.verificar_cancelacion()
            self.log.refresh_from_db(fields=["errores_count"])
            err_antes = self.log.errores_count
            self.log.registrar_inicio_paso("paso1", "Saldos Bancarios (Reintento)")
            sc, sa = self._paso1_saldos_bancarios(anio)
            self.log.saldos_creados += sc
            self.log.saldos_actualizados += sa
            self.log.save(update_fields=["saldos_creados", "saldos_actualizados"])
            self.log.registrar_fin_paso(
                "paso1",
                {"creados": sc, "actualizados": sa},
                estado=_obtener_estado_paso(self.log, err_antes),
            )

        if needs_run("paso3"):
            self.log.verificar_cancelacion()
            self.log.refresh_from_db(fields=["errores_count"])
            err_antes = self.log.errores_count
            self.log.registrar_inicio_paso("paso2", "Derivación de Cuentas HKONT")
            cuentas_derivadas = self._paso2_derivar_hkont()
            self.log.registrar_fin_paso(
                "paso2",
                {"cuentas_obtenidas": len(cuentas_derivadas)},
                estado=_obtener_estado_paso(self.log, err_antes),
            )

            self.log.verificar_cancelacion()
            self.log.refresh_from_db(fields=["errores_count"])
            err_antes = self.log.errores_count
            self.log.registrar_inicio_paso(
                "paso3", "Partidas por Rango de Fechas (Reintento)"
            )
            pc, pa = self._paso3_partidas_por_fechas(
                fecha_inicio, fecha_fin, cuentas_derivadas
            )
            self.log.partidas_creadas += pc
            self.log.partidas_actualizadas += pa
            self.log.save(update_fields=["partidas_creadas", "partidas_actualizadas"])
            self.log.registrar_fin_paso(
                "paso3",
                {"creadas": pc, "actualizadas": pa},
                estado=_obtener_estado_paso(self.log, err_antes),
            )

        if needs_run("paso5"):
            self.log.verificar_cancelacion()
            self.log.refresh_from_db(fields=["errores_count"])
            err_antes = self.log.errores_count
            self.log.registrar_inicio_paso("paso4", "Extracción de Rangos AUGBL")
            datos_augbl = self._paso4_rangos_augbl(fecha_inicio, fecha_fin)
            total_identificados = len(datos_augbl[0]) + len(datos_augbl[1])
            self.log.registrar_fin_paso(
                "paso4",
                {"grupos_identificados": total_identificados},
                estado=_obtener_estado_paso(self.log, err_antes),
            )

            self.log.verificar_cancelacion()
            self.log.refresh_from_db(fields=["errores_count"])
            err_antes = self.log.errores_count
            self.log.registrar_inicio_paso(
                "paso5", "Sincronización de Compensaciones (Reintento)"
            )
            n_comp = self._paso5_compensaciones(datos_augbl)
            self.log.compensaciones_proc += n_comp
            self.log.save(update_fields=["compensaciones_proc"])
            self.log.registrar_fin_paso(
                "paso5",
                {"procesadas": n_comp},
                estado=_obtener_estado_paso(self.log, err_antes),
            )

        if needs_run("paso6"):
            self.log.verificar_cancelacion()
            self.log.refresh_from_db(fields=["errores_count"])
            err_antes = self.log.errores_count
            self.log.registrar_inicio_paso(
                "paso6", "Partidas complementarias por BELNR (Reintento)"
            )
            pc2, pa2 = self._paso6_partidas_por_belnr()
            self.log.partidas_creadas += pc2
            self.log.partidas_actualizadas += pa2
            self.log.save(update_fields=["partidas_creadas", "partidas_actualizadas"])
            self.log.registrar_fin_paso(
                "paso6",
                {"creadas": pc2, "actualizadas": pa2},
                estado=_obtener_estado_paso(self.log, err_antes),
            )

        if needs_run("paso7"):
            self.log.verificar_cancelacion()
            self.log.refresh_from_db(fields=["errores_count"])
            err_antes = self.log.errores_count
            self.log.registrar_inicio_paso(
                "paso7", "Actualización de Tasas BCV (Reintento)"
            )
            n_tasas = self._paso7_tasas_bcv(fecha_inicio, fecha_fin)
            self.log.tasas_procesadas += n_tasas
            self.log.save(update_fields=["tasas_procesadas"])
            self.log.registrar_fin_paso(
                "paso7",
                {"nuevas_procesadas": n_tasas},
                estado=_obtener_estado_paso(self.log, err_antes),
            )

        if needs_run("paso8"):
            self.log.verificar_cancelacion()
            self.paso8_calculo_disponibilidad(fecha_inicio, fecha_fin)

        self.log.refresh_from_db(fields=["errores_count"])
        estado_final = "PARCIAL" if self.log.errores_count > 0 else "EXITOSO"
        self.log.marcar_finalizado(estado_final)

    def _paso1_saldos_bancarios(self, anio: str) -> tuple[int, int]:
        client = SAPODataClient(base_url=SAPServiceURL.SALDOS_BANCARIOS)
        filtro_saldos = f"Ryear eq '{anio}'"

        self.log.actualizar_progreso_paso("paso1", "Extrayendo registros de SAP...")
        registros, errores = client.get_data(
            "ZFI_SALDO_BANCARIO", filters=filtro_saldos
        )

        if errores:
            for err in errores:
                self.log.registrar_error(
                    1, err, contexto={"filtro_saldos": filtro_saldos}
                )
            if not registros:
                raise RuntimeError(f"Error fatal en SALDOS_BANCARIOS: {errores}")

        creados = actualizados = 0
        campos_tsl = ["tslvt"] + [f"tsl{str(i).zfill(2)}" for i in range(1, 17)]

        self.log.actualizar_progreso_paso(
            "paso1", f"Guardando {len(registros or [])} registros en BD..."
        )
        for chunk in _chunked_list(registros or [], 1000):
            bukrs_set = {r["Bukrs"] for r in chunk}
            ryear_set = {r["Ryear"] for r in chunk}
            hkont_set = {r["Hkont"] for r in chunk}

            candidatos = SaldoBancario.objects.filter(
                bukrs__in=bukrs_set, ryear__in=ryear_set, hkont__in=hkont_set
            )
            existentes = {
                (p.bukrs, p.ryear, p.hkont, p.waers, p.drcrk): p for p in candidatos
            }

            to_create, to_update = [], []

            for rec in chunk:
                llave = (
                    rec["Bukrs"],
                    rec["Ryear"],
                    rec["Hkont"],
                    rec["Waers"],
                    rec["Drcrk"],
                )
                valores = {
                    "tslvt": Decimal(str(rec.get("Tslvt", 0) or 0)),
                    **{
                        f"tsl{str(i).zfill(2)}": Decimal(
                            str(rec.get(f"Tsl{str(i).zfill(2)}", 0) or 0)
                        )
                        for i in range(1, 17)
                    },
                }

                if llave in existentes:
                    obj = existentes[llave]
                    hay_diferencia = any(
                        Decimal(str(getattr(obj, f, 0) or 0)) != valores[f]
                        for f in campos_tsl
                    )
                    if hay_diferencia:
                        for f in campos_tsl:
                            setattr(obj, f, valores[f])
                        obj.sincronizado_en = timezone.now()
                        to_update.append(obj)
                else:
                    obj = SaldoBancario(
                        bukrs=llave[0],
                        ryear=llave[1],
                        hkont=llave[2],
                        waers=llave[3],
                        drcrk=llave[4],
                        **valores,
                    )
                    to_create.append(obj)
                    existentes[llave] = obj

            if to_create:
                SaldoBancario.objects.bulk_create(to_create, batch_size=1000)
                creados += len(to_create)
            if to_update:
                SaldoBancario.objects.bulk_update(
                    to_update, campos_tsl + ["sincronizado_en"], batch_size=1000
                )
                actualizados += len(to_update)

        return creados, actualizados

    def _paso2_derivar_hkont(self) -> list[str]:
        hkonts_base = SaldoBancario.objects.values_list("hkont", flat=True).distinct()
        cuentas = set()
        for hkont in hkonts_base:
            cuentas.add(hkont)
            base = hkont[:-1]
            for sufijo in range(1, 8):
                cuentas.add(f"{base}{sufijo}")
        return sorted(cuentas)

    def _paso3_partidas_por_fechas(
        self, fecha_inicio: date, fecha_fin: date, cuentas: list[str]
    ) -> tuple[int, int]:
        client = SAPODataClient(base_url=SAPServiceURL.PARTIDAS)
        total_creadas = total_actualizadas = 0

        filtro_fecha = f"Budat ge {fecha_sap(str(fecha_inicio))} and Budat le {fecha_sap(str(fecha_fin))}"
        filtros_posiciones = [
            f"({filtro_fecha}) and Ractt eq '{cuenta}'" for cuenta in cuentas
        ]

        chunks_filtros_pos = list(_chunked_list(filtros_posiciones, 50))

        def cb_posiciones(registros):
            _bulk_upsert_filtros(registros)

        _procesar_y_guardar_en_paralelo_sap_batch(
            client,
            "ZFI_PARTIDAS_POSICIONES",
            chunks_filtros_pos,
            db_callback=cb_posiciones,
            max_workers=settings.SAP_MAX_WORKERS_DEFAULT,
            is_raw=True,
            paso_log=3,
            log_obj=self.log,
            paso_id="paso3",
        )

        llaves_filtro = (
            PartidaPosicionFiltro.objects.filter(
                budat__gte=fecha_inicio, budat__lte=fecha_fin
            )
            .values("bukrs", "docnr", "ryear")
            .distinct()
        )

        pks_batch = [
            {"Bukrs": item["bukrs"], "Belnr": item["docnr"], "Gjahr": item["ryear"]}
            for item in llaves_filtro
        ]
        if not pks_batch:
            return 0, 0

        chunks_pks_cab = list(_chunked_list(pks_batch, 50))

        def cb_cabeceras(registros):
            return _guardar_partidas_desde_sap(registros)

        resultados = _procesar_y_guardar_en_paralelo_sap_batch(
            client,
            "ZFI_PARTIDAS",
            chunks_pks_cab,
            db_callback=cb_cabeceras,
            max_workers=settings.SAP_MAX_WORKERS_DEFAULT,
            expand="toPosiciones",
            use_filters=True,
            paso_log=3,
            log_obj=self.log,
            paso_id="paso3",
        )

        for c, a in resultados:
            total_creadas += c
            total_actualizadas += a

        return total_creadas, total_actualizadas

    def _paso4_rangos_augbl(self, fecha_inicio: date, fecha_fin: date) -> tuple:
        augbls = (
            PartidaPosicion.objects.filter(
                partida__budat__gte=fecha_inicio, partida__budat__lte=fecha_fin
            )
            .exclude(augbl="")
            .exclude(augbl__isnull=True)
            .values_list("augbl", flat=True)
            .distinct()
        )

        lista_limpia = [a for a in augbls if a and a.strip()]

        try:
            lista_ordenada = sorted(lista_limpia, key=int)
        except ValueError:
            lista_ordenada = sorted(lista_limpia)

        rangos, individuales = [], []

        if not lista_ordenada:
            return rangos, individuales

        inicio = previo = lista_ordenada[0]
        conteo = 1
        MAX_RANGO = 50

        def guardar_grupo(ini, fin, cnt):
            if cnt == 1:
                individuales.append(ini)
            else:
                rangos.append((ini, fin))

        for actual in lista_ordenada[1:]:
            try:
                es_contiguo = int(actual) - int(previo) == 1
            except ValueError:
                es_contiguo = False

            if es_contiguo and conteo < MAX_RANGO:
                previo = actual
                conteo += 1
            else:
                guardar_grupo(inicio, previo, conteo)
                inicio = previo = actual
                conteo = 1

        guardar_grupo(inicio, previo, conteo)
        return rangos, individuales

    def _paso5_compensaciones(self, datos_augbl: tuple) -> int:
        rangos, individuales = datos_augbl
        if not rangos and not individuales:
            return 0

        client = SAPODataClient(base_url=SAPServiceURL.COMPENSACIONES)
        filtros_odata = []

        for chunk in _chunked_list(rangos, 5):
            partes = [f"(Augbl ge '{r[0]}' and Augbl le '{r[1]}')" for r in chunk]
            filtros_odata.append(" or ".join(partes))

        for chunk in _chunked_list(individuales, 10):
            partes = [f"Augbl eq '{val}'" for val in chunk]
            filtros_odata.append(" or ".join(partes))

        chunks_filtros = list(_chunked_list(filtros_odata, 5))

        def cb_compensaciones(registros):
            count_local = 0
            mapper = GeneradorDinamicoSAP("Compensacion")
            campos_update = [
                regla.campo_django
                for regla in mapper.reglas
                if regla.campo_django not in ("bukrs", "belnr", "gjahr", "buzei")
            ]

            for chunk_rec in _chunked_list(registros, settings.DB_BATCH_SIZE_LARGE):
                bukrs_set = {r.get("Bukrs", "") for r in chunk_rec}
                belnr_set = {r.get("Belnr", "") for r in chunk_rec}
                gjahr_set = {r.get("Gjahr", "") for r in chunk_rec}
                buzei_set = {r.get("Buzei", "") for r in chunk_rec}

                candidatos = Compensacion.objects.filter(
                    bukrs__in=bukrs_set,
                    belnr__in=belnr_set,
                    gjahr__in=gjahr_set,
                    buzei__in=buzei_set,
                )
                existentes = {
                    (p.bukrs, p.belnr, p.gjahr, p.buzei): p for p in candidatos
                }

                to_create, to_update = [], []

                for rec in chunk_rec:
                    datos_kwargs = mapper.construir_kwargs(rec)
                    llave = (
                        datos_kwargs.get("bukrs", ""),
                        datos_kwargs.get("belnr", ""),
                        datos_kwargs.get("gjahr", ""),
                        datos_kwargs.get("buzei", ""),
                    )

                    if llave in existentes:
                        obj = existentes[llave]
                        hay_cambios = False
                        for campo, nuevo_valor in datos_kwargs.items():
                            if getattr(obj, campo) != nuevo_valor:
                                setattr(obj, campo, nuevo_valor)
                                hay_cambios = True
                        if hay_cambios:
                            to_update.append(obj)
                    else:
                        obj = Compensacion(**datos_kwargs)
                        to_create.append(obj)
                        existentes[llave] = obj

                if to_create:
                    Compensacion.objects.bulk_create(
                        to_create, batch_size=settings.DB_BATCH_SIZE_LARGE
                    )
                    count_local += len(to_create)
                if to_update:
                    Compensacion.objects.bulk_update(
                        to_update,
                        campos_update,
                        batch_size=settings.DB_BATCH_SIZE_LARGE,
                    )
                    count_local += len(to_update)
            return count_local

        resultados = _procesar_y_guardar_en_paralelo_sap_batch(
            client,
            "ZFI_COMPENSACIONES",
            chunks_filtros,
            db_callback=cb_compensaciones,
            max_workers=settings.SAP_MAX_WORKERS_HEAVY,
            is_raw=True,
            paso_log=5,
            log_obj=self.log,
            paso_id="paso5",
        )

        return sum(resultados) if resultados else 0

    def _paso6_partidas_por_belnr(self) -> tuple[int, int]:
        llaves_comp = list(
            Compensacion.objects.values("bukrs", "belnr", "gjahr").distinct()
        )
        if not llaves_comp:
            return 0, 0

        belnrs_requeridos = [item["belnr"] for item in llaves_comp]
        existentes_qs = Partida.objects.filter(belnr__in=belnrs_requeridos).values_list(
            "bukrs", "belnr", "gjahr"
        )
        existentes_set = set(existentes_qs)

        llaves_faltantes = [
            item
            for item in llaves_comp
            if (item["bukrs"], item["belnr"], item["gjahr"]) not in existentes_set
        ]

        if not llaves_faltantes:
            self.log.actualizar_progreso_paso(
                "paso6", "Todos los documentos ya existen localmente. Omitiendo SAP."
            )
            return 0, 0

        client = SAPODataClient(base_url=SAPServiceURL.PARTIDAS)
        filtros_partidas = []

        for chunk_llaves in _chunked_list(llaves_faltantes, 10):
            partes_or = [
                f"(Bukrs eq '{item['bukrs']}' and Belnr eq '{item['belnr']}' and Gjahr eq '{item['gjahr']}')"
                for item in chunk_llaves
            ]
            filtros_partidas.append(" or ".join(partes_or))

        chunks_filtros = list(_chunked_list(filtros_partidas, 10))

        def cb_partidas_compo(registros):
            return _guardar_partidas_desde_sap(registros)

        resultados = _procesar_y_guardar_en_paralelo_sap_batch(
            client,
            "ZFI_PARTIDAS",
            chunks_filtros,
            db_callback=cb_partidas_compo,
            max_workers=settings.SAP_MAX_WORKERS_HEAVY,
            expand="toPosiciones",
            is_raw=True,
            paso_log=6,
            log_obj=self.log,
            paso_id="paso6",
        )

        if not resultados:
            return 0, 0

        total_creadas = sum(r[0] for r in resultados)
        total_actualizadas = sum(r[1] for r in resultados)

        return total_creadas, total_actualizadas

    def _paso7_tasas_bcv(self, fecha_inicio: date, fecha_fin: date) -> int:
        cliente_tasas = SAPTasaBCVClient(
            username=USERNAME, password=PASSWORD, sap_client=AMBIENTE_SAP
        )

        fechas_unicas = list(
            Partida.objects.filter(bldat__gte=fecha_inicio, bldat__lte=fecha_fin)
            .values_list("bldat", flat=True)
            .distinct()
        )
        if not fechas_unicas:
            return 0

        self.log.actualizar_progreso_paso(
            "paso7", f"Consultando {len(fechas_unicas)} fechas..."
        )

        fechas_en_bd = set(
            TasaBCV.objects.filter(fecha__in=fechas_unicas)
            .values_list("fecha", flat=True)
            .distinct()
        )
        fechas_a_consultar = [f for f in fechas_unicas if f not in fechas_en_bd]
        tasas_sap = cliente_tasas.obtener_tasas_lote(fechas_a_consultar)

        n_tasas_nuevas = 0
        for fecha_key, lista_tasas in tasas_sap.items():
            for t in lista_tasas:
                moneda = t.get("MONEDA", "")
                if not moneda:
                    continue
                TasaBCV.objects.update_or_create(
                    fecha=fecha_key,
                    moneda=moneda,
                    defaults={
                        "tasa": t.get("TASA", 0),
                        "descripcion": t.get("DESCRIPCION", ""),
                    },
                )
                n_tasas_nuevas += 1

        return n_tasas_nuevas

    def paso8_calculo_disponibilidad(self, fecha_inicio, fecha_fin):
        self.log.registrar_inicio_paso(
            "paso8", "Conciliación y Cálculo de Disponibilidad"
        )

        cuentas_conf = CuentaConfiguracion.objects.filter(activa=True)
        set_impuestos = set(
            cuentas_conf.filter(tipo="IMPUESTO").values_list("cuenta", flat=True)
        )
        set_dif_cambio = set(
            cuentas_conf.filter(tipo="DIF_CAMBIO").values_list("cuenta", flat=True)
        )
        set_comision = set(
            cuentas_conf.filter(tipo="COMISION").values_list("cuenta", flat=True)
        )

        mapeo_gastos = {
            obj.cuenta_gasto: obj for obj in ClasificacionGasto.objects.all()
        }

        hkonts_saldos = set(
            SaldoBancario.objects.values_list("hkont", flat=True).distinct()
        )
        hkonts_partidas = set(
            PartidaPosicion.objects.filter(ractt__startswith="11")
            .values_list("ractt", flat=True)
            .distinct()
        )
        todas_las_cuentas_db = hkonts_saldos | hkonts_partidas

        (
            cuentas_reales,
            cuentas_todas,
            cuentas_t_2,
            cuentas_t_3,
            cuentas_egresos,
            cuentas_ingresos,
        ) = set(), set(), set(), set(), set(), set()
        familias = defaultdict(set)

        for cta in todas_las_cuentas_db:
            cta_str = str(cta)
            if not cta_str:
                continue
            base = cta_str[:-1]
            ultimo = cta_str[-1]
            familias[base].add(ultimo)

        for base, terminaciones in familias.items():
            cuenta_cero = f"{base}0"
            cuentas_reales.add(cuenta_cero)
            cuentas_todas.add(cuenta_cero)

            subcuentas = [t for t in terminaciones if t in "1234567"]

            if not subcuentas:
                cuentas_egresos.add(cuenta_cero)
                cuentas_ingresos.add(cuenta_cero)
            else:
                for sufijo in subcuentas:
                    cta = f"{base}{sufijo}"
                    cuentas_todas.add(cta)
                    if sufijo in ("1", "2", "7"):
                        cuentas_egresos.add(cta)
                    if sufijo in ("3", "4", "6"):
                        cuentas_ingresos.add(cta)
                    if sufijo == "2":
                        cuentas_t_2.add(cta)
                    if sufijo == "3":
                        cuentas_t_3.add(cta)

        q_base = Q(
            ractt__in=cuentas_todas | set_comision | cuentas_reales,
            partida__budat__gte=fecha_inicio,
            partida__budat__lte=fecha_fin,
        )

        augbl_list = (
            PartidaPosicion.objects.filter(q_base)
            .exclude(augbl="")
            .exclude(augbl__isnull=True)
            .values_list("augbl", flat=True)
            .distinct()
        )
        augbl_set = set(augbl_list)
        documentos_con_augbls = PartidaPosicion.objects.filter(
            augbl__in=augbl_set
        ).values_list("partida__belnr", flat=True)

        zps_belnrs = set(
            Partida.objects.filter(blart="ZP")
            .filter(
                Q(budat__gte=fecha_inicio, budat__lte=fecha_fin)
                | Q(belnr__in=augbl_set)
                | Q(belnr__in=documentos_con_augbls)
            )
            .values_list("belnr", flat=True)
        )

        augbls_de_zps = set(
            PartidaPosicion.objects.filter(partida__belnr__in=zps_belnrs)
            .exclude(augbl="")
            .exclude(augbl__isnull=True)
            .values_list("augbl", flat=True)
        )

        zrs_zh_relacionados = set(
            PartidaPosicion.objects.filter(
                Q(augbl__in=augbls_de_zps) | Q(partida__belnr__in=augbls_de_zps),
                partida__blart__in=["ZR", "ZH", "XX"],
            ).values_list("partida__belnr", flat=True)
        )

        facturas_pagadas_por_zp = set(
            PartidaPosicion.objects.filter(augbl__in=zps_belnrs).values_list(
                "partida__belnr", flat=True
            )
        )

        q_final = (
            q_base
            | Q(partida__belnr__in=augbl_set)
            | Q(augbl__in=augbl_set)
            | Q(partida__belnr__in=zps_belnrs)
            | Q(partida__belnr__in=zrs_zh_relacionados)
            | Q(partida__belnr__in=facturas_pagadas_por_zp)
        )

        partidas_db = (
            PartidaPosicion.objects.filter(q_final)
            .select_related("partida")
            .only(
                "partida__id",
                "partida__belnr",
                "partida__blart",
                "partida__budat",
                "partida__bktxt",
                "ractt",
                "wsl",
                "rwcur",
                "lifnr",
                "kunnr",
                "augbl",
                "zuonr",
                "koart",
                "drcrk",  # Aseguramos tener el drcrk para standalone
            )
            .iterator(chunk_size=10_000)
        )

        todas_posiciones_brutas = list(partidas_db)
        mapa_banco_real = {}
        todas_posiciones = []
        documentos_con_banco = set()

        for pos in todas_posiciones_brutas:
            if pos.ractt in cuentas_reales:
                mapa_banco_real[pos.partida.belnr] = pos.ractt

            if pos.ractt in cuentas_todas or pos.ractt in cuentas_reales:
                documentos_con_banco.add(pos.partida.belnr)

            todas_posiciones.append(pos)

        # ⚡ DESCUBRIMIENTO DINÁMICO DE STANDALONES
        cuentas_cero_del_periodo = {
            pos.ractt for pos in todas_posiciones if str(pos.ractt).endswith("0")
        }
        cuentas_standalone = descubrir_cuentas_standalone(cuentas_cero_del_periodo)

        operaciones_internas, todas_posiciones = procesar_transferencias_y_divisas(
            todas_posiciones, cuentas_todas | cuentas_reales, set_dif_cambio
        )
        comisiones, todas_posiciones = procesar_comisiones_bancarias(
            todas_posiciones, mapa_banco_real, cuentas_comision=set_comision
        )

        facturas_agrupadas = defaultdict(list)
        partidas_restantes, balde_solo_zps, balde_solo_zrs = [], [], []
        mapa_factura_zp = {}

        for pos in todas_posiciones:
            belnr_doc = pos.partida.belnr
            cuenta_str = pos.ractt or ""

            if belnr_doc in zps_belnrs or pos.partida.blart == "ZP":
                if belnr_doc not in zps_belnrs:
                    zps_belnrs.add(belnr_doc)
                if cuenta_str in cuentas_todas:
                    balde_solo_zps.append(pos)
                else:
                    facturas_agrupadas[belnr_doc].append(pos)
                    mapa_factura_zp[belnr_doc] = belnr_doc

            elif belnr_doc in zrs_zh_relacionados or (
                pos.partida.blart in ("ZR", "ZH", "XX")
            ):
                if cuenta_str in cuentas_todas:
                    if cuenta_str.endswith("0") and pos.drcrk == "S":
                        partidas_restantes.append(pos)
                    # ⚡ FIX: Forzamos a que si termina en 0 y es "H" (Egreso), vaya al balde correcto
                    elif cuenta_str in cuentas_egresos or (
                        cuenta_str.endswith("0") and pos.drcrk == "H"
                    ):
                        balde_solo_zrs.append(pos)
                    else:
                        partidas_restantes.append(pos)
                else:
                    # ⚡ NUEVO: Si no es cuenta de banco, es una LÍNEA DE GASTO dentro del propio ZR.
                    facturas_agrupadas[belnr_doc].append(pos)

            elif belnr_doc in facturas_pagadas_por_zp or (
                pos.augbl and pos.augbl in zps_belnrs
            ):
                if pos.augbl and pos.augbl in zps_belnrs:
                    mapa_factura_zp[belnr_doc] = pos.augbl
                facturas_agrupadas[belnr_doc].append(pos)

            else:
                # ⚡ NUEVO: Si está compensado por un grupo bancario (sin ZP), lo guardamos para el ZR.
                if pos.augbl and pos.augbl in augbl_set:
                    facturas_agrupadas[pos.augbl].append(pos)

                # Siempre a restantes para que el módulo de ingresos lo pueda evaluar
                partidas_restantes.append(pos)

        for belnr_factura, posiciones_factura in facturas_agrupadas.items():
            augbls_presentes = set(p.augbl for p in posiciones_factura if p.augbl)
            if len(augbls_presentes) == 1:
                unico_augbl = list(augbls_presentes)[0]
                for p in posiciones_factura:
                    if not p.augbl:
                        p.augbl = unico_augbl
            elif len(augbls_presentes) == 0:
                doc_pago_zp = mapa_factura_zp.get(belnr_factura, "")
                for p in posiciones_factura:
                    if not p.augbl:
                        p.augbl = doc_pago_zp

        # ⚡ SE INYECTA EL SET DE STANDALONES
        resultados_zr, zps_aud, zrs_aud = conciliar_cadena_zr_zp_facturas(
            balde_solo_zps,
            balde_solo_zrs,
            facturas_agrupadas,
            mapa_factura_zp,
            cuentas_impuestos=set_impuestos,
            cuentas_dif_cambio=set_dif_cambio,
            cuentas_standalone=cuentas_standalone,
            cuentas_bancarias=cuentas_todas,
        )

        self.log.actualizar_progreso_paso(
            "paso8", "Identificando clientes y proveedores en documentos de ingreso..."
        )

        documentos_ingreso = set(pos.partida.belnr for pos in partidas_restantes)

        lineas_socios = PartidaPosicion.objects.filter(
            partida__belnr__in=documentos_ingreso, koart__in=["D", "K"]
        ).values("partida__belnr", "lifnr", "kunnr")

        mapa_socios = {linea["partida__belnr"]: linea for linea in lineas_socios}

        for pos in partidas_restantes:
            socio = mapa_socios.get(pos.partida.belnr)
            if socio:
                pos.lifnr = socio.get("lifnr") or ""
                pos.kunnr = socio.get("kunnr") or ""

        # ⚡ SE INYECTA EL SET DE STANDALONES
        ingresos_validados, ingresos_aud = procesar_ingresos_bancarios(
            partidas_restantes,
            cuentas_ingresos,
            documentos_con_banco,
            cuentas_standalone,
        )

        objetos_dashboard, objetos_auditoria = [], []

        for res in resultados_zr:
            doc_primario = (
                res["documento_banco"]
                if res["documento_banco"] != "EN_TRANSITO"
                else res["documento_pago"]
            )

            mapeo_obj = mapeo_gastos.get(res["cuenta_gasto"])
            cat_gasto = (
                mapeo_obj.categoria if mapeo_obj else "OTROS GASTOS (No Mapeados)"
            )
            sub_cat_gasto = mapeo_obj.sub_categoria if mapeo_obj else "Sin clasificar"

            objetos_dashboard.append(
                DashboardConsolidado(
                    tipo_operacion="EGRESOS",
                    categoria=cat_gasto,
                    sub_categoria=sub_cat_gasto,
                    cuenta_contable=res["cuenta_banco"],
                    cuenta_gasto=res["cuenta_gasto"],
                    lifnr=res["proveedor"],
                    kunnr="",
                    monto_base=res["monto"],
                    monto_total=res["monto"],
                    rwcur=res.get("rwcur", ""),
                    fecha_contabilizacion=res["fecha"],
                    documento_primario=doc_primario,
                    documento_secundario=(
                        f"ZP:{res['documento_pago']} FAC:{res['documento_factura']}"
                        if res["documento_factura"]
                        else f"ZP:{res['documento_pago']}"
                    ),
                    referencia=res["referencia"],
                    referencia1=res.get("referencia1", ""),
                )
            )

        for op in operaciones_internas:
            objetos_dashboard.append(
                DashboardConsolidado(
                    tipo_operacion=op["tipo"],
                    categoria=op["tipo"],
                    sub_categoria=op.get("sub_categoria", ""),
                    cuenta_contable=op["cuenta_salida"],
                    cuenta_gasto="",
                    lifnr="",
                    kunnr="",
                    monto_base=-abs(float(op["monto_salida"])),
                    monto_total=-abs(float(op["monto_salida"])),
                    rwcur=op.get("rwcur_salida", ""),
                    fecha_contabilizacion=op["fecha"],
                    documento_primario=op["salida"].partida.belnr,
                    documento_secundario=f"DESTINO: {op['entrada'].partida.belnr}",
                    referencia=op["ref"],
                    referencia1=(op["salida"].partida.bktxt or "").strip(),
                )
            )

            objetos_dashboard.append(
                DashboardConsolidado(
                    tipo_operacion=op["tipo"],
                    categoria=op["tipo"],
                    sub_categoria=op.get("sub_categoria", ""),
                    cuenta_contable=op["cuenta_entrada"],
                    cuenta_gasto="",
                    lifnr="",
                    kunnr="",
                    monto_base=abs(float(op["monto_entrada"])),
                    monto_total=abs(float(op["monto_entrada"])),
                    rwcur=op.get("rwcur_entrada", ""),
                    fecha_contabilizacion=op["fecha"],
                    documento_primario=op["entrada"].partida.belnr,
                    documento_secundario=f"ORIGEN: {op['salida'].partida.belnr}",
                    referencia=op["ref"],
                    referencia1=(op["entrada"].partida.bktxt or "").strip(),
                )
            )

        for cb in comisiones:
            cat_comision = mapeo_gastos.get(cb["cuenta_gasto"], "COMISION_BANCARIA")
            objetos_dashboard.append(
                DashboardConsolidado(
                    tipo_operacion="EGRESOS",
                    categoria=cat_comision,
                    sub_categoria="COMISION_BANCARIA",
                    cuenta_contable=cb["cuenta_banco"],
                    cuenta_gasto=cb["cuenta_gasto"],
                    lifnr="",
                    kunnr="",
                    monto_base=cb["monto"],
                    monto_total=cb["monto"],
                    rwcur=cb.get("rwcur", ""),
                    fecha_contabilizacion=cb["fecha"],
                    documento_primario=cb["documento_primario"],
                    documento_secundario="",
                    referencia=cb["referencia"],
                    referencia1=cb.get("referencia1", ""),
                )
            )

        for res in ingresos_validados:
            cat = (
                "INGRESO_TARJETA"
                if res["cuenta_banco"].endswith("4")
                else "INGRESO_DEPOSITO"
            )

            objetos_dashboard.append(
                DashboardConsolidado(
                    tipo_operacion="INGRESOS",
                    categoria=cat,
                    sub_categoria=res.get("sub_categoria", ""),
                    cuenta_contable=res["cuenta_banco"],
                    cuenta_gasto="",
                    lifnr=res.get("lifnr", ""),
                    kunnr=res.get("kunnr", ""),
                    monto_base=res["monto"],
                    monto_total=res["monto"],
                    rwcur=res.get("rwcur", ""),
                    fecha_contabilizacion=res["fecha"],
                    documento_primario=res["documento_primario"],
                    documento_secundario=res["documento_secundario"],
                    referencia=res["referencia"],
                    referencia1=res.get("referencia1", ""),
                )
            )

        def _agregar_auditoria(posicion, motivo):
            objetos_auditoria.append(
                AsientoAuditoria(
                    bukrs=posicion.partida.bukrs,
                    belnr=posicion.partida.belnr,
                    gjahr=posicion.partida.gjahr,
                    blart=posicion.partida.blart,
                    cuenta_contable=posicion.ractt,
                    monto=abs(float(posicion.wsl)),
                    rwcur=posicion.rwcur or "",
                    fecha=posicion.partida.budat,
                    motivo_descarte=motivo,
                    texto_cabecera=(posicion.partida.bktxt or "").strip(),
                )
            )

        for zp in zps_aud:
            _agregar_auditoria(zp, "ZP abierto. Falló conciliación contra ZR.")
        for zr in zrs_aud:
            _agregar_auditoria(
                zr,
                f"{zr.partida.blart} abierto. Falló conciliación de pago contra lote ZP.",
            )
        for pos, motivo in ingresos_aud:
            _agregar_auditoria(pos, motivo)

        docs_primarios = [
            obj.documento_primario
            for obj in objetos_dashboard
            if obj.documento_primario
        ]
        if docs_primarios:
            DashboardConsolidado.objects.filter(
                documento_primario__in=docs_primarios
            ).delete()
            AsientoAuditoria.objects.filter(belnr__in=docs_primarios).delete()

        DashboardConsolidado.objects.filter(
            fecha_contabilizacion__gte=fecha_inicio,
            fecha_contabilizacion__lte=fecha_fin,
        ).delete()
        AsientoAuditoria.objects.filter(
            fecha__gte=fecha_inicio, fecha__lte=fecha_fin
        ).delete()

        if objetos_dashboard:
            DashboardConsolidado.objects.bulk_create(objetos_dashboard, batch_size=2000)
        if objetos_auditoria:
            AsientoAuditoria.objects.bulk_create(objetos_auditoria, batch_size=2000)

        self.log.refresh_from_db(fields=["errores_count"])
        err_antes = self.log.errores_count

        self.log.registrar_fin_paso(
            "paso8",
            {
                "dashboard_registros": len(objetos_dashboard),
                "auditoria_registros": len(objetos_auditoria),
                "cadena_zr_zp_filas": len(resultados_zr),
                "operaciones_internas": len(operaciones_internas),
                "comisiones_detectadas": len(comisiones),
                "ingresos_validados": len(ingresos_validados),
            },
            estado=_obtener_estado_paso(self.log, err_antes),
        )
