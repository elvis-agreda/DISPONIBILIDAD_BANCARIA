"""
tasks.py — Workers Huey para el pipeline de sincronización SAP ECC (8 pasos).
"""

import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from django.db.models import Q
from django.utils import timezone
from huey import crontab
from huey.contrib.djhuey import db_periodic_task, db_task
from services.sap_client import (
    AMBIENTE_SAP,
    PASSWORD,
    USERNAME,
    SAPODataClient,
    SAPServiceURL,
    SAPTasaBCVClient,
    fecha_sap,
)
from utils.utils import (
    conciliar_cadena_zr_zp_facturas,
    procesar_comisiones_bancarias,
    procesar_ingresos_bancarios,
    procesar_transferencias_y_divisas,
    sap_date_to_python,
)

from .models import (
    AsientoAuditoria,
    Compensacion,
    DashboardConsolidado,
    Partida,
    PartidaPosicion,
    PartidaPosicionFiltro,
    SaldoBancario,
    SaldoBancarioManager,
    SincronizacionLog,
    TasaBCV,
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


def _obtener_estado_paso(log: SincronizacionLog, errores_antes: int) -> str:
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

    for chunk in _chunked_list(registros_pos, 2000):
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

        to_create = []
        to_update = []

        for pos in chunk:
            llave = (
                pos.get("Bukrs", ""),
                pos.get("Docnr", ""),
                pos.get("Ryear", ""),
                pos.get("Docln", ""),
            )
            ractt = pos.get("Ractt", "")
            budat = sap_date_to_python(pos.get("Budat"))

            if llave in existentes:
                obj = existentes[llave]
                if obj.ractt != ractt or obj.budat != budat:
                    obj.ractt = ractt
                    obj.budat = budat
                    to_update.append(obj)
            else:
                obj = PartidaPosicionFiltro(
                    bukrs=llave[0],
                    docnr=llave[1],
                    ryear=llave[2],
                    docln=llave[3],
                    ractt=ractt,
                    budat=budat,
                )
                to_create.append(obj)
                existentes[llave] = obj

        if to_create:
            PartidaPosicionFiltro.objects.bulk_create(to_create, batch_size=2000)
        if to_update:
            PartidaPosicionFiltro.objects.bulk_update(
                to_update, ["ractt", "budat"], batch_size=2000
            )


def _guardar_posiciones_bulk(posiciones_raw: list):
    if not posiciones_raw:
        return

    for chunk in _chunked_list(posiciones_raw, 2000):
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

        to_create = []
        to_update = []

        for pos in chunk:
            llave = (
                pos.get("Bukrs", ""),
                pos.get("Docnr", ""),
                pos.get("Ryear", ""),
                pos.get("Docln", ""),
            )
            partida_obj = pos.get("_partida_ref")

            if not partida_obj or not partida_obj.pk:
                continue

            ractt = pos.get("Ractt", "")
            wsl = Decimal(str(pos.get("Wsl", 0) or 0))
            drcrk = pos.get("Drcrk", "")
            rwcur = pos.get("Rwcur", "")
            lifnr = pos.get("Lifnr", "")
            kunnr = pos.get("Kunnr", "")
            koart = pos.get("Koart", "")
            augbl = pos.get("Augbl", "")
            zuonr = pos.get("Zuonr", "")
            budat = sap_date_to_python(pos.get("Budat"))

            if llave in existentes:
                obj = existentes[llave]
                if (
                    obj.ractt != ractt
                    or obj.wsl != wsl
                    or obj.drcrk != drcrk
                    or obj.rwcur != rwcur
                    or obj.lifnr != lifnr
                    or obj.kunnr != kunnr
                    or obj.koart != koart
                    or obj.augbl != augbl
                    or obj.zuonr != zuonr
                    or obj.partida_id != partida_obj.pk
                ):
                    obj.partida = partida_obj
                    obj.ractt = ractt
                    obj.wsl = wsl
                    obj.drcrk = drcrk
                    obj.rwcur = rwcur
                    obj.lifnr = lifnr
                    obj.kunnr = kunnr
                    obj.koart = koart
                    obj.augbl = augbl
                    obj.zuonr = zuonr
                    obj.budat = budat
                    to_update.append(obj)
            else:
                obj = PartidaPosicion(
                    partida=partida_obj,
                    bukrs=llave[0],
                    docnr=llave[1],
                    ryear=llave[2],
                    docln=llave[3],
                    ractt=ractt,
                    wsl=wsl,
                    drcrk=drcrk,
                    rwcur=rwcur,
                    lifnr=lifnr,
                    kunnr=kunnr,
                    koart=koart,
                    augbl=augbl,
                    zuonr=zuonr,
                    budat=budat,
                )
                to_create.append(obj)
                existentes[llave] = obj

        if to_create:
            PartidaPosicion.objects.bulk_create(to_create, batch_size=2000)
        if to_update:
            PartidaPosicion.objects.bulk_update(
                to_update,
                [
                    "partida",
                    "ractt",
                    "wsl",
                    "drcrk",
                    "rwcur",
                    "lifnr",
                    "kunnr",
                    "koart",
                    "augbl",
                    "zuonr",
                    "budat",
                ],
                batch_size=2000,
            )


def _guardar_partidas_desde_sap(registros_sap: list) -> tuple[int, int]:
    if not registros_sap:
        return 0, 0
    total_creadas = total_actualizadas = 0

    for chunk in _chunked_list(registros_sap, 1000):
        bukrs_set = {d.get("Bukrs", "") for d in chunk}
        belnr_set = {d.get("Belnr", "") for d in chunk}
        gjahr_set = {d.get("Gjahr", "") for d in chunk}

        candidatos = Partida.objects.filter(
            bukrs__in=bukrs_set, belnr__in=belnr_set, gjahr__in=gjahr_set
        )
        existentes = {(p.bukrs, p.belnr, p.gjahr): p for p in candidatos}

        to_create = []
        to_update = []
        posiciones_raw_list = []

        for doc in chunk:
            llave = (doc.get("Bukrs", ""), doc.get("Belnr", ""), doc.get("Gjahr", ""))
            blart = doc.get("Blart", "")
            bktxt = doc.get("Bktxt", "")
            bldat = sap_date_to_python(doc.get("Bldat"))
            budat = sap_date_to_python(doc.get("Budat"))

            if llave in existentes:
                p = existentes[llave]
                if (
                    p.blart != blart
                    or p.bldat != bldat
                    or p.budat != budat
                    or p.bktxt != bktxt
                ):
                    p.blart = blart
                    p.bktxt = bktxt
                    p.bldat = bldat
                    p.budat = budat
                    to_update.append(p)
            else:
                p = Partida(
                    bukrs=llave[0],
                    belnr=llave[1],
                    gjahr=llave[2],
                    blart=blart,
                    bktxt=bktxt,
                    bldat=bldat,
                    budat=budat,
                )
                to_create.append(p)
                existentes[llave] = p

        if to_create:
            Partida.objects.bulk_create(to_create, batch_size=1000)
            total_creadas += len(to_create)
            nuevos = Partida.objects.filter(
                bukrs__in=bukrs_set, belnr__in=belnr_set, gjahr__in=gjahr_set
            )
            for n in nuevos:
                existentes[(n.bukrs, n.belnr, n.gjahr)] = n

        if to_update:
            Partida.objects.bulk_update(
                to_update, ["blart", "bktxt", "bldat", "budat"], batch_size=1000
            )
            total_actualizadas += len(to_update)

        for doc in chunk:
            llave_padre = (
                doc.get("Bukrs", ""),
                doc.get("Belnr", ""),
                doc.get("Gjahr", ""),
            )
            partida_obj = existentes.get(llave_padre)

            for pos in doc.get("toPosiciones", {}).get("results", []):
                pos["_partida_ref"] = partida_obj
                posiciones_raw_list.append(pos)

        _guardar_posiciones_bulk(posiciones_raw_list)

    return total_creadas, total_actualizadas


@db_periodic_task(crontab(hour="1", minute="0"))
def tarea_sync_automatica():
    ayer = date.today() - timedelta(days=1)
    ejecutar_sync_sap(fecha_inicio=ayer, fecha_fin=ayer, tipo="AUTO")


@db_task()
def ejecutar_sync_sap(
    fecha_inicio: date,
    fecha_fin: date,
    tipo: str = "MANUAL",
    sync_log_id: Optional[int] = None,
):
    anio, periodo = _fecha_a_anio_periodo(fecha_inicio)

    if sync_log_id:
        log = SincronizacionLog.objects.get(pk=sync_log_id)
        log.anio = anio
        log.periodo = periodo
    else:
        log = SincronizacionLog.objects.create(
            tipo=tipo,
            estado="INICIADO",
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            anio=anio,
            periodo=periodo,
        )

    log.estado = "EN_CURSO"
    log.save(update_fields=["estado", "anio", "periodo"])

    try:
        log.verificar_cancelacion()
        log.refresh_from_db(fields=["errores_count"])
        err_antes = log.errores_count
        log.registrar_inicio_paso("paso1", "Saldos Bancarios")
        sc, sa = _paso1_saldos_bancarios(anio, log)
        log.saldos_creados += sc
        log.saldos_actualizados += sa
        log.save(update_fields=["saldos_creados", "saldos_actualizados"])
        log.registrar_fin_paso(
            "paso1",
            {"creados": sc, "actualizados": sa},
            estado=_obtener_estado_paso(log, err_antes),
        )

        log.verificar_cancelacion()
        log.refresh_from_db(fields=["errores_count"])
        err_antes = log.errores_count
        log.registrar_inicio_paso("paso2", "Derivación de Cuentas HKONT")
        cuentas_derivadas = _paso2_derivar_hkont()
        log.registrar_fin_paso(
            "paso2",
            {"cuentas_obtenidas": len(cuentas_derivadas)},
            estado=_obtener_estado_paso(log, err_antes),
        )

        log.verificar_cancelacion()
        log.refresh_from_db(fields=["errores_count"])
        err_antes = log.errores_count
        log.registrar_inicio_paso("paso3", "Partidas por Rango de Fechas")
        pc, pa = _paso3_partidas_por_fechas(
            fecha_inicio, fecha_fin, cuentas_derivadas, log
        )
        log.partidas_creadas += pc
        log.partidas_actualizadas += pa
        log.save(update_fields=["partidas_creadas", "partidas_actualizadas"])
        log.registrar_fin_paso(
            "paso3",
            {"creadas": pc, "actualizadas": pa},
            estado=_obtener_estado_paso(log, err_antes),
        )

        log.verificar_cancelacion()
        log.refresh_from_db(fields=["errores_count"])
        err_antes = log.errores_count
        log.registrar_inicio_paso("paso4", "Extracción de Rangos AUGBL")
        datos_augbl = _paso4_rangos_augbl(fecha_inicio, fecha_fin)
        total_identificados = len(datos_augbl[0]) + len(datos_augbl[1])
        log.registrar_fin_paso(
            "paso4",
            {"grupos_identificados": total_identificados},
            estado=_obtener_estado_paso(log, err_antes),
        )

        log.verificar_cancelacion()
        log.refresh_from_db(fields=["errores_count"])
        err_antes = log.errores_count
        log.registrar_inicio_paso("paso5", "Sincronización de Compensaciones")
        n_comp = _paso5_compensaciones(datos_augbl, log)
        log.compensaciones_proc += n_comp
        log.save(update_fields=["compensaciones_proc"])
        log.registrar_fin_paso(
            "paso5", {"procesadas": n_comp}, estado=_obtener_estado_paso(log, err_antes)
        )

        log.verificar_cancelacion()
        log.refresh_from_db(fields=["errores_count"])
        err_antes = log.errores_count
        log.registrar_inicio_paso("paso6", "Partidas complementarias por BELNR")
        pc2, pa2 = _paso6_partidas_por_belnr(log)
        log.partidas_creadas += pc2
        log.partidas_actualizadas += pa2
        log.save(update_fields=["partidas_creadas", "partidas_actualizadas"])
        log.registrar_fin_paso(
            "paso6",
            {"creadas": pc2, "actualizadas": pa2},
            estado=_obtener_estado_paso(log, err_antes),
        )

        log.verificar_cancelacion()
        log.refresh_from_db(fields=["errores_count"])
        err_antes = log.errores_count
        log.registrar_inicio_paso("paso7", "Actualización de Tasas BCV")
        n_tasas = _paso7_tasas_bcv(fecha_inicio, fecha_fin, log)
        log.tasas_procesadas += n_tasas
        log.save(update_fields=["tasas_procesadas"])
        log.registrar_fin_paso(
            "paso7",
            {"nuevas_procesadas": n_tasas},
            estado=_obtener_estado_paso(log, err_antes),
        )

        log.verificar_cancelacion()
        log.refresh_from_db(fields=["errores_count"])
        err_antes = log.errores_count
        log.registrar_inicio_paso("paso8", "Conciliacion y Cálculo de Disponibilidad")
        _paso8_calculo_disponibilidad(fecha_inicio, fecha_fin, log)

        log.refresh_from_db(fields=["errores_count"])
        estado_final = "PARCIAL" if log.errores_count > 0 else "EXITOSO"
        log.marcar_finalizado(estado_final)

    except InterruptedError as exc:
        log.registrar_error(paso=0, mensaje=str(exc))
        log.marcar_finalizado("CANCELADO")
    except Exception as exc:
        log.registrar_error(paso=0, mensaje=f"Error fatal general: {exc}")
        log.marcar_finalizado("FALLIDO")
        raise


@db_task()
def ejecutar_paso8_manual(fecha_inicio: date, fecha_fin: date):
    anio, periodo = _fecha_a_anio_periodo(fecha_inicio)
    log = SincronizacionLog.objects.create(
        tipo="MANUAL",
        estado="EN_CURSO",
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        anio=anio,
        periodo=periodo,
    )
    try:
        _paso8_calculo_disponibilidad(fecha_inicio, fecha_fin, log)
        log.marcar_finalizado("EXITOSO")
    except Exception as exc:
        log.registrar_error(8, mensaje=f"Error fatal en reprocesamiento manual: {exc}")
        log.marcar_finalizado("FALLIDO")
        raise


@db_task()
def reintentar_sincronizacion(log_id: int):
    try:
        log = SincronizacionLog.objects.get(pk=log_id)
    except SincronizacionLog.DoesNotExist:
        return

    if log.estado not in ["FALLIDO", "PARCIAL", "CANCELADO"]:
        return

    log.estado = "EN_CURSO"
    log.save(update_fields=["estado"])

    progreso = log.progreso_detalle or {}

    def needs_run(paso_key):
        if paso_key not in progreso:
            return True
        return progreso[paso_key].get("estado") != "EXITOSO"

    try:
        fecha_inicio = log.fecha_inicio
        fecha_fin = log.fecha_fin
        anio = log.anio

        if needs_run("paso1"):
            log.verificar_cancelacion()
            log.refresh_from_db(fields=["errores_count"])
            err_antes = log.errores_count
            log.registrar_inicio_paso("paso1", "Saldos Bancarios (Reintento)")
            sc, sa = _paso1_saldos_bancarios(anio, log)
            log.saldos_creados += sc
            log.saldos_actualizados += sa
            log.save(update_fields=["saldos_creados", "saldos_actualizados"])
            log.registrar_fin_paso(
                "paso1",
                {"creados": sc, "actualizados": sa},
                estado=_obtener_estado_paso(log, err_antes),
            )

        if needs_run("paso3"):
            log.verificar_cancelacion()
            log.refresh_from_db(fields=["errores_count"])
            err_antes = log.errores_count
            log.registrar_inicio_paso("paso2", "Derivación de Cuentas HKONT")
            cuentas_derivadas = _paso2_derivar_hkont()
            log.registrar_fin_paso(
                "paso2",
                {"cuentas_obtenidas": len(cuentas_derivadas)},
                estado=_obtener_estado_paso(log, err_antes),
            )

            log.verificar_cancelacion()
            log.refresh_from_db(fields=["errores_count"])
            err_antes = log.errores_count
            log.registrar_inicio_paso(
                "paso3", "Partidas por Rango de Fechas (Reintento)"
            )
            pc, pa = _paso3_partidas_por_fechas(
                fecha_inicio, fecha_fin, cuentas_derivadas, log
            )
            log.partidas_creadas += pc
            log.partidas_actualizadas += pa
            log.save(update_fields=["partidas_creadas", "partidas_actualizadas"])
            log.registrar_fin_paso(
                "paso3",
                {"creadas": pc, "actualizadas": pa},
                estado=_obtener_estado_paso(log, err_antes),
            )

        if needs_run("paso5"):
            log.verificar_cancelacion()
            log.refresh_from_db(fields=["errores_count"])
            err_antes = log.errores_count
            log.registrar_inicio_paso("paso4", "Extracción de Rangos AUGBL")
            datos_augbl = _paso4_rangos_augbl(fecha_inicio, fecha_fin)
            total_identificados = len(datos_augbl[0]) + len(datos_augbl[1])
            log.registrar_fin_paso(
                "paso4",
                {"grupos_identificados": total_identificados},
                estado=_obtener_estado_paso(log, err_antes),
            )

            log.verificar_cancelacion()
            log.refresh_from_db(fields=["errores_count"])
            err_antes = log.errores_count
            log.registrar_inicio_paso(
                "paso5", "Sincronización de Compensaciones (Reintento)"
            )
            n_comp = _paso5_compensaciones(datos_augbl, log)
            log.compensaciones_proc += n_comp
            log.save(update_fields=["compensaciones_proc"])
            log.registrar_fin_paso(
                "paso5",
                {"procesadas": n_comp},
                estado=_obtener_estado_paso(log, err_antes),
            )

        if needs_run("paso6"):
            log.verificar_cancelacion()
            log.refresh_from_db(fields=["errores_count"])
            err_antes = log.errores_count
            log.registrar_inicio_paso(
                "paso6", "Partidas complementarias por BELNR (Reintento)"
            )
            pc2, pa2 = _paso6_partidas_por_belnr(log)
            log.partidas_creadas += pc2
            log.partidas_actualizadas += pa2
            log.save(update_fields=["partidas_creadas", "partidas_actualizadas"])
            log.registrar_fin_paso(
                "paso6",
                {"creadas": pc2, "actualizadas": pa2},
                estado=_obtener_estado_paso(log, err_antes),
            )

        if needs_run("paso7"):
            log.verificar_cancelacion()
            log.refresh_from_db(fields=["errores_count"])
            err_antes = log.errores_count
            log.registrar_inicio_paso("paso7", "Actualización de Tasas BCV (Reintento)")
            n_tasas = _paso7_tasas_bcv(fecha_inicio, fecha_fin, log)
            log.tasas_procesadas += n_tasas
            log.save(update_fields=["tasas_procesadas"])
            log.registrar_fin_paso(
                "paso7",
                {"nuevas_procesadas": n_tasas},
                estado=_obtener_estado_paso(log, err_antes),
            )

        if needs_run("paso8"):
            log.verificar_cancelacion()
            _paso8_calculo_disponibilidad(fecha_inicio, fecha_fin, log)

        log.refresh_from_db(fields=["errores_count"])
        estado_final = "PARCIAL" if log.errores_count > 0 else "EXITOSO"
        log.marcar_finalizado(estado_final)

    except InterruptedError as exc:
        log.registrar_error(paso=0, mensaje=str(exc))
        log.marcar_finalizado("CANCELADO")
    except Exception as exc:
        log.registrar_error(paso=0, mensaje=f"Error fatal en reintento: {exc}")
        log.marcar_finalizado("FALLIDO")
        raise


def _paso1_saldos_bancarios(anio: str, log: SincronizacionLog) -> tuple[int, int]:
    client = SAPODataClient(base_url=SAPServiceURL.SALDOS_BANCARIOS)
    filtro_saldos = f"Ryear eq '{anio}'"

    log.actualizar_progreso_paso("paso1", "Extrayendo registros de SAP...")
    registros, errores = client.get_data("ZFI_SALDO_BANCARIO", filters=filtro_saldos)

    if errores:
        for err in errores:
            log.registrar_error(1, err, contexto={"filtro_saldos": filtro_saldos})
        if not registros:
            raise RuntimeError(f"Error fatal en SALDOS_BANCARIOS: {errores}")

    creados = actualizados = 0
    campos_tsl = SaldoBancarioManager.SALDO_FIELDS

    log.actualizar_progreso_paso(
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

        to_create = []
        to_update = []

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


def _paso2_derivar_hkont() -> list[str]:
    hkonts_base = SaldoBancario.objects.values_list("hkont", flat=True).distinct()
    cuentas = set()
    for hkont in hkonts_base:
        cuentas.add(hkont)
        base = hkont[:-1]
        for sufijo in range(1, 8):
            cuentas.add(f"{base}{sufijo}")
    return sorted(cuentas)


def _paso3_partidas_por_fechas(
    fecha_inicio: date, fecha_fin: date, cuentas: list[str], log: SincronizacionLog
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
        max_workers=5,
        is_raw=True,
        paso_log=3,
        log_obj=log,
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
        max_workers=5,
        expand="toPosiciones",
        use_filters=True,
        paso_log=3,
        log_obj=log,
        paso_id="paso3",
    )

    for c, a in resultados:
        total_creadas += c
        total_actualizadas += a

    return total_creadas, total_actualizadas


def _paso4_rangos_augbl(fecha_inicio: date, fecha_fin: date) -> tuple:
    augbls = (
        PartidaPosicion.objects.filter(
            partida__budat__gte=fecha_inicio, partida__budat__lte=fecha_fin
        )
        .exclude(augbl="")
        .values_list("augbl", flat=True)
        .distinct()
    )

    lista_limpia = [a for a in augbls if a.strip()]
    try:
        lista_ordenada = sorted(lista_limpia, key=int)
    except ValueError:
        lista_ordenada = sorted(lista_limpia)

    rangos = []
    individuales = []

    if not lista_ordenada:
        return rangos, individuales

    inicio = lista_ordenada[0]
    previo = lista_ordenada[0]
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
            inicio = actual
            previo = actual
            conteo = 1

    guardar_grupo(inicio, previo, conteo)
    return rangos, individuales


def _paso5_compensaciones(datos_augbl: tuple, log: SincronizacionLog) -> int:
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
        for chunk_rec in _chunked_list(registros, 2000):
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
            existentes = {(p.bukrs, p.belnr, p.gjahr, p.buzei): p for p in candidatos}

            to_create, to_update = [], []

            for rec in chunk_rec:
                llave = (
                    rec.get("Bukrs", ""),
                    rec.get("Belnr", ""),
                    rec.get("Gjahr", ""),
                    rec.get("Buzei", ""),
                )

                shkzg = rec.get("Shkzg", "")
                pswsl = rec.get("Pswsl", "")
                zuonr = rec.get("Zuonr", "")
                sgtxt = rec.get("Sgtxt", "")
                saknr = rec.get("Saknr", "")
                hkont = rec.get("Hkont", "")
                kunnr = rec.get("Kunnr", "")
                lifnr = rec.get("Lifnr", "")
                augbl = rec.get("Augbl", "")
                bschl = rec.get("Bschl", "")
                koart = rec.get("Koart", "")
                dmbtr = Decimal(str(rec.get("Dmbtr", 0) or 0))
                wrbtr = Decimal(str(rec.get("Wrbtr", 0) or 0))
                pswbt = Decimal(str(rec.get("Pswbt", 0) or 0))
                augdt = sap_date_to_python(rec.get("Augdt"))
                augcp = sap_date_to_python(rec.get("Augcp"))

                if llave in existentes:
                    obj = existentes[llave]
                    obj.shkzg = shkzg
                    obj.dmbtr = dmbtr
                    obj.wrbtr = wrbtr
                    obj.pswbt = pswbt
                    obj.pswsl = pswsl
                    obj.zuonr = zuonr
                    obj.sgtxt = sgtxt
                    obj.saknr = saknr
                    obj.hkont = hkont
                    obj.kunnr = kunnr
                    obj.lifnr = lifnr
                    obj.augdt = augdt
                    obj.augcp = augcp
                    obj.augbl = augbl
                    obj.bschl = bschl
                    obj.koart = koart
                    to_update.append(obj)
                else:
                    obj = Compensacion(
                        bukrs=llave[0],
                        belnr=llave[1],
                        gjahr=llave[2],
                        buzei=llave[3],
                        shkzg=shkzg,
                        dmbtr=dmbtr,
                        wrbtr=wrbtr,
                        pswbt=pswbt,
                        pswsl=pswsl,
                        zuonr=zuonr,
                        sgtxt=sgtxt,
                        saknr=saknr,
                        hkont=hkont,
                        kunnr=kunnr,
                        lifnr=lifnr,
                        augdt=augdt,
                        augcp=augcp,
                        augbl=augbl,
                        bschl=bschl,
                        koart=koart,
                    )
                    to_create.append(obj)
                    existentes[llave] = obj

            if to_create:
                Compensacion.objects.bulk_create(to_create, batch_size=2000)
                count_local += len(to_create)
            if to_update:
                Compensacion.objects.bulk_update(
                    to_update,
                    [
                        "shkzg",
                        "dmbtr",
                        "wrbtr",
                        "pswbt",
                        "pswsl",
                        "zuonr",
                        "sgtxt",
                        "saknr",
                        "hkont",
                        "kunnr",
                        "lifnr",
                        "augdt",
                        "augcp",
                        "augbl",
                        "bschl",
                        "koart",
                    ],
                    batch_size=2000,
                )
                count_local += len(to_update)
        return count_local

    resultados = _procesar_y_guardar_en_paralelo_sap_batch(
        client,
        "ZFI_COMPENSACIONES",
        chunks_filtros,
        db_callback=cb_compensaciones,
        max_workers=8,
        is_raw=True,
        paso_log=5,
        log_obj=log,
        paso_id="paso5",
    )

    return sum(resultados) if resultados else 0


def _paso6_partidas_por_belnr(log: SincronizacionLog) -> tuple[int, int]:
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
        log.actualizar_progreso_paso(
            "paso6", "Todos los documentos ya existen localmente. Omitiendo SAP."
        )
        return 0, 0

    client = SAPODataClient(base_url=SAPServiceURL.PARTIDAS)
    filtros_partidas = []

    for chunk_llaves in _chunked_list(llaves_faltantes, 10):
        partes_or = []
        for item in chunk_llaves:
            partes_or.append(
                f"(Bukrs eq '{item['bukrs']}' and Belnr eq '{item['belnr']}' and Gjahr eq '{item['gjahr']}')"
            )
        filtros_partidas.append(" or ".join(partes_or))

    chunks_filtros = list(_chunked_list(filtros_partidas, 10))

    def cb_partidas_compo(registros):
        return _guardar_partidas_desde_sap(registros)

    resultados = _procesar_y_guardar_en_paralelo_sap_batch(
        client,
        "ZFI_PARTIDAS",
        chunks_filtros,
        db_callback=cb_partidas_compo,
        max_workers=8,
        expand="toPosiciones",
        is_raw=True,
        paso_log=6,
        log_obj=log,
        paso_id="paso6",
    )

    if not resultados:
        return 0, 0

    total_creadas = sum(r[0] for r in resultados)
    total_actualizadas = sum(r[1] for r in resultados)

    return total_creadas, total_actualizadas


def _paso7_tasas_bcv(
    fecha_inicio: date, fecha_fin: date, log: SincronizacionLog
) -> int:
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

    log.actualizar_progreso_paso("paso7", f"Consultando {len(fechas_unicas)} fechas...")

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


def _paso8_calculo_disponibilidad(fecha_inicio, fecha_fin, log):
    log.registrar_inicio_paso("paso8", "Conciliación y Cálculo de Disponibilidad")

    hkonts_base = SaldoBancario.objects.values_list("hkont", flat=True).distinct()

    cuentas_reales = set()
    cuentas_todas = set()
    cuentas_t_2 = set()
    cuentas_t_3 = set()
    cuentas_egresos = set()
    cuentas_ingresos = set()

    for hkont in hkonts_base:
        cuentas_reales.add(hkont)
        base = hkont[:-1]
        for sufijo in range(1, 8):
            cta = f"{base}{sufijo}"
            cuentas_todas.add(cta)

            suf_str = str(sufijo)
            if suf_str in ("1", "2", "7"):
                cuentas_egresos.add(cta)
            if suf_str in ("3", "4", "6"):
                cuentas_ingresos.add(cta)
            if suf_str == "2":
                cuentas_t_2.add(cta)
            if suf_str == "3":
                cuentas_t_3.add(cta)

    q_base = Q(
        ractt__in=cuentas_todas | {"525010103"} | cuentas_reales,
        partida__budat__gte=fecha_inicio,
        partida__budat__lte=fecha_fin,
    )

    augbl_list = (
        PartidaPosicion.objects.filter(q_base)
        .exclude(augbl="")
        .exclude(augbl__isnull=True)
        .exclude(partida__blart="SK")
        .values_list("augbl", flat=True)
        .distinct()
    )
    augbl_set = set(augbl_list)

    zps_belnrs = set(
        Partida.objects.filter(blart="ZP")
        .filter(
            Q(budat__gte=fecha_inicio, budat__lte=fecha_fin) | Q(belnr__in=augbl_set)
        )
        .values_list("belnr", flat=True)
    )

    zrs_relacionados = set(
        PartidaPosicion.objects.filter(partida__belnr__in=zps_belnrs)
        .exclude(augbl="")
        .exclude(augbl__in=zps_belnrs)
        .exclude(partida__blart="SK")
        .values_list("augbl", flat=True)
    )

    facturas_pagadas_por_zp = set(
        PartidaPosicion.objects.filter(augbl__in=zps_belnrs)
        .exclude(partida__blart="SK")
        .values_list("partida__belnr", flat=True)
    )

    q_final = (
        q_base
        | Q(partida__belnr__in=augbl_set)
        | Q(augbl__in=augbl_set)
        | Q(partida__belnr__in=zps_belnrs)
        | Q(partida__belnr__in=zrs_relacionados)
        | Q(partida__belnr__in=facturas_pagadas_por_zp)
    )

    partidas_db = (
        PartidaPosicion.objects.filter(q_final)
        .exclude(partida__blart="SK")
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
        )
        .iterator(chunk_size=10_000)
    )

    todas_posiciones_brutas = list(partidas_db)

    mapa_banco_real = {}
    todas_posiciones = []

    for pos in todas_posiciones_brutas:
        if pos.ractt in cuentas_reales:
            mapa_banco_real[pos.partida.belnr] = pos.ractt
        else:
            todas_posiciones.append(pos)

    operaciones_internas, todas_posiciones = procesar_transferencias_y_divisas(
        todas_posiciones, cuentas_todas
    )

    comisiones, todas_posiciones = procesar_comisiones_bancarias(
        todas_posiciones, mapa_banco_real
    )

    facturas_agrupadas = defaultdict(list)
    partidas_restantes = []
    balde_solo_zps = []
    balde_solo_zrs = []
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

        elif belnr_doc in zrs_relacionados or (
            pos.partida.blart == "ZR" and cuenta_str in cuentas_egresos
        ):
            if cuenta_str in cuentas_todas:
                balde_solo_zrs.append(pos)

        elif belnr_doc in facturas_pagadas_por_zp or (
            pos.augbl and pos.augbl in zps_belnrs
        ):
            if pos.augbl and pos.augbl in zps_belnrs:
                mapa_factura_zp[belnr_doc] = pos.augbl
            facturas_agrupadas[belnr_doc].append(pos)

        else:
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

    resultados_zr, zps_aud, zrs_aud = conciliar_cadena_zr_zp_facturas(
        balde_solo_zps,
        balde_solo_zrs,
        facturas_agrupadas,
        mapa_factura_zp,
    )

    ingresos_validados, ingresos_aud = procesar_ingresos_bancarios(
        partidas_restantes, cuentas_ingresos
    )

    objetos_dashboard = []
    objetos_auditoria = []

    for res in resultados_zr:
        doc_primario = (
            res["zr_belnr"] if res["zr_belnr"] != "EN_TRANSITO" else res["zp_belnr"]
        )
        objetos_dashboard.append(
            DashboardConsolidado(
                tipo_operacion="EGRESO",
                categoria="PROPUESTA_PAGO",
                sub_categoria="",
                cuenta_contable=res["cuenta_banco"],
                cuenta_gasto=res["cuenta_gasto"],
                lifnr=res["lifnr"],
                kunnr=res["kunnr"],
                monto_base=res["monto_base"],
                monto_total=res["monto_total"],
                rwcur=res.get("rwcur", ""),
                fecha_contabilizacion=res["fecha_contabilizacion"],
                documento_primario=doc_primario,
                documento_secundario=(
                    f"ZP:{res['zp_belnr']} FAC:{res['factura_belnr']}"
                    if res["factura_belnr"]
                    else f"ZP:{res['zp_belnr']}"
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
                sub_categoria="",
                cuenta_contable=f"{op['cuenta_salida']}→{op['cuenta_entrada']}",
                cuenta_gasto="",
                lifnr="",
                kunnr="",
                monto_base=op["monto_salida"],
                monto_total=op["monto_salida"],
                rwcur=op.get("rwcur_salida", ""),
                fecha_contabilizacion=op["fecha"],
                documento_primario=op["salida"].partida.belnr,
                documento_secundario=op["entrada"].partida.belnr,
                referencia=op["ref"],
                referencia1=(op["salida"].partida.bktxt or "").strip(),
            )
        )

    for cb in comisiones:
        objetos_dashboard.append(
            DashboardConsolidado(
                tipo_operacion="EGRESO",
                categoria="COMISION_BANCARIA",
                sub_categoria="",
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
        cat = "INGRESO_TARJETA" if res["cuenta"].endswith("4") else "INGRESO_DEPOSITO"
        objetos_dashboard.append(
            DashboardConsolidado(
                tipo_operacion="INGRESO",
                categoria=cat,
                sub_categoria="",
                cuenta_contable=res["cuenta"],
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
        _agregar_auditoria(zr, "ZR abierto. Falló conciliación de pago contra lote ZP.")
    for pos, motivo in ingresos_aud:
        _agregar_auditoria(pos, motivo)

    docs_primarios = [
        obj.documento_primario for obj in objetos_dashboard if obj.documento_primario
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
        fecha__gte=fecha_inicio,
        fecha__lte=fecha_fin,
    ).delete()

    if objetos_dashboard:
        DashboardConsolidado.objects.bulk_create(objetos_dashboard, batch_size=2000)
    if objetos_auditoria:
        AsientoAuditoria.objects.bulk_create(objetos_auditoria, batch_size=2000)

    log.refresh_from_db(fields=["errores_count"])
    err_antes = log.errores_count

    log.registrar_fin_paso(
        "paso8",
        {
            "dashboard_registros": len(objetos_dashboard),
            "auditoria_registros": len(objetos_auditoria),
            "cadena_zr_zp_filas": len(resultados_zr),
            "operaciones_internas": len(operaciones_internas),
            "comisiones_detectadas": len(comisiones),
            "ingresos_validados": len(ingresos_validados),
        },
        estado=_obtener_estado_paso(log, err_antes),
    )
