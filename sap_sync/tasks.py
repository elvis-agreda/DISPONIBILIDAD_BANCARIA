import logging
import traceback
from datetime import date, timedelta
from typing import Optional

from huey import crontab
from huey.contrib.djhuey import db_periodic_task, db_task

from core.models import Notificacion

from .models import SincronizacionLog
from .services.orchestrator import SAPSyncOrchestrator, _fecha_a_anio_periodo

logger = logging.getLogger(__name__)


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
    usuario_id: Optional[int] = None,
):
    anio, periodo = _fecha_a_anio_periodo(fecha_inicio)

    if sync_log_id:
        log = SincronizacionLog.objects.get(pk=sync_log_id)
        log.anio = anio
        log.periodo = periodo
        if usuario_id:  # 👈 AGREGAR ESTO para asegurar el guardado
            log.usuario_id = usuario_id  # type: ignore
    else:
        log = SincronizacionLog.objects.create(
            tipo=tipo,
            estado="INICIADO",
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            anio=anio,
            periodo=periodo,
            usuario_id=usuario_id,
        )

    log.estado = "EN_CURSO"
    log.save(update_fields=["estado", "anio", "periodo"])

    # Aquí delegamos todo el trabajo pesado a nuestra nueva Service Layer
    orchestrator = SAPSyncOrchestrator(log)
    try:
        orchestrator.ejecutar_sync_completa(fecha_inicio, fecha_fin, anio)
        if usuario_id:
            Notificacion.objects.create(
                usuario_id=usuario_id,
                mensaje=f"Sincronización SAP ({fecha_inicio} al {fecha_fin}) completada.",
                tipo="success",
            )
    except InterruptedError as exc:
        log.registrar_error(paso=0, mensaje=str(exc))
        log.marcar_finalizado("CANCELADO")
    except Exception as exc:
        detalle_error = traceback.format_exc()
        log.registrar_error(
            paso=0,
            mensaje=f"Error fatal general: {exc}",
            contexto={"traceback": detalle_error},
        )
        log.marcar_finalizado("FALLIDO")
        if usuario_id:
            Notificacion.objects.create(
                usuario_id=usuario_id,
                mensaje=f"Error en Sincronización SAP: {str(exc)}",
                tipo="error",
            )
        raise


@db_task()
def ejecutar_paso8_manual(fecha_inicio: date, fecha_fin: date, usuario_id):
    anio, periodo = _fecha_a_anio_periodo(fecha_inicio)
    log = SincronizacionLog.objects.create(
        tipo="MANUAL",
        estado="EN_CURSO",
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        anio=anio,
        periodo=periodo,
        usuario_id=usuario_id,
    )

    orchestrator = SAPSyncOrchestrator(log)
    try:
        orchestrator.paso8_calculo_disponibilidad(fecha_inicio, fecha_fin)
        if usuario_id:
            Notificacion.objects.create(
                usuario_id=usuario_id,
                mensaje="Cálculo de Disponibilidad (Paso 8) finalizado con éxito.",
                tipo="success",
            )
        log.marcar_finalizado("EXITOSO")
    except Exception as exc:
        detalle_error = traceback.format_exc()
        log.registrar_error(
            8,
            mensaje=f"Error fatal en reprocesamiento manual: {exc}",
            contexto={"traceback": detalle_error},
        )
        log.marcar_finalizado("FALLIDO")
        if usuario_id:
            Notificacion.objects.create(
                usuario_id=usuario_id,
                mensaje=f"Error en Paso 8: {str(exc)}",
                tipo="error",
            )
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

    orchestrator = SAPSyncOrchestrator(log)
    try:
        anio_seguro = log.anio or str(log.fecha_inicio.year)
        orchestrator.ejecutar_reintento(log.fecha_inicio, log.fecha_fin, anio_seguro)
    except InterruptedError as exc:
        log.registrar_error(paso=0, mensaje=str(exc))
        log.marcar_finalizado("CANCELADO")
    except Exception as exc:
        detalle_error = traceback.format_exc()
        log.registrar_error(
            paso=0,
            mensaje=f"Error fatal en reintento: {exc}",
            contexto={"traceback": detalle_error},
        )
        log.marcar_finalizado("FALLIDO")
        raise
