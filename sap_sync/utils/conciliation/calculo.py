import logging
import operator
from collections import defaultdict
from datetime import date
from decimal import Decimal
from functools import reduce

# Importamos Count y Sum de Django
from django.db.models import Count, Q, Sum

from core.models import ClasificacionGasto, DashboardConsolidado, SaldoBancario
from sap_sync.models import PartidaPosicion
from sap_sync.services.orchestrator import _fecha_a_anio_periodo

logger = logging.getLogger(__name__)

TIPOS_DOCUMENTO_BANCARIO = frozenset(["ZR", "ZH"])


def _to_date(valor) -> date:
    if isinstance(valor, str):
        anio, mes, dia = map(int, valor.split("-"))
        return date(anio, mes, dia)
    return valor


def calculo_conciliacion(fecha_inicio, fecha_fin):
    logger.info(f"Calculo de conciliacion para periodo: {fecha_inicio} - {fecha_fin}")

    fecha_obj = _to_date(fecha_inicio)

    hkonts_ctas_real: set[str] = set(
        SaldoBancario.objects.filter(ryear=_fecha_a_anio_periodo(fecha_obj)[0])
        .values_list("hkont", flat=True)
        .distinct()
    )

    raw_banco_docs = list(
        PartidaPosicion.objects.filter(
            ractt__in=hkonts_ctas_real,
            partida__budat__range=[fecha_inicio, fecha_fin],
            partida__blart__in=TIPOS_DOCUMENTO_BANCARIO,
            partida__stblg__isnull=True,
        ).values("docnr")
    )

    raw_partidas = list(
        PartidaPosicion.objects.filter(
            docnr__in=[doc["docnr"] for doc in raw_banco_docs],
            partida__stblg__isnull=True,
        ).values(
            "docnr",
            "partida__blart",
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
            "partida__bktxt",
        )
    )

    prefijos_hkont = {str(h)[:-1] for h in (hkonts_ctas_real) if h}

    if prefijos_hkont:
        condicion_busqueda = reduce(
            operator.or_, (Q(ractt__startswith=p) for p in prefijos_hkont)
        )
        hkonts_partidas: set[str] = set(
            PartidaPosicion.objects.filter(condicion_busqueda)
            .filter(
                partida__budat__range=[fecha_inicio, fecha_fin],
                partida__stblg__isnull=True,
            )
            .values_list("ractt", flat=True)
            .distinct()
        )
        cuentas_transitorias = hkonts_partidas - hkonts_ctas_real
        prefijos_con_transitorias = {str(c)[:-1] for c in cuentas_transitorias if c}
        cuentas_reales_en_partidas = hkonts_partidas & hkonts_ctas_real
        cuentas_standalone = {
            c
            for c in cuentas_reales_en_partidas
            if str(c)[:-1] not in prefijos_con_transitorias
        }
    else:
        cuentas_transitorias = cuentas_standalone = set()

    # 1. Obtener todas las cuentas que fungen como contrapartida en este periodo
    cuentas_contra_unicas = {
        p["ractt"]
        for p in raw_partidas
        if p.get("ractt") not in hkonts_ctas_real - cuentas_standalone
    }
    print(f"Cuentas contra únicas detectadas: {cuentas_contra_unicas}")
    # 2. Consultar el historial EXCLUSIVAMENTE dentro de documentos bancarios (ZR/ZH)
    conteo_historico = defaultdict(
        lambda: {
            "S_count": 0,
            "H_count": 0,
            "S_monto": Decimal("0"),
            "H_monto": Decimal("0"),
        }
    )

    if cuentas_contra_unicas:
        estadisticas = (
            PartidaPosicion.objects.filter(
                ractt__in=cuentas_contra_unicas,
                drcrk__in=["S", "H"],
                partida__blart__in=TIPOS_DOCUMENTO_BANCARIO,
                partida__stblg__isnull=True,
            )
            .values("ractt", "drcrk")
            .annotate(total_items=Count("id"), total_monto=Sum("wsl"))
        )

        for est in estadisticas:
            cta = est["ractt"]
            drcrk = est["drcrk"]
            conteo_historico[cta][f"{drcrk}_count"] = est["total_items"]
            conteo_historico[cta][f"{drcrk}_monto"] = abs(
                est["total_monto"] or Decimal("0")
            )

    # 3. Determinar la naturaleza aplicando la regla del 90% COMBINADA
    naturaleza_cuenta = {}
    for cta, datos in conteo_historico.items():
        total_count = datos["S_count"] + datos["H_count"]
        total_monto = datos["S_monto"] + datos["H_monto"]

        if total_count > 0 and total_monto > 0:
            pct_s_count = datos["S_count"] / total_count
            pct_h_count = datos["H_count"] / total_count

            pct_s_monto = float(datos["S_monto"]) / float(total_monto)
            pct_h_monto = float(datos["H_monto"]) / float(total_monto)

            if pct_s_count >= 0.90 and pct_s_monto >= 0.90:
                naturaleza_cuenta[cta] = "S"
            elif pct_h_count >= 0.90 and pct_h_monto >= 0.90:
                naturaleza_cuenta[cta] = "H"
            else:
                naturaleza_cuenta[cta] = None

    registros_dashboard = []

    partidas_por_doc = defaultdict(list)
    for p in raw_partidas:
        partidas_por_doc[p["docnr"]].append(p)

    for docnr, lineas in partidas_por_doc.items():
        lineas_banco = [l for l in lineas if l.get("ractt") in hkonts_ctas_real]
        lineas_contra = [l for l in lineas if l.get("ractt") not in hkonts_ctas_real]

        # --- LÓGICA DE DETECCIÓN DE REVERSO Y EXTRACCIÓN DE TRANSITORIA ---
        es_reverso = False
        cta_contra_principal = ""

        for contra in lineas_contra:
            cta_contra = contra.get("ractt")

            # Guardamos la primera que encontremos por si es un doc normal
            if not cta_contra_principal:
                cta_contra_principal = cta_contra

            signo_contra = contra.get("drcrk")
            naturaleza_esperada = naturaleza_cuenta.get(cta_contra)

            # Si la cuenta superó la prueba de pureza
            if naturaleza_esperada is not None:
                if signo_contra != naturaleza_esperada:
                    es_reverso = True
                    # Si detectamos reverso, priorizamos registrar la cuenta que lo causó
                    cta_contra_principal = cta_contra
                    break
        # ------------------------------------------------------------------

        for partida in lineas_banco:
            ractt = partida.get("ractt")
            drcrk = partida.get("drcrk", "")

            if drcrk == "S":
                if es_reverso:
                    tipo_operacion = "EGRESO"
                    multiplicador_signo = -1
                else:
                    tipo_operacion = "INGRESO"
                    multiplicador_signo = 1

            elif drcrk == "H":
                if es_reverso:
                    tipo_operacion = "INGRESO"
                    multiplicador_signo = -1
                else:
                    tipo_operacion = "EGRESO"
                    multiplicador_signo = 1
            else:
                tipo_operacion = "DESCONOCIDO"
                multiplicador_signo = 1

            monto_final = abs(partida.get("wsl", 0)) * multiplicador_signo

            categoria_str = ""
            sub_categoria_str = ""

            registro = DashboardConsolidado(
                tipo_operacion=tipo_operacion,
                categoria=categoria_str,
                sub_categoria=sub_categoria_str,
                cuenta_real=ractt,
                cuenta_transitoria=cta_contra_principal,
                cuenta_gasto="",
                monto_base=monto_final,
                monto_total=monto_final,
                rwcur=partida.get("rwcur", "") or "",
                fecha_contabilizacion=partida.get("budat"),
                documento_primario=partida.get("docnr", ""),
                referencia=partida.get("zuonr", "") or "",
                referencia1=partida.get("partida__bktxt", "") or "",
                lifnr=partida.get("lifnr", "") or "",
                kunnr=partida.get("kunnr", "") or "",
            )
            registros_dashboard.append(registro)

    DashboardConsolidado.objects.filter(
        fecha_contabilizacion__range=[fecha_inicio, fecha_fin]
    ).delete()

    n_dashboard = 0
    if registros_dashboard:
        DashboardConsolidado.objects.bulk_create(registros_dashboard, batch_size=1000)
        n_dashboard = len(registros_dashboard)

    gastos_directos = DashboardConsolidado.objects.filter(
        fecha_contabilizacion__range=[fecha_inicio, fecha_fin]
    ).exclude(cuenta_transitoria__in=cuentas_transitorias)

    for registro in gastos_directos:
        clasificacion = ClasificacionGasto.objects.filter(
            cuenta_gasto=registro.cuenta_transitoria
        ).first()

        if clasificacion:
            registro.categoria = clasificacion.categoria
        else:
            registro.categoria = "Registrar clasificaciongasto para esta cuenta"
        registro.save(update_fields=["categoria"])

    gastos_sin_clasificar = DashboardConsolidado.objects.filter(
        fecha_contabilizacion__range=[fecha_inicio, fecha_fin],
        cuenta_transitoria__in=cuentas_transitorias,
    ).values("documento_primario")

    todos_documentos = PartidaPosicion.objects.filter(
        docnr__in=[g["documento_primario"] for g in gastos_sin_clasificar],
        augbl__isnull=False,
    )

    for registro in todos_documentos:
        print(f"Gasto sin clasificar: doc {registro.docnr} -> augbl {registro.augbl}")

    logger.info(f"Conciliación completada | dashboard={n_dashboard}")
    return {"dashboard": n_dashboard}
