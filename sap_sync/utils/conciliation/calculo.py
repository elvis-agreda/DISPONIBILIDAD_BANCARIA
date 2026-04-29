"""
Motor de Conciliación Bancaria — SAP FI
========================================
Arquitectura de Trazabilidad en 4 Niveles con:
 - Alto Rendimiento: Bypass del ORM usando DTOs (FastPos) y Query Chunking.
 - Extracción Configurable de Cuentas Especiales con Redistribución Proporcional.
 - Emparejador Unificado N:M (Hash Maps O(N)).
 - Cascada de Prioridades Quirúrgica y Metadatos de Cabecera.
"""

import logging
import operator
from collections import defaultdict
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from functools import reduce

from django.db.models import Q

from core.models import (
    AsientoAuditoria,
    ClasificacionGasto,
    DashboardConsolidado,
    SaldoBancario,
)
from sap_sync.models import CuentaConfiguracion, PartidaPosicion
from sap_sync.services.orchestrator import _fecha_a_anio_periodo

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes SAP FI
# ---------------------------------------------------------------------------
TIPOS_DOCUMENTO_BANCARIO = frozenset(["ZR", "ZH"])
DIGITOS_EGRESO = frozenset(["1", "2", "7"])
DIGITOS_INGRESO = frozenset(["3", "4", "6"])

_CATEGORIA_ESPECIAL = {
    "IMPUESTO": ("IMPUESTO", "IMPUESTO_RETENIDO"),
    "DIF_CAMBIO": ("DIFERENCIAL_CAMBIARIO", "DIFERENCIAL_CAMBIARIO"),
    "COMISION": ("COMISION_BANCARIA", "COMISION_BANCARIA"),
}

CENTAVO = Decimal("0.01")


# ---------------------------------------------------------------------------
# CONFIGURACIÓN DE CUENTAS DE EXTRACCIÓN ESPECIAL
# ---------------------------------------------------------------------------
CUENTAS_EXTRACCION_ESPECIAL: list[dict] = [
    {
        "ractt": "525010103",
        "blart": "ZR",
        "categoria": "COMISION_BANCARIA",
        "sub_categoria": "COMISION_BANCARIA",
        "redistribuir": False,
        "inyectar": True,  # ZR: cuenta real ↔ 525010103, no pasa por transitorias
    },
]

# Índice interno: (ractt, blart) → config_dict
_INDICE_ESPECIALES: dict[tuple[str, str | None], dict] = {}
for _cfg in CUENTAS_EXTRACCION_ESPECIAL:
    _INDICE_ESPECIALES[(_cfg["ractt"], _cfg["blart"])] = _cfg
    if _cfg["blart"] is not None:
        _INDICE_ESPECIALES.setdefault((_cfg["ractt"], None), _cfg)

_CFGS_INYECTAR: list[dict] = [
    c for c in CUENTAS_EXTRACCION_ESPECIAL if c.get("inyectar")
]

_RACCTS_EXTRACCION_ESPECIAL: frozenset[str] = frozenset(
    c["ractt"] for c in CUENTAS_EXTRACCION_ESPECIAL
)


def _buscar_config_especial(ractt: str, blart: str | None) -> dict | None:
    return _INDICE_ESPECIALES.get((ractt, blart)) or _INDICE_ESPECIALES.get(
        (ractt, None)
    )


# ---------------------------------------------------------------------------
# DTO DE ALTO RENDIMIENTO (FastPos)
# ---------------------------------------------------------------------------
class FastPos:
    __slots__ = [
        "id",
        "bukrs",
        "ractt",
        "wsl",
        "abs_wsl",
        "rwcur",
        "augbl",
        "zuonr",
        "lifnr",
        "kunnr",
        "drcrk",
        "belnr",
        "gjahr",
        "blart",
        "budat",
        "bktxt",
        "stblg",
    ]

    def __init__(self, d: dict) -> None:
        self.id = d["id"]
        self.bukrs = d["bukrs"]
        self.ractt = str(d["ractt"] or "").strip()
        self.wsl = Decimal(str(d["wsl"])) if d["wsl"] is not None else Decimal("0")
        self.abs_wsl = abs(self.wsl)
        self.rwcur = d["rwcur"]
        self.augbl = d["augbl"]
        self.zuonr = str(d["zuonr"] or "").strip()
        self.lifnr = str(d["lifnr"] or "").strip()
        self.kunnr = str(d["kunnr"] or "").strip()
        self.drcrk = str(d["drcrk"] or "").upper()
        self.belnr = d["partida__belnr"]
        self.gjahr = d["partida__gjahr"]
        self.blart = d["partida__blart"]
        self.budat = d["partida__budat"]
        self.bktxt = str(d["partida__bktxt"] or "").strip()
        self.stblg = str(d.get("partida__stblg") or "").strip()

    def __repr__(self) -> str:
        return f"<FastPos id={self.id} ractt={self.ractt} blart={self.blart} belnr={self.belnr} wsl={self.wsl}>"


CAMPOS_QUERY = [
    "id",
    "bukrs",
    "ractt",
    "wsl",
    "rwcur",
    "augbl",
    "zuonr",
    "lifnr",
    "kunnr",
    "drcrk",
    "partida__belnr",
    "partida__gjahr",
    "partida__blart",
    "partida__budat",
    "partida__bktxt",
    "partida__stblg",
]


def _fetch_fast_chunks(
    queryset, filter_kwarg: str, values_set: set, chunk_size: int = 2500
) -> list[FastPos]:
    results: list[FastPos] = []
    v_list = list(values_set)
    for i in range(0, len(v_list), chunk_size):
        chunk = v_list[i : i + chunk_size]
        raw_dicts = list(
            queryset.filter(**{f"{filter_kwarg}__in": chunk}).values(*CAMPOS_QUERY)
        )
        results.extend(FastPos(d) for d in raw_dicts)
    return results


def _to_date(valor) -> date:
    if isinstance(valor, str):
        anio, mes, dia = map(int, valor.split("-"))
        return date(anio, mes, dia)
    return valor


# ---------------------------------------------------------------------------
# Motor principal
# ---------------------------------------------------------------------------
def calculo_conciliacion(fecha_inicio, fecha_fin):
    logger.info("Iniciando conciliación | %s → %s", fecha_inicio, fecha_fin)

    # 1. CONFIGURACIONES Y MAPEOS
    cuentas_conf = CuentaConfiguracion.objects.filter(activa=True)
    hkont_por_tipo: dict[str, set[str]] = defaultdict(set)
    for obj in cuentas_conf.values("tipo", "cuenta"):
        hkont_por_tipo[obj["tipo"]].add(str(obj["cuenta"]))

    hkont_impuestos = hkont_por_tipo["IMPUESTO"]
    hkont_dif_cambio = hkont_por_tipo["DIF_CAMBIO"]
    hkont_comision = hkont_por_tipo["COMISION"]

    cuentas_especiales = (
        hkont_impuestos
        | hkont_dif_cambio
        | hkont_comision
        | _RACCTS_EXTRACCION_ESPECIAL
    )

    mapeo_gastos: dict[str, ClasificacionGasto] = {
        str(obj.cuenta_gasto): obj for obj in ClasificacionGasto.objects.all()
    }
    cuentas_gasto_puro = set(mapeo_gastos.keys()) - cuentas_especiales
    fecha_obj = _to_date(fecha_inicio)

    # 2. IDENTIFICACIÓN DE CUENTAS BANCARIAS
    hkonts_ctas_real: set[str] = set(
        SaldoBancario.objects.filter(ryear=_fecha_a_anio_periodo(fecha_obj)[0])
        .values_list("hkont", flat=True)
        .distinct()
    )

    prefijos_hkont = [str(h)[:-1] for h in hkonts_ctas_real if h]

    if prefijos_hkont:
        condicion_busqueda = reduce(
            operator.or_, (Q(ractt__startswith=p) for p in prefijos_hkont)
        )
        hkonts_partidas: set[str] = set(
            PartidaPosicion.objects.filter(condicion_busqueda)
            .filter(partida__budat__range=[fecha_inicio, fecha_fin])
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

    cuentas_egresos = {c for c in cuentas_transitorias if str(c)[-1] in DIGITOS_EGRESO}
    cuentas_ingresos = {
        c for c in cuentas_transitorias if str(c)[-1] in DIGITOS_INGRESO
    }
    todas_las_cuentas_bancarias = (
        hkonts_ctas_real | cuentas_transitorias | cuentas_standalone
    )

    # 3. PRE-FETCH MASIVO DE BANCOS Y PUENTES
    raw_bancos = list(
        PartidaPosicion.objects.filter(
            ractt__in=cuentas_transitorias | cuentas_standalone,
            partida__budat__range=[fecha_inicio, fecha_fin],
            partida__blart__in=TIPOS_DOCUMENTO_BANCARIO,
        ).values(*CAMPOS_QUERY)
    )

    # ── Inyección de documentos con inyectar=True ───────────────────────────
    raw_inyectados = []
    belnrs_inyectar = set()
    if _CFGS_INYECTAR:
        condiciones_inyectar = reduce(
            operator.or_,
            (
                Q(
                    ractt=_c["ractt"],
                    partida__blart=_c["blart"],
                    partida__budat__range=[fecha_inicio, fecha_fin],
                )
                if _c["blart"]
                else Q(
                    ractt=_c["ractt"], partida__budat__range=[fecha_inicio, fecha_fin]
                )
                for _c in _CFGS_INYECTAR
            ),
        )
        belnrs_inyectar = set(
            PartidaPosicion.objects.filter(condiciones_inyectar)
            .values_list("partida__belnr", flat=True)
            .distinct()
        )

        if belnrs_inyectar:
            ids_ya_cargados = {d["id"] for d in raw_bancos}

            # SOLUCIÓN 1: Traer TODAS las líneas del comprobante (no filtramos por hkonts_ctas_real)
            raw_inyectados = list(
                PartidaPosicion.objects.filter(
                    partida__belnr__in=belnrs_inyectar,
                    partida__blart__in=TIPOS_DOCUMENTO_BANCARIO,
                ).values(*CAMPOS_QUERY)
            )

            for d in raw_inyectados:
                # SOLUCIÓN 2: Inyectar un augbl virtual para obligar al motor a agrupar el documento
                if not d["augbl"]:
                    d["augbl"] = d["partida__belnr"]

                # Añadir a raw_bancos SOLO la línea de la cuenta real
                if d["ractt"] in hkonts_ctas_real and d["id"] not in ids_ya_cargados:
                    raw_bancos.append(d)
                    ids_ya_cargados.add(d["id"])

    posiciones_bancarias = [FastPos(d) for d in raw_bancos]

    if not posiciones_bancarias:
        logger.info("Sin movimientos bancarios para el período.")
        return {"dashboard": 0, "auditoria": 0}

    augbls_relevantes = {p.augbl for p in posiciones_bancarias if p.augbl}
    partidas_por_augbl_list = _fetch_fast_chunks(
        PartidaPosicion.objects.all(), "augbl", augbls_relevantes
    )

    # SOLUCIÓN 3: Inyectar las posiciones especiales (ej. 525010103) al pool de FastPos en memoria
    if raw_inyectados:
        ids_en_lista = {p.id for p in partidas_por_augbl_list}
        for d in raw_inyectados:
            if d["id"] not in ids_en_lista:
                fast_p = FastPos(d)
                if not fast_p.augbl:
                    fast_p.augbl = fast_p.belnr
                partidas_por_augbl_list.append(fast_p)
                ids_en_lista.add(fast_p.id)

    partidas_por_augbl: dict[str, list[FastPos]] = defaultdict(list)
    belnrs_puente_global: set[str] = set()

    for pp in partidas_por_augbl_list:
        partidas_por_augbl[pp.augbl].append(pp)
        if pp.blart not in TIPOS_DOCUMENTO_BANCARIO:
            belnrs_puente_global.add(pp.belnr)

    registros_dashboard: list[DashboardConsolidado] = []
    registros_auditoria: list[AsientoAuditoria] = []

    # 3.1 EXTRACCIÓN Y REDISTRIBUCIÓN DE CUENTAS ESPECIALES
    monto_extra_por_pos_id: dict[int, Decimal] = defaultdict(Decimal)

    for augbl, pp_list in partidas_por_augbl.items():
        bancos = [
            p
            for p in pp_list
            if p.blart in TIPOS_DOCUMENTO_BANCARIO
            and p.ractt in todas_las_cuentas_bancarias
        ]

        for p in pp_list:
            cfg = _buscar_config_especial(p.ractt, p.blart)
            if not cfg or p.abs_wsl == Decimal("0"):
                continue

            banco_match = next(
                (b for b in bancos if b.belnr == p.belnr and b.abs_wsl >= p.abs_wsl),
                None,
            )
            if banco_match is None and bancos:
                banco_match = bancos[0]
            if banco_match is None:
                continue

            if cfg["redistribuir"]:
                monto_extra_por_pos_id[banco_match.id] += p.abs_wsl
                p.abs_wsl = Decimal("0")
                p.wsl = Decimal("0")

                pos_match = next(
                    (pb for pb in posiciones_bancarias if pb.id == p.id), None
                )
                if pos_match:
                    pos_match.abs_wsl = Decimal("0")
                    pos_match.wsl = Decimal("0")
            else:
                # ── Modo registro independiente (Neteo de Gasto) ────────────
                # Se mantiene siempre como EGRESO.
                # Si es Debe ('S'), es un cobro de comisión -> Positivo
                # Si es Haber ('H'), es devolución de comisión -> Negativo
                monto_real = p.abs_wsl if p.drcrk == "S" else -p.abs_wsl

                kwargs_especial = {
                    "tipo_operacion": "EGRESO",
                    "categoria": cfg["categoria"],
                    "sub_categoria": cfg["sub_categoria"],
                    "cuenta_contable": banco_match.ractt,
                    "cuenta_gasto": p.ractt,
                    "monto_base": monto_real,
                    "monto_total": monto_real,
                    "rwcur": p.rwcur,
                    "fecha_contabilizacion": p.budat,
                    "documento_primario": p.belnr,
                    "documento_secundario": "",
                    "referencia": p.zuonr[:50],
                    "lifnr": "",
                    "kunnr": "",
                }
                if hasattr(DashboardConsolidado, "referencia1"):
                    kwargs_especial["referencia1"] = p.bktxt[:50]

                registros_dashboard.append(DashboardConsolidado(**kwargs_especial))

                # Ajuste matemático unificado para el emparejador N:M
                banco_match.abs_wsl -= p.abs_wsl
                banco_match.wsl = (
                    -(banco_match.abs_wsl)
                    if banco_match.wsl < Decimal("0")
                    else banco_match.abs_wsl
                )

                # Sincronizar con el objeto FastPos global
                pos_match = next(
                    (pb for pb in posiciones_bancarias if pb.id == banco_match.id), None
                )
                if pos_match:
                    pos_match.abs_wsl = banco_match.abs_wsl
                    pos_match.wsl = banco_match.wsl

                # Neutralizar la línea especial para el emparejador N:M
                p.abs_wsl = Decimal("0")
                p.wsl = Decimal("0")

    # Base forense del puente y bktxt
    info_puente_list = _fetch_fast_chunks(
        PartidaPosicion.objects.all(), "partida__belnr", belnrs_puente_global
    )
    info_puente_por_belnr: dict[str, list[dict]] = defaultdict(list)
    zp_bktxt_map: dict[str, str] = {}

    for pp in info_puente_list:
        info_puente_por_belnr[pp.belnr].append(
            {"ractt": pp.ractt, "lifnr": pp.lifnr, "kunnr": pp.kunnr}
        )
        zp_bktxt_map[pp.belnr] = pp.bktxt

    # 4. EL EMPAREJADOR N:M UNIFICADO
    augbl_allocations: dict[int, list[dict]] = defaultdict(list)

    for augbl, pp_list in partidas_por_augbl.items():
        if not augbl:
            continue

        by_bukrs: dict[str, list[FastPos]] = defaultdict(list)
        for p in pp_list:
            by_bukrs[p.bukrs].append(p)

        for bukrs, b_list in by_bukrs.items():
            zrs = [
                p
                for p in b_list
                if p.blart in TIPOS_DOCUMENTO_BANCARIO
                and p.ractt in todas_las_cuentas_bancarias
            ]
            zps = [
                p
                for p in b_list
                if p.blart not in TIPOS_DOCUMENTO_BANCARIO
                or (
                    p.blart in TIPOS_DOCUMENTO_BANCARIO
                    and p.ractt not in todas_las_cuentas_bancarias
                )
            ]

            if not zps or not zrs:
                continue

            zp_totals: dict[str, Decimal] = defaultdict(Decimal)
            for zp in zps:
                zp_totals[zp.belnr] += zp.abs_wsl

            zps_pool = [
                {"belnr": k, "wsl": v} for k, v in zp_totals.items() if v > Decimal("0")
            ]
            zrs_pool = [
                {"id": zr.id, "wsl": zr.abs_wsl}
                for zr in zrs
                if zr.abs_wsl > Decimal("0")
            ]

            allocations_temp: dict[int, list[dict]] = defaultdict(list)

            # Pasada 1: 1:1 Exacto
            zps_dict: dict[Decimal, list[dict]] = defaultdict(list)
            for zp in zps_pool:
                zps_dict[zp["wsl"]].append(zp)

            zrs_pool_restantes = []
            for zr in zrs_pool:
                if zps_dict[zr["wsl"]]:
                    zp_match = zps_dict[zr["wsl"]].pop(0)
                    allocations_temp[zr["id"]].append(
                        {"zp_belnr": zp_match["belnr"], "monto": zr["wsl"]}
                    )
                else:
                    zrs_pool_restantes.append(zr)

            zps_pool = [zp for zp_list in zps_dict.values() for zp in zp_list]
            zrs_pool = zrs_pool_restantes

            # Pasada 2: 1:N Exacto
            zrs_pool_restantes = []
            zps_pool.sort(key=lambda x: x["wsl"], reverse=True)

            for zr in zrs_pool:
                suma = Decimal("0")
                matched_zps = []
                matched_indices = []

                for idx, zp in enumerate(zps_pool):
                    if suma + zp["wsl"] <= zr["wsl"]:
                        suma += zp["wsl"]
                        matched_zps.append(zp)
                        matched_indices.append(idx)
                    if suma == zr["wsl"]:
                        break

                if suma == zr["wsl"] and zr["wsl"] > Decimal("0"):
                    for m in matched_zps:
                        allocations_temp[zr["id"]].append(
                            {"zp_belnr": m["belnr"], "monto": m["wsl"]}
                        )
                    for idx in reversed(matched_indices):
                        zps_pool.pop(idx)
                else:
                    zrs_pool_restantes.append(zr)

            zrs_pool = zrs_pool_restantes

            # Pasada 3: Cascada (FIFO) para residuos
            zrs_pool.sort(key=lambda x: x["wsl"], reverse=True)
            zps_pool.sort(key=lambda x: x["wsl"], reverse=True)

            zp_idx = 0
            for zr in zrs_pool:
                remaining_zr = zr["wsl"]
                while remaining_zr > Decimal("0") and zp_idx < len(zps_pool):
                    zp = zps_pool[zp_idx]
                    if zp["wsl"] <= Decimal("0"):
                        zp_idx += 1
                        continue

                    alloc_amt = min(remaining_zr, zp["wsl"])
                    if alloc_amt <= Decimal("0"):
                        break

                    allocations_temp[zr["id"]].append(
                        {"zp_belnr": zp["belnr"], "monto": alloc_amt}
                    )
                    remaining_zr -= alloc_amt
                    zp["wsl"] -= alloc_amt

                    if zp["wsl"] <= Decimal("0"):
                        zp_idx += 1

                if remaining_zr > Decimal("0") and allocations_temp[zr["id"]]:
                    allocations_temp[zr["id"]][-1]["monto"] += remaining_zr
                elif remaining_zr > Decimal("0") and zps:
                    allocations_temp[zr["id"]].append(
                        {"zp_belnr": zps[0].belnr, "monto": remaining_zr}
                    )

            for k, v in allocations_temp.items():
                augbl_allocations[k].extend(v)

    # 5. EXTRACCIÓN GLOBAL DE FACTURAS ORIGEN
    belnrs_primer_salto = {
        alloc["zp_belnr"] for allocs in augbl_allocations.values() for alloc in allocs
    }

    segundo_salto_list = _fetch_fast_chunks(
        PartidaPosicion.objects.all(), "augbl", belnrs_primer_salto
    )
    segundo_salto_por_belnr: dict[str, set[str]] = defaultdict(set)
    for pp in segundo_salto_list:
        if pp.blart not in TIPOS_DOCUMENTO_BANCARIO:
            segundo_salto_por_belnr[pp.augbl].add(pp.belnr)

    belnrs_origen_global: set[str] = set()
    for belnr_intermedio in belnrs_primer_salto:
        if segundo_salto_por_belnr.get(belnr_intermedio):
            belnrs_origen_global |= segundo_salto_por_belnr[belnr_intermedio]
        else:
            belnrs_origen_global.add(belnr_intermedio)

    lineas_origen_completas_list = _fetch_fast_chunks(
        PartidaPosicion.objects.all(), "partida__belnr", belnrs_origen_global
    )

    lineas_origen_completas: dict[str, list[FastPos]] = defaultdict(list)
    invoice_metadata: dict[str, dict] = defaultdict(
        lambda: {"lifnr": "", "kunnr": "", "bktxt": "", "zuonr": ""}
    )

    for pp in lineas_origen_completas_list:
        lineas_origen_completas[pp.belnr].append(pp)
        if pp.lifnr:
            invoice_metadata[pp.belnr]["lifnr"] = pp.lifnr
        if pp.kunnr:
            invoice_metadata[pp.belnr]["kunnr"] = pp.kunnr
        if pp.bktxt:
            invoice_metadata[pp.belnr]["bktxt"] = pp.bktxt
        if pp.zuonr:
            invoice_metadata[pp.belnr]["zuonr"] = pp.zuonr

    # 6. MOTOR DE TRAZABILIDAD Y PRORRATEO AISLADO
    _traspaso_cache: dict[tuple, bool] = {}

    def _es_traspaso_interno(augbl: str, bukrs: str) -> bool:
        if not augbl:
            return False
        cache_key = (augbl, bukrs)
        if cache_key in _traspaso_cache:
            return _traspaso_cache[cache_key]

        raccts_compensadas = {
            pp.ractt for pp in partidas_por_augbl.get(augbl, []) if pp.bukrs == bukrs
        }
        if not raccts_compensadas:
            _traspaso_cache[cache_key] = False
            return False

        if (raccts_compensadas & cuentas_egresos) and (
            raccts_compensadas & cuentas_ingresos
        ):
            _traspaso_cache[cache_key] = True
            return True

        belnrs_puente = {
            pp.belnr for pp in partidas_por_augbl.get(augbl, []) if pp.bukrs == bukrs
        }
        for belnr in belnrs_puente:
            for linea in info_puente_por_belnr.get(belnr, []):
                if linea["lifnr"] or linea["kunnr"]:
                    _traspaso_cache[cache_key] = False
                    return False
                if linea["ractt"] in cuentas_gasto_puro:
                    _traspaso_cache[cache_key] = False
                    return False

        _traspaso_cache[cache_key] = True
        return True

    for pos in posiciones_bancarias:
        if not pos.augbl or pos.abs_wsl == Decimal("0"):
            continue

        if _es_traspaso_interno(pos.augbl, pos.bukrs):
            tipo_operacion = "TRASPASO"
        elif pos.ractt in cuentas_egresos:
            tipo_operacion = "EGRESO"
        elif pos.ractt in cuentas_ingresos:
            tipo_operacion = "INGRESO"
        elif pos.ractt in cuentas_standalone:
            tipo_operacion = "INGRESO" if pos.drcrk == "S" else "EGRESO"
        else:
            tipo_operacion = "TRASPASO"

        ref_banco = (pos.bktxt or pos.zuonr)[:50]
        allocations = augbl_allocations.get(pos.id, [])

        if tipo_operacion == "TRASPASO":
            registros_dashboard.append(
                DashboardConsolidado(
                    tipo_operacion="TRASPASO",
                    categoria="TRASPASO_INTERNO",
                    sub_categoria="TRASPASO_INTERNO",
                    cuenta_contable=pos.ractt,
                    cuenta_gasto="",
                    monto_base=pos.abs_wsl,
                    monto_total=pos.abs_wsl,
                    rwcur=pos.rwcur,
                    fecha_contabilizacion=pos.budat,
                    documento_primario=pos.belnr,
                    documento_secundario="",
                    referencia=ref_banco,
                    lifnr="",
                    kunnr="",
                )
            )
            continue

        if not allocations:
            continue

        monto_extra = monto_extra_por_pos_id.get(pos.id, Decimal("0"))
        if monto_extra > Decimal("0"):
            total_alloc = sum(a["monto"] for a in allocations)
            if total_alloc > Decimal("0"):
                extra_acumulado = Decimal("0")
                allocations_ajustadas = []
                for i, alloc in enumerate(allocations):
                    if i == len(allocations) - 1:
                        extra_tramo = monto_extra - extra_acumulado
                    else:
                        extra_tramo = (
                            monto_extra * alloc["monto"] / total_alloc
                        ).quantize(CENTAVO, rounding=ROUND_HALF_UP)
                        extra_acumulado += extra_tramo
                    allocations_ajustadas.append(
                        {**alloc, "monto": alloc["monto"] + extra_tramo}
                    )
                allocations = allocations_ajustadas

        for alloc in allocations:
            zp_belnr = alloc["zp_belnr"]
            monto_a_prorratear = alloc["monto"]

            bktxt_puente_zp = zp_bktxt_map.get(zp_belnr, "")
            factura_belnrs = segundo_salto_por_belnr.get(zp_belnr) or {zp_belnr}
            doc_secundarios_str = ",".join(factura_belnrs)

            lineas_origen = [
                linea
                for f in factura_belnrs
                for linea in lineas_origen_completas.get(f, [])
                if linea.bukrs == pos.bukrs
                and linea.ractt not in _RACCTS_EXTRACCION_ESPECIAL
            ]

            lineas_acreedor_deudor = [l for l in lineas_origen if l.lifnr or l.kunnr]
            lineas_gasto_puro = [
                l
                for l in lineas_origen
                if l.ractt in cuentas_gasto_puro and l not in lineas_acreedor_deudor
            ]
            lineas_no_mapeadas = [
                l
                for l in lineas_origen
                if l.ractt not in cuentas_gasto_puro
                and l.ractt not in cuentas_especiales
                and l.ractt not in todas_las_cuentas_bancarias
                and l not in lineas_acreedor_deudor
            ]
            lineas_impuestos_comisiones = [
                l
                for l in lineas_origen
                if (l.ractt in hkont_impuestos or l.ractt in hkont_comision)
                and l not in lineas_acreedor_deudor
            ]
            lineas_dif_cambio = [
                l
                for l in lineas_origen
                if l.ractt in hkont_dif_cambio and l not in lineas_acreedor_deudor
            ]

            if lineas_gasto_puro:
                lineas_activas = lineas_gasto_puro
            elif lineas_no_mapeadas:
                lineas_activas = lineas_no_mapeadas
            elif lineas_impuestos_comisiones:
                lineas_activas = lineas_impuestos_comisiones
            elif lineas_acreedor_deudor:
                lineas_activas = lineas_acreedor_deudor
            elif lineas_dif_cambio:
                lineas_activas = lineas_dif_cambio
            else:
                lineas_activas = [l for l in lineas_origen if l.ractt != pos.ractt]
                if not lineas_activas:
                    lineas_activas = lineas_origen

            total_gasto_base = sum(l.abs_wsl for l in lineas_activas)
            if total_gasto_base == Decimal("0"):
                continue

            agrupado: dict[tuple, dict] = defaultdict(
                lambda: {
                    "monto_linea": Decimal("0"),
                    "categoria": "",
                    "sub_categoria": "",
                    "ref1": "",
                }
            )

            for linea in lineas_activas:
                cuenta_str = linea.ractt
                inv_meta = invoice_metadata[linea.belnr]
                lifnr_real = inv_meta["lifnr"]
                kunnr_real = inv_meta["kunnr"]
                ref1_real = bktxt_puente_zp or inv_meta["bktxt"] or inv_meta["zuonr"]

                clave_grp = (cuenta_str, lifnr_real, kunnr_real)
                grp = agrupado[clave_grp]
                grp["monto_linea"] += linea.abs_wsl
                grp["ref1"] = ref1_real[:50]

                if not grp["categoria"]:
                    if cuenta_str in mapeo_gastos:
                        clf = mapeo_gastos[cuenta_str]
                        grp["categoria"], grp["sub_categoria"] = (
                            clf.categoria,
                            clf.sub_categoria,
                        )
                    elif cuenta_str in hkont_impuestos:
                        grp["categoria"], grp["sub_categoria"] = _CATEGORIA_ESPECIAL[
                            "IMPUESTO"
                        ]
                    elif cuenta_str in hkont_dif_cambio:
                        grp["categoria"], grp["sub_categoria"] = _CATEGORIA_ESPECIAL[
                            "DIF_CAMBIO"
                        ]
                    elif cuenta_str in hkont_comision:
                        grp["categoria"], grp["sub_categoria"] = _CATEGORIA_ESPECIAL[
                            "COMISION"
                        ]
                    else:
                        cfg_cuenta = _buscar_config_especial(cuenta_str, None)
                        if cfg_cuenta:
                            grp["categoria"], grp["sub_categoria"] = (
                                cfg_cuenta["categoria"],
                                cfg_cuenta["sub_categoria"],
                            )
                        else:
                            grp["categoria"] = grp["sub_categoria"] = "SIN_CLASIFICAR"

            items_prorrateo = list(agrupado.items())
            monto_acumulado = Decimal("0")

            for i, ((cuenta_str, lifnr, kunnr), grp) in enumerate(items_prorrateo):
                es_ultimo = i == len(items_prorrateo) - 1
                proporcion = grp["monto_linea"] / total_gasto_base

                if es_ultimo:
                    monto_prorrateado = monto_a_prorratear - monto_acumulado
                    if monto_prorrateado < Decimal("0"):
                        monto_prorrateado = (monto_a_prorratear * proporcion).quantize(
                            CENTAVO, rounding=ROUND_HALF_UP
                        )
                else:
                    monto_prorrateado = (monto_a_prorratear * proporcion).quantize(
                        CENTAVO, rounding=ROUND_HALF_UP
                    )
                    monto_acumulado += monto_prorrateado

                kwargs_dashboard: dict = {
                    "tipo_operacion": tipo_operacion,
                    "categoria": grp["categoria"],
                    "sub_categoria": grp["sub_categoria"],
                    "cuenta_contable": pos.ractt,
                    "cuenta_gasto": cuenta_str,
                    "monto_base": pos.abs_wsl,
                    "monto_total": monto_prorrateado,
                    "rwcur": pos.rwcur,
                    "fecha_contabilizacion": pos.budat,
                    "documento_primario": pos.belnr,
                    "documento_secundario": doc_secundarios_str,
                    "referencia": ref_banco,
                    "lifnr": lifnr,
                    "kunnr": kunnr,
                }
                if hasattr(DashboardConsolidado, "referencia1"):
                    kwargs_dashboard["referencia1"] = grp["ref1"]

                registros_dashboard.append(DashboardConsolidado(**kwargs_dashboard))

    # 7. LIMPIEZA DEL RANGO + GUARDADO MASIVO
    del_dash = DashboardConsolidado.objects.filter(
        fecha_contabilizacion__range=[fecha_inicio, fecha_fin]
    ).delete()
    del_aud = AsientoAuditoria.objects.filter(
        fecha__range=[fecha_inicio, fecha_fin]
    ).delete()
    logger.info(
        "Limpieza previa | dashboard=%s | auditoria=%s", del_dash[0], del_aud[0]
    )

    n_dashboard = n_auditoria = 0

    if registros_dashboard:
        DashboardConsolidado.objects.bulk_create(registros_dashboard, batch_size=1000)
        n_dashboard = len(registros_dashboard)

    if registros_auditoria:
        AsientoAuditoria.objects.bulk_create(registros_auditoria, batch_size=1000)
        n_auditoria = len(registros_auditoria)

    logger.info(
        "Conciliación completada | dashboard=%d | auditoria=%d",
        n_dashboard,
        n_auditoria,
    )
    return {"dashboard": n_dashboard, "auditoria": n_auditoria}
