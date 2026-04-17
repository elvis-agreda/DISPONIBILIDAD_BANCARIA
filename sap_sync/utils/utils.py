import re
from collections import defaultdict
from datetime import datetime, timezone


def sap_date_to_python(sap_date_str):
    if not sap_date_str:
        return None
    millis = int(re.search(r"\d+", sap_date_str).group())
    return datetime.fromtimestamp(millis / 1000.0, tz=timezone.utc).date()


def _monto_seguro(valor) -> float:
    try:
        return round(abs(float(valor)), 2)
    except (TypeError, ValueError):
        return 0.0


def _clean_ref(texto):
    if not texto:
        return ""
    return re.sub(r"[^A-Z0-9]", "", str(texto).upper())


# ── SE AGREGÓ LA CUENTA 213011100 ──
CUENTAS_IMPUESTOS = {
    "117010100",
    "213010500",
    "213010600",
    "213011100",
    "525010103",
    "525010104",
}
CUENTAS_DIF_CAMBIO = {"411050117", "526010102"}


def procesar_transferencias_y_divisas(posiciones, cuentas_todas):
    """
    Identifica TRANSFERENCIA_FONDOS y COMPRA_VENTA_DIVISAS agrupando por AUGBL.
    Obligatorio: Las cuentas de salida y entrada deben ser DISTINTAS.
    """
    operaciones = []
    usados = set()

    # FASE 1: Cruce preciso usando AUGBL
    grupos_augbl = defaultdict(list)
    for p in posiciones:
        if p.augbl and str(p.augbl).strip():
            grupos_augbl[str(p.augbl).strip()].append(p)

    for augbl, pos_list in grupos_augbl.items():
        pos_banco = [
            p for p in pos_list if p.ractt in cuentas_todas and p.id not in usados
        ]
        salidas = [p for p in pos_banco if float(p.wsl) < 0]
        entradas = [p for p in pos_banco if float(p.wsl) > 0]

        for salida in salidas:
            if salida.id in usados:
                continue

            best_entrada = None
            best_diff = float("inf")
            is_fx = False

            for entrada in entradas:
                if entrada.id in usados:
                    continue

                # ── REGLA ESTRICTA: Las cuentas bancarias DEBEN ser diferentes ──
                if (salida.ractt or "").strip() == (entrada.ractt or "").strip():
                    continue

                sal_suf = salida.ractt[-1] if salida.ractt else ""
                ent_suf = entrada.ractt[-1] if entrada.ractt else ""

                # Regla: Compra Venta de Divisas
                condicion_fx = (
                    (sal_suf in ("2", "7") and ent_suf in ("3", "6"))
                    or (sal_suf in ("3", "6") and ent_suf in ("2", "7"))
                ) and (salida.rwcur != entrada.rwcur)

                if condicion_fx:
                    best_entrada = entrada
                    is_fx = True
                    break  # FX no requiere cuadre exacto de WSL (son distintas monedas)

                # Regla: Transferencias (misma moneda, buscamos el monto más parecido)
                elif salida.rwcur == entrada.rwcur:
                    diff = abs(abs(float(salida.wsl)) - abs(float(entrada.wsl)))
                    if diff < best_diff:
                        best_diff = diff
                        best_entrada = entrada
                        is_fx = False

            if best_entrada:
                if is_fx or best_diff <= 0.05:
                    tipo_op = (
                        "COMPRA_VENTA_DIVISAS" if is_fx else "TRANSFERENCIA_FONDOS"
                    )
                    operaciones.append(
                        {
                            "tipo": tipo_op,
                            "salida": salida,
                            "entrada": best_entrada,
                            "monto_salida": abs(float(salida.wsl)),
                            "monto_entrada": abs(float(best_entrada.wsl)),
                            "rwcur_salida": salida.rwcur or "",
                            "rwcur_entrada": best_entrada.rwcur or "",
                            "ref": augbl,
                            "cuenta_salida": salida.ractt,
                            "cuenta_entrada": best_entrada.ractt,
                            "fecha": best_entrada.partida.budat,
                        }
                    )
                    usados.add(salida.id)
                    usados.add(best_entrada.id)
                    entradas.remove(best_entrada)

    # FASE 2: Transferencias huérfanas por ZUONR (Solo aplican misma moneda)
    restantes_transito = [
        p for p in posiciones if p.id not in usados and p.ractt in cuentas_todas
    ]
    salidas_por_ref = defaultdict(list)
    entradas_por_ref = defaultdict(list)

    for p in restantes_transito:
        ref = (p.zuonr or "")[-5:].strip()
        if not ref:
            continue
        monto = float(p.wsl)
        if monto < 0:
            salidas_por_ref[ref].append(p)
        elif monto > 0:
            entradas_por_ref[ref].append(p)

    todas_refs = set(salidas_por_ref.keys()) & set(entradas_por_ref.keys())
    for ref in todas_refs:
        for salida in salidas_por_ref[ref]:
            if salida.id in usados:
                continue
            monto_salida = abs(float(salida.wsl))

            for entrada in entradas_por_ref[ref]:
                if entrada.id in usados:
                    continue

                # ── REGLA ESTRICTA: Las cuentas bancarias DEBEN ser diferentes ──
                if (salida.ractt or "").strip() == (entrada.ractt or "").strip():
                    continue

                monto_entrada = abs(float(entrada.wsl))
                dias = abs((salida.partida.budat - entrada.partida.budat).days)

                if (
                    abs(monto_salida - monto_entrada) <= 0.05
                    and dias <= 5
                    and salida.rwcur == entrada.rwcur
                ):
                    operaciones.append(
                        {
                            "tipo": "TRANSFERENCIA_FONDOS",
                            "salida": salida,
                            "entrada": entrada,
                            "monto_salida": monto_salida,
                            "monto_entrada": monto_entrada,
                            "rwcur_salida": salida.rwcur or "",
                            "rwcur_entrada": entrada.rwcur or "",
                            "ref": ref,
                            "cuenta_salida": salida.ractt,
                            "cuenta_entrada": entrada.ractt,
                            "fecha": entrada.partida.budat,
                        }
                    )
                    usados.add(salida.id)
                    usados.add(entrada.id)
                    break

    restantes = [p for p in posiciones if p.id not in usados]
    return operaciones, restantes


def procesar_comisiones_bancarias(posiciones, mapa_banco_real):
    zrs_con_comision = {
        p.partida.belnr
        for p in posiciones
        if p.partida.blart == "ZR" and p.ractt == "525010103"
    }

    comisiones = []
    usados = set()

    pos_por_doc = defaultdict(list)
    for p in posiciones:
        if p.partida.belnr in zrs_con_comision:
            pos_por_doc[p.partida.belnr].append(p)

    for belnr, pos_list in pos_por_doc.items():
        pos_comision = next(
            (p for p in pos_list if p.ractt == "525010103" and float(p.wsl) > 0), None
        )
        if not pos_comision:
            pos_comision = next((p for p in pos_list if p.ractt == "525010103"), None)

        if pos_comision:
            cuenta_banco = mapa_banco_real.get(belnr, "")

            comisiones.append(
                {
                    "cuenta_banco": cuenta_banco,
                    "cuenta_gasto": pos_comision.ractt,
                    "monto": abs(float(pos_comision.wsl)),
                    "fecha": pos_comision.partida.budat,
                    "documento_primario": belnr,
                    "referencia": pos_comision.zuonr or "",
                    "referencia1": (pos_comision.partida.bktxt or "").strip(),
                    "rwcur": pos_comision.rwcur or "",
                }
            )
            usados.add(pos_comision.id)

    restantes = [p for p in posiciones if p.id not in usados]
    return comisiones, restantes


def _distribuir_impuesto_en_gastos(
    posiciones_factura: list,
) -> tuple[list[dict], str, str, float]:
    gastos = []
    impuestos = []
    dif_cambio = []
    cxp = []

    total_gasto_abs = 0.0
    total_impuesto_original = 0.0

    proveedor = ""
    cliente = ""

    for pos in posiciones_factura:
        cuenta = pos.ractt or ""
        monto_orig = float(pos.wsl)
        monto_abs = abs(monto_orig)

        if pos.lifnr:
            proveedor = pos.lifnr
        if pos.kunnr:
            cliente = pos.kunnr

        if cuenta in CUENTAS_IMPUESTOS:
            impuestos.append(pos)
            total_impuesto_original += monto_orig
        elif getattr(pos, "koart", "") in ("K", "D"):
            cxp.append(pos)
        elif cuenta in CUENTAS_DIF_CAMBIO:
            dif_cambio.append(pos)
        else:
            gastos.append(pos)
            total_gasto_abs += monto_abs

    if not gastos:
        if dif_cambio:
            gastos = dif_cambio
            total_gasto_abs = sum(abs(float(g.wsl)) for g in gastos)
        elif cxp:
            gastos = cxp
            total_gasto_abs = sum(abs(float(g.wsl)) for g in gastos)
        elif impuestos:
            gastos = impuestos
            total_gasto_abs = sum(abs(float(g.wsl)) for g in gastos)
            impuestos = []
            total_impuesto_original = 0.0

    lineas = []
    suma_totales_lineas = 0.0

    if gastos and impuestos and total_gasto_abs != 0:
        for gasto in gastos:
            monto_orig = float(gasto.wsl)
            proporcion = abs(monto_orig) / total_gasto_abs
            impuesto_asignado = total_impuesto_original * proporcion
            monto_total = monto_orig + impuesto_asignado

            lineas.append(
                {
                    "posicion": gasto,
                    "cuenta_gasto": gasto.ractt,
                    "monto_base": monto_orig,
                    "monto_total": monto_total,
                }
            )
            suma_totales_lineas += abs(monto_total)
    else:
        for gasto in gastos:
            monto_orig = float(gasto.wsl)
            lineas.append(
                {
                    "posicion": gasto,
                    "cuenta_gasto": gasto.ractt,
                    "monto_base": monto_orig,
                    "monto_total": monto_orig,
                }
            )
            suma_totales_lineas += abs(monto_orig)

    return lineas, proveedor, cliente, suma_totales_lineas


def conciliar_cadena_zr_zp_facturas(
    balde_solo_zps,
    balde_solo_zrs,
    facturas_agrupadas: dict,
    mapa_factura_zp: dict,
) -> tuple[list, list, list]:
    zrs_usados = set()
    zps_usados = set()
    emparejamientos = []

    zp_a_facturas = defaultdict(list)
    for belnr_factura, belnr_zp in mapa_factura_zp.items():
        if belnr_zp:
            zp_a_facturas[belnr_zp].append(belnr_factura)

    zrs_por_augbl_cuenta = defaultdict(list)
    for zr in balde_solo_zrs:
        if zr.augbl and str(zr.augbl).strip():
            zrs_por_augbl_cuenta[(str(zr.augbl).strip(), zr.ractt)].append(zr)

    lotes_zps_fase1 = defaultdict(list)
    for zp in balde_solo_zps:
        if not zp.augbl:
            continue
        ref = _clean_ref(zp.partida.bktxt) or _clean_ref(zp.zuonr)
        if not ref:
            ref = f"SIN_REF_{zp.id}"
        lotes_zps_fase1[(str(zp.augbl).strip(), zp.ractt, ref)].append(zp)

    for (augbl, cuenta, ref), lista_zps in lotes_zps_fase1.items():
        sum_zp = sum(_monto_seguro(zp.wsl) for zp in lista_zps)

        candidatos_zr = zrs_por_augbl_cuenta.get((augbl, cuenta), [])
        # Ordenamos preferiendo líneas de Haber (wsl < 0) para que cuadre con salidas
        candidatos_zr_sorted = sorted(candidatos_zr, key=lambda x: float(x.wsl))
        for zr in candidatos_zr_sorted:
            if zr.id in zrs_usados:
                continue
            if abs(sum_zp - _monto_seguro(zr.wsl)) <= 0.02:
                emparejamientos.append((lista_zps, [zr]))
                for zp in lista_zps:
                    zps_usados.add(zp.id)
                zrs_usados.add(zr.id)
                break

    zps_pendientes_fase2 = defaultdict(list)
    for zp in balde_solo_zps:
        if zp.id in zps_usados or not zp.augbl:
            continue
        zps_pendientes_fase2[(str(zp.augbl).strip(), zp.ractt)].append(zp)

    for (augbl, cuenta), lista_zps in zps_pendientes_fase2.items():
        candidatos_zr = [
            zr
            for zr in zrs_por_augbl_cuenta.get((augbl, cuenta), [])
            if zr.id not in zrs_usados
        ]
        candidatos_zr_sorted = sorted(candidatos_zr, key=lambda x: float(x.wsl))
        if len(candidatos_zr_sorted) > 0:
            sum_zp = sum(_monto_seguro(zp.wsl) for zp in lista_zps)
            for zr in candidatos_zr_sorted:
                if abs(sum_zp - _monto_seguro(zr.wsl)) <= 0.02:
                    emparejamientos.append((lista_zps, [zr]))
                    for zp in lista_zps:
                        zps_usados.add(zp.id)
                    zrs_usados.add(zr.id)
                    break

    lotes_zps_fase3 = defaultdict(list)
    for zp in balde_solo_zps:
        if zp.id in zps_usados:
            continue
        ref = _clean_ref(zp.partida.bktxt) or _clean_ref(zp.zuonr)
        if not ref:
            ref = f"SIN_REF_{zp.id}"
        lotes_zps_fase3[(zp.ractt, ref)].append(zp)

    for (cuenta, ref), lista_zps in lotes_zps_fase3.items():
        if ref.startswith("SIN_REF_"):
            continue
        sum_zp = sum(_monto_seguro(zp.wsl) for zp in lista_zps)

        candidatos_zrs_filtrados = [
            zr
            for zr in balde_solo_zrs
            if zr.id not in zrs_usados and zr.ractt == cuenta
        ]
        candidatos_zr_sorted = sorted(
            candidatos_zrs_filtrados, key=lambda x: float(x.wsl)
        )

        for zr in candidatos_zr_sorted:
            ref_zr = _clean_ref(zr.zuonr) or _clean_ref(zr.partida.bktxt)

            if (ref in ref_zr) or (ref_zr in ref and len(ref_zr) >= 4):
                dias = abs((zr.partida.budat - lista_zps[0].partida.budat).days)
                if abs(sum_zp - _monto_seguro(zr.wsl)) <= 0.02 and dias <= 15:
                    emparejamientos.append((lista_zps, [zr]))
                    for zp in lista_zps:
                        zps_usados.add(zp.id)
                    zrs_usados.add(zr.id)
                    break

    for zp in balde_solo_zps:
        if zp.id in zps_usados:
            continue
        monto_zp = _monto_seguro(zp.wsl)
        cuenta = zp.ractt

        candidatos = []
        for zr in balde_solo_zrs:
            if zr.id in zrs_usados or zr.ractt != cuenta:
                continue
            dias = abs((zr.partida.budat - zp.partida.budat).days)
            if abs(monto_zp - _monto_seguro(zr.wsl)) <= 0.02 and dias <= 10:
                candidatos.append(zr)

        candidatos_sorted = sorted(candidatos, key=lambda x: float(x.wsl))
        if len(candidatos_sorted) >= 1:
            zr = candidatos_sorted[0]
            emparejamientos.append(([zp], [zr]))
            zps_usados.add(zp.id)
            zrs_usados.add(zr.id)

    for zp in balde_solo_zps:
        if zp.id not in zps_usados:
            emparejamientos.append(([zp], "EN_TRANSITO"))
            zps_usados.add(zp.id)

    resultados_agrupados = {}

    for lista_zps, zrs_objs in emparejamientos:
        zps_agrupados = defaultdict(list)
        for zp in lista_zps:
            zps_agrupados[zp.partida.belnr].append(zp)

        for belnr_zp, lineas_banco_zp in zps_agrupados.items():
            monto_total_zp_abs = round(
                abs(sum(float(zp.wsl) for zp in lineas_banco_zp)), 2
            )

            if zrs_objs == "EN_TRANSITO":
                zr_belnr = "EN_TRANSITO"
                fecha_contab = lineas_banco_zp[0].partida.budat
                ref_propuesta = (lineas_banco_zp[0].partida.bktxt or "").strip()
            else:
                zr_primario = zrs_objs[0]
                zr_belnr = zr_primario.partida.belnr
                fecha_contab = zr_primario.partida.budat
                ref_propuesta = (zr_primario.zuonr or "").strip() or (
                    lineas_banco_zp[0].partida.bktxt or ""
                ).strip()

            cuenta_banco = lineas_banco_zp[0].ractt
            rwcur_lote = lineas_banco_zp[0].rwcur or ""
            referencia1_del_zp = (lineas_banco_zp[0].partida.bktxt or "").strip()

            zp_procesados = set()
            if belnr_zp in zp_procesados:
                continue
            zp_procesados.add(belnr_zp)

            facturas_del_zp = zp_a_facturas.get(belnr_zp, [])
            externas = [f for f in facturas_del_zp if f != belnr_zp]

            posiciones_globales = []
            for belnr_factura in facturas_del_zp:
                is_zp_itself = belnr_factura == belnr_zp
                todas_pos_doc = facturas_agrupadas.get(belnr_factura, [])

                if is_zp_itself:
                    tiene_gasto_real = True
                else:
                    tiene_gasto_real = any(
                        p.ractt not in CUENTAS_IMPUESTOS
                        and p.ractt not in CUENTAS_DIF_CAMBIO
                        and getattr(p, "koart", "") not in ("K", "D")
                        for p in todas_pos_doc
                    )

                for p in todas_pos_doc:
                    pertenece = False
                    if p.augbl == belnr_zp:
                        pertenece = True
                    elif is_zp_itself:
                        pertenece = True

                    if pertenece:
                        if is_zp_itself and externas:
                            if (
                                p.ractt not in CUENTAS_IMPUESTOS
                                and p.ractt not in CUENTAS_DIF_CAMBIO
                            ):
                                continue

                        if p.ractt in CUENTAS_DIF_CAMBIO and tiene_gasto_real:
                            continue

                        posiciones_globales.append(p)

            lineas_a_emitir = []
            gran_total_facturas_abs = 0.0

            if not posiciones_globales:
                lineas_a_emitir.append(
                    {
                        "zp_belnr": belnr_zp,
                        "factura_belnr": "",
                        "cuenta_gasto": lineas_banco_zp[0].ractt,
                        "lifnr": lineas_banco_zp[0].lifnr or "",
                        "kunnr": lineas_banco_zp[0].kunnr or "",
                        "referencia1_zp": referencia1_del_zp,
                        "monto_base": float(lineas_banco_zp[0].wsl),
                        "monto_total": float(lineas_banco_zp[0].wsl),
                    }
                )
                gran_total_facturas_abs += abs(float(lineas_banco_zp[0].wsl))
            else:
                lineas, prov, cli, suma_fac = _distribuir_impuesto_en_gastos(
                    posiciones_globales
                )
                gran_total_facturas_abs += suma_fac
                refs_facturas = ",".join(externas) if externas else ""

                for linea in lineas:
                    linea["zp_belnr"] = belnr_zp
                    linea["factura_belnr"] = refs_facturas
                    linea["lifnr"] = prov or lineas_banco_zp[0].lifnr or ""
                    linea["kunnr"] = cli or lineas_banco_zp[0].kunnr or ""
                    linea["referencia1_zp"] = referencia1_del_zp
                    lineas_a_emitir.append(linea)

            monto_base_acumulado = 0.0
            monto_total_acumulado = 0.0

            lineas_validas = [
                line
                for line in lineas_a_emitir
                if abs(line["monto_total"]) > 0.001 or abs(line["monto_base"]) > 0.001
            ]
            if not lineas_validas and lineas_a_emitir:
                lineas_validas = lineas_a_emitir

            for i, linea in enumerate(lineas_validas):
                if gran_total_facturas_abs > 0.001:
                    if i == len(lineas_validas) - 1:
                        monto_total_final = round(
                            monto_total_zp_abs - monto_total_acumulado, 2
                        )
                        monto_base_final = round(
                            monto_total_zp_abs - monto_base_acumulado, 2
                        )
                    else:
                        proporcion_t = (
                            abs(linea["monto_total"]) / gran_total_facturas_abs
                        )
                        proporcion_b = (
                            abs(linea["monto_base"]) / gran_total_facturas_abs
                        )
                        monto_total_final = round(monto_total_zp_abs * proporcion_t, 2)
                        monto_base_final = round(monto_total_zp_abs * proporcion_b, 2)

                        monto_total_acumulado += monto_total_final
                        monto_base_acumulado += monto_base_final
                else:
                    if i == len(lineas_validas) - 1:
                        monto_total_final = round(
                            monto_total_zp_abs - monto_total_acumulado, 2
                        )
                        monto_base_final = round(
                            monto_total_zp_abs - monto_base_acumulado, 2
                        )
                    else:
                        monto_total_final = round(abs(linea["monto_total"]), 2)
                        monto_base_final = round(abs(linea["monto_base"]), 2)
                        monto_total_acumulado += monto_total_final
                        monto_base_acumulado += monto_base_final

                monto_base_final = abs(monto_base_final)
                monto_total_final = abs(monto_total_final)

                if monto_total_final == 0.0 and monto_base_final == 0.0:
                    continue

                llave_agrupacion = (
                    zr_belnr,
                    linea["zp_belnr"],
                    linea["factura_belnr"],
                    cuenta_banco,
                    linea["cuenta_gasto"],
                    linea["lifnr"],
                    linea["kunnr"],
                    ref_propuesta,
                    fecha_contab,
                    linea["referencia1_zp"],
                    rwcur_lote,
                )

                if llave_agrupacion not in resultados_agrupados:
                    resultados_agrupados[llave_agrupacion] = {
                        "monto_base": 0.0,
                        "monto_total": 0.0,
                    }

                resultados_agrupados[llave_agrupacion]["monto_base"] += monto_base_final
                resultados_agrupados[llave_agrupacion]["monto_total"] += (
                    monto_total_final
                )

    resultados = []
    for key, montos in resultados_agrupados.items():
        if round(montos["monto_total"], 2) == 0.0:
            continue
        resultados.append(
            {
                "zr_belnr": key[0],
                "zp_belnr": key[1],
                "factura_belnr": key[2],
                "cuenta_banco": key[3],
                "cuenta_gasto": key[4],
                "lifnr": key[5],
                "kunnr": key[6],
                "referencia": key[7],
                "fecha_contabilizacion": key[8],
                "referencia1": key[9],
                "rwcur": key[10],
                "monto_base": montos["monto_base"],
                "monto_total": montos["monto_total"],
            }
        )

    zps_auditoria = []
    zrs_auditoria = [zr for zr in balde_solo_zrs if zr.id not in zrs_usados]

    return resultados, zps_auditoria, zrs_auditoria


def procesar_ingresos_bancarios(posiciones, cuentas_ingreso_validas):
    ingresos_validados = []
    asientos_auditoria = []
    usados = set()

    dz_da = [
        p
        for p in posiciones
        if p.partida.blart in ("DZ", "DA") and p.ractt in cuentas_ingreso_validas
    ]
    zrs = [
        p
        for p in posiciones
        if p.partida.blart == "ZR" and p.ractt in cuentas_ingreso_validas
    ]

    dz_da_por_augbl = defaultdict(list)
    zrs_por_augbl = defaultdict(list)

    for p in dz_da:
        if p.augbl:
            dz_da_por_augbl[str(p.augbl).strip()].append(p)
    for p in zrs:
        if p.augbl:
            zrs_por_augbl[str(p.augbl).strip()].append(p)

    todas_augbls = set(dz_da_por_augbl.keys()) | set(zrs_por_augbl.keys())
    for augbl in todas_augbls:
        lista_dz = dz_da_por_augbl[augbl]
        lista_zr = zrs_por_augbl[augbl]

        for dz in lista_dz:
            if dz.id in usados:
                continue
            monto_dz = abs(float(dz.wsl))

            best_zr = None
            best_diff = float("inf")
            for zr in lista_zr:
                if zr.id in usados:
                    continue
                diff = abs(monto_dz - abs(float(zr.wsl)))
                if diff < best_diff:
                    best_diff = diff
                    best_zr = zr

            if best_zr and best_diff <= 0.05:
                ingresos_validados.append(
                    {
                        "cuenta": best_zr.ractt,
                        "monto": monto_dz,
                        "fecha": best_zr.partida.budat,
                        "documento_primario": best_zr.partida.belnr,
                        "documento_secundario": dz.partida.belnr,
                        "referencia": best_zr.zuonr or "",
                        "referencia1": (best_zr.partida.bktxt or "").strip(),
                        "rwcur": best_zr.rwcur or dz.rwcur or "",
                        "kunnr": dz.kunnr or best_zr.kunnr or "",
                        "lifnr": dz.lifnr or best_zr.lifnr or "",
                    }
                )
                usados.add(dz.id)
                usados.add(best_zr.id)

        remanente_dz = [dz for dz in lista_dz if dz.id not in usados]
        remanente_zr = [zr for zr in lista_zr if zr.id not in usados]

        if remanente_dz and remanente_zr:
            sum_dz = sum(abs(float(dz.wsl)) for dz in remanente_dz)
            sum_zr = sum(abs(float(zr.wsl)) for zr in remanente_zr)

            if abs(sum_dz - sum_zr) <= 0.05:
                zrs_pendientes = [
                    {"obj": zr, "saldo": abs(float(zr.wsl))} for zr in remanente_zr
                ]
                dzs_pendientes = [
                    {"obj": dz, "saldo": abs(float(dz.wsl))} for dz in remanente_dz
                ]

                idx_zr = 0
                idx_dz = 0
                while idx_zr < len(zrs_pendientes) and idx_dz < len(dzs_pendientes):
                    zr_item = zrs_pendientes[idx_zr]
                    dz_item = dzs_pendientes[idx_dz]
                    if zr_item["saldo"] <= 0.001:
                        idx_zr += 1
                        continue
                    if dz_item["saldo"] <= 0.001:
                        idx_dz += 1
                        continue

                    monto_asignar = min(zr_item["saldo"], dz_item["saldo"])
                    ingresos_validados.append(
                        {
                            "cuenta": zr_item["obj"].ractt,
                            "monto": monto_asignar,
                            "fecha": zr_item["obj"].partida.budat,
                            "documento_primario": zr_item["obj"].partida.belnr,
                            "documento_secundario": dz_item["obj"].partida.belnr,
                            "referencia": zr_item["obj"].zuonr or "",
                            "referencia1": (zr_item["obj"].partida.bktxt or "").strip(),
                            "rwcur": zr_item["obj"].rwcur or "",
                            "kunnr": dz_item["obj"].kunnr or zr_item["obj"].kunnr or "",
                            "lifnr": dz_item["obj"].lifnr or zr_item["obj"].lifnr or "",
                        }
                    )

                    zr_item["saldo"] -= monto_asignar
                    dz_item["saldo"] -= monto_asignar

                    if zr_item["saldo"] <= 0.001:
                        usados.add(zr_item["obj"].id)
                    if dz_item["saldo"] <= 0.001:
                        usados.add(dz_item["obj"].id)

    remanente_dz = [dz for dz in dz_da if dz.id not in usados]
    remanente_zr = [zr for zr in zrs if zr.id not in usados]

    for dz in remanente_dz:
        monto_dz = abs(float(dz.wsl))
        ref_dz = _clean_ref(dz.zuonr)
        if len(ref_dz) < 4:
            continue

        for zr in remanente_zr:
            if zr.id in usados:
                continue
            ref_zr = _clean_ref(zr.zuonr)

            if (ref_dz in ref_zr or ref_zr in ref_dz) and len(ref_zr) >= 4:
                dias = abs((zr.partida.budat - dz.partida.budat).days)
                if abs(monto_dz - abs(float(zr.wsl))) <= 0.05 and dias <= 15:
                    ingresos_validados.append(
                        {
                            "cuenta": zr.ractt,
                            "monto": monto_dz,
                            "fecha": zr.partida.budat,
                            "documento_primario": zr.partida.belnr,
                            "documento_secundario": dz.partida.belnr,
                            "referencia": zr.zuonr or "",
                            "referencia1": (zr.partida.bktxt or "").strip(),
                            "rwcur": zr.rwcur or dz.rwcur or "",
                            "kunnr": dz.kunnr or zr.kunnr or "",
                            "lifnr": dz.lifnr or zr.lifnr or "",
                        }
                    )
                    usados.add(dz.id)
                    usados.add(zr.id)
                    break

    for dz in [
        p
        for p in posiciones
        if p.id not in usados
        and p.ractt.endswith("4")
        and p.partida.blart in ("ZR", "DZ", "DA")
    ]:
        ingresos_validados.append(
            {
                "cuenta": dz.ractt,
                "monto": abs(float(dz.wsl)),
                "fecha": dz.partida.budat,
                "documento_primario": dz.partida.belnr,
                "documento_secundario": "",
                "referencia": dz.zuonr or "",
                "referencia1": (dz.partida.bktxt or "").strip(),
                "rwcur": dz.rwcur or "",
                "kunnr": dz.kunnr or "",
                "lifnr": dz.lifnr or "",
            }
        )
        usados.add(dz.id)

    for zr in [p for p in zrs if p.id not in usados]:
        ingresos_validados.append(
            {
                "cuenta": zr.ractt,
                "monto": abs(float(zr.wsl)),
                "fecha": zr.partida.budat,
                "documento_primario": zr.partida.belnr,
                "documento_secundario": "",
                "referencia": zr.zuonr or "",
                "referencia1": (zr.partida.bktxt or "").strip(),
                "rwcur": zr.rwcur or "",
                "kunnr": zr.kunnr or "",
                "lifnr": zr.lifnr or "",
            }
        )
        usados.add(zr.id)

    for dz in [p for p in dz_da if p.id not in usados]:
        if not dz.ractt.endswith("4"):
            asientos_auditoria.append(
                (dz, "Depósito (DZ/DA) no encontró Extracto Bancario (ZR) equivalente")
            )

    return ingresos_validados, asientos_auditoria
