# sap_sync/utils/conciliation/gastos.py
from collections import defaultdict


class DistribuidorGastos:
    def __init__(self, cuentas_impuestos, cuentas_dif_cambio):
        self.cuentas_impuestos = cuentas_impuestos
        self.cuentas_dif_cambio = cuentas_dif_cambio


def rastrear_cadena_completa(
    semillas_belnr,
    semillas_augbl,
    mapa_facturas_por_zp,
    facturas_agrupadas,
    cuentas_impuestos,
    cuentas_dif_cambio,
    cuentas_bancarias,  # ⚡ NUEVO CORTAFUEGOS
    max_depth=2,  # ⚡ LÍMITE REDUCIDO A 2 PARA EVITAR CONGELAMIENTO
):
    """
    Viaja recursivamente por la telaraña de compensaciones de SAP, pero con
    un límite estricto de profundidad y sanitización de strings.
    """
    visitados_belnr = set()
    visitados_augbl = set()
    por_visitar_belnr = set(semillas_belnr)
    por_visitar_augbl = set(semillas_augbl)
    profundidad = 0

    # ⚡ SANITIZACIÓN EXTREMA: Quitar ceros a la izquierda para garantizar el bloqueo
    impuestos_str = {str(c).lstrip("0") for c in cuentas_impuestos if c}
    dif_cambio_str = {str(c).lstrip("0") for c in cuentas_dif_cambio if c}
    bancos_str = {str(c).lstrip("0") for c in cuentas_bancarias if c}

    while (por_visitar_belnr or por_visitar_augbl) and profundidad < max_depth:
        actuales_belnr = por_visitar_belnr.copy()
        actuales_augbl = por_visitar_augbl.copy()
        por_visitar_belnr.clear()
        por_visitar_augbl.clear()

        for augbl in actuales_augbl:
            if augbl and augbl not in visitados_augbl:
                visitados_augbl.add(augbl)
                for d in mapa_facturas_por_zp.get(augbl, []):
                    if d not in visitados_belnr:
                        por_visitar_belnr.add(d)

        for belnr in actuales_belnr:
            if belnr and belnr not in visitados_belnr:
                visitados_belnr.add(belnr)
                for p in facturas_agrupadas.get(belnr, []):
                    # ⚡ FIX: Sanitizar la cuenta de la línea actual
                    cuenta_str = str(p.ractt).lstrip("0")

                    # ⚡ CORTAFUEGOS TOTAL: Si es impuesto, dif. cambio o banco, NO seguimos
                    if (
                        cuenta_str in impuestos_str
                        or cuenta_str in dif_cambio_str
                        or cuenta_str in bancos_str
                    ):
                        continue

                    if p.augbl and p.augbl not in visitados_augbl:
                        por_visitar_augbl.add(p.augbl)

        profundidad += 1

    return visitados_belnr


def conciliar_cadena_zr_zp_facturas(
    balde_solo_zps,
    balde_solo_zrs,
    facturas_agrupadas: dict,
    mapa_factura_zp: dict,
    cuentas_impuestos: set,
    cuentas_dif_cambio: set,
    cuentas_standalone: set,
    cuentas_bancarias: set,
) -> tuple[list, list, list]:

    resultados = []
    zps_procesados = set()
    zrs_auditoria = []

    # --- MAPAS DE MEMORIA O(1) ---
    mapa_zps_por_augbl = defaultdict(list)
    mapa_zps_por_belnr = defaultdict(list)
    mapa_zps_por_zuonr = defaultdict(list)

    for zp in balde_solo_zps:
        mapa_zps_por_belnr[zp.partida.belnr].append(zp)
        if zp.augbl:
            mapa_zps_por_augbl[zp.augbl].append(zp)
        if zp.zuonr:
            mapa_zps_por_zuonr[zp.zuonr].append(zp)

    mapa_facturas_por_zp = defaultdict(set)
    for f_belnr, f_posiciones in facturas_agrupadas.items():
        zp_asignado = mapa_factura_zp.get(f_belnr)
        if zp_asignado:
            mapa_facturas_por_zp[zp_asignado].add(f_belnr)
        for p in f_posiciones:
            if p.augbl:
                mapa_facturas_por_zp[p.augbl].add(f_belnr)

    # ⚡ PRE-EMPAREJAMIENTO INTELIGENTE
    mapa_zr_a_zps = defaultdict(list)
    zrs_agrupados = defaultdict(list)

    for zr in balde_solo_zrs:
        if str(zr.ractt).endswith("0") and zr.drcrk == "S":
            continue

        clave_grupo = zr.augbl if zr.augbl else zr.partida.belnr
        zrs_agrupados[clave_grupo].append(zr)

    for clave, zrs_grupo in zrs_agrupados.items():
        zps_grupo = list(mapa_zps_por_augbl.get(clave, []))

        for zp_ref in mapa_zps_por_belnr.get(clave, []):
            if zp_ref not in zps_grupo:
                zps_grupo.append(zp_ref)

        for zr in zrs_grupo:
            for zp_huerfano in mapa_zps_por_augbl.get(zr.partida.belnr, []):
                if zp_huerfano not in zps_grupo:
                    zps_grupo.append(zp_huerfano)

            if zr.zuonr:
                for zp_ref in mapa_zps_por_zuonr.get(zr.zuonr, []):
                    if zp_ref not in zps_grupo:
                        zps_grupo.append(zp_ref)

                for zp_ref in mapa_zps_por_belnr.get(zr.zuonr, []):
                    if zp_ref not in zps_grupo:
                        zps_grupo.append(zp_ref)

            for zp_ref in mapa_zps_por_zuonr.get(zr.partida.belnr, []):
                if zp_ref not in zps_grupo:
                    zps_grupo.append(zp_ref)

        if not zps_grupo:
            for zr in zrs_grupo:
                mapa_zr_a_zps[zr.id] = []
            continue

        es_grupo_compensado = any(zr.augbl == clave for zr in zrs_grupo) and (
            any(zp.augbl == clave for zp in zps_grupo)
            or any(zp.partida.belnr == clave for zp in zps_grupo)
        )

        if es_grupo_compensado:
            for zr in zrs_grupo:
                mapa_zr_a_zps[zr.id] = list(zps_grupo)
            continue

        zrs_restantes = list(zrs_grupo)
        zps_restantes = list(zps_grupo)

        # 1 a 1
        for zr in list(zrs_restantes):
            monto_zr = abs(float(zr.wsl))
            mejor_zp = None
            menor_dif = float("inf")
            for zp in zps_restantes:
                dif = abs(abs(float(zp.wsl)) - monto_zr)
                if dif < menor_dif:
                    menor_dif = dif
                    mejor_zp = zp
            if mejor_zp and menor_dif <= (monto_zr * 0.10):
                mapa_zr_a_zps[zr.id] = [mejor_zp]
                zps_restantes.remove(mejor_zp)
                zrs_restantes.remove(zr)

        # N a 1
        if zrs_restantes and zps_restantes:
            for zp in list(zps_restantes):
                monto_zp = abs(float(zp.wsl))
                zrs_restantes.sort(key=lambda x: abs(float(x.wsl)), reverse=True)
                suma_temp = 0.0
                zrs_usados = []
                for zr in zrs_restantes:
                    suma_temp += abs(float(zr.wsl))
                    zrs_usados.append(zr)
                    if abs(monto_zp - suma_temp) <= (monto_zp * 0.10):
                        for zr_usado in zrs_usados:
                            mapa_zr_a_zps[zr_usado.id] = [zp]
                            zrs_restantes.remove(zr_usado)
                        zps_restantes.remove(zp)
                        break

        # 1 a M
        if zrs_restantes and zps_restantes:
            for zr in list(zrs_restantes):
                monto_zr = abs(float(zr.wsl))
                zps_restantes.sort(key=lambda x: abs(float(x.wsl)), reverse=True)
                suma_temp = 0.0
                zps_usados = []
                for zp in zps_restantes:
                    suma_temp += abs(float(zp.wsl))
                    zps_usados.append(zp)
                    if abs(monto_zr - suma_temp) <= (monto_zr * 0.10):
                        mapa_zr_a_zps[zr.id] = list(zps_usados)
                        for zp_usado in zps_usados:
                            zps_restantes.remove(zp_usado)
                        zrs_restantes.remove(zr)
                        break

        # N a M
        if zrs_restantes and zps_restantes:
            suma_zrs = sum(abs(float(zr.wsl)) for zr in zrs_restantes)
            suma_zps = sum(abs(float(zp.wsl)) for zp in zps_restantes)
            augbls_zrs = set(zr.augbl for zr in zrs_restantes if zr.augbl)
            augbls_zps = set(zp.augbl for zp in zps_restantes if zp.augbl)
            comparten_augbl = bool(augbls_zrs & augbls_zps)

            if (
                abs(suma_zrs - suma_zps) <= (max(suma_zrs, suma_zps) * 0.10)
                or comparten_augbl
            ):
                for zr in zrs_restantes:
                    mapa_zr_a_zps[zr.id] = list(zps_restantes)
                zrs_restantes.clear()
                zps_restantes.clear()

        # Fallback Grupal
        if zrs_restantes:
            for zr in zrs_restantes:
                mapa_zr_a_zps[zr.id] = zps_restantes if zps_restantes else zps_grupo
        elif zps_restantes and zrs_grupo:
            zr_mayor = max(zrs_grupo, key=lambda x: abs(float(x.wsl)))
            if zr_mayor.id in mapa_zr_a_zps:
                para_agregar = [
                    zp for zp in zps_restantes if zp not in mapa_zr_a_zps[zr_mayor.id]
                ]
                mapa_zr_a_zps[zr_mayor.id].extend(para_agregar)

    # ⚡ RESCATE GLOBAL (PROTEGIDO)
    zps_asignados_ids = set()
    for zps_list in mapa_zr_a_zps.values():
        for zp_asig in zps_list:
            zps_asignados_ids.add(zp_asig.id)

    zrs_huerfanos = [
        zr for zr in balde_solo_zrs if not mapa_zr_a_zps.get(zr.id) and not zr.augbl
    ]
    zps_huerfanos = [
        zp for zp in balde_solo_zps if zp.id not in zps_asignados_ids and not zp.augbl
    ]

    if zrs_huerfanos and zps_huerfanos:
        for zr in list(zrs_huerfanos):
            monto_zr = abs(float(zr.wsl))
            mejor_zp = None
            menor_dif = float("inf")
            for zp in zps_huerfanos:
                dif = abs(abs(float(zp.wsl)) - monto_zr)
                if dif < menor_dif:
                    menor_dif = dif
                    mejor_zp = zp
            if mejor_zp and menor_dif <= (monto_zr * 0.10):
                mapa_zr_a_zps[zr.id] = [mejor_zp]
                zps_huerfanos.remove(mejor_zp)
                zrs_huerfanos.remove(zr)

        if zrs_huerfanos and zps_huerfanos:
            for zp in list(zps_huerfanos):
                monto_zp = abs(float(zp.wsl))
                zrs_huerfanos.sort(key=lambda x: abs(float(x.wsl)), reverse=True)
                suma_temp = 0.0
                zrs_usados = []
                for zr in zrs_huerfanos:
                    suma_temp += abs(float(zr.wsl))
                    zrs_usados.append(zr)
                    if abs(monto_zp - suma_temp) <= (monto_zp * 0.10):
                        for zr_usado in zrs_usados:
                            mapa_zr_a_zps[zr_usado.id] = [zp]
                            zrs_huerfanos.remove(zr_usado)
                        zps_huerfanos.remove(zp)
                        break

        if zrs_huerfanos and zps_huerfanos:
            for zr in list(zrs_huerfanos):
                monto_zr = abs(float(zr.wsl))
                zps_huerfanos.sort(key=lambda x: abs(float(x.wsl)), reverse=True)
                suma_temp = 0.0
                zps_usados = []
                for zp in zps_huerfanos:
                    suma_temp += abs(float(zp.wsl))
                    zps_usados.append(zp)
                    if abs(monto_zr - suma_temp) <= (monto_zr * 0.10):
                        mapa_zr_a_zps[zr.id] = list(zps_usados)
                        for zp_usado in zps_usados:
                            zps_huerfanos.remove(zp_usado)
                        zrs_huerfanos.remove(zr)
                        break

    # --- FIN DE PRE-EMPAREJAMIENTO ---

    for zr in balde_solo_zrs:
        zps_relacionados = mapa_zr_a_zps.get(zr.id, [])

        if not zps_relacionados:
            # ⚡ ZR DIRECTOS Y AUTOCOMPENSADOS
            semillas_b = {zr.partida.belnr}
            semillas_a = {zr.augbl} if zr.augbl else set()

            # ⚡ LLAMADA SEGURA AL RASTREADOR
            todos_los_docs_red = rastrear_cadena_completa(
                semillas_b,
                semillas_a,
                mapa_facturas_por_zp,
                facturas_agrupadas,
                cuentas_impuestos,
                cuentas_dif_cambio,
                cuentas_bancarias,
            )

            lineas_directas = []
            for f_id in todos_los_docs_red:
                if f_id != zr.partida.belnr:
                    lineas_directas.extend(facturas_agrupadas.get(f_id, []))

            if lineas_directas:
                monto_total_zr = abs(float(zr.wsl))
                monto_gastos = 0.0
                lineas_validas = []
                lineas_de_impuesto = []
                proveedor_doc = ""

                for p in lineas_directas:
                    if p.lifnr or p.kunnr:
                        proveedor_doc = p.lifnr or p.kunnr

                    if getattr(p, "koart", "") in ("K", "D"):
                        continue

                    if (
                        str(p.ractt) in cuentas_bancarias
                        or p.ractt in cuentas_dif_cambio
                    ):
                        continue

                    if p.ractt in cuentas_impuestos:
                        lineas_de_impuesto.append(p)
                    else:
                        lineas_validas.append(p)
                        monto_gastos += abs(float(p.wsl))

                if not lineas_validas and lineas_de_impuesto:
                    for p_imp in lineas_de_impuesto:
                        lineas_validas.append(p_imp)
                        monto_gastos += abs(float(p_imp.wsl))

                facturas_involucradas = {
                    p.partida.belnr
                    for p in lineas_directas
                    if p.partida.belnr != zr.partida.belnr
                }
                facturas_str = ", ".join(sorted(list(facturas_involucradas)))

                if not lineas_validas:
                    cuenta_acreedor = "SIN_DETALLE_GASTO"
                    for p in lineas_directas:
                        if getattr(p, "koart", "") in ("K", "D"):
                            cuenta_acreedor = str(p.ractt)
                            break

                    resultados.append(
                        _generar_fila_dashboard(
                            zr=zr,
                            zp=zr,
                            monto=monto_total_zr,
                            cuenta_gasto=cuenta_acreedor,
                            tipo="EGRESOS",
                            proveedor=proveedor_doc,
                            factura=facturas_str,
                        )
                    )
                else:
                    for p in lineas_validas:
                        prop = (
                            abs(float(p.wsl)) / monto_gastos
                            if monto_gastos > 0
                            else 1.0 / len(lineas_validas)
                        )
                        resultados.append(
                            _generar_fila_dashboard(
                                zr=zr,
                                zp=zr,
                                monto=monto_total_zr * prop,
                                cuenta_gasto=p.ractt,
                                tipo="EGRESOS",
                                proveedor=proveedor_doc,
                                factura=facturas_str,
                            )
                        )
                continue

            if str(zr.ractt).endswith("0") and str(zr.ractt) not in cuentas_standalone:
                continue

            resultados.append(
                _generar_fila_dashboard(
                    zr=zr,
                    zp=zr,
                    monto=abs(float(zr.wsl)),
                    cuenta_gasto="SIN_DETALLE_GASTO",
                    tipo="EGRESOS",
                    proveedor="",
                    factura="",
                )
            )
            continue

        for zp in zps_relacionados:
            zps_procesados.add(zp.id)

        monto_total_zr = abs(float(zr.wsl))
        total_zps_monto = sum(abs(float(zp.wsl)) for zp in zps_relacionados)
        resultados_crudos = []

        # ⚡ CASCADA NIVEL 1
        for zp in zps_relacionados:
            if total_zps_monto > 0:
                proporcion_zp = abs(float(zp.wsl)) / total_zps_monto
            else:
                proporcion_zp = 1.0 / len(zps_relacionados)

            monto_zr_para_zp = monto_total_zr * proporcion_zp

            semillas_b = {zp.partida.belnr}
            semillas_a = {zp.augbl} if zp.augbl else set()

            # ⚡ LLAMADA SEGURA AL RASTREADOR
            facturas_ids = rastrear_cadena_completa(
                semillas_b,
                semillas_a,
                mapa_facturas_por_zp,
                facturas_agrupadas,
                cuentas_impuestos,
                cuentas_dif_cambio,
                cuentas_bancarias,
            )
            facturas_ids = {f for f in facturas_ids if f != zp.partida.belnr}

            if not facturas_ids:
                from sap_sync.models import PartidaPosicion

                linea_prov = (
                    PartidaPosicion.objects.filter(
                        partida=zp.partida, koart__in=["K", "D"]
                    )
                    .values_list("ractt", flat=True)
                    .first()
                )
                cuenta_gasto = linea_prov if linea_prov else "SIN_DETALLE_GASTO"

                resultados_crudos.append(
                    {
                        "zp": zp,
                        "factura": "",
                        "prov": "",
                        "cuenta": cuenta_gasto,
                        "monto": monto_zr_para_zp,
                    }
                )
                continue

            # ⚡ CASCADA NIVEL 2
            datos_facturas = {}
            suma_gastos_todas_facturas_zp = 0.0

            for f_id in facturas_ids:
                lineas_por_prov = defaultdict(list)
                monto_por_prov = defaultdict(float)
                proveedores_en_doc = set()
                impuestos_huerfanos = []

                for p in facturas_agrupadas[f_id]:
                    prov = p.lifnr or p.kunnr or ""
                    if getattr(p, "koart", "") in ("K", "D"):
                        if prov:
                            proveedores_en_doc.add(prov)
                        continue

                    if (
                        str(p.ractt) in cuentas_bancarias
                        or p.ractt in cuentas_dif_cambio
                    ):
                        continue

                    if p.ractt in cuentas_impuestos:
                        impuestos_huerfanos.append((prov, p))
                    else:
                        monto_gasto = abs(float(p.wsl))
                        lineas_por_prov[prov].append(
                            {"cuenta": p.ractt, "monto": monto_gasto}
                        )
                        monto_por_prov[prov] += monto_gasto

                if not lineas_por_prov and impuestos_huerfanos:
                    for prov_imp, p_imp in impuestos_huerfanos:
                        monto_gasto = abs(float(p_imp.wsl))
                        lineas_por_prov[prov_imp].append(
                            {"cuenta": p_imp.ractt, "monto": monto_gasto}
                        )
                        monto_por_prov[prov_imp] += monto_gasto

                if "" in lineas_por_prov and len(proveedores_en_doc) == 1:
                    unico_prov = list(proveedores_en_doc)[0]
                    lineas_por_prov[unico_prov].extend(lineas_por_prov[""])
                    monto_por_prov[unico_prov] += monto_por_prov[""]
                    del lineas_por_prov[""]
                    del monto_por_prov[""]
                elif "" in lineas_por_prov and len(proveedores_en_doc) > 1:
                    lineas_huerfanas = lineas_por_prov.pop("")
                    for lh in lineas_huerfanas:
                        monto_dividido = lh["monto"] / len(proveedores_en_doc)
                        for prov in proveedores_en_doc:
                            lineas_por_prov[prov].append(
                                {"cuenta": lh["cuenta"], "monto": monto_dividido}
                            )
                            monto_por_prov[prov] += monto_dividido
                    del monto_por_prov[""]

                for prov in proveedores_en_doc:
                    if prov not in lineas_por_prov or not lineas_por_prov[prov]:
                        monto_dummy = 0.0
                        cuenta_acreedor = "CUENTA_CONTABLE_ND"
                        for p in facturas_agrupadas[f_id]:
                            if getattr(p, "koart", "") in ("K", "D") and (
                                p.lifnr == prov or p.kunnr == prov
                            ):
                                cuenta_acreedor = str(p.ractt)
                                monto_dummy += abs(float(p.wsl))

                        monto_dummy = monto_dummy or 1.0
                        lineas_por_prov[prov].append(
                            {"cuenta": cuenta_acreedor, "monto": monto_dummy}
                        )
                        monto_por_prov[prov] += monto_dummy

                for prov, lineas in lineas_por_prov.items():
                    if not prov and proveedores_en_doc:
                        continue

                    llave_dt = f"{f_id}_{prov}"
                    datos_facturas[llave_dt] = {
                        "factura_id": f_id,
                        "suma_gastos": monto_por_prov[prov],
                        "lineas": lineas,
                        "prov": prov,
                    }
                    suma_gastos_todas_facturas_zp += monto_por_prov[prov]

            # ⚡ CASCADA NIVEL 3
            for f_llave, f_data in datos_facturas.items():
                if suma_gastos_todas_facturas_zp > 0:
                    proporcion_factura = (
                        f_data["suma_gastos"] / suma_gastos_todas_facturas_zp
                    )
                else:
                    proporcion_factura = 1.0 / len(datos_facturas)

                monto_zr_para_factura = monto_zr_para_zp * proporcion_factura
                if monto_zr_para_factura <= 0:
                    continue

                for linea in f_data["lineas"]:
                    if f_data["suma_gastos"] > 0:
                        proporcion_cuenta = linea["monto"] / f_data["suma_gastos"]
                    else:
                        proporcion_cuenta = 1.0 / len(f_data["lineas"])

                    monto_final_cuenta = monto_zr_para_factura * proporcion_cuenta
                    resultados_crudos.append(
                        {
                            "zp": zp,
                            "factura": f_data["factura_id"],
                            "prov": f_data["prov"],
                            "cuenta": linea["cuenta"],
                            "monto": monto_final_cuenta,
                        }
                    )

        agrupados = {}
        for r in resultados_crudos:
            llave = (r["zp"].id, r["cuenta"], r["prov"])
            if llave not in agrupados:
                agrupados[llave] = {
                    "zr": zr,
                    "zp": r["zp"],
                    "monto": 0.0,
                    "facturas": set(),
                    "cuenta_gasto": r["cuenta"],
                    "proveedor": r["prov"],
                }

            agrupados[llave]["monto"] += r["monto"]
            if r["factura"]:
                agrupados[llave]["facturas"].add(r["factura"])

        for data in agrupados.values():
            facturas_str = ", ".join(sorted(list(data["facturas"])))
            resultados.append(
                _generar_fila_dashboard(
                    zr=data["zr"],
                    zp=data["zp"],
                    monto=data["monto"],
                    cuenta_gasto=data["cuenta_gasto"],
                    tipo="EGRESOS",
                    proveedor=data["proveedor"],
                    factura=facturas_str,
                )
            )

    zps_auditoria = [zp for zp in balde_solo_zps if zp.id not in zps_procesados]

    return resultados, zps_auditoria, zrs_auditoria


def _generar_fila_dashboard(
    zr, zp, monto, cuenta_gasto, tipo, proveedor="", factura=""
):
    return {
        "tipo_operacion": tipo,
        "cuenta_banco": zr.ractt,
        "cuenta_gasto": cuenta_gasto,
        "monto": round(monto, 2),
        "fecha": zr.partida.budat,
        "documento_banco": zr.partida.belnr,
        "documento_pago": zp.partida.belnr,
        "documento_factura": factura,
        "proveedor": proveedor,
        "referencia": zr.zuonr or zp.zuonr or "",
        "referencia1": (zp.partida.bktxt or "").strip(),
        "rwcur": zr.rwcur or "",
    }
