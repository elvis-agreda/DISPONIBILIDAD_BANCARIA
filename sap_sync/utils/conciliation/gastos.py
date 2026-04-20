# sap_sync/utils/conciliation/gastos.py
from collections import defaultdict


class DistribuidorGastos:
    def __init__(self, cuentas_impuestos, cuentas_dif_cambio):
        self.cuentas_impuestos = cuentas_impuestos
        self.cuentas_dif_cambio = cuentas_dif_cambio

    def distribuir(self, posiciones_factura):
        pass


def conciliar_cadena_zr_zp_facturas(
    balde_solo_zps,
    balde_solo_zrs,
    facturas_agrupadas: dict,
    mapa_factura_zp: dict,
    cuentas_impuestos: set,
    cuentas_dif_cambio: set,
) -> tuple[list, list, list]:

    resultados = []
    zps_procesados = set()
    zrs_auditoria = []

    # --- MAPAS DE MEMORIA O(1) ---
    mapa_zps_por_augbl = defaultdict(list)
    mapa_zps_por_belnr = {}
    for zp in balde_solo_zps:
        mapa_zps_por_belnr[zp.partida.belnr] = zp
        if zp.augbl:
            mapa_zps_por_augbl[zp.augbl].append(zp)

    mapa_facturas_por_zp = defaultdict(set)
    for f_belnr, f_posiciones in facturas_agrupadas.items():
        zp_asignado = mapa_factura_zp.get(f_belnr)
        if zp_asignado:
            mapa_facturas_por_zp[zp_asignado].add(f_belnr)
        for p in f_posiciones:
            if p.augbl:
                mapa_facturas_por_zp[p.augbl].add(f_belnr)

    # ⚡ NUEVA LÓGICA: PRE-EMPAREJAMIENTO INTELIGENTE (1 a 1 y N a 1)
    mapa_zr_a_zps = defaultdict(list)
    zrs_por_augbl = defaultdict(list)

    for zr in balde_solo_zrs:
        if zr.augbl:
            zrs_por_augbl[zr.augbl].append(zr)
        else:
            mapa_zr_a_zps[zr.id] = []

    for augbl, zrs_grupo in zrs_por_augbl.items():
        zps_grupo = list(mapa_zps_por_augbl.get(augbl, []))
        if augbl in mapa_zps_por_belnr and mapa_zps_por_belnr[augbl] not in zps_grupo:
            zps_grupo.append(mapa_zps_por_belnr[augbl])

        if not zps_grupo:
            for zr in zrs_grupo:
                mapa_zr_a_zps[zr.id] = []
            continue

        zrs_restantes = list(zrs_grupo)
        zps_restantes = list(zps_grupo)

        # 1. Búsqueda de parejas exactas (1 a 1) con tolerancia del 5%
        for zr in list(zrs_restantes):
            monto_zr = abs(float(zr.wsl))
            mejor_zp = None
            menor_dif = float("inf")

            for zp in zps_restantes:
                dif = abs(abs(float(zp.wsl)) - monto_zr)
                if dif < menor_dif:
                    menor_dif = dif
                    mejor_zp = zp

            if mejor_zp and menor_dif <= (monto_zr * 0.05):
                mapa_zr_a_zps[zr.id] = [mejor_zp]
                zps_restantes.remove(mejor_zp)
                zrs_restantes.remove(zr)

        # 2. Búsqueda de múltiples pagos para una sola factura (N a 1)
        if zrs_restantes and zps_restantes:
            for zp in list(zps_restantes):
                monto_zp = abs(float(zp.wsl))
                # Ordenar los pagos de mayor a menor para calzar mejor
                zrs_restantes.sort(key=lambda x: abs(float(x.wsl)), reverse=True)

                suma_temp = 0.0
                zrs_usados = []
                for zr in zrs_restantes:
                    suma_temp += abs(float(zr.wsl))
                    zrs_usados.append(zr)

                    if abs(monto_zp - suma_temp) <= (monto_zp * 0.05):
                        # ¡Bingo! Estos ZRs sumados pagan este ZP
                        for zr_usado in zrs_usados:
                            mapa_zr_a_zps[zr_usado.id] = [zp]
                            zrs_restantes.remove(zr_usado)
                        zps_restantes.remove(zp)
                        break

        # 3. Fallback: Lo que sobre se reparte entre los ZPs que quedaron
        for zr in zrs_restantes:
            mapa_zr_a_zps[zr.id] = zps_restantes if zps_restantes else zps_grupo

    # --- FIN DE PRE-EMPAREJAMIENTO ---

    for zr in balde_solo_zrs:
        # Ahora le preguntamos al mapa pre-calculado a qué ZP(s) pertenece este ZR
        zps_relacionados = mapa_zr_a_zps.get(zr.id, [])

        if not zps_relacionados:
            zrs_auditoria.append(zr)
            continue

        for zp in zps_relacionados:
            zps_procesados.add(zp.id)

        monto_total_zr = abs(float(zr.wsl))
        total_zps_monto = sum(abs(float(zp.wsl)) for zp in zps_relacionados)

        resultados_crudos = []

        # ⚡ CASCADA NIVEL 1: Reparto de Banco (ZR) hacia Pago (ZP)
        for zp in zps_relacionados:
            if total_zps_monto > 0:
                proporcion_zp = abs(float(zp.wsl)) / total_zps_monto
            else:
                proporcion_zp = 1.0 / len(zps_relacionados)

            monto_zr_para_zp = monto_total_zr * proporcion_zp

            facturas_ids = mapa_facturas_por_zp.get(zp.partida.belnr, set())
            facturas_ids = {f for f in facturas_ids if f != zp.partida.belnr}

            if not facturas_ids:
                resultados_crudos.append(
                    {
                        "zp": zp,
                        "factura": "",
                        "prov": "",
                        "cuenta": "SIN_DETALLE_GASTO",
                        "monto": monto_zr_para_zp,
                    }
                )
                continue

            # ⚡ CASCADA NIVEL 2: Aislamiento absoluto de Facturas y Proveedores
            datos_facturas = {}
            suma_gastos_todas_facturas_zp = 0.0

            for f_id in facturas_ids:
                # Agrupar las líneas de esta factura por proveedor
                lineas_por_prov = defaultdict(list)
                monto_por_prov = defaultdict(float)
                proveedores_en_doc = set()

                # 1. Identificar proveedores y asociar líneas de gasto a cada uno
                for p in facturas_agrupadas[f_id]:
                    prov = p.lifnr or p.kunnr or ""
                    if getattr(p, "koart", "") in ("K", "D"):
                        if prov:
                            proveedores_en_doc.add(prov)

                    if (
                        p.ractt not in cuentas_impuestos
                        and p.ractt not in cuentas_dif_cambio
                        and getattr(p, "koart", "") not in ("K", "D")
                    ):
                        monto_gasto = abs(float(p.wsl))
                        lineas_por_prov[prov].append(
                            {"cuenta": p.ractt, "monto": monto_gasto}
                        )
                        monto_por_prov[prov] += monto_gasto

                # 2. Manejo de líneas huérfanas (sin proveedor explícito en la línea)
                if "" in lineas_por_prov and len(proveedores_en_doc) == 1:
                    # Si hay un solo proveedor, le asignamos las líneas huérfanas
                    unico_prov = list(proveedores_en_doc)[0]
                    lineas_por_prov[unico_prov].extend(lineas_por_prov[""])
                    monto_por_prov[unico_prov] += monto_por_prov[""]
                    del lineas_por_prov[""]
                    del monto_por_prov[""]
                elif "" in lineas_por_prov and len(proveedores_en_doc) > 1:
                    # Si hay varios proveedores, repartimos las líneas huérfanas equitativamente
                    lineas_huerfanas = lineas_por_prov.pop("")
                    for lh in lineas_huerfanas:
                        monto_dividido = lh["monto"] / len(proveedores_en_doc)
                        for prov in proveedores_en_doc:
                            lineas_por_prov[prov].append(
                                {"cuenta": lh["cuenta"], "monto": monto_dividido}
                            )
                            monto_por_prov[prov] += monto_dividido
                    del monto_por_prov[""]

                # 3. Fallback: Si un proveedor se quedó sin líneas de gasto, usar dummy
                for prov in proveedores_en_doc:
                    if prov not in lineas_por_prov or not lineas_por_prov[prov]:
                        monto_dummy = (
                            sum(
                                abs(float(p.wsl))
                                for p in facturas_agrupadas[f_id]
                                if getattr(p, "koart", "") in ("K", "D")
                                and (p.lifnr == prov or p.kunnr == prov)
                            )
                            or 1.0
                        )
                        lineas_por_prov[prov].append(
                            {"cuenta": "CUENTA_CONTABLE_ND", "monto": monto_dummy}
                        )
                        monto_por_prov[prov] += monto_dummy

                # 4. Consolidar la data aislada
                for prov, lineas in lineas_por_prov.items():
                    if not prov and proveedores_en_doc:
                        continue  # Ignorar huérfanos si ya se procesaron

                    llave_dt = f"{f_id}_{prov}"
                    datos_facturas[llave_dt] = {
                        "factura_id": f_id,
                        "suma_gastos": monto_por_prov[prov],
                        "lineas": lineas,
                        "prov": prov,
                    }
                    suma_gastos_todas_facturas_zp += monto_por_prov[prov]

            # ⚡ CASCADA NIVEL 3: Distribuir a la Factura y luego a sus Propias Cuentas
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

                # Distribuir estrictamente dentro del contenedor de ESTA factura y ESTE proveedor
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

        # ⚡ AGRUPACIÓN ESTÉTICA: Unimos líneas idénticas para no ensuciar el Dashboard
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
