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
    cuentas_dif_cambio: set
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

    for zr in balde_solo_zrs:
        zps_relacionados = []
        
        # Vincular ZPs al ZR
        zps_relacionados.extend(mapa_zps_por_augbl.get(zr.partida.belnr, []))
        if zr.augbl and zr.augbl in mapa_zps_por_belnr:
            zps_relacionados.append(mapa_zps_por_belnr[zr.augbl])
        if zr.augbl:
            zps_relacionados.extend(mapa_zps_por_augbl.get(zr.augbl, []))

        zps_relacionados = list({zp.id: zp for zp in zps_relacionados}.values())

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
                resultados_crudos.append({
                    "zp": zp, "factura": "", "prov": "", 
                    "cuenta": "SIN_DETALLE_GASTO", "monto": monto_zr_para_zp
                })
                continue

            # ⚡ CASCADA NIVEL 2: Aislamiento absoluto de Facturas
            datos_facturas = {}
            suma_gastos_todas_facturas_zp = 0.0

            for f_id in facturas_ids:
                prov_factura = ""
                lineas_gasto_factura = []
                suma_gastos_factura = 0.0

                # 1. Obtener proveedor y recolectar las líneas exclusivas de esta factura
                for p in facturas_agrupadas[f_id]:
                    if p.lifnr: prov_factura = p.lifnr
                    elif p.kunnr and not prov_factura: prov_factura = p.kunnr
                    
                    if p.ractt not in cuentas_impuestos and p.ractt not in cuentas_dif_cambio and getattr(p, "koart", "") not in ("K", "D"):
                        monto_gasto = abs(float(p.wsl))
                        suma_gastos_factura += monto_gasto
                        lineas_gasto_factura.append({
                            "cuenta": p.ractt,
                            "monto": monto_gasto
                        })

                # Fallback: Si no tiene líneas de gasto legibles, usamos un dummy para no perder el dinero
                if not lineas_gasto_factura:
                    monto_dummy = sum(abs(float(p.wsl)) for p in facturas_agrupadas[f_id] if getattr(p, "koart", "") in ("K", "D")) or 1.0
                    suma_gastos_factura = monto_dummy
                    lineas_gasto_factura.append({"cuenta": "CUENTA_CONTABLE_ND", "monto": monto_dummy})

                datos_facturas[f_id] = {
                    "suma_gastos": suma_gastos_factura,
                    "lineas": lineas_gasto_factura,
                    "prov": prov_factura
                }
                suma_gastos_todas_facturas_zp += suma_gastos_factura

            # ⚡ CASCADA NIVEL 3: Distribuir a la Factura y luego a sus Propias Cuentas
            for f_id, f_data in datos_facturas.items():
                if suma_gastos_todas_facturas_zp > 0:
                    proporcion_factura = f_data["suma_gastos"] / suma_gastos_todas_facturas_zp
                else:
                    proporcion_factura = 1.0 / len(datos_facturas)

                monto_zr_para_factura = monto_zr_para_zp * proporcion_factura

                if monto_zr_para_factura <= 0:
                    continue

                # Distribuir estrictamente dentro del contenedor de ESTA factura
                for linea in f_data["lineas"]:
                    if f_data["suma_gastos"] > 0:
                        proporcion_cuenta = linea["monto"] / f_data["suma_gastos"]
                    else:
                        proporcion_cuenta = 1.0 / len(f_data["lineas"])

                    monto_final_cuenta = monto_zr_para_factura * proporcion_cuenta

                    resultados_crudos.append({
                        "zp": zp,
                        "factura": f_id,
                        "prov": f_data["prov"],
                        "cuenta": linea["cuenta"],
                        "monto": monto_final_cuenta
                    })

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
                    "proveedor": r["prov"]
                }
                
            agrupados[llave]["monto"] += r["monto"]
            if r["factura"]:
                agrupados[llave]["facturas"].add(r["factura"])

        for data in agrupados.values():
            facturas_str = ", ".join(sorted(list(data["facturas"])))
            resultados.append(_generar_fila_dashboard(
                zr=data["zr"],
                zp=data["zp"],
                monto=data["monto"],
                cuenta_gasto=data["cuenta_gasto"],
                tipo="EGRESOS",
                proveedor=data["proveedor"],
                factura=facturas_str
            ))

    zps_auditoria = [zp for zp in balde_solo_zps if zp.id not in zps_procesados]

    return resultados, zps_auditoria, zrs_auditoria

def _generar_fila_dashboard(zr, zp, monto, cuenta_gasto, tipo, proveedor="", factura=""):
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