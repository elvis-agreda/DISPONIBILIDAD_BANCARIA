# sap_sync/utils/conciliation/gastos.py

class DistribuidorGastos:
    """Se encarga de clasificar las posiciones de una factura en gastos, impuestos y diferencias de cambio."""
    def __init__(self, cuentas_impuestos, cuentas_dif_cambio):
        self.cuentas_impuestos = cuentas_impuestos
        self.cuentas_dif_cambio = cuentas_dif_cambio

    def distribuir(self, posiciones_factura):
        gastos, impuestos, dif_cambio, cxp = [], [], [], []
        total_gasto = total_impuesto = 0.0
        prov = cli = ""

        for pos in posiciones_factura:
            cuenta = pos.ractt or ""
            monto_orig = float(pos.wsl)
            
            if pos.lifnr: prov = pos.lifnr
            if pos.kunnr: cli = pos.kunnr

            if cuenta in self.cuentas_impuestos:
                impuestos.append(pos)
                total_impuesto += monto_orig
            elif getattr(pos, "koart", "") in ("K", "D"):
                cxp.append(pos)
            elif cuenta in self.cuentas_dif_cambio:
                dif_cambio.append(pos)
            else:
                gastos.append(pos)
                total_gasto += abs(monto_orig)

        return gastos, cxp, total_gasto, prov, cli

def conciliar_cadena_zr_zp_facturas(
    balde_solo_zps,
    balde_solo_zrs,
    facturas_agrupadas: dict,
    mapa_factura_zp: dict,
    cuentas_impuestos: set,
    cuentas_dif_cambio: set
) -> tuple[list, list, list]:
    
    resultados = []
    zps_auditoria = []
    zrs_auditoria = []

    distribuidor = DistribuidorGastos(cuentas_impuestos, cuentas_dif_cambio)

    for zp in balde_solo_zps:
        zr_match = next((zr for zr in balde_solo_zrs if zr.augbl == zp.partida.belnr), None)
        
        if not zr_match:
            zps_auditoria.append(zp)
            continue

        facturas = [
            f for f, fp in facturas_agrupadas.items()
            if mapa_factura_zp.get(f) == zp.partida.belnr or any(p.augbl == zp.partida.belnr for p in fp)
        ]

        if not facturas:
            resultados.append(_crear_resultado_base(zr_match, zp, zr_match.wsl, "SIN_FACTURA_ASOCIADA"))
            balde_solo_zrs.remove(zr_match)
            continue

        posiciones_globales = [pos for f in facturas for pos in facturas_agrupadas[f]]
        
        if len(facturas) > 1:
            monto = sum(abs(float(p.wsl)) for p in posiciones_globales if getattr(p, "koart", "") in ("K", "D"))
            if not monto:
                monto = abs(float(zr_match.wsl))
            res = _crear_resultado_base(zr_match, zp, monto, "PAGO_MULTIPLE_AGRUPADO")
            resultados.append(res)
        else:
            gastos, cxp, suma_fac, prov, cli = distribuidor.distribuir(posiciones_globales)
            cuenta_gasto = gastos[0].ractt if gastos else (cxp[0].ractt if cxp else "SIN_CUENTA_GASTO")
            monto_base = suma_fac if suma_fac > 0 else abs(float(zr_match.wsl))
            
            res = _crear_resultado_base(zr_match, zp, monto_base, cuenta_gasto, prov, cli, facturas[0])
            resultados.append(res)
            
        balde_solo_zrs.remove(zr_match)

    zrs_auditoria.extend(balde_solo_zrs)
    return resultados, zps_auditoria, zrs_auditoria

def _crear_resultado_base(zr, zp, monto_base, cuenta_gasto, lifnr="", kunnr="", factura=""):
    return {
        "cuenta_banco": zr.ractt,
        "cuenta_gasto": cuenta_gasto,
        "monto_base": abs(float(monto_base)),
        "monto_total": abs(float(zr.wsl)),
        "fecha_contabilizacion": zr.partida.budat,
        "zr_belnr": zr.partida.belnr,
        "zp_belnr": zp.partida.belnr,
        "factura_belnr": factura,
        "referencia": zr.zuonr or zp.zuonr or "",
        "referencia1": (zr.partida.bktxt or "").strip(),
        "lifnr": lifnr,
        "kunnr": kunnr,
        "rwcur": zr.rwcur or "",
    }