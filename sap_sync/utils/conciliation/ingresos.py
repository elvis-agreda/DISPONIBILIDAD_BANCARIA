# sap_sync/utils/conciliation/ingresos.py

def procesar_ingresos_bancarios(posiciones, cuentas_ingreso):
    validados = []
    auditoria = []

    for pos in posiciones:
        if pos.ractt not in cuentas_ingreso:
            continue

        monto = float(pos.wsl)
        if monto <= 0:
            auditoria.append((pos, f"Monto negativo o cero en cuenta de ingreso: {monto}"))
            continue

        validados.append({
            "cuenta": pos.ractt,
            "monto": monto,
            "fecha": pos.partida.budat,
            "documento_primario": pos.partida.belnr,
            "documento_secundario": pos.augbl or "",
            "referencia": pos.zuonr or "",
            "referencia1": (pos.partida.bktxt or "").strip(),
            "rwcur": pos.rwcur or "",
            "lifnr": pos.lifnr or "",
            "kunnr": pos.kunnr or "",
        })

    return validados, auditoria