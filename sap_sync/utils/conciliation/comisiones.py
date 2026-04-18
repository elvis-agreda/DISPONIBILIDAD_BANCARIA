# sap_sync/utils/conciliation/comisiones.py
from collections import defaultdict

def procesar_comisiones_bancarias(posiciones, mapa_banco_real, cuentas_comision):
    zrs_con_comision = {
        p.partida.belnr
        for p in posiciones
        if p.partida.blart == "ZR" and p.ractt in cuentas_comision
    }

    comisiones = []
    usados = set()

    pos_por_doc = defaultdict(list)
    for p in posiciones:
        if p.partida.belnr in zrs_con_comision:
            pos_por_doc[p.partida.belnr].append(p)

    for belnr, pos_list in pos_por_doc.items():
        pos_comision = next((p for p in pos_list if p.ractt in cuentas_comision and float(p.wsl) > 0), None)
        if not pos_comision:
            pos_comision = next((p for p in pos_list if p.ractt in cuentas_comision), None)

        if pos_comision:
            cuenta_banco = mapa_banco_real.get(belnr, "")
            comisiones.append({
                "cuenta_banco": cuenta_banco,
                "cuenta_gasto": pos_comision.ractt,
                "monto": abs(float(pos_comision.wsl)),
                "fecha": pos_comision.partida.budat,
                "documento_primario": belnr,
                "referencia": pos_comision.zuonr or "",
                "referencia1": (pos_comision.partida.bktxt or "").strip(),
                "rwcur": pos_comision.rwcur or "",
            })
            usados.add(pos_comision.id)

    restantes = [p for p in posiciones if p.id not in usados]
    return comisiones, restantes