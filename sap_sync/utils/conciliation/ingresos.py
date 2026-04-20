# sap_sync/utils/conciliation/ingresos.py
from collections import defaultdict

def procesar_ingresos_bancarios(posiciones, cuentas_ingreso):
    validados = []
    auditoria = []

    # 1. Clasificación inicial
    zrs_banco = []
    pagos_originales = []

    for pos in posiciones:
        if pos.ractt not in cuentas_ingreso:
            continue
            
        if pos.partida.blart == "ZR":
            zrs_banco.append(pos)
        else:
            pagos_originales.append(pos)

    # 2. Indexación para cruces rápidos
    mapa_pagos_por_augbl = defaultdict(list)
    mapa_pagos_por_belnr = {}

    for p in pagos_originales:
        mapa_pagos_por_belnr[p.partida.belnr] = p
        if p.augbl:
            mapa_pagos_por_augbl[p.augbl].append(p)

    procesados_ids = set()

    # 3. Conciliación centrada en el Extracto (ZR)
    for zr in zrs_banco:
        relacionados = []
        
        # Cruces por compensación
        relacionados.extend(mapa_pagos_por_augbl.get(zr.partida.belnr, []))
        if zr.augbl and zr.augbl in mapa_pagos_por_belnr:
            relacionados.append(mapa_pagos_por_belnr[zr.augbl])
        if zr.augbl:
            relacionados.extend(mapa_pagos_por_augbl.get(zr.augbl, []))

        # Unificar únicos
        relacionados = list({p.id: p for p in relacionados}.values())
        
        socio_id = zr.kunnr or zr.lifnr or ""
        sub_cat = ""

        for p in relacionados:
            procesados_ids.add(p.id)
            # Rescatamos el socio del pago original (enriquecido por el orquestador)
            if not socio_id:
                socio_id = p.kunnr or p.lifnr or ""
            # Regla de Cobranza
            if p.partida.blart in ["DZ", "DA"]:
                sub_cat = "COBRANZA"

        # Monto: Si hay pago, respetamos su signo (positivo), si es ZR puro, absoluto
        monto_final = float(relacionados[0].wsl) if relacionados else abs(float(zr.wsl))
        docs_sec = ", ".join(set([p.partida.belnr for p in relacionados]))

        validados.append({
            "tipo_operacion": "INGRESOS",
            "sub_categoria": sub_cat,
            "cuenta_banco": zr.ractt,
            "monto": monto_final,
            "fecha": zr.partida.budat,
            "documento_primario": zr.partida.belnr,
            "documento_secundario": f"PAGO: {docs_sec}" if docs_sec else "",
            "referencia": zr.zuonr or "",
            "referencia1": (zr.partida.bktxt or "").strip(),
            "rwcur": zr.rwcur or "",
            "lifnr": socio_id if any(p.lifnr for p in relacionados + [zr]) else "",
            "kunnr": socio_id if any(p.kunnr for p in relacionados + [zr]) else "",
        })

    # 4. Pagos "En Tránsito" (Sin ZR todavía)
    for p in pagos_originales:
        if p.id not in procesados_ids:
            validados.append({
                "tipo_operacion": "INGRESOS",
                "sub_categoria": "COBRANZA" if p.partida.blart in ["DZ", "DA"] else "",
                "cuenta_banco": p.ractt,
                "monto": float(p.wsl),
                "fecha": p.partida.budat,
                "documento_primario": p.partida.belnr,
                "documento_secundario": "EN TRANSITO",
                "referencia": p.zuonr or "",
                "referencia1": (p.partida.bktxt or "").strip(),
                "rwcur": p.rwcur or "",
                "lifnr": p.lifnr or "",
                "kunnr": p.kunnr or "",
            })

    return validados, auditoria