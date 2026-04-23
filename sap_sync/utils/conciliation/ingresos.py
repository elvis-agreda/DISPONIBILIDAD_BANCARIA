# sap_sync/utils/conciliation/ingresos.py
from collections import defaultdict


def procesar_ingresos_bancarios(
    posiciones, cuentas_ingreso, documentos_con_banco, cuentas_standalone
):
    validados = []
    auditoria = []

    # 1. Clasificación inicial
    zrs_banco = []
    pagos_originales = []

    for pos in posiciones:
        if pos.ractt not in cuentas_ingreso:
            continue

        # ⚡ PROTECCIÓN PARA CUENTAS REALES
        if str(pos.ractt).endswith("0") and str(pos.ractt) not in cuentas_standalone:
            continue

        # ⚡ DESCARTAMOS EGRESOS EN CUENTAS STANDALONE ('H' = Haber/Egreso)
        if str(pos.ractt).endswith("0") and pos.drcrk == "H":
            continue

        # Integrado: "XX" como documento de banco junto a ZR y ZH
        if pos.partida.blart in ("ZR", "ZH", "XX"):
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

    # ⚡ PRE-EMPAREJAMIENTO INTELIGENTE Y AGRUPACIÓN N a N
    mapa_zr_a_pagos = defaultdict(list)
    zrs_agrupados = defaultdict(list)

    for zr in zrs_banco:
        clave_grupo = zr.augbl if zr.augbl else zr.partida.belnr
        zrs_agrupados[clave_grupo].append(zr)

    for clave, zrs_grupo in zrs_agrupados.items():
        pagos_grupo = list(mapa_pagos_por_augbl.get(clave, []))

        if (
            clave in mapa_pagos_por_belnr
            and mapa_pagos_por_belnr[clave] not in pagos_grupo
        ):
            pagos_grupo.append(mapa_pagos_por_belnr[clave])

        for zr in zrs_grupo:
            for pago_huerfano in mapa_pagos_por_augbl.get(zr.partida.belnr, []):
                if pago_huerfano not in pagos_grupo:
                    pagos_grupo.append(pago_huerfano)

        if not pagos_grupo:
            for zr in zrs_grupo:
                mapa_zr_a_pagos[zr.id] = []
            continue

        zrs_restantes = list(zrs_grupo)
        pagos_restantes = list(pagos_grupo)

        # 1. Búsqueda de parejas exactas (1 a 1)
        for zr in list(zrs_restantes):
            monto_zr = abs(float(zr.wsl))
            mejor_pago = None
            menor_dif = float("inf")

            for p in pagos_restantes:
                dif = abs(abs(float(p.wsl)) - monto_zr)
                if dif < menor_dif:
                    menor_dif = dif
                    mejor_pago = p

            if mejor_pago and menor_dif <= (monto_zr * 0.05):
                mapa_zr_a_pagos[zr.id] = [mejor_pago]
                pagos_restantes.remove(mejor_pago)
                zrs_restantes.remove(zr)

        # 2. Búsqueda N a 1 (Varios cobros de banco para 1 documento SAP)
        if zrs_restantes and pagos_restantes:
            for p in list(pagos_restantes):
                monto_pago = abs(float(p.wsl))
                zrs_restantes.sort(key=lambda x: abs(float(x.wsl)), reverse=True)

                suma_temp = 0.0
                zrs_usados = []
                for zr in zrs_restantes:
                    suma_temp += abs(float(zr.wsl))
                    zrs_usados.append(zr)

                    if abs(monto_pago - suma_temp) <= (monto_pago * 0.05):
                        for zr_usado in zrs_usados:
                            mapa_zr_a_pagos[zr_usado.id] = [p]
                            zrs_restantes.remove(zr_usado)
                        pagos_restantes.remove(p)
                        break

        # 3. BÚSQUEDA 1 a N (Especial para Tarjetas de Crédito/Débito)
        if zrs_restantes and pagos_restantes:
            for zr in list(zrs_restantes):
                monto_zr = abs(float(zr.wsl))
                pagos_restantes.sort(key=lambda x: abs(float(x.wsl)), reverse=True)

                suma_temp = 0.0
                pagos_usados = []
                for p in pagos_restantes:
                    suma_temp += abs(float(p.wsl))
                    pagos_usados.append(p)

                    if abs(monto_zr - suma_temp) <= (monto_zr * 0.10):
                        mapa_zr_a_pagos[zr.id] = pagos_usados
                        zrs_restantes.remove(zr)
                        for p_usado in pagos_usados:
                            pagos_restantes.remove(p_usado)
                        break

        # 4. Fallback de Confianza
        for zr in zrs_restantes:
            mapa_zr_a_pagos[zr.id] = pagos_restantes if pagos_restantes else pagos_grupo

    # --- FIN DE PRE-EMPAREJAMIENTO ---

    procesados_ids = set()

    # 3. Conciliación centrada en el Extracto
    for zr in zrs_banco:
        relacionados = mapa_zr_a_pagos.get(zr.id, [])

        if not relacionados and not zr.augbl:
            relacionados.extend(mapa_pagos_por_augbl.get(zr.partida.belnr, []))

        relacionados = list({p.id: p for p in relacionados}.values())

        socio_id = zr.kunnr or zr.lifnr or ""

        for p in relacionados:
            procesados_ids.add(p.id)
            if not socio_id:
                socio_id = p.kunnr or p.lifnr or ""

        es_tarjeta = str(zr.ractt).endswith("4")

        if es_tarjeta:
            sub_cat = "TARJETAS"
        elif any(getattr(p.partida, "blart", "") in ["DZ", "DA"] for p in relacionados):
            sub_cat = "COBRANZA"
        else:
            sub_cat = "OTROS INGRESOS"

        # SIEMPRE ABSOLUTO
        monto_final = abs(float(zr.wsl))

        docs_sec = ", ".join(set([p.partida.belnr for p in relacionados]))

        validados.append(
            {
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
                "lifnr": socio_id
                if any(getattr(p, "lifnr", "") for p in relacionados + [zr])
                else "",
                "kunnr": socio_id
                if any(getattr(p, "kunnr", "") for p in relacionados + [zr])
                else "",
            }
        )

    # 4. Pagos "En Tránsito"
    pagos_en_transito_agrupados = defaultdict(list)

    for p in pagos_originales:
        if p.id not in procesados_ids:
            if p.partida.blart not in ["DZ", "DA"]:
                continue

            es_tarjeta = str(p.ractt).endswith("4")

            if es_tarjeta and p.augbl:
                continue

            pagos_en_transito_agrupados[p.partida.belnr].append(p)

    for belnr, posiciones_doc in pagos_en_transito_agrupados.items():
        if belnr not in documentos_con_banco:
            continue

        monto_neto = sum(float(p.wsl) for p in posiciones_doc)

        if abs(monto_neto) < 0.01:
            continue

        p_ref = posiciones_doc[0]
        es_tarjeta = str(p_ref.ractt).endswith("4")
        sub_cat = "TARJETAS" if es_tarjeta else "COBRANZA"

        validados.append(
            {
                "tipo_operacion": "INGRESOS",
                "sub_categoria": sub_cat,
                "cuenta_banco": p_ref.ractt,
                "monto": abs(float(monto_neto)),
                "fecha": p_ref.partida.budat,
                "documento_primario": p_ref.partida.belnr,
                "documento_secundario": "EN TRANSITO",
                "referencia": p_ref.zuonr or "",
                "referencia1": (p_ref.partida.bktxt or "").strip(),
                "rwcur": p_ref.rwcur or "",
                "lifnr": p_ref.lifnr or "",
                "kunnr": p_ref.kunnr or "",
            }
        )

    return validados, auditoria
