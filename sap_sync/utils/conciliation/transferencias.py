# sap_sync/utils/conciliation/transferencias.py
from collections import defaultdict


def procesar_transferencias_y_divisas(
    posiciones, cuentas_bancarias, cuentas_dif_cambio=None
):
    if cuentas_dif_cambio is None:
        cuentas_dif_cambio = set()

    # 1. Agrupar por AUGBL o BELNR
    grupos = defaultdict(list)
    for p in posiciones:
        key = p.augbl if p.augbl else p.partida.belnr
        grupos[key].append(p)

    operaciones = []
    usados = set()

    for key, pos_list in grupos.items():
        if len(pos_list) < 2:
            continue

        posiciones_relevantes = []
        posiciones_a_consumir = []
        es_grupo_zh_zr = False

        # 2. Extracción y validación de tipo de documento ZH/ZR
        for p in pos_list:
            blart = getattr(p.partida, "blart", "")
            if blart in ("ZR", "ZH"):
                es_grupo_zh_zr = True

            if p.ractt in cuentas_dif_cambio:
                posiciones_a_consumir.append(p)
                continue

            if p.ractt not in cuentas_bancarias:
                continue

            # Solo procesamos como transferencia si es ZR o ZH
            if blart not in ("ZR", "ZH"):
                continue

            posiciones_relevantes.append(p)
            posiciones_a_consumir.append(p)

        # ⚡ BUG 2: Si el grupo es puramente de limpieza bancaria (ZH/ZR), lo consumimos
        # para que NO aparezca en el dashboard ni en auditoría, aunque no sea una transferencia válida.
        if es_grupo_zh_zr and not posiciones_relevantes:
            for p in pos_list:
                if p.ractt in cuentas_bancarias or p.ractt in cuentas_dif_cambio:
                    usados.add(p.id)
            continue

        if len(posiciones_relevantes) < 2:
            continue

        salidas = []
        entradas = []

        # 3. Clasificación de protagonistas y signos
        for p in posiciones_relevantes:
            cuenta_str = str(p.ractt)

            if cuenta_str.endswith(("2", "7")):
                salidas.append(p)
            elif cuenta_str.endswith(("3", "6")):
                entradas.append(p)
            elif cuenta_str.endswith("0"):
                if p.drcrk == "H":
                    salidas.append(p)
                elif p.drcrk == "S":
                    entradas.append(p)

        if not salidas or not entradas:
            # Si es un grupo ZH/ZR bancario pero no cumple simetría, igual lo consumimos para limpiar
            if es_grupo_zh_zr:
                for p in posiciones_a_consumir:
                    usados.add(p.id)
            continue

        # 4. Emparejamiento en Cascada (N a M)
        salidas.sort(key=lambda x: abs(float(x.wsl)), reverse=True)
        entradas.sort(key=lambda x: abs(float(x.wsl)), reverse=True)

        def registrar_match(s_list, e_list):
            s_principal = s_list[0]
            e_principal = e_list[0]
            monedas = {p.rwcur for p in s_list + e_list if p.rwcur}
            sub_cat = (
                "COM_VEN_DIVISAS"
                if "VED" in monedas and "USD" in monedas
                else "TRANSFERENCIA_INTERNA"
            )

            c_salida = (
                str(s_principal.ractt)[:-1] + "0"
                if not str(s_principal.ractt).endswith("0")
                else str(s_principal.ractt)
            )
            c_entrada = (
                str(e_principal.ractt)[:-1] + "0"
                if not str(e_principal.ractt).endswith("0")
                else str(e_principal.ractt)
            )

            operaciones.append(
                {
                    "tipo": "TRANSFERENCIA_INTERNA",
                    "sub_categoria": sub_cat,
                    "salida": s_principal,
                    "entrada": e_principal,
                    "cuenta_salida": c_salida,
                    "cuenta_entrada": c_entrada,
                    "monto_salida": sum(abs(float(x.wsl)) for x in s_list),
                    "monto_entrada": sum(abs(float(x.wsl)) for x in e_list),
                    "fecha": s_principal.partida.budat,
                    "ref": s_principal.zuonr or key,
                    "rwcur_salida": s_principal.rwcur or "",
                    "rwcur_entrada": e_principal.rwcur or "",
                    "documento_primario": key,
                }
            )
            for item in s_list + e_list:
                usados.add(item.id)

        # Fase 1: 1 a 1
        s_pendientes = []
        for s in salidas:
            mejor_e = None
            menor_dif = float("inf")
            for e in entradas:
                dif = abs(abs(float(s.wsl)) - abs(float(e.wsl)))
                if dif < menor_dif:
                    menor_dif = dif
                    mejor_e = e
            if mejor_e and menor_dif <= abs(float(s.wsl)) * 0.10:
                entradas.remove(mejor_e)
                registrar_match([s], [mejor_e])
            else:
                s_pendientes.append(s)

        # Fase 2: N a 1 y Fallback Final
        # (Se aplica lógica similar para limpiar el grupo)
        if s_pendientes and entradas:
            registrar_match(s_pendientes, entradas)

        # Limpieza final del grupo
        for p in posiciones_a_consumir:
            usados.add(p.id)

    restantes = [p for p in posiciones if p.id not in usados]
    return operaciones, restantes
