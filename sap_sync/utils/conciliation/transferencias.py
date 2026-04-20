# sap_sync/utils/conciliation/transferencias.py
from collections import defaultdict


def procesar_transferencias_y_divisas(
    posiciones, cuentas_bancarias, cuentas_dif_cambio=None
):
    if cuentas_dif_cambio is None:
        cuentas_dif_cambio = set()

    # 1. Agrupar por AUGBL o BELNR (Para atrapar las compensadas y las directas en tránsito)
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

        # 2. Extracción exclusiva de bancos
        for p in pos_list:
            if p.ractt in cuentas_dif_cambio:
                posiciones_a_consumir.append(p)
                continue

            if p.ractt not in cuentas_bancarias:
                continue

            # ⚡ En lugar de rechazar TODO el grupo, simplemente ignoramos las líneas que no sean ZR.
            # Esto evita que un documento cruzado accidentalmente dañe la transferencia.
            if getattr(p.partida, "blart", "") != "ZR":
                continue

            posiciones_relevantes.append(p)
            posiciones_a_consumir.append(p)

        if len(posiciones_relevantes) < 2:
            continue

        salidas = []
        entradas = []

        # 3. ⚡ CORRECCIÓN DE SIGNOS PARA SUBCUENTAS
        for p in posiciones_relevantes:
            monto = float(p.wsl)
            cuenta_str = str(p.ractt)

            # Positivo en subcuenta 2 o 7 = Salida de banco real
            if monto > 0 and cuenta_str.endswith(("2", "7")):
                salidas.append(p)
            # Negativo en subcuenta 3 o 6 = Entrada a banco real
            elif monto < 0 and cuenta_str.endswith(("3", "6")):
                entradas.append(p)

        if not salidas or not entradas:
            continue

        # 4. EMPAREJAMIENTO 1 a 1 (Soporte N transferencias)
        salidas.sort(key=lambda x: abs(float(x.wsl)), reverse=True)
        entradas.sort(key=lambda x: abs(float(x.wsl)), reverse=True)

        for s in salidas:
            if not entradas:
                break

            mejor_e = None
            menor_dif = float("inf")

            for e in entradas:
                dif = abs(abs(float(s.wsl)) - abs(float(e.wsl)))
                if dif < menor_dif:
                    menor_dif = dif
                    mejor_e = e

            if mejor_e:
                if menor_dif <= abs(float(s.wsl)) * 0.10:
                    entradas.remove(mejor_e)

                    monedas = {s.rwcur, mejor_e.rwcur}
                    sub_cat = (
                        "COM_VEN_DIVISAS"
                        if "VED" in monedas and "USD" in monedas
                        else "TRANSFERENCIA_INTERNA"
                    )

                    # FORZAR CUENTA REAL: Tomamos la subcuenta y le ponemos un 0 al final
                    cuenta_real_salida = str(s.ractt)[:-1] + "0"
                    cuenta_real_entrada = str(mejor_e.ractt)[:-1] + "0"

                    operaciones.append(
                        {
                            "tipo": "TRANSFERENCIA_INTERNA",
                            "sub_categoria": sub_cat,
                            "salida": s,
                            "entrada": mejor_e,
                            "cuenta_salida": cuenta_real_salida,
                            "cuenta_entrada": cuenta_real_entrada,
                            "monto_salida": abs(float(s.wsl)),
                            "monto_entrada": abs(float(mejor_e.wsl)),
                            "fecha": s.partida.budat,
                            "ref": s.zuonr or key,
                            "rwcur_salida": s.rwcur or "",
                            "rwcur_entrada": mejor_e.rwcur or "",
                            "documento_primario": key,
                        }
                    )

                    usados.add(s.id)
                    usados.add(mejor_e.id)

        # 5. Limpiamos solo las cuentas transitorias de los documentos que procesamos exitosamente
        belnrs_exitosos = {s.partida.belnr for s in salidas if s.id in usados}.union(
            {e.partida.belnr for e in entradas if e.id in usados}
        )

        for p in posiciones_a_consumir:
            if p.partida.belnr in belnrs_exitosos:
                usados.add(p.id)

    restantes = [p for p in posiciones if p.id not in usados]
    return operaciones, restantes
