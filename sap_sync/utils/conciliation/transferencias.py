# sap_sync/utils/conciliation/transferencias.py
from collections import defaultdict

def procesar_transferencias_y_divisas(posiciones, cuentas_bancarias):
    """Filtra y agrupa las posiciones que corresponden a transferencias entre cuentas propias."""
    grupos_belnr = defaultdict(list)
    for p in posiciones:
        grupos_belnr[p.partida.belnr].append(p)

    operaciones = []
    usados = set()

    for belnr, pos_list in grupos_belnr.items():
        if len(pos_list) < 2:
            continue

        candidatos = [p for p in pos_list if p.ractt in cuentas_bancarias]
        if len(candidatos) < 2:
            continue

        salidas = [p for p in candidatos if float(p.wsl) < 0]
        entradas = [p for p in candidatos if float(p.wsl) > 0]

        if salidas and entradas:
            salida = salidas[0]
            entrada = entradas[0]
            
            operaciones.append({
                "tipo": 'TRANSFERENCIA_INTERNA',
                "salida": salida,
                "entrada": entrada,
                "cuenta_salida": salida.ractt,
                "cuenta_entrada": entrada.ractt,
                "monto_salida": abs(float(salida.wsl)),
                "monto_entrada": abs(float(entrada.wsl)),
                "fecha": salida.partida.budat,
                "ref": salida.zuonr or "",
                "rwcur_salida": salida.rwcur or "",
                "rwcur_entrada": entrada.rwcur or "",
            })
            usados.update(p.id for p in candidatos)

    restantes = [p for p in posiciones if p.id not in usados]
    return operaciones, restantes