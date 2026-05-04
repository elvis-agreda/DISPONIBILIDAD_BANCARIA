import os

import django

os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings"
django.setup()

from sap_sync.utils.conciliation.calculo import calculo_conciliacion

calculo_conciliacion("2022-04-01", "2022-04-30")
# hkonts_ctas_real = set(SaldoBancario.objects.values_list('hkont', flat=True).distinct())
# prefijos = [str(h)[:-1] for h in hkonts_ctas_real if h]
# cond = reduce(Q.__or__, (Q(ractt__startswith=p) for p in prefijos))
# all_ctas = set(PartidaPosicion.objects.filter(cond).values_list('ractt', flat=True).distinct())
# cuentas_transitorias = all_ctas - hkonts_ctas_real
# prefijos_trans = {str(c)[:-1] for c in cuentas_transitorias if c}
# cuentas_standalone = {c for c in (all_ctas & hkonts_ctas_real) if str(c)[:-1] not in prefijos_trans}
# todas_bancarias = hkonts_ctas_real | cuentas_transitorias | cuentas_standalone
#
# RACCTS_ESP = frozenset(['525010103', '214010100'])
#
# print("=== Checks de cuentas ===")
# for cuenta in ['112010113', '114011000', '526010118', '117010300']:
#    print(f"  {cuenta} in bancarias? {cuenta in todas_bancarias}")
#
# print("\n=== Doc 1400207595: lineas_origen simulado ===")
# for p in PartidaPosicion.objects.filter(partida__belnr='1400207595').values('ractt','wsl','lifnr','kunnr'):
#    incl = p['ractt'] not in RACCTS_ESP and p['ractt'] not in todas_bancarias
#    print(f"  {p['ractt']} wsl={p['wsl']} lifnr={p['lifnr']} kunnr={p['kunnr']} incluido={incl}")
#
## Ahora veamos qué pasa en el segundo salto
# print("\n=== Segundo salto desde 1400207595 ===")
# TIPOS_DOC_BANCARIO = frozenset(["ZR", "ZH"])
# for p in PartidaPosicion.objects.filter(augbl='1400207595').values('ractt','wsl','lifnr','kunnr','partida__belnr','partida__blart'):
#    in_bank = p['partida__blart'] not in TIPOS_DOC_BANCARIO
#    print(f"  belnr={p['partida__belnr']} blart={p['partida__blart']} ractt={p['ractt']} wsl={p['wsl']} kunnr={p['kunnr']} -> es_factura={in_bank}")
#
## Ver si factura 5887041 está en lineas_origen
# print("\n=== Doc 5887041 (factura RV): lineas_origen simulado ===")
# for p in PartidaPosicion.objects.filter(partida__belnr='5887041').values('ractt','wsl','lifnr','kunnr'):
#    incl = p['ractt'] not in RACCTS_ESP and p['ractt'] not in todas_bancarias
#    print(f"  {p['ractt']} wsl={p['wsl']} lifnr={p['lifnr']} kunnr={p['kunnr']} incluido={incl}")
#
