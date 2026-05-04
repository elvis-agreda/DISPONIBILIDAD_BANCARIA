import os
import django

os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings"
django.setup()

from sap_sync.models import PartidaPosicion

docs = ["2001002696", "2001002710"]
partidas = PartidaPosicion.objects.filter(docnr__in=docs).values("docnr", "ractt", "drcrk", "wsl")
for p in partidas:
    print(p)
