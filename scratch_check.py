import os
import sys
import django

sys.path.append('c:\\Users\\elvis\\Desktop\\DISPONIBILIDAD_BANCARIA')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from sap_sync.models import SincronizacionLog

logs = SincronizacionLog.objects.all()[:5]
for log in logs:
    print(f"[{log.iniciado_en}] {log.tipo} - {log.estado}")
