import os
import sys
import django
from datetime import date

sys.path.append('c:\\Users\\elvis\\Desktop\\DISPONIBILIDAD_BANCARIA')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from sap_sync.services.orchestrator import SAPSyncOrchestrator
from sap_sync.models import SincronizacionLog

log = SincronizacionLog.objects.create(
    tipo="MANUAL",
    estado="EN_CURSO",
    fecha_inicio=date(2023, 1, 1),
    fecha_fin=date(2026, 12, 31)
)

orchestrator = SAPSyncOrchestrator(log)
print("STARTING PASO 9...")
n_entidades = orchestrator._paso9_entidades_contables(date(2023, 1, 1), date(2026, 12, 31))
print("PROCESADAS:", n_entidades)
