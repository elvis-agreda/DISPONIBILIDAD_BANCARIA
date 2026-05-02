import os
import sys
import django

sys.path.append('c:\\Users\\elvis\\Desktop\\DISPONIBILIDAD_BANCARIA')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from sap_sync.services.sap_client import SAPODataClient, SAPServiceURL

client = SAPODataClient(base_url=SAPServiceURL.ENTIDADES)

codigos = [str(100050 + i) for i in range(25)]
filtro = " or ".join([f"Codigo eq '{cod}'" for cod in codigos])

print("Testing Batch with 25 ORs...")
res2, err2 = client.execute_batch(
    "ZFI_ACREEDORES_DEUDORES",
    raw_filters=[filtro]
)
print("BATCH ERROR:", err2)
if not err2:
    print("SUCCESS!")
