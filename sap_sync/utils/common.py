# sap_sync/utils/common.py

import re
from datetime import date, datetime

def sap_date_to_python(date_str) -> date | None:
    """
    Convierte múltiples formatos de fecha provenientes de SAP a un objeto date de Python.
    """
    if not date_str or str(date_str) == "00000000":
        return None
        
    date_str = str(date_str).strip()
    
    # Formato 1: OData v2 Timestamp (Ej: /Date(1643673600000)/)
    if date_str.startswith("/Date(") and date_str.endswith(")/"):
        try:
            # Extraer solo los milisegundos numéricos
            match = re.search(r'\d+', date_str)
            if match:
                ms = int(match.group())
                return date.fromtimestamp(ms / 1000.0)
        except (ValueError, AttributeError, OSError):
            return None

    # Formato 2: Estándar ISO con guiones (Ej: 2024-01-31 o 2024-01-31T00:00:00)
    if "-" in date_str:
        try:
            # Separamos en la 'T' por si viene con horas y tomamos solo la fecha
            return datetime.fromisoformat(date_str.split("T")[0]).date()
        except ValueError:
            pass

    # Formato 3: String ABAP clásico (Ej: 20240131)
    if len(date_str) >= 8 and date_str[:8].isdigit():
        try:
            return date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
        except ValueError:
            return None

    return None