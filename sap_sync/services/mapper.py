# sap_sync/services/mapper.py

import logging
from decimal import Decimal
from sap_sync.models import MapeoCampo
from sap_sync.utils.utils import sap_date_to_python

logger = logging.getLogger(__name__)

class GeneradorDinamicoSAP:
    """
    Motor que transforma diccionarios crudos de SAP en kwargs listos
    para ser inyectados en los modelos de Django, basándose en reglas de BD.
    """
    
    def __init__(self, nombre_modelo):
        # Carga las reglas activas de la BD a la memoria (solo 1 query por ejecución)
        self.reglas = list(MapeoCampo.objects.filter(modelo_destino=nombre_modelo, activo=True))
        if not self.reglas:
            logger.warning(f"No hay reglas de mapeo configuradas para el modelo {nombre_modelo}.")

    def _convertir_valor(self, valor_sap, tipo_dato):
        """Transforma el dato de SAP a un tipo de Python seguro."""
        if valor_sap is None or str(valor_sap).strip() == "":
            return Decimal("0.00") if tipo_dato == 'DECIMAL' else None

        try:
            if tipo_dato == 'TEXTO':
                return str(valor_sap).strip()
            
            elif tipo_dato == 'FECHA':
                return sap_date_to_python(str(valor_sap))
                
            elif tipo_dato == 'DECIMAL':
                return Decimal(str(valor_sap).replace(',', ''))
                
            elif tipo_dato == 'ENTERO':
                return int(valor_sap)
                
            elif tipo_dato == 'BOOLEANO':
                return str(valor_sap).strip().upper() in ('X', 'TRUE', '1')
                
        except (ValueError, TypeError) as e:
            logger.error(f"Error convirtiendo '{valor_sap}' a {tipo_dato}: {e}")
            return Decimal("0.00") if tipo_dato == 'DECIMAL' else None
            
        return valor_sap

    def construir_kwargs(self, registro_sap):
        """
        Recibe: {"Bukrs": "1000", "Wsl": "150.50"}
        Devuelve: {"bukrs": "1000", "wsl": Decimal("150.50")}
        """
        datos_limpios = {}
        for regla in self.reglas:
            valor_crudo = registro_sap.get(regla.campo_sap)
            datos_limpios[regla.campo_django] = self._convertir_valor(valor_crudo, regla.tipo_dato)
            
        return datos_limpios