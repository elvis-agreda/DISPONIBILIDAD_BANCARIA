from unittest.mock import patch
from .services.orchestrator import SAPSyncOrchestrator
from .models import SincronizacionLog
from datetime import date
from unittest.mock import MagicMock
from django.test import TestCase
from decimal import Decimal
from .utils.conciliation.ingresos import procesar_ingresos_bancarios

class TestConciliacionIngresos(TestCase):
    def setUp(self):
        """Se ejecuta antes de cada prueba para preparar datos limpios."""
        self.cuentas_ingreso = {"411050117", "411050118"}

    def _crear_posicion_mock(self, cuenta, monto, blart="ZR", fecha="2024-01-01"):
        """Función auxiliar para crear un objeto falso que imite a PartidaPosicion de Django."""
        pos = MagicMock()
        pos.ractt = cuenta
        pos.wsl = Decimal(str(monto))
        pos.rwcur = "VED"
        pos.zuonr = "REF123"
        pos.augbl = "COMP456"
        pos.lifnr = ""
        pos.kunnr = "CLI001"
        
        pos.partida = MagicMock()
        pos.partida.belnr = "DOC789"
        pos.partida.blart = blart
        pos.partida.budat = date.fromisoformat(fecha)
        pos.partida.bktxt = "Texto de Cabecera"
        
        return pos

    def test_ingreso_valido_se_procesa_correctamente(self):
        """Prueba: Un monto positivo en cuenta de ingreso debe validarse."""
        pos1 = self._crear_posicion_mock(cuenta="411050117", monto=500.00)
        posiciones = [pos1]

        validados, auditoria = procesar_ingresos_bancarios(posiciones, self.cuentas_ingreso)

        # Verificaciones (Asserts)
        self.assertEqual(len(validados), 1, "Debería haber 1 ingreso validado")
        self.assertEqual(len(auditoria), 0, "No debería haber auditorías")
        self.assertEqual(validados[0]["monto"], 500.00)
        self.assertEqual(validados[0]["documento_primario"], "DOC789")

   def test_ingreso_negativo_es_validado(self):
        """Prueba: Un monto negativo o cero ya NO debe ser enviado a auditoría."""
        pos_mala = self._crear_posicion_mock(cuenta="411050117", monto=-100.00)
        
        validados, auditoria = procesar_ingresos_bancarios([pos_mala], self.cuentas_ingreso)

        self.assertEqual(len(validados), 1, "Debe validar montos negativos tras la nueva regla")
        self.assertEqual(len(auditoria), 0, "No debe enviarse a auditoría")
        
        # Verificamos que el monto haya ingresado correctamente
        self.assertEqual(validados[0]["monto"], -100.00)

    def test_ignora_cuentas_que_no_son_de_ingreso(self):
        """Prueba: Cuentas de gastos o impuestos no deben ser procesadas aquí."""
        pos_gasto = self._crear_posicion_mock(cuenta="525010103", monto=1000.00) # Cuenta que no está en el set
        
        validados, auditoria = procesar_ingresos_bancarios([pos_gasto], self.cuentas_ingreso)

        self.assertEqual(len(validados), 0)
        self.assertEqual(len(auditoria), 0)

class TestOrquestador(TestCase):
    def setUp(self):
        # Creamos un log real en la base de datos de prueba
        self.log = SincronizacionLog.objects.create(
            tipo="MANUAL", estado="EN_CURSO",
            fecha_inicio=date(2024, 1, 1), fecha_fin=date(2024, 1, 31)
        )
        self.orquestador = SAPSyncOrchestrator(self.log)

    # El patch "secuestra" la función get_data del cliente SAP real
    @patch('sap_sync.services.sap_client.SAPODataClient.get_data')
    def test_paso1_falla_si_sap_devuelve_error(self, mock_get_data):
        """Prueba: Si SAP da error 500 o timeout, el orquestador debe registrar el error."""
        
        # Configuramos nuestra respuesta falsa (Simulamos un error de SAP)
        mock_get_data.return_value = (None, ["Error 500: Internal Server Error en SAP"])

        # Intentamos ejecutar el paso 1 (Saldos Bancarios)
        with self.assertRaises(RuntimeError) as contexto:
            self.orquestador._paso1_saldos_bancarios(anio="2024")

        # Verificamos que el error detuvo el flujo y registró el problema
        self.assertIn("Error fatal en SALDOS_BANCARIOS", str(contexto.exception))
        
        # Verificamos que se guardó en el log
        self.log.refresh_from_db()
        self.assertEqual(self.log.errores_count, 1)