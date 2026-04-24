from django.core.management.base import BaseCommand

from core.models import ClasificacionGasto, ColumnaDrillDown
from sap_sync.models import CuentaConfiguracion, MapeoCampo
from users.models import TransaccionSAP


class Command(BaseCommand):
    help = "Puebla la base de datos con los mapeos, cuentas, jerarquías y drill-down sin sobreescribir."

    def handle(self, *args, **kwargs):
        self.stdout.write("Iniciando carga de configuraciones por defecto...")

        # -------------------------------------------------------------------
        # 1. CARGA DE CUENTAS DE CONFIGURACIÓN
        # -------------------------------------------------------------------
        self.stdout.write("Cargando Cuentas de Configuración...")

        cuentas_iniciales = [
            ("117010100", "IMPUESTO", "I.V.A CREDITO FISCAL"),
            ("213010400", "IMPUESTO", "ISLR RET A EMPLEADOS"),
            ("213010500", "IMPUESTO", "ISLR RET A TERCEROS"),
            ("213010600", "IMPUESTO", "RETENCION IVA A TERCEROS"),
            ("213011100", "IMPUESTO", "PERCEPCION IGTF"),
            ("525010104", "IMPUESTO", "IMPUESTOS TRANSACCIONES FINANCIERAS"),
            ("411050117", "DIF_CAMBIO", "GANANCIA EN CAMBIO"),
            ("526010102", "DIF_CAMBIO", "PERDIDA EN CAMBIO"),
            ("525010103", "COMISION", "COMISIONES BANCARIAS"),
        ]

        cuentas_creadas = 0
        for cuenta, tipo, descripcion in cuentas_iniciales:
            # ⚡ FIX: Solo crea, nunca actualiza
            obj, created = CuentaConfiguracion.objects.get_or_create(
                cuenta=cuenta,
                defaults={"tipo": tipo, "descripcion": descripcion, "activa": True},
            )
            if created:
                cuentas_creadas += 1

        # -------------------------------------------------------------------
        # 1.5 CARGA DE MAPEOS DE GASTOS (DRILL-DOWN JERÁRQUICO)
        # -------------------------------------------------------------------
        self.stdout.write("Cargando Mapeos de Gastos (Drill-Down)...")

        gastos_iniciales = [
            ("514010101", "HONORARIOS MEDICOS", "Honorarios medicos y tecnicos"),
            ("514010100", "HONORARIOS MEDICOS", "Honorarios Profesionales"),
            ("513010105", "HONORARIOS MEDICOS", "Equipos Menores"),
            ("513010103", "HONORARIOS MEDICOS", "MATERIAL Y ARTICULO DE LIMPIEZA"),
            ("114010200", "REINTEGROS", "Reintegros"),
            ("SIN_DETALLE_GASTO", "NO CLASIFICADO", "Movimiento de Banco sin Desglose"),
        ]

        gastos_creados = 0
        for cuenta, cat, sub_cat in gastos_iniciales:
            #  Usamos get_or_create para no sobreescribir tus cambios en admin
            obj, created = ClasificacionGasto.objects.get_or_create(
                cuenta_gasto=cuenta,
                defaults={"categoria": cat, "sub_categoria": sub_cat},
            )
            if created:
                gastos_creados += 1

        # -------------------------------------------------------------------
        # 2. CARGA DE MAPEOS DE CAMPOS
        # -------------------------------------------------------------------
        self.stdout.write("Cargando Mapeos de Campos SAP...")

        mapeos_iniciales = [
            ("Partida", "Bukrs", "bukrs", "TEXTO"),
            ("Partida", "Belnr", "belnr", "TEXTO"),
            ("Partida", "Gjahr", "gjahr", "TEXTO"),
            ("Partida", "Blart", "blart", "TEXTO"),
            ("Partida", "Bktxt", "bktxt", "TEXTO"),
            ("Partida", "Bldat", "bldat", "FECHA"),
            ("Partida", "Budat", "budat", "FECHA"),
            ("PartidaPosicion", "Bukrs", "bukrs", "TEXTO"),
            ("PartidaPosicion", "Docnr", "docnr", "TEXTO"),
            ("PartidaPosicion", "Ryear", "ryear", "TEXTO"),
            ("PartidaPosicion", "Docln", "docln", "TEXTO"),
            ("PartidaPosicion", "Ractt", "ractt", "TEXTO"),
            ("PartidaPosicion", "Wsl", "wsl", "DECIMAL"),
            ("PartidaPosicion", "Drcrk", "drcrk", "TEXTO"),
            ("PartidaPosicion", "Rwcur", "rwcur", "TEXTO"),
            ("PartidaPosicion", "Lifnr", "lifnr", "TEXTO"),
            ("PartidaPosicion", "Kunnr", "kunnr", "TEXTO"),
            ("PartidaPosicion", "Koart", "koart", "TEXTO"),
            ("PartidaPosicion", "Augbl", "augbl", "TEXTO"),
            ("PartidaPosicion", "Zuonr", "zuonr", "TEXTO"),
            ("PartidaPosicion", "Budat", "budat", "FECHA"),
            ("PartidaPosicionFiltro", "Bukrs", "bukrs", "TEXTO"),
            ("PartidaPosicionFiltro", "Docnr", "docnr", "TEXTO"),
            ("PartidaPosicionFiltro", "Ryear", "ryear", "TEXTO"),
            ("PartidaPosicionFiltro", "Docln", "docln", "TEXTO"),
            ("PartidaPosicionFiltro", "Ractt", "ractt", "TEXTO"),
            ("PartidaPosicionFiltro", "Budat", "budat", "FECHA"),
            ("Compensacion", "Bukrs", "bukrs", "TEXTO"),
            ("Compensacion", "Belnr", "belnr", "TEXTO"),
            ("Compensacion", "Gjahr", "gjahr", "TEXTO"),
            ("Compensacion", "Buzei", "buzei", "TEXTO"),
            ("Compensacion", "Shkzg", "shkzg", "TEXTO"),
            ("Compensacion", "Dmbtr", "dmbtr", "DECIMAL"),
            ("Compensacion", "Wrbtr", "wrbtr", "DECIMAL"),
            ("Compensacion", "Pswbt", "pswbt", "DECIMAL"),
            ("Compensacion", "Pswsl", "pswsl", "TEXTO"),
            ("Compensacion", "Zuonr", "zuonr", "TEXTO"),
            ("Compensacion", "Sgtxt", "sgtxt", "TEXTO"),
            ("Compensacion", "Saknr", "saknr", "TEXTO"),
            ("Compensacion", "Hkont", "hkont", "TEXTO"),
            ("Compensacion", "Kunnr", "kunnr", "TEXTO"),
            ("Compensacion", "Lifnr", "lifnr", "TEXTO"),
            ("Compensacion", "Augdt", "augdt", "FECHA"),
            ("Compensacion", "Augcp", "augcp", "FECHA"),
            ("Compensacion", "Augbl", "augbl", "TEXTO"),
            ("Compensacion", "Bschl", "bschl", "TEXTO"),
            ("Compensacion", "Koart", "koart", "TEXTO"),
        ]

        mapeos_creados = 0
        for modelo, campo_sap, campo_django, tipo_dato in mapeos_iniciales:
            # ⚡ FIX: Cambiado a get_or_create para no pisar si lo desactivas en admin
            obj, created = MapeoCampo.objects.get_or_create(
                modelo_destino=modelo,
                campo_sap=campo_sap,
                defaults={
                    "campo_django": campo_django,
                    "tipo_dato": tipo_dato,
                    "activo": True,
                },
            )
            if created:
                mapeos_creados += 1

        # -------------------------------------------------------------------
        # 3. CARGA DE ROLES POR TRANSACCIÓN
        # -------------------------------------------------------------------
        self.stdout.write("Cargando Configuración de Roles por Transacción...")

        transacciones_iniciales = [
            ("SE16N", "ADMINISTRADOR", 10),
            ("FBL1N", "ANALISTA", 5),
            ("FBL3N", "ANALISTA", 5),
            ("VF01", "AUDITOR", 1),
            ("FB03", "AUDITOR", 1),
        ]

        transacciones_creadas = 0
        for tcode, rol, jerarquia in transacciones_iniciales:
            obj, created = TransaccionSAP.objects.get_or_create(
                tcode=tcode, defaults={"rol_asociado": rol, "jerarquia": jerarquia}
            )
            if created:
                transacciones_creadas += 1

        # -------------------------------------------------------------------
        # 4. CARGA DE COLUMNAS PARA EL DRILL-DOWN (NUEVO)
        # -------------------------------------------------------------------
        self.stdout.write("Cargando Columnas del Modal Drill-Down...")

        columnas_drilldown = [
            # campo_bd, etiqueta, tipo, orden, es_buscable, abre_documento
            ("documento_primario", "Doc. Banco (ZR)", "TEXTO", 1, True, True),
            ("documento_secundario", "Doc. Origen (ZP/Fac)", "TEXTO", 2, True, True),
            ("fecha_contabilizacion", "Fecha", "FECHA", 3, False, False),
            ("cuenta_contable", "Cuenta Banco", "TEXTO", 4, True, False),
            ("cuenta_gasto", "Cuenta Gasto", "TEXTO", 5, True, False),
            ("lifnr", "Socio (LIFNR/KUNNR)", "TEXTO", 6, True, False),
            ("referencia", "Referencia", "TEXTO", 7, True, False),
            ("monto_total", "Monto", "MONTO", 8, False, False),
        ]

        columnas_creadas = 0
        for campo, etiqueta, tipo, orden, buscable, abre in columnas_drilldown:
            obj, created = ColumnaDrillDown.objects.get_or_create(
                campo_bd=campo,
                defaults={
                    "etiqueta": etiqueta,
                    "tipo_dato": tipo,
                    "orden": orden,
                    "es_buscable": buscable,
                    "abre_documento": abre,
                    "activo": True,
                },
            )
            if created:
                columnas_creadas += 1

        # --- MENSAJE FINAL ---
        self.stdout.write(
            self.style.SUCCESS(
                f"Finalizado: {cuentas_creadas} cuentas | "
                f"{gastos_creados} Mapeos de Gasto | "
                f"{mapeos_creados} mapeos SAP | "
                f"{transacciones_creadas} roles | "
                f"{columnas_creadas} columnas drill-down."
            )
        )
