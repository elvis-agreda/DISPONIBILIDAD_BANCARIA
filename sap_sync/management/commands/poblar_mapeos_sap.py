from django.core.management.base import BaseCommand

from sap_sync.models import CuentaConfiguracion, MapeoCampo
from users.models import TransaccionSAP  # 👈 IMPORTAMOS EL NUEVO MODELO DE ROLES


class Command(BaseCommand):
    help = "Puebla la base de datos con los mapeos y cuentas iniciales de SAP"

    def handle(self, *args, **kwargs):
        self.stdout.write("Iniciando carga de configuraciones por defecto...")

        # -------------------------------------------------------------------
        # 1. CARGA DE CUENTAS DE CONFIGURACIÓN
        # -------------------------------------------------------------------
        self.stdout.write("Cargando Cuentas de Configuración...")

        cuentas_iniciales = [
            # --- IMPUESTOS ---
            ("117010100", "IMPUESTO", "Impuesto Estándar"),
            ("213010500", "IMPUESTO", "Impuesto Estándar"),
            ("213010600", "IMPUESTO", "Impuesto Estándar"),
            ("213011100", "IMPUESTO", "Impuesto Estándar"),
            ("525010104", "IMPUESTO", "Impuesto Estándar"),
            # --- DIFERENCIA EN CAMBIO ---
            ("411050117", "DIF_CAMBIO", "Diferencia en Cambio"),
            ("526010102", "DIF_CAMBIO", "Diferencia en Cambio"),
            # --- COMISIONES ---
            ("525010103", "COMISION", "Comisión Bancaria"),
        ]

        cuentas_creadas = 0
        for cuenta, tipo, descripcion in cuentas_iniciales:
            obj, created = CuentaConfiguracion.objects.get_or_create(
                cuenta=cuenta,
                defaults={"tipo": tipo, "descripcion": descripcion, "activa": True},
            )
            if created:
                cuentas_creadas += 1

        # -------------------------------------------------------------------
        # 2. CARGA DE MAPEOS DE CAMPOS
        # -------------------------------------------------------------------
        self.stdout.write("Cargando Mapeos de Campos SAP...")

        mapeos_iniciales = [
            # --- MODELO: PARTIDA (CABECERA) ---
            ("Partida", "Bukrs", "bukrs", "TEXTO"),
            ("Partida", "Belnr", "belnr", "TEXTO"),
            ("Partida", "Gjahr", "gjahr", "TEXTO"),
            ("Partida", "Blart", "blart", "TEXTO"),
            ("Partida", "Bktxt", "bktxt", "TEXTO"),
            ("Partida", "Bldat", "bldat", "FECHA"),
            ("Partida", "Budat", "budat", "FECHA"),
            # --- MODELO: PARTIDA POSICION (DETALLE) ---
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
            # --- MODELO: PARTIDA POSICION FILTRO ---
            ("PartidaPosicionFiltro", "Bukrs", "bukrs", "TEXTO"),
            ("PartidaPosicionFiltro", "Docnr", "docnr", "TEXTO"),
            ("PartidaPosicionFiltro", "Ryear", "ryear", "TEXTO"),
            ("PartidaPosicionFiltro", "Docln", "docln", "TEXTO"),
            ("PartidaPosicionFiltro", "Ractt", "ractt", "TEXTO"),
            ("PartidaPosicionFiltro", "Budat", "budat", "FECHA"),
            # --- MODELO: COMPENSACION ---
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
        mapeos_actualizados = 0

        for modelo, campo_sap, campo_django, tipo_dato in mapeos_iniciales:
            obj, created = MapeoCampo.objects.update_or_create(
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
            else:
                mapeos_actualizados += 1

        # -------------------------------------------------------------------
        # 3. CARGA DE ROLES POR TRANSACCIÓN (NUEVO)
        # -------------------------------------------------------------------
        self.stdout.write("Cargando Configuración de Roles por Transacción...")

        transacciones_iniciales = [
            # TCODE, ROL, JERARQUÍA
            ("SE16N", "ADMINISTRADOR", 10),  # Basado en tu captura de Postman
            ("FBL1N", "ANALISTA", 5),  # Basado en tu captura de Postman
            ("FBL3N", "ANALISTA", 5),
            ("VF01", "AUDITOR", 1),  # Basado en tu captura de Postman
            ("FB03", "AUDITOR", 1),
        ]

        transacciones_creadas = 0
        for tcode, rol, jerarquia in transacciones_iniciales:
            obj, created = TransaccionSAP.objects.get_or_create(
                tcode=tcode, defaults={"rol_asociado": rol, "jerarquia": jerarquia}
            )
            if created:
                transacciones_creadas += 1

        # --- MENSAJE FINAL ---
        self.stdout.write(
            self.style.SUCCESS(
                f"Finalizado: {cuentas_creadas} cuentas | "
                f"{mapeos_creados} mapeos creados | "
                f"{transacciones_creadas} transacciones/roles agregados."
            )
        )
