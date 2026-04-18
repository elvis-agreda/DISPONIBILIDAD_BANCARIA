from decimal import Decimal

from django.db import models


class SincronizacionLog(models.Model):
    """
    Controla el estado de las tareas asíncronas de Huey para los 8 pasos de SAP.
    """

    TIPO_CHOICES = [
        ("AUTO", "Automática (Crontab)"),
        ("MANUAL", "Manual (Usuario)"),
    ]
    ESTADO_CHOICES = [
        ("INICIADO", "Iniciado"),
        ("EN_CURSO", "En Curso"),
        ("EXITOSO", "Exitoso"),
        ("PARCIAL", "Completado con Errores"),
        ("FALLIDO", "Fallido"),
        ("CANCELADO", "Cancelado"),
    ]

    tipo = models.CharField(
        "Tipo de Ejecución", max_length=10, choices=TIPO_CHOICES, default="MANUAL"
    )
    estado = models.CharField(
        "Estado", max_length=15, choices=ESTADO_CHOICES, default="INICIADO"
    )

    fecha_inicio = models.DateField("Fecha de Inicio (Filtro SAP)")
    fecha_fin = models.DateField("Fecha de Fin (Filtro SAP)")
    anio = models.CharField("Año Fiscal", max_length=4, blank=True, null=True)
    periodo = models.CharField("Periodo", max_length=2, blank=True, null=True)

    # Contadores de la sincronización (Actualizados por tasks.py)
    saldos_creados = models.IntegerField(default=0)
    saldos_actualizados = models.IntegerField(default=0)
    partidas_creadas = models.IntegerField(default=0)
    partidas_actualizadas = models.IntegerField(default=0)
    compensaciones_proc = models.IntegerField(default=0)
    tasas_procesadas = models.IntegerField(default=0)

    errores_count = models.IntegerField("Cantidad de Errores", default=0)
    progreso_detalle = models.JSONField(
        "Detalle por Paso", default=dict, blank=True, null=True
    )

    iniciado_en = models.DateTimeField("Iniciado el", auto_now_add=True)
    finalizado_en = models.DateTimeField("Finalizado el", blank=True, null=True)

    class Meta:
        verbose_name = "Log de Sincronización"
        verbose_name_plural = "Logs de Sincronización"
        ordering = ["-iniciado_en"]

    def __str__(self):
        # Convertimos la tupla de opciones en un diccionario y buscamos el texto
        estado_texto = dict(self.ESTADO_CHOICES).get(self.estado, self.estado)
        return f"{self.tipo} - {estado_texto} ({self.fecha_inicio} a {self.fecha_fin})"

    def marcar_finalizado(self, estado_final):
        from django.utils import timezone

        self.estado = estado_final
        self.finalizado_en = timezone.now()
        self.save(update_fields=["estado", "finalizado_en"])

    def verificar_cancelacion(self):
        self.refresh_from_db(fields=["estado"])
        if self.estado == "CANCELADO":
            raise InterruptedError("La sincronización fue cancelada por el usuario.")

    def registrar_error(self, paso, mensaje, contexto=None):
        from django.utils import timezone

        self.errores_count += 1

        if self.progreso_detalle is None:
            self.progreso_detalle = {}

        paso_key = f"paso{paso}" if isinstance(paso, int) else paso
        if paso_key not in self.progreso_detalle:
            self.progreso_detalle[paso_key] = {"errores": []}

        lista_errores = self.progreso_detalle[paso_key].get("errores", [])
        lista_errores.append(
            {
                "hora": timezone.now().strftime("%H:%M:%S"),
                "mensaje": str(mensaje),
                "contexto": contexto or {},
            }
        )
        self.progreso_detalle[paso_key]["errores"] = lista_errores

        self.save(update_fields=["errores_count", "progreso_detalle"])
        print(f"[{paso_key.upper()}] ERROR: {mensaje}")

    def registrar_inicio_paso(self, paso_id, nombre):
        from django.utils import timezone

        if self.progreso_detalle is None:
            self.progreso_detalle = {}

        self.progreso_detalle[paso_id] = {
            "nombre": nombre,
            "estado": "EN_CURSO",
            "iniciado_en": timezone.now().isoformat(),
            "mensajes": [],
            "metricas": {},
            "errores": [],
        }
        self.save(update_fields=["progreso_detalle"])

    def actualizar_progreso_paso(self, paso_id, mensaje):
        from django.utils import timezone

        if self.progreso_detalle is None:
            self.progreso_detalle = {}

        if paso_id not in self.progreso_detalle:
            self.progreso_detalle[paso_id] = {"mensajes": []}

        # Agregamos la hora al mensaje
        timestamp = timezone.now().strftime("%H:%M:%S")
        mensajes = self.progreso_detalle[paso_id].get("mensajes", [])
        mensajes.append(f"[{timestamp}] {mensaje}")

        # Mantener solo los últimos 10 mensajes para que el JSON no explote la BD
        self.progreso_detalle[paso_id]["mensajes"] = mensajes[-10:]

        self.save(update_fields=["progreso_detalle"])

    def registrar_fin_paso(self, paso_id, metricas, estado):
        from django.utils import timezone

        if self.progreso_detalle is None:
            self.progreso_detalle = {}

        if paso_id not in self.progreso_detalle:
            self.progreso_detalle[paso_id] = {}

        self.progreso_detalle[paso_id].update(
            {
                "estado": estado,
                "metricas": metricas,
                "finalizado_en": timezone.now().isoformat(),
            }
        )
        self.save(update_fields=["progreso_detalle"])


class Partida(models.Model):
    """
    Cabecera de los documentos contables extraídos de SAP (ZFI_PARTIDAS).
    """

    bukrs = models.CharField("Sociedad", max_length=4, db_index=True)
    belnr = models.CharField("Documento", max_length=10, db_index=True)
    gjahr = models.CharField("Ejercicio", max_length=4, db_index=True)

    blart = models.CharField("Clave Doc.", max_length=2, db_index=True)
    bktxt = models.CharField("Texto Cabecera", max_length=255, blank=True, null=True)
    bldat = models.DateField("Fecha Documento", blank=True, null=True)
    budat = models.DateField("Fecha Contabilización", db_index=True)

    class Meta:
        verbose_name = "Partida (Cabecera)"
        verbose_name_plural = "Partidas (Cabeceras)"
        unique_together = ("bukrs", "belnr", "gjahr")

    def __str__(self):
        return f"{self.blart} - {self.belnr} ({self.gjahr})"


class PartidaPosicion(models.Model):
    """
    Líneas de detalle de los documentos contables.
    Relacionada a Partida mediante ForeignKey.
    """

    partida = models.ForeignKey(
        Partida, on_delete=models.CASCADE, related_name="posiciones"
    )

    # Llaves primarias de la posición en SAP
    bukrs = models.CharField("Sociedad", max_length=4)
    docnr = models.CharField("Documento (SAP)", max_length=10)
    ryear = models.CharField("Ejercicio", max_length=4)
    docln = models.CharField("Línea", max_length=6)

    ractt = models.CharField("Cuenta", max_length=10, db_index=True)
    wsl = models.DecimalField(
        "Monto", max_digits=20, decimal_places=2, default=Decimal("0.00")
    )
    drcrk = models.CharField("Indicador D/H", max_length=1, blank=True, null=True)
    rwcur = models.CharField("Moneda", max_length=5, blank=True, null=True)

    lifnr = models.CharField(
        "Proveedor", max_length=10, blank=True, null=True, db_index=True
    )
    kunnr = models.CharField(
        "Cliente", max_length=10, blank=True, null=True, db_index=True
    )
    koart = models.CharField("Clase Cta.", max_length=1, blank=True, null=True)

    # Claves para el cruce de disponibilidad
    augbl = models.CharField(
        "Doc. Compensación", max_length=10, blank=True, null=True, db_index=True
    )
    zuonr = models.CharField(
        "Asignación / Ref", max_length=18, blank=True, null=True, db_index=True
    )

    budat = models.DateField("Fecha Contab.", blank=True, null=True, db_index=True)

    class Meta:
        verbose_name = "Posición de Partida"
        verbose_name_plural = "Posiciones de Partida"
        unique_together = ("bukrs", "docnr", "ryear", "docln")

    def __str__(self):
        return f"Línea {self.docln} - {self.ractt}: {self.wsl} {self.rwcur}"


class PartidaPosicionFiltro(models.Model):
    """
    Tabla ultraligera usada exclusivamente en el Paso 3 para pre-filtrar
    qué documentos debemos traer completos desde SAP.
    """

    bukrs = models.CharField(max_length=4, db_index=True)
    docnr = models.CharField(max_length=10, db_index=True)
    ryear = models.CharField(max_length=4, db_index=True)
    docln = models.CharField(max_length=6)
    ractt = models.CharField(max_length=10)
    budat = models.DateField(db_index=True)

    class Meta:
        unique_together = ("bukrs", "docnr", "ryear", "docln")


class Compensacion(models.Model):
    """
    Registro de documentos de compensación extraídos de SAP (ZFI_COMPENSACIONES).
    """

    bukrs = models.CharField("Sociedad", max_length=4)
    belnr = models.CharField("Documento", max_length=10, db_index=True)
    gjahr = models.CharField("Ejercicio", max_length=4)
    buzei = models.CharField("Línea", max_length=3)

    shkzg = models.CharField("D/H", max_length=1, blank=True, null=True)
    dmbtr = models.DecimalField(
        "Monto Local", max_digits=20, decimal_places=2, default=Decimal("0.00")
    )
    wrbtr = models.DecimalField(
        "Monto Doc.", max_digits=20, decimal_places=2, default=Decimal("0.00")
    )
    pswbt = models.DecimalField(
        "Monto Mayor", max_digits=20, decimal_places=2, default=Decimal("0.00")
    )
    pswsl = models.CharField("Moneda Mayor", max_length=5, blank=True, null=True)

    zuonr = models.CharField("Asignación", max_length=18, blank=True, null=True)
    sgtxt = models.CharField("Texto", max_length=50, blank=True, null=True)
    saknr = models.CharField(
        "Cuenta Mayor", max_length=10, blank=True, null=True, db_index=True
    )
    hkont = models.CharField("Cta. Mayor G/L", max_length=10, blank=True, null=True)
    kunnr = models.CharField("Cliente", max_length=10, blank=True, null=True)
    lifnr = models.CharField("Proveedor", max_length=10, blank=True, null=True)

    augdt = models.DateField("Fecha Compensación", blank=True, null=True)
    augcp = models.DateField("Fecha Registro", blank=True, null=True)
    augbl = models.CharField(
        "Doc. Compensación", max_length=10, blank=True, null=True, db_index=True
    )
    bschl = models.CharField("Clave Contab.", max_length=2, blank=True, null=True)
    koart = models.CharField("Clase Cta.", max_length=1, blank=True, null=True)

    class Meta:
        verbose_name = "Compensación"
        verbose_name_plural = "Compensaciones"
        unique_together = ("bukrs", "belnr", "gjahr", "buzei")


class TasaBCV(models.Model):
    """
    Almacena el histórico de tasas del BCV extraídas de SAP para conversión de moneda.
    """

    fecha = models.DateField("Fecha", db_index=True)
    moneda = models.CharField("Moneda", max_length=5)
    tasa = models.DecimalField("Tasa de Cambio", max_digits=15, decimal_places=6)
    descripcion = models.CharField("Descripción", max_length=50, blank=True, null=True)

    creada_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Tasa BCV"
        verbose_name_plural = "Tasas BCV"
        unique_together = ("fecha", "moneda")

    def __str__(self):
        return f"{self.fecha} | {self.moneda}: {self.tasa}"

class CuentaConfiguracion(models.Model):
    TIPO_CHOICES = [
        ('IMPUESTO', 'Cuenta de Impuestos'),
        ('DIF_CAMBIO', 'Diferencia en Cambio'),
        ('COMISION', 'Comisión Bancaria'),
    ]
    
    cuenta = models.CharField("Cuenta Contable", max_length=20, unique=True)
    tipo = models.CharField("Tipo de Cuenta", max_length=20, choices=TIPO_CHOICES)
    descripcion = models.CharField("Descripción / Nombre", max_length=255, blank=True, null=True)
    activa = models.BooleanField("Activa", default=True, help_text="Desmarca para ignorar esta cuenta sin borrarla.")

    class Meta:
        verbose_name = "Configuración de Cuenta"
        verbose_name_plural = "Configuración de Cuentas"

    def __str__(self):
        return f"{self.cuenta} - {self.get_tipo_display()}"