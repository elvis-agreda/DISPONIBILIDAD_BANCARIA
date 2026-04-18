from decimal import Decimal

from django.db import models


class SaldoBancario(models.Model):
    """
    Almacena los saldos iniciales y movimientos por periodo extraídos de SAP (ZFI_SALDO_BANCARIO).
    """

    bukrs = models.CharField("Sociedad", max_length=4, db_index=True)
    ryear = models.CharField("Ejercicio", max_length=4, db_index=True)
    hkont = models.CharField("Cuenta Mayor", max_length=10, db_index=True)
    waers = models.CharField("Moneda", max_length=5)
    drcrk = models.CharField("Indicador D/H", max_length=1)

    tslvt = models.DecimalField(
        "Saldo Arrastre", max_digits=20, decimal_places=2, default=Decimal("0")
    )
    tsl01 = models.DecimalField(
        "Periodo 01", max_digits=20, decimal_places=2, default=Decimal("0")
    )
    tsl02 = models.DecimalField(
        "Periodo 02", max_digits=20, decimal_places=2, default=Decimal("0")
    )
    tsl03 = models.DecimalField(
        "Periodo 03", max_digits=20, decimal_places=2, default=Decimal("0")
    )
    tsl04 = models.DecimalField(
        "Periodo 04", max_digits=20, decimal_places=2, default=Decimal("0")
    )
    tsl05 = models.DecimalField(
        "Periodo 05", max_digits=20, decimal_places=2, default=Decimal("0")
    )
    tsl06 = models.DecimalField(
        "Periodo 06", max_digits=20, decimal_places=2, default=Decimal("0")
    )
    tsl07 = models.DecimalField(
        "Periodo 07", max_digits=20, decimal_places=2, default=Decimal("0")
    )
    tsl08 = models.DecimalField(
        "Periodo 08", max_digits=20, decimal_places=2, default=Decimal("0")
    )
    tsl09 = models.DecimalField(
        "Periodo 09", max_digits=20, decimal_places=2, default=Decimal("0")
    )
    tsl10 = models.DecimalField(
        "Periodo 10", max_digits=20, decimal_places=2, default=Decimal("0")
    )
    tsl11 = models.DecimalField(
        "Periodo 11", max_digits=20, decimal_places=2, default=Decimal("0")
    )
    tsl12 = models.DecimalField(
        "Periodo 12", max_digits=20, decimal_places=2, default=Decimal("0")
    )
    tsl13 = models.DecimalField(
        "Periodo 13", max_digits=20, decimal_places=2, default=Decimal("0")
    )
    tsl14 = models.DecimalField(
        "Periodo 14", max_digits=20, decimal_places=2, default=Decimal("0")
    )
    tsl15 = models.DecimalField(
        "Periodo 15", max_digits=20, decimal_places=2, default=Decimal("0")
    )
    tsl16 = models.DecimalField(
        "Periodo 16", max_digits=20, decimal_places=2, default=Decimal("0")
    )

    sincronizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Saldo Bancario"
        verbose_name_plural = "Saldos Bancarios"
        unique_together = (
            "bukrs",
            "ryear",
            "hkont",
            "waers",
            "drcrk",
        )  # Evita duplicados exactos

    def __str__(self):
        return f"{self.bukrs} - {self.hkont} ({self.ryear})"


class DashboardConsolidado(models.Model):
    """
    El modelo principal para la Disponibilidad Bancaria.
    Contiene las operaciones ya conciliadas y limpias.
    """

    tipo_operacion = models.CharField(
        "Tipo Operación", max_length=50, db_index=True
    )  # EJ: EGRESO, INGRESO, TRANSFERENCIA
    categoria = models.CharField("Categoría", max_length=100, db_index=True)
    sub_categoria = models.CharField(
        "Sub Categoría", max_length=50, blank=True, null=True
    )

    cuenta_contable = models.CharField("Cuenta Bancaria", max_length=20, db_index=True)
    cuenta_gasto = models.CharField(
        "Cuenta Gasto", max_length=20, blank=True, null=True
    )
    lifnr = models.CharField("Proveedor (LIFNR)", max_length=20, blank=True, null=True)
    kunnr = models.CharField("Cliente (KUNNR)", max_length=20, blank=True, null=True)

    monto_base = models.DecimalField("Monto Base", max_digits=20, decimal_places=2)
    monto_total = models.DecimalField("Monto Total", max_digits=20, decimal_places=2)
    rwcur = models.CharField("Moneda", max_length=5)

    fecha_contabilizacion = models.DateField("Fecha Contabilización", db_index=True)

    documento_primario = models.CharField(
        "Doc. Primario (BELNR)", max_length=20, db_index=True
    )
    documento_secundario = models.CharField(
        "Doc. Secundario", max_length=50, blank=True, null=True
    )
    referencia = models.CharField(
        "Referencia (ZUONR)", max_length=50, blank=True, null=True
    )
    referencia1 = models.CharField(
        "Referencia 1 (BKTXT)", max_length=50, blank=True, null=True
    )

    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Registro Dashboard"
        verbose_name_plural = "Dashboard Consolidado"
        ordering = ["-fecha_contabilizacion"]

    def __str__(self):
        return f"{self.tipo_operacion} - {self.cuenta_contable} : {self.monto_total} {self.rwcur}"


class AsientoAuditoria(models.Model):
    """
    Registra operaciones que fallaron la conciliación lógica para revisión manual.
    """

    bukrs = models.CharField("Sociedad", max_length=4)
    belnr = models.CharField("Documento", max_length=20, db_index=True)
    gjahr = models.CharField("Ejercicio", max_length=4)
    blart = models.CharField("Clave Doc.", max_length=5)
    cuenta_contable = models.CharField("Cuenta", max_length=20)

    monto = models.DecimalField("Monto", max_digits=20, decimal_places=2)
    rwcur = models.CharField("Moneda", max_length=5)
    fecha = models.DateField("Fecha Contabilización")

    motivo_descarte = models.CharField("Motivo de Descarte", max_length=255)
    texto_cabecera = models.CharField(
        "Texto Cabecera", max_length=255, blank=True, null=True
    )

    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Asiento de Auditoría"
        verbose_name_plural = "Asientos de Auditoría"

    def __str__(self):
        return f"AUDIT: {self.belnr} - {self.motivo_descarte}"
