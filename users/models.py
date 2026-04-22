from django.contrib.auth.models import AbstractUser
from django.db import models


class UsuarioSAP(AbstractUser):
    ROLES_CHOICES = [
        ("ADMINISTRADOR", "Administrador (Control Total)"),
        ("ANALISTA", "Analista (Dashboard y Ejecución)"),
        ("AUDITOR", "Auditor (Solo Lectura)"),
    ]

    rol = models.CharField(
        "Rol en el Sistema", max_length=20, choices=ROLES_CHOICES, default="ANALISTA"
    )
    transacciones_sap = models.JSONField("Transacciones SAP", default=list, blank=True)

    def __str__(self):
        return f"{self.username} - {self.get_rol_display()}"


class TransaccionSAP(models.Model):
    tcode = models.CharField("Transacción SAP (TCODE)", max_length=20, unique=True)
    rol_asociado = models.CharField(
        "Rol que otorga", max_length=20, choices=UsuarioSAP.ROLES_CHOICES
    )
    jerarquia = models.IntegerField(
        "Jerarquía",
        default=1,
        help_text="Mayor número = Mayor peso. Si el usuario tiene varias transacciones, se le asigna el rol con mayor jerarquía.",
    )

    class Meta:
        verbose_name = "Mapeo de Transacción SAP"
        verbose_name_plural = "Configuración de Roles por Transacción"
        ordering = ["-jerarquia"]

    def __str__(self):
        return f"{self.tcode} -> {self.get_rol_asociado_display()}"
