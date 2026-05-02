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
    intentos_fallidos = models.IntegerField("Intentos fallidos", default=0)
    bloqueado = models.BooleanField("Bloqueado", default=False)
    detalles_sap = models.JSONField("Detalles de SAP", default=dict, blank=True)

    def __str__(self):
        return f"{self.username} - {self.get_rol_display()}"  # type: ignore


class RolSAP(models.Model):
    rol_sap = models.CharField("Rol SAP", max_length=100, unique=True)
    rol_django = models.CharField(
        "Rol en el Sistema", max_length=20, choices=UsuarioSAP.ROLES_CHOICES
    )
    jerarquia = models.IntegerField(
        "Jerarquía",
        default=1,
        help_text="Mayor número = Mayor peso. Si el usuario tiene varios roles, se le asigna el rol con mayor jerarquía.",
    )

    class Meta:
        verbose_name = "Mapeo de Rol SAP"
        verbose_name_plural = "Configuración de Roles SAP"
        ordering = ["-jerarquia"]

    def __str__(self):
        return f"{self.rol_sap} -> {self.get_rol_django_display()}"  # type: ignore


class IntentoLogin(models.Model):
    usuario = models.ForeignKey(UsuarioSAP, on_delete=models.CASCADE, related_name="intentos_login")
    ip = models.GenericIPAddressField("Dirección IP")
    pc_name = models.CharField("Nombre del PC", max_length=255, blank=True, null=True)
    fecha = models.DateTimeField("Fecha y Hora", auto_now_add=True)
    exitoso = models.BooleanField("Exitoso", default=False)

    class Meta:
        verbose_name = "Intento de Login"
        verbose_name_plural = "Intentos de Login"
        ordering = ["-fecha"]

    def __str__(self):
        return f"{self.usuario.username} - {'Exito' if self.exitoso else 'Fallo'} - {self.fecha.strftime('%Y-%m-%d %H:%M:%S')}"

