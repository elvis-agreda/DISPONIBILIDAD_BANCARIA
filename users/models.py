# users/models.py
from django.contrib.auth.models import AbstractUser
from django.db import models
from django.utils import timezone

class UsuarioSAP(AbstractUser):
    ROLES_CHOICES = [
        ('ADMINISTRADOR', 'Administrador (Control Total)'),
        ('ANALISTA', 'Analista (Dashboard y Ejecución)'),
        ('AUDITOR', 'Auditor (Solo Lectura)'),
    ]

    rol = models.CharField("Rol en el Sistema", max_length=20, choices=ROLES_CHOICES, default='ANALISTA')
    
    # --- SISTEMA DE APROBACIÓN ---
    aprobado = models.BooleanField("¿Aprobado?", default=False)
    aprobado_por = models.ForeignKey(
        'self', on_delete=models.SET_NULL, null=True, blank=True, 
        related_name='usuarios_aprobados', verbose_name="Aprobado por"
    )
    fecha_aprobacion = models.DateTimeField("Fecha de Aprobación", null=True, blank=True)
    
    # --- AUDITORÍA DE CAMBIOS ---
    modificado_por = models.CharField("Última modificación por", max_length=150, blank=True, null=True)
    fecha_modificacion = models.DateTimeField("Fecha de modificación", auto_now=True)

    # --- SOLUCIÓN AL ERROR E304 (CLASH) ---
    groups = models.ManyToManyField(
        'auth.Group',
        related_name='usuario_sap_groups',
        blank=True,
        verbose_name='groups',
        help_text='The groups this user belongs to.'
    )
    user_permissions = models.ManyToManyField(
        'auth.Permission',
        related_name='usuario_sap_permissions',
        blank=True,
        verbose_name='user permissions',
        help_text='Specific permissions for this user.'
    )

    class Meta:
        verbose_name = "Usuario del Sistema"
        verbose_name_plural = "Usuarios del Sistema"

    def __str__(self):
        return f"{self.username} - {self.get_rol_display()}"