from django.contrib import admin
from django.contrib.auth.admin import UserAdmin

from .models import TransaccionSAP, UsuarioSAP


@admin.register(UsuarioSAP)
class UsuarioSAPAdmin(UserAdmin):
    list_display = ["username", "email", "rol", "is_active", "is_staff"]
    fieldsets = UserAdmin.fieldsets + (
        ("Información SAP", {"fields": ("rol", "transacciones_sap")}),
    )


@admin.register(TransaccionSAP)
class TransaccionSAPAdmin(admin.ModelAdmin):
    list_display = ["tcode", "rol_asociado", "jerarquia"]
    list_editable = ["rol_asociado", "jerarquia"]
    ordering = ["-jerarquia"]
