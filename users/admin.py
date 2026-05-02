from django.contrib import admin
from django.contrib.auth.admin import UserAdmin

from .models import UsuarioSAP, IntentoLogin, RolSAP


@admin.register(UsuarioSAP)
class UsuarioSAPAdmin(UserAdmin):
    list_display = ["username", "email", "rol", "is_active", "bloqueado", "intentos_fallidos"]
    list_filter = UserAdmin.list_filter + ("bloqueado",)
    fieldsets = UserAdmin.fieldsets + (
        ("Información SAP", {"fields": ("rol", "detalles_sap")}),
        ("Seguridad", {"fields": ("bloqueado", "intentos_fallidos")}),
    )
    readonly_fields = ("detalles_sap",)


@admin.register(IntentoLogin)
class IntentoLoginAdmin(admin.ModelAdmin):
    list_display = ["usuario", "ip", "pc_name", "fecha", "exitoso"]
    list_filter = ["exitoso", "fecha"]
    search_fields = ["usuario__username", "ip", "pc_name"]
    readonly_fields = ["usuario", "ip", "pc_name", "fecha", "exitoso"]


@admin.register(RolSAP)
class RolSAPAdmin(admin.ModelAdmin):
    list_display = ["rol_sap", "rol_django", "jerarquia"]
    list_editable = ["rol_django", "jerarquia"]
    ordering = ["-jerarquia"]

