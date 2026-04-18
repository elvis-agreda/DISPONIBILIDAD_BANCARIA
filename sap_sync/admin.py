# sap_sync/admin.py
from django.apps import apps
from django.contrib import admin
from .models import CuentaConfiguracion, MapeoCampo

@admin.register(CuentaConfiguracion)
class CuentaConfiguracionAdmin(admin.ModelAdmin):
    list_display = ['cuenta', 'tipo', 'descripcion', 'activa']
    list_filter = ['tipo', 'activa']
    search_fields = ['cuenta', 'descripcion']

@admin.register(MapeoCampo)
class MapeoCampoAdmin(admin.ModelAdmin):
    list_display = ['modelo_destino', 'campo_sap', 'campo_django', 'tipo_dato', 'activo']
    list_filter = ['modelo_destino', 'tipo_dato', 'activo']
    search_fields = ['campo_sap', 'campo_django']
    list_editable = ['activo']

app_models = apps.get_app_config("sap_sync").get_models()

modelos_configuracion = [CuentaConfiguracion, MapeoCampo]

for modelo in app_models:
    if modelo in modelos_configuracion:
        continue

    class ModeloAdminDinamico(admin.ModelAdmin):
        list_display = [
            f.name for f in modelo._meta.fields if f.name != "progreso_detalle"
        ]

        search_fields = [
            f.name
            for f in modelo._meta.fields
            if f.get_internal_type() in ("CharField", "TextField")
        ]

        list_filter = [
            f.name
            for f in modelo._meta.fields
            if f.get_internal_type() in ("DateField", "DateTimeField")
        ]

        def get_readonly_fields(self, request, obj=None):
            return [f.name for f in self.model._meta.fields]
    try:
        admin.site.register(modelo, ModeloAdminDinamico)
    except admin.sites.AlreadyRegistered:
        pass