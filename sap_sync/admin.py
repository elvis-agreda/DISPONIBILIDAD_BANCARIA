from django.apps import apps
from django.contrib import admin
from .models import CuentaConfiguracion

@admin.register(CuentaConfiguracion)
class CuentaConfiguracionAdmin(admin.ModelAdmin):
    list_display = ['cuenta', 'tipo', 'descripcion', 'activa']
    list_filter = ['tipo', 'activa']
    search_fields = ['cuenta', 'descripcion']

app_models = apps.get_app_config("sap_sync").get_models()

for modelo in app_models:
    # Si es el modelo que acabamos de registrar arriba, lo saltamos
    if modelo == CuentaConfiguracion:
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