from django.apps import apps
from django.contrib import admin

# Obtenemos TODOS los modelos de la app 'sap_sync'
app_models = apps.get_app_config("sap_sync").get_models()

for modelo in app_models:

    class ModeloAdminDinamico(admin.ModelAdmin):
        # Excluimos 'progreso_detalle' (JSONField) del list_display porque rompería la tabla visualmente
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

        # Como esto es data cruda de SAP, por seguridad ponemos todo en modo "Solo Lectura"
        # para que nadie modifique la data contable manualmente desde el Admin.
        def get_readonly_fields(self, request, obj=None):
            return [f.name for f in self.model._meta.fields]

    try:
        admin.site.register(modelo, ModeloAdminDinamico)
    except admin.sites.AlreadyRegistered:
        pass  # Ignorar si ya se registró por accidente
