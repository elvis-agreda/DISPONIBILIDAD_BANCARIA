from django.contrib import admin

from .models import AsientoAuditoria, DashboardConsolidado, SaldoBancario


def registrar_modelo_dinamico(modelo):
    """
    Lee dinámicamente los campos de un modelo y configura su Admin.
    """

    class ModeloAdminDinamico(admin.ModelAdmin):
        # Mostrar todos los campos físicos (ignorando relaciones complejas inversas)
        list_display = [f.name for f in modelo._meta.fields]

        # Hacer buscables automáticamente todos los campos de texto
        search_fields = [
            f.name
            for f in modelo._meta.fields
            if f.get_internal_type() in ("CharField", "TextField")
        ]

        # Crear filtros laterales para fechas y booleanos
        list_filter = [
            f.name
            for f in modelo._meta.fields
            if f.get_internal_type()
            in ("DateField", "DateTimeField", "BooleanField", "CharField")
            and f.name
            not in (
                "bukrs",
                "waers",
                "drcrk",
            )  # Excluir campos muy cortos si no son útiles de filtro
        ]

    # Registrar en el admin
    admin.site.register(modelo, ModeloAdminDinamico)


registrar_modelo_dinamico(SaldoBancario)
registrar_modelo_dinamico(DashboardConsolidado)
registrar_modelo_dinamico(AsientoAuditoria)
