from django import forms
from django.contrib import admin

from .models import (
    AsientoAuditoria,
    ClasificacionGasto,
    ColumnaDrillDown,
    DashboardConsolidado,
    SaldoBancario,
)


def registrar_modelo_dinamico(modelo):
    class ModeloAdminDinamico(admin.ModelAdmin):
        list_display = [f.name for f in modelo._meta.fields]
        search_fields = [
            f.name
            for f in modelo._meta.fields
            if f.get_internal_type() in ("CharField", "TextField")
        ]
        list_filter = [
            f.name
            for f in modelo._meta.fields
            if f.get_internal_type()
            in ("DateField", "DateTimeField", "BooleanField", "CharField")
            and f.name not in ("bukrs", "waers", "drcrk")
        ]

    admin.site.register(modelo, ModeloAdminDinamico)


registrar_modelo_dinamico(SaldoBancario)
registrar_modelo_dinamico(DashboardConsolidado)
registrar_modelo_dinamico(AsientoAuditoria)
registrar_modelo_dinamico(ClasificacionGasto)  # ⚡ Registramos el nuevo modelo


def obtener_campos_dashboard():
    opciones = []
    for campo in DashboardConsolidado._meta.get_fields():
        if campo.concrete:
            nombre_visible = getattr(campo, "verbose_name", campo.name).title()
            opciones.append((campo.name, f"{nombre_visible} ({campo.name})"))
    return sorted(opciones, key=lambda x: x[1])


class ColumnaDrillDownForm(forms.ModelForm):
    campo_bd = forms.ChoiceField(
        choices=[],
        label="Campo en Base de Datos",
        help_text="Seleccione un campo existente en el modelo DashboardConsolidado.",
    )

    class Meta:
        model = ColumnaDrillDown
        fields = "__all__"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["campo_bd"].choices = obtener_campos_dashboard()


@admin.register(ColumnaDrillDown)
class ColumnaDrillDownAdmin(admin.ModelAdmin):
    form = ColumnaDrillDownForm
    list_display = ["etiqueta", "campo_bd", "tipo_dato", "orden", "activo"]
    list_editable = ["orden", "activo"]
    list_filter = ["tipo_dato", "activo"]
    ordering = ["orden"]
