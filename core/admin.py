from django.contrib import admin
from django import forms
from .models import AsientoAuditoria, DashboardConsolidado, SaldoBancario, ColumnaDrillDown


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
#registrar_modelo_dinamico(ColumnaDrillDown)

# --- 1. Función para leer los campos dinámicamente ---
def obtener_campos_dashboard():
    opciones = []
    # Recorremos todos los campos del modelo DashboardConsolidado
    for campo in DashboardConsolidado._meta.get_fields():
        # Filtramos para usar solo los campos reales de BD (ignoramos relaciones inversas de Django)
        if campo.concrete:
            nombre_visible = getattr(campo, 'verbose_name', campo.name).title()
            opciones.append((campo.name, f"{nombre_visible} ({campo.name})"))
    
    # Devolvemos la lista ordenada alfabéticamente
    return sorted(opciones, key=lambda x: x[1])

# --- 2. Formulario personalizado para inyectar el desplegable ---
class ColumnaDrillDownForm(forms.ModelForm):
    # Sobrescribimos el campo_bd para que sea un ChoiceField (Desplegable) en vez de un texto libre
    campo_bd = forms.ChoiceField(
        choices=[], 
        label="Campo en Base de Datos",
        help_text="Seleccione un campo existente en el modelo DashboardConsolidado."
    )

    class Meta:
        model = ColumnaDrillDown
        fields = '__all__'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Cargamos las opciones dinámicamente cada vez que se abre el formulario
        self.fields['campo_bd'].choices = obtener_campos_dashboard()

# --- 3. Registro avanzado del Admin ---
@admin.register(ColumnaDrillDown)
class ColumnaDrillDownAdmin(admin.ModelAdmin):
    form = ColumnaDrillDownForm
    # Mejoramos la vista de lista para que puedas ordenar y activar/desactivar rápido
    list_display = ['etiqueta', 'campo_bd', 'tipo_dato', 'orden', 'activo']
    list_editable = ['orden', 'activo']
    list_filter = ['tipo_dato', 'activo']
    ordering = ['orden']