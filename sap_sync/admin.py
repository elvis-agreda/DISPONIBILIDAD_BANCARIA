from django.apps import apps
from django.contrib import admin
from django.utils.safestring import mark_safe
from .models import CuentaConfiguracion, MapeoCampo, SincronizacionLog

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

@admin.register(SincronizacionLog)
class SincronizacionLogAdmin(admin.ModelAdmin):
    list_display = ['tipo_icon', 'estado_label', 'fecha_inicio', 'fecha_fin', 'errores_badge', 'iniciado_en']
    list_filter = ['estado', 'tipo', 'iniciado_en']
    search_fields = ['anio', 'periodo']
    readonly_fields = [f.name for f in SincronizacionLog._meta.fields if f.name != 'progreso_detalle'] + ['progreso_visual']
    exclude = ['progreso_detalle']

    # --- LISTA PRINCIPAL CON COLORES NATIVOS ---
    def tipo_icon(self, obj):
        if obj.tipo == "AUTO":
            return "Automática (Crontab)"
        else:
            nombre = obj.usuario.username.upper() if obj.usuario else "Desconocido"
            return f"Manual ({nombre})"
            
    tipo_icon.short_description = "Tipo de Ejecución"

    def estado_label(self, obj):
        # Usamos colores hexadecimales fijos que contrastan bien en fondos oscuros y claros
        colors = {
            'EXITOSO': '#28a745',     # Verde brillante
            'FALLIDO': '#dc3545',     # Rojo
            'EN_CURSO': '#3399ff',    # Azul claro
            'PARCIAL': '#ffc107',     # Amarillo/Naranja
            'CANCELADO': '#6c757d'    # Gris
        }
        color = colors.get(obj.estado, '#888888')
        
        return mark_safe(
            f'<span style="color: {color}; border: 1px solid {color}; padding: 4px 8px; '
            f'border-radius: 12px; font-size: 11px; font-weight: bold; display: inline-block; '
            f'vertical-align: middle; line-height: 1; margin-top: 2px;">'
            f'{obj.estado}</span>'
        )
    estado_label.short_description = "Estado"

    def errores_badge(self, obj):
        # Si hay errores, fondo rojo. Si hay 0 errores, fondo verde.
        bg_color = '#dc3545' if obj.errores_count > 0 else '#28a745'
        
        return mark_safe(
            f'<span style="background: {bg_color}; color: #ffffff; padding: 4px 8px; '
            f'border-radius: 12px; font-weight: bold; font-size: 11px; display: inline-block; '
            f'vertical-align: middle; line-height: 1; min-width: 20px; text-align: center; margin-top: 2px;">'
            f'{obj.errores_count}</span>'
        )
    errores_badge.short_description = "Errores"

    # --- MONITOR VISUAL ADAPTABLE (MODO OSCURO/CLARO) ---
    def progreso_visual(self, obj):
        if not obj.progreso_detalle:
            return "No hay datos de ejecución disponibles."

        # Contenedor principal usando variables de fondo y borde de Django
        html = [
            '<div style="background: var(--body-bg); border: 1px solid var(--border-color); border-radius: 8px; padding: 20px; color: var(--body-fg); font-family: sans-serif;">'
        ]
        
        pasos_ordenados = sorted(obj.progreso_detalle.items())

        for key, info in pasos_ordenados:
            estado = info.get('estado', 'PENDIENTE')
            
            # Colores lógicos basados en el estado
            status_color = "var(--success-fg)" if estado == "EXITOSO" else "var(--error-fg)" if estado == "FALLIDO" else "var(--link-fg)"
            
            # Caja de cada paso
            html.append(f'''
                <div style="margin-bottom: 20px; border-left: 4px solid {status_color}; background: var(--darkened-bg, rgba(0,0,0,0.02)); padding: 15px; border-radius: 0 8px 8px 0; border-top: 1px solid var(--border-color); border-right: 1px solid var(--border-color); border-bottom: 1px solid var(--border-color);">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <h3 style="margin: 0; color: var(--body-fg); font-size: 16px;">
                            <span style="color: {status_color}; margin-right: 10px;">●</span> {info.get('nombre', key)}
                        </h3>
                        <span style="font-size: 11px; color: var(--secondary);">{info.get('iniciado_en', '')[:19].replace('T', ' ')}</span>
                    </div>
                    
                    <div style="margin-top: 10px; font-size: 13px;">
            ''')

            # Métricas con etiquetas de estilo "chip"
            metricas = info.get('metricas', {})
            if metricas:
                html.append('<div style="display: flex; gap: 10px; margin: 10px 0; flex-wrap: wrap;">')
                for m_key, m_val in metricas.items():
                    html.append(f'<span style="background: var(--selected-bg); color: var(--selected-fg); padding: 3px 12px; border-radius: 15px; border: 1px solid var(--border-color); font-size: 11px;"><strong>{m_key.replace("_", " ").title()}:</strong> {m_val}</span>')
                html.append('</div>')

            # Mensajes del Log (Estilo terminal discreta)
            mensajes = info.get('mensajes', [])
            if mensajes:
                html.append('<div style="margin: 10px 0; padding: 10px; background: var(--body-bg); border-radius: 4px; border: 1px solid var(--border-color); color: var(--secondary); font-size: 12px; font-family: monospace;">')
                for msg in mensajes[-3:]:
                    html.append(f'<div style="margin-bottom: 2px;">{msg}</div>')
                html.append('</div>')

            # Errores técnicos colapsables
            errores = info.get('errores', [])
            if errores:
                html.append(f'''
                    <details style="margin-top: 10px;">
                        <summary style="cursor: pointer; color: var(--error-fg); font-weight: bold; font-size: 12px;">⚠️ Mostrar {len(errores)} detalle(s) de error</summary>
                        <div style="background: var(--body-bg); border: 1px solid var(--error-fg); padding: 10px; border-radius: 4px; margin-top: 5px; font-family: monospace; font-size: 11px; color: var(--body-fg);">
                ''')
                for err in errores:
                    # Incluimos el contexto/traceback si existe
                    trace = err.get('contexto', {}).get('traceback', '')
                    html.append(f'<div style="margin-bottom: 8px; border-bottom: 1px solid var(--border-color); padding-bottom: 5px;">')
                    html.append(f'<strong>[{err.get("hora")}]</strong> {err.get("mensaje")}')
                    if trace:
                        html.append(f'<pre style="margin-top: 5px; white-space: pre-wrap; font-size: 10px; color: var(--secondary); opacity: 0.8;">{trace}</pre>')
                    html.append('</div>')
                html.append('</div></details>')

            html.append('</div></div>')

        html.append('</div>')
        return mark_safe("".join(html))
    
    progreso_visual.short_description = "Monitor de Ejecución SAP"

app_models = apps.get_app_config("sap_sync").get_models()
modelos_configuracion = [CuentaConfiguracion, MapeoCampo, SincronizacionLog]

for modelo in app_models:
    if modelo in modelos_configuracion:
        continue

    class ModeloAdminDinamico(admin.ModelAdmin):
        list_display = [f.name for f in modelo._meta.fields if f.name != "progreso_detalle"]
        search_fields = [f.name for f in modelo._meta.fields if f.get_internal_type() in ("CharField", "TextField")]
        list_filter = [f.name for f in modelo._meta.fields if f.get_internal_type() in ("DateField", "DateTimeField")]

        def get_readonly_fields(self, request, obj=None):
            return [f.name for f in self.model._meta.fields]     
    try:
        admin.site.register(modelo, ModeloAdminDinamico)
    except admin.sites.AlreadyRegistered:
        pass