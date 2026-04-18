# users/admin.py
from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from django.utils import timezone
from .models import UsuarioSAP

@admin.register(UsuarioSAP)
class UsuarioSAPAdmin(UserAdmin):
    list_display = ('username', 'rol', 'is_active', 'aprobado', 'aprobado_por', 'fecha_aprobacion', 'modificado_por')
    list_filter = ('rol', 'is_active', 'aprobado')
    search_fields = ('username',)
    readonly_fields = ('aprobado_por', 'fecha_aprobacion', 'modificado_por', 'fecha_modificacion')
    
    # Añadimos nuestros campos personalizados a la pantalla de edición
    fieldsets = UserAdmin.fieldsets + (
        ('Control de Acceso y Roles', {'fields': ('rol', 'aprobado', 'aprobado_por', 'fecha_aprobacion')}),
        ('Auditoría', {'fields': ('modificado_por', 'fecha_modificacion')}),
    )

    actions = ['aprobar_usuarios_seleccionados']

    @admin.action(description="Aprobar y Activar Usuarios Seleccionados")
    def aprobar_usuarios_seleccionados(self, request, queryset):
        # Acción masiva: Aprueba, activa y registra quién lo hizo
        queryset.update(
            is_active=True,
            aprobado=True,
            aprobado_por=request.user,
            fecha_aprobacion=timezone.now(),
            modificado_por=f"{request.user.username} (Aprobación masiva)",
            is_staff=False # Por defecto no entran al admin. Solo si les pones rol ADMIN luego.
        )
        self.message_user(request, "Usuarios aprobados exitosamente.")

    def save_model(self, request, obj, form, change):
        # Cada vez que alguien edite a un usuario (ej. cambiarle el rol o desactivarlo)
        # Registramos quién hizo el cambio
        if change:
            obj.modificado_por = request.user.username
            
            # Si le están dando rol ADMINISTRADOR, le damos permiso is_staff automáticamente
            if obj.rol == 'ADMINISTRADOR':
                obj.is_staff = True
            else:
                obj.is_staff = False
                
        super().save_model(request, obj, form, change)