import socket
import requests
import urllib3
from django.conf import settings
from django.contrib import messages
from django.contrib.auth.backends import BaseBackend

from sap_sync.services.sap_client import SAPServiceURL

from .models import UsuarioSAP, IntentoLogin, RolSAP
from .dencrypt import sap_issha_verify

# Desactivar advertencias de certificados SSL si no usas HTTPS estricto hacia SAP
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AutenticacionSAPBackend(BaseBackend):
    def authenticate(self, request, username=None, password=None, **kwargs):
        if not username or not password:
            return None

        usuario, created = UsuarioSAP.objects.get_or_create(username=username.upper())
        if usuario.bloqueado or usuario.intentos_fallidos >= 3:
            if request:
                messages.error(request, "Usuario bloqueado. Contacte al administrador del sistema para desbloquearlo.")
            return None

        ip = "127.0.0.1"
        pc_name = "Unknown"
        if request:
            ip = request.META.get('HTTP_X_FORWARDED_FOR', request.META.get('REMOTE_ADDR', '127.0.0.1'))
            if ',' in ip:
                ip = ip.split(',')[0].strip()
            try:
                pc_name = socket.gethostbyaddr(ip)[0]
            except Exception:
                pc_name = ip

        url_check = f"{SAPServiceURL.CHECK_USER_INFO}"

        try:
            respuesta = requests.get(
                url_check,
                params={"sap-client": settings.SAP_AMBIENTE, "USERNAME": username.upper()},
                auth=(settings.SAP_USERNAME, settings.SAP_PASSWORD),
                timeout=15,
                verify=False,
            )

            if respuesta.status_code == 200:
                data = respuesta.json()
                
                check_messages = data.get("IT_CHECK_MESSAGES", [])
                
                error_msgs = [m["RESULT"] for m in check_messages if m.get("MSGTY") == "E"]
                if error_msgs:
                    self._registrar_intento(usuario, ip, pc_name, False)
                    if request:
                        for err in error_msgs:
                            messages.error(request, f"Acceso Denegado (SAP): {err}")
                    return None

                for msg in check_messages:
                    if msg.get("MSGTY") == "W" and request:
                        messages.warning(request, f"SAP: {msg.get('RESULT')}")

                logon_data = data.get("IT_LOGON_DATA", [])
                if not logon_data:
                    self._registrar_intento(usuario, ip, pc_name, False)
                    if request:
                        messages.error(request, "SAP: No se recibió información de inicio de sesión.")
                    return None

                pwd_hash = logon_data[0].get("PWDSALTEDHASH", "")
                if not pwd_hash or not sap_issha_verify(password, pwd_hash):
                    self._registrar_intento(usuario, ip, pc_name, False)
                    if request:
                        messages.error(request, "Contraseña incorrecta.")
                    return None
                    
                roles = data.get("IT_ROLES", [])
                roles_sap = [r.get("AGR_NAME") for r in roles]
                
                roles_config = RolSAP.objects.all().order_by("-jerarquia")
                rol_asignado = "ANALISTA"
                is_admin = False
                
                for config in roles_config:
                    if config.rol_sap in roles_sap:
                        rol_asignado = config.rol_django
                        if rol_asignado == "ADMINISTRADOR":
                            is_admin = True
                        break

                user_details = data.get("IT_USER_DETAILS", [])
                if user_details:
                    usuario.detalles_sap = user_details[0]
                    usuario.first_name = user_details[0].get("FIRSTNAME", "")
                    usuario.last_name = user_details[0].get("LASTNAME", "")
                    usuario.email = user_details[0].get("E_MAIL", "")

                usuario.is_active = True
                usuario.rol = rol_asignado
                usuario.is_staff = is_admin
                usuario.is_superuser = is_admin

                usuario.intentos_fallidos = 0
                usuario.set_password(password)
                usuario.save()

                self._registrar_intento(usuario, ip, pc_name, True)
                return usuario
                
            else:
                if request:
                    messages.error(request, f"Error del servidor SAP (HTTP {respuesta.status_code})")
                return None

        except requests.RequestException:
            if request:
                messages.error(request, "Error de red: No se pudo contactar al servidor SAP.")
            return None

    def _registrar_intento(self, usuario, ip, pc_name, exitoso):
        IntentoLogin.objects.create(usuario=usuario, ip=ip, pc_name=pc_name, exitoso=exitoso)
        if not exitoso:
            usuario.intentos_fallidos += 1
            if usuario.intentos_fallidos >= 3:
                usuario.bloqueado = True
            usuario.save()

    def get_user(self, user_id):
        try:
            return UsuarioSAP.objects.get(pk=user_id)
        except UsuarioSAP.DoesNotExist:
            return None
