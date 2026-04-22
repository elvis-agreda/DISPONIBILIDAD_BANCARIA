import requests
import urllib3
from django.conf import settings
from django.contrib import messages
from django.contrib.auth.backends import BaseBackend

from sap_sync.services.sap_client import SAPServiceURL

from .models import TransaccionSAP, UsuarioSAP

# Desactivar advertencias de certificados SSL si no usas HTTPS estricto hacia SAP
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AutenticacionSAPBackend(BaseBackend):
    def authenticate(self, request, username=None, password=None, **kwargs):
        if not username or not password:
            return None

        # 1. Buscamos qué transacciones debemos consultar a SAP
        tcodes_config = TransaccionSAP.objects.all().order_by("-jerarquia")

        if not tcodes_config.exists():
            if request:
                messages.error(
                    request,
                    "Error de Sistema: No hay transacciones SAP configuradas para mapear roles. Contacte al administrador.",
                )
            return None

        lista_tcodes = [{"TCODE": t.tcode} for t in tcodes_config]

        # 2. Construimos la petición a SAP
        url_check = f"{SAPServiceURL.USER_TCODE_CHECK}"
        payload = {
            "USER_LOGON": [{"USERNAME": username.upper(), "PASSWORD": password}],
            "TCODES_CHECK": lista_tcodes,
        }

        try:
            respuesta = requests.get(
                url_check,
                params={"sap-client": settings.SAP_AMBIENTE},
                json=payload,
                auth=(settings.SAP_USERNAME, settings.SAP_PASSWORD),
                timeout=15,
                verify=False,
            )

            if respuesta.status_code == 200:
                data = respuesta.json()
                mensaje_sap = data.get("MESSAGE", "")

                # 3. Validar la respuesta de SAP
                if mensaje_sap == "USUARIO VALIDO":
                    # Extraer transacciones que vinieron con "ALLOWED": "X"
                    tcodes_permitidos = [
                        item["TCODE"]
                        for item in data.get("TCODES_CHECK", [])
                        if item.get("ALLOWED") == "X"
                    ]

                    if not tcodes_permitidos:
                        if request:
                            messages.error(
                                request,
                                "Acceso Denegado: Su usuario es válido en SAP pero no posee ninguna transacción autorizada para este portal.",
                            )
                        return None

                    # 4. Asignar el rol basado en la transacción de mayor jerarquía permitida
                    rol_asignado = "ANALISTA"  # Default
                    is_staff = False
                    is_superuser = False

                    for config in tcodes_config:
                        if config.tcode in tcodes_permitidos:
                            rol_asignado = config.rol_asociado
                            if rol_asignado == "ADMINISTRADOR":
                                is_staff = True
                                is_superuser = True
                            break

                    # 5. Crear o actualizar el usuario en Django
                    usuario, created = UsuarioSAP.objects.get_or_create(
                        username=username.upper()
                    )
                    usuario.is_active = True
                    usuario.rol = rol_asignado
                    usuario.is_staff = is_staff
                    usuario.is_superuser = is_superuser
                    usuario.transacciones_sap = tcodes_permitidos
                    usuario.set_password(
                        password
                    )  # Lo guardamos para tener sincronía local
                    usuario.save()

                    return usuario
                else:
                    if request:
                        messages.error(request, f"SAP: {mensaje_sap}")
                    return None

            else:
                if request:
                    messages.error(
                        request,
                        f"Error del servidor SAP (HTTP {respuesta.status_code})",
                    )
                return None

        except requests.RequestException:
            if request:
                messages.error(
                    request, "Error de red: No se pudo contactar al servidor SAP."
                )
            return None

    def get_user(self, user_id):
        try:
            return UsuarioSAP.objects.get(pk=user_id)
        except UsuarioSAP.DoesNotExist:
            return None
