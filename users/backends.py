import requests
from django.contrib.auth.backends import BaseBackend
from django.contrib.auth import get_user_model
from sap_sync.services.sap_client import AMBIENTE_SAP, SAPServiceURL

UsuarioSAP = get_user_model()

class AutenticacionSAPBackend(BaseBackend):
    def authenticate(self, request, username=None, password=None, **kwargs):
        if not username or not password:
            return None

        url_ping = f"{AMBIENTE_SAP}{SAPServiceURL.SALDOS_BANCARIOS}/$metadata"
        
        try:
            respuesta = requests.get(url_ping, auth=(username, password), timeout=5)

            if respuesta.status_code == 200:
                # SAP dice que la contraseña es correcta. ¿Existe en Django?
                try:
                    usuario = UsuarioSAP.objects.get(username=username)
                except UsuarioSAP.DoesNotExist:
                    # ES NUEVO: Se crea, pero se bloquea (is_active=False)
                    UsuarioSAP.objects.create(
                        username=username,
                        is_active=False,  # <-- BLOQUEADO HASTA APROBACIÓN
                        is_staff=False,   # <-- NO ENTRA AL ADMIN
                        aprobado=False
                    )
                    return None # No lo dejamos entrar hoy.

                # Si ya existía, verificamos si un Admin lo aprobó y activó
                if not usuario.is_active or not usuario.aprobado:
                    return None

                return usuario

            return None # Contraseña mala en SAP

        except requests.RequestException:
            return None

    def get_user(self, user_id):
        try:
            return UsuarioSAP.objects.get(pk=user_id)
        except UsuarioSAP.DoesNotExist:
            return None