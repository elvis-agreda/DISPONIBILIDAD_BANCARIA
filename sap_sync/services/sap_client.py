import json
import re
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import quote

import requests
import urllib3

# ── Importamos las configuraciones de Django ──
from django.conf import settings
from requests.auth import HTTPBasicAuth

# ── Parámetros de conexión dinámicos ─────────────────────────────────────────
USERNAME = settings.SAP_USERNAME
PASSWORD = settings.SAP_PASSWORD
AMBIENTE_SAP = settings.SAP_AMBIENTE
URL_BASE = settings.SAP_URL
SAP_PORT = settings.SAP_PORT

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ── URLs de servicios ───────────────────────────────────────────────────────
class SAPServiceURL:
    PARTIDAS = f"{URL_BASE}:{SAP_PORT}/sap/opu/odata/sap/ZFI_PARTIDAS_CDS"
    COMPENSACIONES = f"{URL_BASE}:{SAP_PORT}/sap/opu/odata/sap/ZFI_COMPENSACIONES_CDS"
    SALDOS_BANCARIOS = (
        f"{URL_BASE}:{SAP_PORT}/sap/opu/odata/sap/ZFI_SALDO_BANCARIO_CDS/"
    )
    Tasa_BCV = f"{URL_BASE}:{SAP_PORT}/fmcall/ZFI_TASA_BCV"


# ── Cliente OData V2 principal ──────────────────────────────────────────────
class SAPODataClient:
    """
    Cliente genérico para servicios OData V2 de SAP ECC.
    """

    def __init__(
        self,
        base_url: str,
        username: str = USERNAME,
        password: str = PASSWORD,
        sap_client: str = AMBIENTE_SAP,
        metadata_ttl: int = 3600,
        page_size: int = 500,
    ):
        self.base_url = base_url.rstrip("/")
        self.page_size = page_size  # $top por página en paginación automática
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.sap_client = sap_client

        self.base_params: Dict = {
            "sap-client": self.sap_client,
            "$format": "json",
        }

        self.metadata_ttl = metadata_ttl
        self._metadata_cache: Dict = {}
        self._metadata_last_fetch: float = 0
        self.last_errors: List[str] = []

    # ── Metadata ─────────────────────────────────────────────────────────────

    def _fetch_and_parse_metadata(self) -> None:
        url = f"{self.base_url}/$metadata"
        headers = {"Accept": "application/xml"}
        try:
            response = self.session.get(
                url,
                params={"sap-client": self.sap_client},
                headers=headers,
                verify=False,
            )
            response.raise_for_status()

            if (
                "<html" in response.text.lower()
                or "Anmeldung fehlgeschlagen" in response.text
            ):
                raise ValueError("SAP devolvió HTML de error de autenticación.")

            ns = {
                "edmx": "http://schemas.microsoft.com/ado/2007/06/edmx",
                "edm": "http://schemas.microsoft.com/ado/2008/09/edm",
            }
            try:
                root = ET.fromstring(response.content)
            except ET.ParseError:
                raise ValueError("El contenido devuelto por SAP no es XML válido.")

            entity_types: Dict = {}
            for et_node in root.findall(".//edm:EntityType", ns):
                type_name = et_node.attrib.get("Name")
                properties = [
                    p.attrib.get("Name") for p in et_node.findall("edm:Property", ns)
                ]
                nav_props = [
                    n.attrib.get("Name")
                    for n in et_node.findall("edm:NavigationProperty", ns)
                ]
                keys = []
                key_node = et_node.find("edm:Key", ns)
                if key_node is not None:
                    keys = [
                        pr.attrib.get("Name")
                        for pr in key_node.findall("edm:PropertyRef", ns)
                    ]
                entity_types[type_name] = {
                    "properties": properties + nav_props,
                    "keys": keys,
                }

            new_cache: Dict = {}
            for es_node in root.findall(".//edm:EntitySet", ns):
                set_name = es_node.attrib.get("Name")
                type_full_name = es_node.attrib.get("EntityType", "")
                type_name = type_full_name.split(".")[-1]
                if type_name in entity_types:
                    new_cache[set_name] = entity_types[type_name]

            self._metadata_cache = new_cache
            self._metadata_last_fetch = time.time()

        except requests.exceptions.RequestException as e:
            self.last_errors.append(f"Error de red al obtener metadata: {e}")
        except ValueError as ve:
            self.last_errors.append(f"Error procesando metadata: {ve}")
        except Exception as e:
            self.last_errors.append(f"Error inesperado en metadata: {e}")

    def _ensure_metadata(self) -> None:
        if (
            time.time() - self._metadata_last_fetch
        ) > self.metadata_ttl or not self._metadata_cache:
            self._fetch_and_parse_metadata()

    def validate_entity_and_properties(
        self,
        entity_set: str,
        properties_to_check: List[str] = None,
        keys_to_check: List[str] = None,
    ) -> bool:
        self._ensure_metadata()
        self.last_errors = []

        if not self._metadata_cache:
            self.last_errors.append(
                "Validación cancelada: no se pudo cargar metadata de SAP."
            )
            return False

        if entity_set not in self._metadata_cache:
            self.last_errors.append(
                f"El EntitySet '{entity_set}' no existe en la metadata."
            )
            return False

        entity_info = self._metadata_cache[entity_set]
        valid_properties = entity_info["properties"]
        required_keys = set(entity_info["keys"])

        if properties_to_check:
            for prop in properties_to_check:
                if prop not in valid_properties:
                    self.last_errors.append(
                        f"La propiedad '{prop}' no existe en '{entity_set}'."
                    )
                    return False

        if keys_to_check is not None:
            provided = set(keys_to_check)
            if required_keys != provided:
                self.last_errors.append(
                    f"Llaves incorrectas: SAP espera {list(required_keys)}, "
                    f"se enviaron {list(provided)}."
                )
                return False

        return True

    # ── CSRF ─────────────────────────────────────────────────────────────────

    def _get_csrf_token(self) -> str:
        headers = {"X-CSRF-Token": "Fetch", "Accept": "application/json"}
        try:
            resp = self.session.get(
                self.base_url,
                headers=headers,
                params={"sap-client": self.sap_client},
                verify=False,
            )
            resp.raise_for_status()
            if "<html" in resp.text.lower() or "Anmeldung fehlgeschlagen" in resp.text:
                return ""
            return resp.headers.get("x-csrf-token", "")
        except requests.exceptions.RequestException:
            return ""

    # ── GET con paginación automática ────────────────────────────────────────

    def get_data(
        self,
        entity_set: str,
        keys: Optional[Dict] = None,
        filters: Optional[str] = None,
        expand: Optional[str] = None,
        select: Optional[str] = None,
        top: Optional[int] = None,
    ) -> Tuple[Optional[List[Dict]], List[str]]:
        props_to_validate: List[str] = []
        if select:
            props_to_validate.extend(s.strip() for s in select.split(","))
        if expand:
            props_to_validate.extend(e.strip() for e in expand.split(","))
        if filters:
            filter_fields = re.findall(
                r"([a-zA-Z0-9_]+)\s+(?:eq|ne|gt|ge|lt|le)\s+", filters
            )
            props_to_validate.extend(filter_fields)

        keys_to_check = list(keys.keys()) if keys else None
        if not self.validate_entity_and_properties(
            entity_set,
            properties_to_check=props_to_validate,
            keys_to_check=keys_to_check,
        ):
            return None, self.last_errors

        if keys:
            keys_str = ",".join(f"{k}='{v}'" for k, v in keys.items())
            first_url = f"{self.base_url}/{entity_set}({keys_str})"
            paginate = False
        else:
            first_url = f"{self.base_url}/{entity_set}"
            paginate = True

        headers = {"Accept": "application/json", "Content-Type": "application/json"}

        first_params = self.base_params.copy()
        if filters:
            first_params["$filter"] = filters
        if expand:
            first_params["$expand"] = expand
        if select:
            first_params["$select"] = select

        if not keys:
            if top:
                first_params["$top"] = top
                first_params["$skip"] = 0
            else:
                first_params["$top"] = self.page_size
                first_params["$skip"] = 0

        all_results: List[Dict] = []
        next_url: Optional[str] = first_url
        next_params: Optional[Dict] = first_params
        page_num = 0

        while next_url:
            page_num += 1
            try:
                response = self.session.get(
                    next_url,
                    headers=headers,
                    params=next_params,
                    verify=False,
                    timeout=600,
                )
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.RequestException as e:
                msg = f"Error HTTP en GET (página {page_num}): {e}"
                if hasattr(e, "response") and e.response is not None:
                    msg += f" | Detalle SAP: {e.response.text[:500]}"
                self.last_errors.append(msg)
                return all_results if all_results else None, self.last_errors

            page_results: List[Dict] = []
            if "d" in data:
                d_node = data["d"]
                if "results" in d_node:
                    page_results = d_node["results"]
                else:
                    page_results = [d_node]
            else:
                page_results = data if isinstance(data, list) else []

            all_results.extend(page_results)

            if not paginate or top:
                break

            if len(page_results) < self.page_size:
                break

            raw_next: Optional[str] = None
            if "d" in data:
                raw_next = data["d"].get("__next")
            if not raw_next:
                raw_next = data.get("@odata.nextLink")

            if raw_next:
                next_url = raw_next
                next_params = None
            else:
                next_params = first_params.copy()
                next_params["$skip"] = len(all_results)
                next_url = first_url

        return all_results, self.last_errors

    # ── Batch ────────────────────────────────────────────────────────────────

    def _build_batch_body(
        self,
        entity_set: str,
        items: Optional[List[Dict[str, str]]] = None,
        expand: Optional[str] = None,
        use_filters: bool = False,
        raw_filters: Optional[List[str]] = None,
    ) -> str:
        boundary = "batch_partida"
        lines: List[str] = []

        # Determinamos sobre qué lista iterar (strings crudos o diccionarios)
        elementos = raw_filters if raw_filters else (items or [])

        for item in elementos:
            lines.append(f"--{boundary}")
            lines.append("Content-Type: application/http")
            lines.append("Content-Transfer-Encoding: binary")
            lines.append("")

            query_params = []
            if expand:
                query_params.append(f"$expand={expand}")

            if raw_filters:
                # Usamos el string exacto que viene en la lista y lo codificamos
                filters_str_encoded = quote(item)
                query_params.append(f"$filter={filters_str_encoded}")
                q_string = "?" + "&".join(query_params)
                lines.append(f"GET {entity_set}{q_string} HTTP/1.1")

            elif use_filters and isinstance(item, dict):
                filters_str = " and ".join(f"{k} eq '{v}'" for k, v in item.items())
                filters_str_encoded = quote(filters_str)
                query_params.append(f"$filter={filters_str_encoded}")
                q_string = "?" + "&".join(query_params)
                lines.append(f"GET {entity_set}{q_string} HTTP/1.1")

            elif isinstance(item, dict):
                keys_str = ",".join(f"{k}='{v}'" for k, v in item.items())
                keys_str_encoded = quote(keys_str)
                q_string = "?" + "&".join(query_params) if query_params else ""
                lines.append(f"GET {entity_set}({keys_str_encoded}){q_string} HTTP/1.1")

            lines.append("Accept: application/json")
            lines.append("sap-context-accept: header")
            lines.append("")
            lines.append("")

        lines.append(f"--{boundary}--")
        return "\r\n".join(lines)

    def _parse_batch_response(self, raw_text: str) -> List[Dict]:
        results: List[Dict] = []
        for line in raw_text.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    data = json.loads(line)
                    if "d" in data:
                        d_node = data["d"]
                        if "results" in d_node:
                            # Cuando se usa $filter, OData devuelve un array "results"
                            results.extend(d_node["results"])
                        else:
                            # Cuando se usa búsqueda por llave, devuelve un objeto directo
                            results.append(d_node)
                    elif "error" in data:
                        msg = (
                            data["error"]
                            .get("message", {})
                            .get("value", "Error desconocido")
                        )
                        self.last_errors.append(f"Error interno Batch SAP: {msg}")
                except json.JSONDecodeError:
                    continue
        return results

    def execute_batch(
        self,
        entity_set: str,
        items: Optional[List[Dict[str, str]]] = None,
        expand: Optional[str] = None,
        use_filters: bool = False,
        raw_filters: Optional[List[str]] = None,
    ) -> Tuple[List[Dict], List[str]]:
        props_to_validate: List[str] = []
        if expand:
            props_to_validate.extend(e.strip() for e in expand.split(","))

        # Validación simplificada: Si usamos raw_filters, confiamos en la construcción externa por ahora
        if not raw_filters and items:
            if use_filters:
                props_to_validate.extend(items[0].keys())
                keys_to_check = None
            else:
                keys_to_check = list(items[0].keys())

            if not self.validate_entity_and_properties(
                entity_set,
                properties_to_check=props_to_validate,
                keys_to_check=keys_to_check,
            ):
                return [], self.last_errors

        url = f"{self.base_url}/$batch"
        csrf_token = self._get_csrf_token()
        if not csrf_token:
            self.last_errors.append("Fallo al obtener Token CSRF.")
            return [], self.last_errors

        # NUEVO: Agregado el Accept-Encoding para ayudar con el manejo de compresión y evitar IncompleteRead
        headers = {
            "Content-Type": "multipart/mixed; boundary=batch_partida",
            "Accept": "multipart/mixed",
            "X-CSRF-Token": csrf_token,
            "Accept-Encoding": "gzip, deflate",
        }

        payload = self._build_batch_body(
            entity_set, items, expand, use_filters, raw_filters
        )

        # NUEVO: Bloque de reintento automático para manejar caídas y cortes de red de SAP
        max_reintentos = 3
        for intento in range(max_reintentos):
            try:
                response = self.session.post(
                    url,
                    headers=headers,
                    params={"sap-client": self.sap_client},
                    data=payload,
                    verify=False,
                    timeout=600,
                )
                response.raise_for_status()
                return self._parse_batch_response(response.text), self.last_errors

            except requests.exceptions.ChunkedEncodingError as e:
                # Atrapamos específicamente el error de IncompleteRead
                if intento < max_reintentos - 1:
                    time.sleep(2)  # Espera 2 segundos antes de reintentar
                    continue
                self.last_errors.append(
                    f"IncompleteRead tras {max_reintentos} intentos (Batch SAP): {e}"
                )
                return [], self.last_errors

            except requests.exceptions.HTTPError as e:
                if response is not None and response.status_code in [
                    408,
                    502,
                    503,
                    504,
                ]:
                    if intento < max_reintentos - 1:
                        time.sleep(3)
                        continue
                self.last_errors.append(
                    f"Error HTTP {response.status_code if response else ''} en POST (Batch): {e}"
                )
                return [], self.last_errors

            except requests.exceptions.RequestException as e:
                self.last_errors.append(f"Error HTTP en POST (Batch): {e}")
                return [], self.last_errors

        return [], self.last_errors


# ── Cliente de Tasa BCV ─────────────────────────────────────────────────────
class SAPTasaBCVClient:
    """
    Cliente REST (fmcall) para el endpoint ZFI_TASA_BCV.
    """

    def __init__(
        self,
        username: str,
        password: str,
        sap_client: str,
    ):
        self.endpoint_url = SAPServiceURL.Tasa_BCV
        self.username = username
        self.password = password
        self.sap_client = sap_client
        self._cache: Dict[str, list] = {}

    def _normalizar_fecha(self, fecha: Union[str, datetime, date]) -> str:
        """Normaliza cualquier formato de fecha a 'YYYYMMDD'."""
        # 1. Manejo de objetos de fecha nativos (datetime, date, pandas, etc.)
        if hasattr(fecha, "strftime"):
            return fecha.strftime("%Y%m%d")
        # 2. Manejo de cadenas de texto (strings)
        if isinstance(fecha, str):
            clean = fecha.replace("-", "").replace("/", "").strip()
            if len(clean) == 8:
                try:
                    # Intentamos validar primero como YYYYMMDD
                    return datetime.strptime(clean, "%Y%m%d").strftime("%Y%m%d")
                except ValueError:
                    pass  # Si falla, no es YYYYMMDD, seguimos intentando
                try:
                    # Intentamos validar como DDMMYYYY
                    return datetime.strptime(clean, "%d%m%Y").strftime("%Y%m%d")
                except ValueError:
                    # Si falla, no es un formato válido reconocido
                    pass
            return clean  # Retorna limpio si no mide 8 caracteres o no se pudo parsear
        # 3. Manejo de tipos de datos incorrectos (evita retornar None en silencio)
        raise TypeError(f"Tipo de dato no soportado para procesar fecha: {type(fecha)}")

    def obtener_tasas(self, fecha) -> list:
        """
        Devuelve la lista de tasas para `fecha`.
        """
        fecha_key = self._normalizar_fecha(fecha)

        if fecha_key in self._cache:
            return self._cache[fecha_key]

        params = {
            "sap-client": self.sap_client,
            "DATE": fecha_key,
        }
        try:
            response = requests.get(
                self.endpoint_url,
                auth=HTTPBasicAuth(self.username, self.password),
                params=params,
                verify=False,
                timeout=600,
            )
            if response.status_code == 200:
                data = response.json()
                tasas = data.get("TAB_TASA_MONEDAS", [])
                self._cache[fecha_key] = tasas
                return tasas
            elif response.status_code == 400:
                error_data = response.json()
                print(
                    f"Bad Request SAP (400): {error_data.get('ERROR_MESSAGE', 'Faltan parámetros')}"
                )
                self._cache[fecha_key] = []
                return []
            else:
                response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error de conexión al consultar Tasa BCV: {e}")
            return []

        return []

    def obtener_tasas_lote(self, fechas: list) -> Dict[str, list]:
        """
        Recibe una lista de fechas, deduplica internamente y hace UN solo GET por fecha.
        """
        fechas_norm = list({self._normalizar_fecha(f) for f in fechas})
        for f in fechas_norm:
            if f not in self._cache:
                self.obtener_tasas(f)
        return {f: self._cache.get(f, []) for f in fechas_norm}


# ── Helper de formato de fecha OData ───────────────────────────────────────
def fecha_sap(fecha_str: str) -> str:
    """Convierte 'YYYY-MM-DD' al literal OData: datetime'YYYY-MM-DDTHH:MM:SS'."""
    try:
        fecha_obj = datetime.strptime(fecha_str, "%Y-%m-%d")
        return f"datetime'{fecha_obj.strftime('%Y-%m-%dT%H:%M:%S')}'"
    except ValueError:
        return f"Error: formato incorrecto para '{fecha_str}'."
