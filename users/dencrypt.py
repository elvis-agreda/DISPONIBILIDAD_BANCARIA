import base64
import hashlib
import hmac
import os


def sap_issha_verify(password: str, stored_hash: str) -> bool:
    header_end = stored_hash.index("}")
    iterations = int(stored_hash[1:header_end].split(",")[1].strip())
    raw = base64.b64decode(stored_hash[header_end + 1 :])
    stored_hv = raw[:20]
    salt = raw[20:]
    pw = password.encode("utf-8")

    # Iteración 1: SHA1(password + salt)
    result = hashlib.sha1(pw + salt).digest()
    # Iteraciones 2..n: SHA1(password + resultado_anterior)
    for _ in range(2, iterations + 1):
        result = hashlib.sha1(pw + result).digest()
    return hmac.compare_digest(result, stored_hv)


def sap_issha_hash(password: str, salt: bytes = None, iterations: int = 1024) -> str:
    if salt is None:
        salt = os.urandom(12)

    pw = password.encode("utf-8")

    result = hashlib.sha1(pw + salt).digest()
    for _ in range(2, iterations + 1):
        result = hashlib.sha1(pw + result).digest()

    encoded = base64.b64encode(result + salt).decode("utf-8")
    return f"{{x-issha, {iterations}}}{encoded}"


#  Prueba

# stored = "{x-issha, 1024}NRk0cOnwqtPEAi5dQCEA5QAs3LRcrr05FDVn91gqHkM="
# password = "Mayo2026*"
