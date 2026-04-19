from django import template

register = template.Library()

@register.filter(name='get_val')
def get_val(dictionary, key):
    """Obtiene el valor de un diccionario usando una variable como llave."""
    if type(dictionary) is dict:
        return dictionary.get(key)
    return None

@register.filter(name='formato_ve')
def formato_ve(value):
    """Fuerza matemáticamente el formato 1.234.567,89 (Puntos y comas)"""
    try:
        if value is None or value == '':
            return "0,00"
        v = float(value)
        # Formateo base: 1,234,567.89 -> Invertimos comas y puntos
        return f"{v:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
    except (ValueError, TypeError):
        return value

@register.filter(name='formato_tasa')
def formato_tasa(value):
    """Mantiene los decimales reales pero corta ceros inútiles a la derecha"""
    try:
        if not value: return "-"
        s = f"{float(value):.6f}" # Toma hasta 6 decimales
        # Corta los '0' a la derecha siempre que queden al menos 2 decimales
        while s.endswith('0') and len(s.split('.')[1]) > 2:
            s = s[:-1]
        return s.replace('.', ',')
    except (ValueError, TypeError):
        return value