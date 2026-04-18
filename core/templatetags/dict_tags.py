from django import template

register = template.Library()

@register.filter(name='get_val')
def get_val(dictionary, key):
    """
    Obtiene el valor de un diccionario usando una variable como llave.
    """
    if type(dictionary) is dict:
        return dictionary.get(key)
    return None