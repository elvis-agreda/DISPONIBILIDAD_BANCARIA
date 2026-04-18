from datetime import date

def sap_date_to_python(date_str: str) -> date | None:
    if not date_str or date_str == "00000000" or len(date_str) < 8:
        return None
    try:
        return date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
    except ValueError:
        return None