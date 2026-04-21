import json
from calendar import monthrange
from datetime import date, datetime, timedelta

from django.contrib.auth.decorators import login_required
from django.db.models import Q, Sum
from django.http import JsonResponse
from django.shortcuts import render
from django.utils import timezone
from django.views.decorators.http import require_POST

from core.models import ColumnaDrillDown, DashboardConsolidado
from sap_sync.models import TasaBCV
from sap_sync.tasks import ejecutar_paso8_manual, ejecutar_sync_sap


@login_required
def dashboard_view(request):
    hoy = timezone.now().date()
    mes_str = str(request.GET.get("mes", hoy.month)).strip()
    anio_str = (
        str(request.GET.get("anio", hoy.year))
        .replace("\xa0", "")
        .replace(" ", "")
        .replace(".", "")
        .replace(",", "")
    )

    mes_sel = int(mes_str)
    anio_sel = int(anio_str)

    inicio_mes = date(anio_sel, mes_sel, 1)
    _, ultimo_dia = monthrange(anio_sel, mes_sel)
    fin_mes = date(anio_sel, mes_sel, ultimo_dia)

    registros = DashboardConsolidado.objects.filter(
        fecha_contabilizacion__range=[inicio_mes, fin_mes]
    )

    # ⚡ SEPARACIÓN POR CATEGORÍA Y MONEDA
    matriz_datos = {}
    for reg in registros:
        moneda = reg.rwcur or "S/M"
        cat_base = reg.categoria
        llave = f"{cat_base} ({moneda})"

        if llave not in matriz_datos:
            matriz_datos[llave] = {
                "dias": {},
                "totales_semana": {},
                "total_mes": 0.0,
                "categoria_base": cat_base,
                "moneda": moneda,
            }

        fecha_str = reg.fecha_contabilizacion.isoformat()
        monto = float(reg.monto_total)
        matriz_datos[llave]["dias"][fecha_str] = (
            matriz_datos[llave]["dias"].get(fecha_str, 0) + monto
        )
        matriz_datos[llave]["total_mes"] += monto

    # Agrupar días en semanas y calcular acumulados
    semanas = []
    semana_actual = {"id": 1, "dias": []}
    cursor = inicio_mes
    week_id = 1

    while cursor <= fin_mes:
        tasa = TasaBCV.objects.filter(fecha=cursor).first()
        semana_actual["dias"].append(
            {
                "fecha_str": cursor.isoformat(),
                "fecha_obj": cursor,
                "numero_dia": cursor.day,
                "tasa_bcv": tasa.tasa if tasa else None,
            }
        )

        if cursor.weekday() == 6 or cursor == fin_mes:
            semanas.append(semana_actual)
            for cat in matriz_datos.keys():
                suma_sem = sum(
                    matriz_datos[cat]["dias"].get(d["fecha_str"], 0)
                    for d in semana_actual["dias"]
                )
                matriz_datos[cat]["totales_semana"][str(week_id)] = suma_sem

            week_id += 1
            semana_actual = {"id": week_id, "dias": []}

        cursor += timedelta(days=1)

    categorias = sorted(matriz_datos.keys())
    meses_lista = [
        (1, "Enero"),
        (2, "Febrero"),
        (3, "Marzo"),
        (4, "Abril"),
        (5, "Mayo"),
        (6, "Junio"),
        (7, "Julio"),
        (8, "Agosto"),
        (9, "Septiembre"),
        (10, "Octubre"),
        (11, "Noviembre"),
        (12, "Diciembre"),
    ]

    return render(
        request,
        "core/dashboard.html",
        {
            "matriz": matriz_datos,
            "categorias": categorias,
            "semanas": semanas,
            "mes_sel": mes_sel,
            "anio_sel": anio_sel,
            "meses_lista": meses_lista,
            "anios_lista": range(hoy.year - 5, hoy.year + 2),
            "hoy": hoy,
        },
    )


@require_POST
@login_required
def disparar_sincronizacion(request):
    try:
        data = json.loads(request.body)
        fecha_inicio_str = data.get("fecha_inicio")
        fecha_fin_str = data.get("fecha_fin")

        if not fecha_inicio_str or not fecha_fin_str:
            return JsonResponse({"error": "Rango de fechas incompleto"}, status=400)

        fecha_inicio = datetime.strptime(fecha_inicio_str, "%Y-%m-%d").date()
        fecha_fin = datetime.strptime(fecha_fin_str, "%Y-%m-%d").date()

        ejecutar_sync_sap(
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            tipo="MANUAL",
            usuario_id=request.user.id,
        )

        return JsonResponse({"status": "Sincronización iniciada correctamente"})
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def detalle_asientos_api(request):
    categoria = request.GET.get("categoria")
    moneda = request.GET.get("moneda")  # ⚡ Nuevo
    fecha = request.GET.get("fecha")
    page = int(request.GET.get("page", 1))
    search = request.GET.get("search", "").strip()
    sort_by = request.GET.get("sort", "")
    order = request.GET.get("order", "asc")
    per_page = 50

    config_columnas = ColumnaDrillDown.objects.filter(activo=True).order_by("orden")

    columnas_info = [
        {"campo": c.campo_bd, "etiqueta": c.etiqueta, "tipo": c.tipo_dato}
        for c in config_columnas
    ]
    campos_para_query = [c.campo_bd for c in config_columnas]

    # ⚡ Filtramos por Categoría Y Moneda
    qs = DashboardConsolidado.objects.filter(
        categoria=categoria, rwcur=moneda, fecha_contabilizacion=fecha
    )

    if search:
        q_obj = Q()
        for c in config_columnas:
            if c.tipo_dato == "TEXTO" or c.campo_bd in [
                "documento_primario",
                "documento_secundario",
                "lifnr",
                "kunnr",
                "referencia",
                "referencia1",
            ]:
                q_obj |= Q(**{f"{c.campo_bd}__icontains": search})
        qs = qs.filter(q_obj)

    if sort_by in campos_para_query:
        if order == "desc":
            qs = qs.order_by(f"-{sort_by}")
        else:
            qs = qs.order_by(sort_by)

    total_records = qs.count()

    totales_qs = qs.values("rwcur").annotate(total=Sum("monto_total"))
    totales_moneda = {
        item["rwcur"] or "S/M": float(item["total"] or 0) for item in totales_qs
    }

    start = (page - 1) * per_page
    end = start + per_page
    asientos = list(qs.values(*campos_para_query)[start:end])

    pages = (total_records // per_page) + (1 if total_records % per_page > 0 else 0)

    return JsonResponse(
        {
            "columnas": columnas_info,
            "datos": asientos,
            "total": total_records,
            "page": page,
            "pages": pages,
            "totales_moneda": totales_moneda,
        }
    )


@require_POST
@login_required
def disparar_paso8_manual(request):
    try:
        data = json.loads(request.body)
        fecha_inicio_str = data.get("fecha_inicio")
        fecha_fin_str = data.get("fecha_fin")

        if not fecha_inicio_str or not fecha_fin_str:
            return JsonResponse({"error": "Rango de fechas incompleto"}, status=400)

        fecha_inicio = datetime.strptime(fecha_inicio_str, "%Y-%m-%d").date()
        fecha_fin = datetime.strptime(fecha_fin_str, "%Y-%m-%d").date()

        ejecutar_paso8_manual(fecha_inicio, fecha_fin)

        return JsonResponse(
            {"status": "Cálculo de Disponibilidad (Paso 8) iniciado correctamente"}
        )
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
