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
from sap_sync.models import PartidaPosicion, TasaBCV
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
    vista = request.GET.get("vista", "SEPARADO")

    mes_sel = int(mes_str)
    anio_sel = int(anio_str)

    inicio_mes = date(anio_sel, mes_sel, 1)
    _, ultimo_dia = monthrange(anio_sel, mes_sel)
    fin_mes = date(anio_sel, mes_sel, ultimo_dia)

    registros = DashboardConsolidado.objects.filter(
        fecha_contabilizacion__range=[inicio_mes, fin_mes]
    )

    # 1. ⚡ CARGA LITERAL DESDE LA BASE DE DATOS
    tasas_dict = {}
    cursor = inicio_mes
    while cursor <= fin_mes:
        # Inicializamos el día
        tasas_dict[cursor.isoformat()] = {}
        # Asumimos que las monedas locales siempre valen 1 contra sí mismas por si faltan en la tabla
        tasas_dict[cursor.isoformat()]["VES"] = 1.0
        tasas_dict[cursor.isoformat()]["VED"] = 1.0
        tasas_dict[cursor.isoformat()]["BS"] = 1.0
        cursor += timedelta(days=1)

    tasas_db = TasaBCV.objects.filter(fecha__range=[inicio_mes, fin_mes])
    for t in tasas_db:
        f_str = t.fecha.isoformat()
        # ⚡ TOMAMOS LA MONEDA LITERAL DE LA BASE DE DATOS (USD, EUR, ETC)
        mon = t.moneda.upper()
        tasas_dict[f_str][mon] = float(t.tasa) if t.tasa else 1.0

    matriz_por_moneda = {}

    for reg in registros:
        # ⚡ TOMAMOS LA MONEDA LITERAL DEL ASIENTO
        moneda_origen = (reg.rwcur or "S/M").upper()

        cat_base = reg.categoria
        fecha_str = reg.fecha_contabilizacion.isoformat()
        monto_original = float(reg.monto_total)

        if vista == "SEPARADO":
            llave_destino = moneda_origen
            monto_final = monto_original
        else:
            llave_destino = vista

            # MATEMÁTICA PURA: Origen -> Moneda Local -> Destino
            tasa_origen_val = tasas_dict[fecha_str].get(moneda_origen, 1.0)
            tasa_destino_val = tasas_dict[fecha_str].get(llave_destino, 1.0)

            monto_en_local = monto_original * tasa_origen_val
            monto_final = monto_en_local / (
                tasa_destino_val if tasa_destino_val > 0 else 1.0
            )

        if llave_destino not in matriz_por_moneda:
            matriz_por_moneda[llave_destino] = {}

        if cat_base not in matriz_por_moneda[llave_destino]:
            matriz_por_moneda[llave_destino][cat_base] = {
                "dias": {},
                "totales_semana": {},
                "total_mes": 0.0,
                "categoria_base": cat_base,
                "moneda": llave_destino,
            }

        matriz_por_moneda[llave_destino][cat_base]["dias"][fecha_str] = (
            matriz_por_moneda[llave_destino][cat_base]["dias"].get(fecha_str, 0)
            + monto_final
        )
        matriz_por_moneda[llave_destino][cat_base]["total_mes"] += monto_final

    semanas = []
    semana_actual = {"id": 1, "dias": [], "tiene_hoy": False}
    cursor = inicio_mes
    week_id = 1

    while cursor <= fin_mes:
        if cursor == hoy:
            semana_actual["tiene_hoy"] = True

        fecha_str = cursor.isoformat()

        semana_actual["dias"].append(
            {
                "fecha_str": fecha_str,
                "fecha_obj": cursor,
                "numero_dia": cursor.day,
                # Pintamos USD y EUR si existen, sino devuelve None
                "tasa_usd": tasas_dict.get(fecha_str, {}).get("USD"),
                "tasa_eur": tasas_dict.get(fecha_str, {}).get("EUR"),
            }
        )

        if cursor.weekday() == 6 or cursor == fin_mes:
            semanas.append(semana_actual)
            for moneda, cats in matriz_por_moneda.items():
                for cat, data in cats.items():
                    suma_sem = sum(
                        data["dias"].get(d["fecha_str"], 0)
                        for d in semana_actual["dias"]
                    )
                    data["totales_semana"][str(week_id)] = suma_sem

            week_id += 1
            semana_actual = {"id": week_id, "dias": [], "tiene_hoy": False}

        cursor += timedelta(days=1)

    matriz_final = []
    for moneda in sorted(matriz_por_moneda.keys()):
        categorias_ordenadas = {
            k: matriz_por_moneda[moneda][k]
            for k in sorted(matriz_por_moneda[moneda].keys())
        }
        matriz_final.append({"moneda": moneda, "categorias": categorias_ordenadas})

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
            "matriz_final": matriz_final,
            "semanas": semanas,
            "mes_sel": mes_sel,
            "anio_sel": anio_sel,
            "vista_sel": vista,
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
    moneda = request.GET.get("moneda")
    fecha = request.GET.get("fecha")
    vista = request.GET.get("vista", "SEPARADO")
    page = int(request.GET.get("page", 1))
    search = request.GET.get("search", "").strip()
    sort_by = request.GET.get("sort", "")
    order = request.GET.get("order", "asc")
    per_page = 50

    config_columnas = ColumnaDrillDown.objects.filter(activo=True).order_by("orden")

    columnas_info = [
        {
            "campo": c.campo_bd,
            "etiqueta": c.etiqueta,
            "tipo": c.tipo_dato,
            "es_buscable": c.es_buscable,
            "abre_documento": c.abre_documento,
        }
        for c in config_columnas
    ]

    campos_para_query = list(
        set([c.campo_bd for c in config_columnas] + ["monto_total", "rwcur"])
    )

    qs = DashboardConsolidado.objects.filter(
        categoria=categoria, fecha_contabilizacion=fecha
    )

    # ⚡ FIX: Solo se filtran los Drill-Downs si la vista no es consolidada
    if vista == "SEPARADO":
        qs = qs.filter(rwcur=moneda)

    if search:
        q_obj = Q()
        for c in config_columnas:
            if c.tipo_dato == "TEXTO" or c.es_buscable or c.abre_documento:
                q_obj |= Q(**{f"{c.campo_bd}__icontains": search})
        qs = qs.filter(q_obj)

    if sort_by in [c.campo_bd for c in config_columnas]:
        if order == "desc":
            qs = qs.order_by(f"-{sort_by}")
        else:
            qs = qs.order_by(sort_by)

    total_records = qs.count()

    totales_qs = qs.order_by().values("rwcur").annotate(total=Sum("monto_total"))
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


@login_required
def detalle_documento_api(request):
    belnr = request.GET.get("belnr")
    augbl = request.GET.get("augbl")

    if augbl:
        posiciones = (
            PartidaPosicion.objects.filter(augbl=augbl)
            .select_related("partida")
            .order_by("partida__belnr", "docln")
        )
    else:
        posiciones = (
            PartidaPosicion.objects.filter(partida__belnr=belnr)
            .select_related("partida")
            .order_by("docln")
        )

    datos = []
    for pos in posiciones:
        datos.append(
            {
                "belnr": pos.partida.belnr,
                "pos": pos.docln,
                "cuenta": pos.ractt,
                "monto": float(pos.wsl),
                "moneda": pos.rwcur or "",
                "dh": pos.drcrk or "",
                "referencia": pos.zuonr or "",
                "compensacion": pos.augbl or "",
                "socio": pos.lifnr or pos.kunnr or "",
            }
        )

    return JsonResponse({"datos": datos})


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
