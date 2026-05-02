import json
from calendar import monthrange
from datetime import date, datetime, timedelta

from django.contrib.auth.decorators import login_required
from django.db.models import Q, Sum
from django.http import JsonResponse
from django.shortcuts import render
from django.utils import timezone
from django.views.decorators.http import require_POST

from core.models import ColumnaDrillDown, DashboardConsolidado, Notificacion
from sap_sync.models import PartidaPosicion, TasaBCV, EntidadContable, SincronizacionLog
from sap_sync.tasks import ejecutar_paso8_manual, ejecutar_sync_sap


@login_required
def leer_notificaciones_api(request):
    nots = Notificacion.objects.filter(usuario=request.user, leida=False)

    data = []
    for n in nots:
        data.append({"id": n.id, "mensaje": n.mensaje, "tipo": n.tipo})

    nots.update(leida=True)
    
    tareas_activas = SincronizacionLog.objects.filter(estado__in=["INICIADO", "EN_CURSO"]).exists()
    
    return JsonResponse({"notificaciones": data, "tareas_activas": tareas_activas})


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

    # ⚡ NUEVO: Obtener monedas únicas disponibles para el select dinámico
    monedas_db = TasaBCV.objects.values_list("moneda", flat=True).distinct()
    monedas_consolidadas = set(m.upper() for m in monedas_db if m)

    # Aseguramos que la moneda local principal siempre esté
    monedas_consolidadas.add("VED")

    # Limpiamos nomenclaturas redundantes si existen en la BD
    if "VES" in monedas_consolidadas:
        monedas_consolidadas.remove("VES")
    if "BS" in monedas_consolidadas:
        monedas_consolidadas.remove("BS")

    # Ordenamos la lista (puedes poner VES de primero si lo deseas con una lógica extra)
    monedas_disponibles = sorted(list(monedas_consolidadas))

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
                "tipo_operacion": reg.tipo_operacion,
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
        # ⚡ NUEVO: Mapeamos los días forzosamente a español
        dias_espanol = ["LUN", "MAR", "MIE", "JUE", "VIE", "SAB", "DOM"]
        nombre_dia = dias_espanol[cursor.weekday()]
        semana_actual["dias"].append(
            {
                "fecha_str": fecha_str,
                "fecha_obj": cursor,
                "numero_dia": cursor.day,
                "nombre_dia": nombre_dia,
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
        
        totales_netos_dias = {}
        totales_netos_semana = {}
        total_neto_mes = 0.0
        
        totales_ingreso_dias = {}
        totales_ingreso_semana = {}
        total_ingreso_mes = 0.0

        totales_egreso_dias = {}
        totales_egreso_semana = {}
        total_egreso_mes = 0.0
        
        for cat_data in categorias_ordenadas.values():
            is_ingreso = cat_data.get("tipo_operacion") == "INGRESO"
            sign = 1 if is_ingreso else -1
            
            for dia, monto in cat_data["dias"].items():
                totales_netos_dias[dia] = totales_netos_dias.get(dia, 0.0) + (monto * sign)
                if is_ingreso:
                    totales_ingreso_dias[dia] = totales_ingreso_dias.get(dia, 0.0) + monto
                else:
                    totales_egreso_dias[dia] = totales_egreso_dias.get(dia, 0.0) + monto
                
            for sem, monto in cat_data["totales_semana"].items():
                totales_netos_semana[sem] = totales_netos_semana.get(sem, 0.0) + (monto * sign)
                if is_ingreso:
                    totales_ingreso_semana[sem] = totales_ingreso_semana.get(sem, 0.0) + monto
                else:
                    totales_egreso_semana[sem] = totales_egreso_semana.get(sem, 0.0) + monto
                
            total_neto_mes += (cat_data["total_mes"] * sign)
            if is_ingreso:
                total_ingreso_mes += cat_data["total_mes"]
            else:
                total_egreso_mes += cat_data["total_mes"]

        matriz_final.append({
            "moneda": moneda, 
            "categorias": categorias_ordenadas,
            "totales_netos_dias": totales_netos_dias,
            "totales_netos_semana": totales_netos_semana,
            "total_neto_mes": total_neto_mes,
            "totales_ingreso_dias": totales_ingreso_dias,
            "totales_ingreso_semana": totales_ingreso_semana,
            "total_ingreso_mes": total_ingreso_mes,
            "totales_egreso_dias": totales_egreso_dias,
            "totales_egreso_semana": totales_egreso_semana,
            "total_egreso_mes": total_egreso_mes,
        })

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
            "monedas_disponibles": monedas_disponibles,
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

    # Enriquecer con nombres de EntidadContable
    codigos_socio = set()
    for row in asientos:
        if row.get("lifnr"):
            codigos_socio.add(row["lifnr"])
        if row.get("kunnr"):
            codigos_socio.add(row["kunnr"])

    if codigos_socio:
        entidades = EntidadContable.objects.filter(codigo__in=codigos_socio)
        nombres_socio = {e.codigo: e.nombre for e in entidades}
        for row in asientos:
            if row.get("lifnr"):
                codigo = row["lifnr"]
                nombre = nombres_socio.get(codigo) or nombres_socio.get(codigo.lstrip("0"))
                if nombre:
                    row["lifnr"] = f"{codigo} - {nombre}"
            if row.get("kunnr"):
                codigo = row["kunnr"]
                nombre = nombres_socio.get(codigo) or nombres_socio.get(codigo.lstrip("0"))
                if nombre:
                    row["kunnr"] = f"{codigo} - {nombre}"

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

    codigos_socio = set()
    for pos in posiciones:
        if pos.lifnr:
            codigos_socio.add(pos.lifnr)
        if pos.kunnr:
            codigos_socio.add(pos.kunnr)
            
    nombres_socio = {}
    if codigos_socio:
        entidades = EntidadContable.objects.filter(codigo__in=codigos_socio)
        nombres_socio = {e.codigo: e.nombre for e in entidades}

    datos = []
    for pos in posiciones:
        codigo_socio = pos.lifnr or pos.kunnr or ""
        nombre_socio = nombres_socio.get(codigo_socio) or nombres_socio.get(codigo_socio.lstrip("0")) if codigo_socio else ""
        socio_display = f"{codigo_socio} - {nombre_socio}" if nombre_socio else codigo_socio
        
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
                "socio": socio_display,
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

        ejecutar_paso8_manual(fecha_inicio, fecha_fin, usuario_id=request.user.id)

        return JsonResponse(
            {"status": "Cálculo de Disponibilidad (Paso 8) iniciado correctamente"}
        )
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
