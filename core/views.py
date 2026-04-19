import json 
from datetime import date, datetime, timedelta
from calendar import monthrange
from django.shortcuts import render
from django.http import JsonResponse
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_POST
from django.utils import timezone
from sap_sync.tasks import ejecutar_sync_sap, ejecutar_paso8_manual
from core.models import DashboardConsolidado
from sap_sync.models import TasaBCV
from sap_sync.tasks import ejecutar_sync_sap
from .models import ColumnaDrillDown

@login_required
def dashboard_view(request):
    hoy = timezone.now().date()
    mes_str = str(request.GET.get('mes', hoy.month)).strip()
    anio_str = str(request.GET.get('anio', hoy.year)).replace('\xa0', '').replace(' ', '').replace('.', '').replace(',', '')
    
    mes_sel = int(mes_str)
    anio_sel = int(anio_str)
    
    inicio_mes = date(anio_sel, mes_sel, 1)
    _, ultimo_dia = monthrange(anio_sel, mes_sel)
    fin_mes = date(anio_sel, mes_sel, ultimo_dia)

    registros = DashboardConsolidado.objects.filter(
        fecha_contabilizacion__range=[inicio_mes, fin_mes]
    )

    categorias = registros.values_list('categoria', flat=True).order_by('categoria').distinct()
    
    # 1. Estructurar matriz con totales
    matriz_datos = {}
    for cat in categorias:
        matriz_datos[cat] = {'dias': {}, 'totales_semana': {}, 'total_mes': 0.0}
        for reg in registros.filter(categoria=cat):
            fecha_str = reg.fecha_contabilizacion.isoformat()
            monto = float(reg.monto_total)
            matriz_datos[cat]['dias'][fecha_str] = matriz_datos[cat]['dias'].get(fecha_str, 0) + monto
            matriz_datos[cat]['total_mes'] += monto

    # 2. Agrupar días en semanas y calcular acumulados
    semanas = []
    semana_actual = {'id': 1, 'dias': []}
    cursor = inicio_mes
    week_id = 1
    
    while cursor <= fin_mes:
        tasa = TasaBCV.objects.filter(fecha=cursor).first()
        semana_actual['dias'].append({
            'fecha_str': cursor.isoformat(),
            'fecha_obj': cursor,
            'numero_dia': cursor.day,
            'tasa_bcv': tasa.tasa if tasa else None
        })
        
        # Si es Domingo (6) o fin de mes, cerramos la semana
        if cursor.weekday() == 6 or cursor == fin_mes:
            semanas.append(semana_actual)
            
            # Calcular total de esta semana por categoría
            for cat in categorias:
                suma_sem = sum(matriz_datos[cat]['dias'].get(d['fecha_str'], 0) for d in semana_actual['dias'])
                matriz_datos[cat]['totales_semana'][str(week_id)] = suma_sem
                
            week_id += 1
            semana_actual = {'id': week_id, 'dias': []}
            
        cursor += timedelta(days=1)

    meses_lista = [(1,'Enero'),(2,'Febrero'),(3,'Marzo'),(4,'Abril'),(5,'Mayo'),(6,'Junio'),(7,'Julio'),(8,'Agosto'),(9,'Septiembre'),(10,'Octubre'),(11,'Noviembre'),(12,'Diciembre')]
    
    return render(request, 'core/dashboard.html', {
        'matriz': matriz_datos,
        'categorias': categorias,
        'semanas': semanas,
        'mes_sel': mes_sel,
        'anio_sel': anio_sel,
        'meses_lista': meses_lista,
        'anios_lista': range(hoy.year - 5, hoy.year + 2),
        'hoy': hoy
    })

@require_POST
@login_required
def disparar_sincronizacion(request):
    try:
        data = json.loads(request.body)
        # Obtenemos las fechas del request
        fecha_inicio_str = data.get('fecha_inicio')
        fecha_fin_str = data.get('fecha_fin')

        if not fecha_inicio_str or not fecha_fin_str:
            return JsonResponse({"error": "Rango de fechas incompleto"}, status=400)

        # Convertimos a objetos date
        fecha_inicio = datetime.strptime(fecha_inicio_str, '%Y-%m-%d').date()
        fecha_fin = datetime.strptime(fecha_fin_str, '%Y-%m-%d').date()

        # Disparamos la tarea de Huey (esto corre en el worker)
        ejecutar_sync_sap(
            fecha_inicio=fecha_inicio, 
            fecha_fin=fecha_fin, 
            tipo="MANUAL",
            usuario_id=request.user.id
        )

        return JsonResponse({"status": "Sincronización iniciada correctamente"})
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)

@login_required
def detalle_asientos_api(request):
    categoria = request.GET.get('categoria')
    fecha = request.GET.get('fecha')
    
    config_columnas = ColumnaDrillDown.objects.filter(activo=True).order_by('orden')
    
    # Creamos una lista de diccionarios con campo y tipo
    columnas_info = []
    for c in config_columnas:
        columnas_info.append({
            'campo': c.campo_bd,
            'etiqueta': c.etiqueta,
            'tipo': c.tipo_dato
        })

    campos_para_query = [c.campo_bd for c in config_columnas]
    asientos = DashboardConsolidado.objects.filter(
        categoria=categoria, 
        fecha_contabilizacion=fecha
    ).values(*campos_para_query)
    
    return JsonResponse({
        "columnas": columnas_info,
        "datos": list(asientos)
    })
@require_POST
@login_required
def disparar_paso8_manual(request):
    try:
        data = json.loads(request.body)
        fecha_inicio_str = data.get('fecha_inicio')
        fecha_fin_str = data.get('fecha_fin')

        if not fecha_inicio_str or not fecha_fin_str:
            return JsonResponse({"error": "Rango de fechas incompleto"}, status=400)

        # Convertimos a objetos date
        fecha_inicio = datetime.strptime(fecha_inicio_str, '%Y-%m-%d').date()
        fecha_fin = datetime.strptime(fecha_fin_str, '%Y-%m-%d').date()

        # Disparamos la tarea de Huey exclusiva del Paso 8
        ejecutar_paso8_manual(fecha_inicio, fecha_fin)

        return JsonResponse({"status": "Cálculo de Disponibilidad (Paso 8) iniciado correctamente"})
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)