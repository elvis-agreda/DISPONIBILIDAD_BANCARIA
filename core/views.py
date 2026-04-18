import json 
from datetime import datetime, timedelta

from django.shortcuts import render
from django.http import JsonResponse
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_POST
from django.utils import timezone

from core.models import DashboardConsolidado
from sap_sync.models import TasaBCV
from sap_sync.tasks import ejecutar_sync_sap

@login_required
def dashboard_view(request):
    # Definimos el rango (ej: mes actual)
    hoy = timezone.now().date()
    inicio_mes = hoy.replace(day=1)
    fin_mes = (inicio_mes + timedelta(days=32)).replace(day=1) - timedelta(days=1)

    # Obtenemos los datos base
    registros = DashboardConsolidado.objects.filter(
        fecha_contabilizacion__range=[inicio_mes, fin_mes]
    )

    # Agrupamos por categoría y fecha para la matriz
    matriz_datos = {}
    categorias = registros.values_list('categoria', flat=True).distinct()
    
    for cat in categorias:
        matriz_datos[cat] = {}
        for reg in registros.filter(categoria=cat):
            fecha_str = reg.fecha_contabilizacion.isoformat()
            matriz_datos[cat][fecha_str] = matriz_datos[cat].get(fecha_str, 0) + float(reg.monto_total)

    context = {
        'matriz': matriz_datos,
        'categorias': categorias,
        'hoy': hoy,
        'colores': {'azul': '#3c4295', 'verde': '#6eb43f'}
    }
    return render(request, 'core/dashboard.html', context)

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
            tipo="MANUAL"
        )

        return JsonResponse({"status": "Sincronización iniciada correctamente"})
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)