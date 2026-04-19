# core/urls.py
from django.urls import path
from . import views

urlpatterns = [
    path('dashboard/', views.dashboard_view, name='dashboard'),
    path('sync-manual/', views.disparar_sincronizacion, name='sync_manual'),
    path('sync-paso8/', views.disparar_paso8_manual, name='sync_paso8'), # <-- NUEVA RUTA
    path('api/detalle-asientos/', views.detalle_asientos_api, name='detalle_asientos_api'),
]