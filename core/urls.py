from django.urls import path
from . import views

urlpatterns = [
    path('dashboard/', views.dashboard_view, name='dashboard'),
    path('sync-manual/', views.disparar_sincronizacion, name='sync_manual'),
]