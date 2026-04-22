# core/urls.py
from django.urls import path

from . import views

urlpatterns = [
    path("dashboard/", views.dashboard_view, name="dashboard"),
    path("sync-manual/", views.disparar_sincronizacion, name="sync_manual"),
    path("sync-paso8/", views.disparar_paso8_manual, name="sync_paso8"),
    path(
        "api/detalle-asientos/", views.detalle_asientos_api, name="detalle_asientos_api"
    ),
    path(
        "api/detalle-documento/",
        views.detalle_documento_api,
        name="detalle_documento_api",
    ),
    path(
        "api/notificaciones/", views.leer_notificaciones_api, name="api_notificaciones"
    ),
]
