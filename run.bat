@echo off
TITLE Proyecto Django + Huey Worker

:: 1. Activar el entorno virtual
echo Activando entorno virtual...
CALL venv\Scripts\activate

:: 2. Verificar migraciones y base de datos
echo Verificando base de datos...
python manage.py migrate

:: 3. Iniciar el Worker de Huey en una nueva ventana
echo Iniciando Huey Worker en ventana separada...

start "Huey Worker" cmd /k "venv\Scripts\activate && python manage.py run_huey -w 5 -v 2"

:: 4. Iniciar el servidor de Django en esta ventana
echo Iniciando Servidor Django...
python manage.py runserver

pause