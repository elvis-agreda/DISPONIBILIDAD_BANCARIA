from django.core.management.base import BaseCommand

from core.models import ClasificacionGasto, ColumnaDrillDown
from sap_sync.models import CuentaConfiguracion, MapeoCampo
from users.models import TransaccionSAP


class Command(BaseCommand):
    help = "Puebla la base de datos con los mapeos, cuentas, jerarquías y drill-down sin sobreescribir."

    def handle(self, *args, **kwargs):
        self.stdout.write("Iniciando carga de configuraciones por defecto...")

        # -------------------------------------------------------------------
        # 1. CARGA DE CUENTAS DE CONFIGURACIÓN
        # -------------------------------------------------------------------
        self.stdout.write("Cargando Cuentas de Configuración...")

        cuentas_iniciales = [
            ("117010100", "IMPUESTO", "I.V.A CREDITO FISCAL"),
            ("213010400", "IMPUESTO", "ISLR RET A EMPLEADOS"),
            ("213010500", "IMPUESTO", "ISLR RET A TERCEROS"),
            ("213010600", "IMPUESTO", "RETENCION IVA A TERCEROS"),
            ("213011100", "IMPUESTO", "PERCEPCION IGTF"),
            ("525010104", "IMPUESTO", "IMPUESTOS TRANSACCIONES FINANCIERAS"),
            ("411050117", "DIF_CAMBIO", "GANANCIA EN CAMBIO"),
            ("526010102", "DIF_CAMBIO", "PERDIDA EN CAMBIO"),
            ("525010103", "COMISION", "COMISIONES BANCARIAS"),
        ]

        cuentas_creadas = 0
        for cuenta, tipo, descripcion in cuentas_iniciales:
            # ⚡ FIX: Solo crea, nunca actualiza
            obj, created = CuentaConfiguracion.objects.get_or_create(
                cuenta=cuenta,
                defaults={"tipo": tipo, "descripcion": descripcion, "activa": True},
            )
            if created:
                cuentas_creadas += 1

        # -------------------------------------------------------------------
        # 1.5 CARGA DE MAPEOS DE GASTOS (DRILL-DOWN JERÁRQUICO)
        # -------------------------------------------------------------------
        self.stdout.write("Cargando Mapeos de Gastos (Drill-Down)...")

        gastos_iniciales = [
            ("113010100", "INVERSIONES TEMPORALES", "INVERSIONES TEMPORALES"),
            (
                "114010100",
                "CUENTAS POR COBRAR JURIDICAS",
                "CUENTAS POR COBRAR JURIDICAS",
            ),
            (
                "114010200",
                "CUENTAS POR COBRAR PARTICULARES",
                "CUENTAS POR COBRAR PARTICULARES",
            ),
            (
                "114010300",
                "CUENTAS POR COBRAR EMPLEADOS",
                "CUENTAS POR COBRAR EMPLEADOS",
            ),
            (
                "114010301",
                "CUENTAS POR COBRAR EMPLEADOS TRANSITORIA",
                "CUENTAS POR COBRAR EMPLEADOS TRANSITORIA",
            ),
            ("114010400", "CUENTAS POR COBRAR MEDICOS", "CUENTAS POR COBRAR MEDICOS"),
            (
                "114010401",
                "CUENTAS POR COBRAR MEDICOS TRANSITORIA",
                "CUENTAS POR COBRAR MEDICOS TRANSITORIA",
            ),
            (
                "114010500",
                "CUENTAS POR COBRAR CONCESIONARIOS",
                "CUENTAS POR COBRAR CONCESIONARIOS",
            ),
            (
                "114010600",
                "CUENTAS POR COBRAR CONCESIONARIOS NO ASISTENCIALES",
                "CUENTAS POR COBRAR CONCESIONARIOS NO ASISTENCIALES",
            ),
            (
                "114010700",
                "CHEQUES / TARJETAS DEVUELTOS",
                "CHEQUES / TARJETAS DEVUELTOS",
            ),
            (
                "114011000",
                "CUENTAS POR COBRAR COMPAÑIAS DE SEGURO",
                "CUENTAS POR COBRAR COMPAÑIAS DE SEGURO",
            ),
            ("114030100", "ANTICIPOS", "ANTICIPOS"),
            ("115010100", "INVENTARIO MEDICINAS", "INVENTARIO MEDICINAS"),
            ("115010200", "INVENTARIO MATERIAL MEDICO", "INVENTARIO MATERIAL MEDICO"),
            ("115010300", "INVENTARIO VACUNAS", "INVENTARIO VACUNAS"),
            (
                "115010400",
                "INVENTARIO REACTIVOS Y MATERIAL DE LABORATORIO",
                "INVENTARIO REACTIVOS Y MATERIAL DE LABORATORIO",
            ),
            (
                "115020100",
                "INVENTARIO PAPELERIA Y ARTICULOS DE OFICINA",
                "INVENTARIO PAPELERIA Y ARTICULOS DE OFICINA",
            ),
            (
                "115020200",
                "INVENTARIO REPUESTOS Y ACCESORIOS",
                "INVENTARIO REPUESTOS Y ACCESORIOS",
            ),
            (
                "115020300",
                "INVENTARIO MATERIALES Y ARTICULOS DE LIMPIEZA",
                "INVENTARIO MATERIALES Y ARTICULOS DE LIMPIEZA",
            ),
            ("116010400", "ISLR PREPAGADO", "ISLR PREPAGADO"),
            ("117010100", "I.V.A CREDITO FISCAL", "I.V.A CREDITO FISCAL"),
            ("117010200", "I.V.A RETENIDO POR TERCEROS", "I.V.A RETENIDO POR TERCEROS"),
            ("117010300", "ISLR RETENIDO POR TERCEROS", "ISLR RETENIDO POR TERCEROS"),
            (
                "119011101",
                "ACTIVOS Y OBRAS EN PROCESO TRANSITO",
                "ACTIVOS Y OBRAS EN PROCESO TRANSITO",
            ),
            (
                "121010100",
                "CUENTAS POR COBRAR RELACIONADAS",
                "CUENTAS POR COBRAR RELACIONADAS",
            ),
            (
                "211010101",
                "PRESTAMOS BANCARIOS TRANSITORIA",
                "PRESTAMOS BANCARIOS TRANSITORIA",
            ),
            (
                "212010100",
                "CUENTAS POR PAGAR COMERCIALES",
                "CUENTAS POR PAGAR COMERCIALES",
            ),
            (
                "212010200",
                "CUENTAS POR PAGAR CONCESIONARIOS",
                "CUENTAS POR PAGAR CONCESIONARIOS",
            ),
            (
                "212010300",
                "CUENTAS POR PAGAR CONCESIONARIOS NO ASISTENCIALES",
                "CUENTAS POR PAGAR CONCESIONARIOS NO ASISTENCIALES",
            ),
            (
                "212010400",
                "CUENTAS POR PAGAR MEDICOS (practica medica)",
                "CUENTAS POR PAGAR MEDICOS (practica medica)",
            ),
            (
                "212010401",
                "CUENTAS POR PAGAR MEDICOS (practica medica) trans",
                "CUENTAS POR PAGAR MEDICOS (practica medica) trans",
            ),
            (
                "212010402",
                "CUENTAS POR PAGAR MEDICOS (Mat/Sum) transitoria",
                "CUENTAS POR PAGAR MEDICOS (Mat/Sum) transitoria",
            ),
            (
                "212030100",
                "RECEPCION FACTURA INVENTARIOS",
                "RECEPCION FACTURA INVENTARIOS",
            ),
            ("213010100", "RETENCION INCES 0.5%", "RETENCION INCES 0.5%"),
            ("213010200", "RETENCION SEGURO SOCIAL", "RETENCION SEGURO SOCIAL"),
            (
                "213010300",
                "RETENCION POLITICA HABITACIONAL",
                "RETENCION POLITICA HABITACIONAL",
            ),
            ("213010400", "ISLR RETENIDO A EMPLEADOS", "ISLR RETENIDO A EMPLEADOS"),
            ("213010500", "ISLR RETENIDO A TERCEROS", "ISLR RETENIDO A TERCEROS"),
            ("213010600", "RETENCION IVA A TERCEROS", "RETENCION IVA A TERCEROS"),
            ("213011100", "PERCEPCION IGTF", "PERCEPCION IGTF"),
            ("214010100", "NOMINA ACUMULADA POR PAGAR", "NOMINA ACUMULADA POR PAGAR"),
            (
                "214010101",
                "NOMINA ACUMULADA POR PAGAR (transitoria)",
                "NOMINA ACUMULADA POR PAGAR (transitoria)",
            ),
            ("214010102", "NOMINAS Y LIQUIDACIONES", "NOMINAS Y LIQUIDACIONES"),
            (
                "214010200",
                "APORTE SEGURO SOCIAL POR PAGAR",
                "APORTE SEGURO SOCIAL POR PAGAR",
            ),
            (
                "214010300",
                "POLITICA HABITACIONAL ACUMULADA POR PAGAR",
                "POLITICA HABITACIONAL ACUMULADA POR PAGAR",
            ),
            ("214010400", "PROVISION INCES EMPRESA", "PROVISION INCES EMPRESA"),
            (
                "214010600",
                "VACACIONES ACUMULADAS POR PAGAR",
                "VACACIONES ACUMULADAS POR PAGAR",
            ),
            ("214010700", "BONO VACACIONAL", "BONO VACACIONAL"),
            ("214010800", "BONO FIN DE AÑO", "BONO FIN DE AÑO"),
            ("215010100", "I.V.A DEBITO FISCAL", "I.V.A DEBITO FISCAL"),
            (
                "216010100",
                "PROVISION CUENTAS POR PAGAR COMERCIALES",
                "PROVISION CUENTAS POR PAGAR COMERCIALES",
            ),
            ("217010100", "REINTEGROS", "REINTEGROS"),
            (
                "217020200",
                "HONORARIOS ABOGADOS POR PAGAR",
                "HONORARIOS ABOGADOS POR PAGAR",
            ),
            (
                "217040100",
                "ALQUILERES COBRADOS POR ANTICIPADOS",
                "ALQUILERES COBRADOS POR ANTICIPADOS",
            ),
            (
                "218010100",
                "CUENTAS POR PAGAR RELACIONADAS",
                "CUENTAS POR PAGAR RELACIONADAS",
            ),
            (
                "220010100",
                "PRESTACIONES SOCIALES POR PAGAR",
                "PRESTACIONES SOCIALES POR PAGAR",
            ),
            (
                "220010200",
                "INTERESES SOBRE PRESTACIONES SOCIALES",
                "INTERESES SOBRE PRESTACIONES SOCIALES",
            ),
            (
                "220010300",
                "OBLIGACION ACTUARIAL PRESTACIONES SOCIALES",
                "OBLIGACION ACTUARIAL PRESTACIONES SOCIALES",
            ),
            (
                "220020100",
                "ANTICIPO SOBRE PRESTACIONES SOCIALES",
                "ANTICIPO SOBRE PRESTACIONES SOCIALES",
            ),
            ("221010100", "CREDITOS DIFERIDOS TAM", "CREDITOS DIFERIDOS TAM"),
            (
                "222020100",
                "DEPOSITOS RECIBIDOS EN GARANTIA PACIENTES",
                "DEPOSITOS RECIBIDOS EN GARANTIA PACIENTES",
            ),
            ("411010101", "INGRESOS SERV. TERAPEUTICOS", "INGRESOS SERV. TERAPEUTICOS"),
            ("411010102", "INGRESOS SERV. HOSPITALARIO", "INGRESOS SERV. HOSPITALARIO"),
            (
                "411010103",
                "INGRESOS ESTUDIOS Y EXAMENES",
                "INGRESOS ESTUDIOS Y EXAMENES",
            ),
            ("411010104", "INGRESOS DE BANCO DE SANGRE", "INGRESOS DE BANCO DE SANGRE"),
            ("411010106", "INGRESOS NUTRICIÓN", "INGRESOS NUTRICIÓN"),
            ("411010107", "INGRESOS TELEMETRÍA", "INGRESOS TELEMETRÍA"),
            ("411010108", "INGRESOS BACTERIOLOGIA", "INGRESOS BACTERIOLOGIA"),
            ("411010109", "INGRESOS HEMODINAMIA", "INGRESOS HEMODINAMIA"),
            (
                "411010110",
                "INGRESOS TERAPIA RESPIRATORIA",
                "INGRESOS TERAPIA RESPIRATORIA",
            ),
            (
                "411010111",
                "INGRESOS ASOCIACION GERENCIAS POR SERVICIOS",
                "INGRESOS ASOCIACION GERENCIAS POR SERVICIOS",
            ),
            (
                "411020101",
                "INGRESOS CONCESIONES ASISTENCIALES",
                "INGRESOS CONCESIONES ASISTENCIALES",
            ),
            (
                "411020102",
                "INGRESOS ASOCIACIONES ASISTENCIALES",
                "INGRESOS ASOCIACIONES ASISTENCIALES",
            ),
            (
                "411030101",
                "INGRESOS ADMINISTRACION MEDICINAS",
                "INGRESOS ADMINISTRACION MEDICINAS",
            ),
            (
                "411030102",
                "INGRESOS ADMINISTRACION MATERIAL MEDICO",
                "INGRESOS ADMINISTRACION MATERIAL MEDICO",
            ),
            (
                "411030104",
                "INGRESOS SERVICIO FARMACEUTICO",
                "INGRESOS SERVICIO FARMACEUTICO",
            ),
            (
                "411040101",
                "USO DE ESPACIOS AREAS MEDICAS",
                "USO DE ESPACIOS AREAS MEDICAS",
            ),
            ("411040102", "USO DE EQUIPOS MEDICOS", "USO DE EQUIPOS MEDICOS"),
            ("411040103", "PARTICIPACIÓN % s/HM", "PARTICIPACIÓN % s/HM"),
            (
                "411040104",
                "HM GERENCIAS POR SERVICIO VARIABLE",
                "HM GERENCIAS POR SERVICIO VARIABLE",
            ),
            ("411040105", "SERV. ADMINISTRATIVO OTROS", "SERV. ADMINISTRATIVO OTROS"),
            ("411040108", "APOYO QUIRURGICO", "APOYO QUIRURGICO"),
            (
                "411050102",
                "INGRESOS POR DONACIONES Y PATROCINIOS",
                "INGRESOS POR DONACIONES Y PATROCINIOS",
            ),
            ("411050103", "INGRESOS POR EVENTOS", "INGRESOS POR EVENTOS"),
            (
                "411050104",
                "INGRESOS ARRENDAMIENTO AREA RENTAL",
                "INGRESOS ARRENDAMIENTO AREA RENTAL",
            ),
            (
                "411050105",
                "APORTE FIPSE MEDICOS.GCIA.SERV",
                "APORTE FIPSE MEDICOS.GCIA.SERV",
            ),
            (
                "411050107",
                "APORTE ASIST.SERV.MEDICOS INTRA",
                "APORTE ASIST.SERV.MEDICOS INTRA",
            ),
            (
                "411050111",
                "APORTE ASIST.SERV.MEDICOS EXTRAMURO",
                "APORTE ASIST.SERV.MEDICOS EXTRAMURO",
            ),
            ("411050117", "GANANCIA EN CAMBIO", "GANANCIA EN CAMBIO"),
            ("411050118", "OTROS INGRESOS", "OTROS INGRESOS"),
            (
                "411050119",
                "USO DE EQUIPOS (DEPRECIACION)",
                "USO DE EQUIPOS (DEPRECIACION)",
            ),
            ("411050120", "OTROS REEMBOLSABLES", "OTROS REEMBOLSABLES"),
            (
                "411050122",
                "INGRESOS PROGRAMAS EDUCACIÓN E INVESTIGACION",
                "INGRESOS PROGRAMAS EDUCACIÓN E INVESTIGACION",
            ),
            ("411060101", "INTERESES GANADOS", "INTERESES GANADOS"),
            (
                "412010101",
                "DESCUENTOS SERVICIOS MEDICOS",
                "DESCUENTOS SERVICIOS MEDICOS",
            ),
            ("511010101", "SUELDOS Y SALARIOS", "SUELDOS Y SALARIOS"),
            ("511010102", "SABADOS DOMINGOS Y FERIADOS", "SABADOS DOMINGOS Y FERIADOS"),
            ("511010103", "SUPLENCIAS", "SUPLENCIAS"),
            ("511010104", "VACACIONES", "VACACIONES"),
            ("511010106", "BONO POR PRODUCTIVIDAD", "BONO POR PRODUCTIVIDAD"),
            ("511010107", "BONO NOCTURNO", "BONO NOCTURNO"),
            ("511010108", "SEGURO SOCIAL OBLIGATORIO", "SEGURO SOCIAL OBLIGATORIO"),
            ("511010110", "LEY POLÍTICA HABITACIONAL", "LEY POLÍTICA HABITACIONAL"),
            ("511010111", "INCES", "INCES"),
            ("511010112", "BONO FIN DE AÑO", "BONO FIN DE AÑO"),
            ("511010113", "BONO VACACIONAL", "BONO VACACIONAL"),
            ("511010114", "OTRAS BONIFICACIONES", "OTRAS BONIFICACIONES"),
            ("511010115", "BONO VARIABLE POR OBJETIVOS", "BONO VARIABLE POR OBJETIVOS"),
            ("511010116", "CESTA TICKET SOCIALISTA", "CESTA TICKET SOCIALISTA"),
            ("511010117", "GASTO DE ESTACIONAMIENTO", "GASTO DE ESTACIONAMIENTO"),
            (
                "511010119",
                "PRESTACIONES SOCIALES ART.108",
                "PRESTACIONES SOCIALES ART.108",
            ),
            (
                "511010120",
                "INTERES SOBRE PRESTACIONES SOCIALES.",
                "INTERES SOBRE PRESTACIONES SOCIALES.",
            ),
            (
                "511010122",
                "UNIFORMES E INSUMOS PARA EL PERSONAL",
                "UNIFORMES E INSUMOS PARA EL PERSONAL",
            ),
            ("511010123", "BECAS", "BECAS"),
            ("511010125", "TRANSPORTE PERSONAL", "TRANSPORTE PERSONAL"),
            ("511010126", "CURSOS Y ENTRENAMIENTOS", "CURSOS Y ENTRENAMIENTOS"),
            (
                "511010127",
                "EVENTOS SOCIALES Y DEPORTIVOS",
                "EVENTOS SOCIALES Y DEPORTIVOS",
            ),
            ("511010129", "SERVICIOS MÉDICOS", "SERVICIOS MÉDICOS"),
            ("511010130", "SEGURO HCM", "SEGURO HCM"),
            (
                "511010131",
                "SEGURO DE VIDA/SERV. FUNERARIOS",
                "SEGURO DE VIDA/SERV. FUNERARIOS",
            ),
            ("511010135", "BONO POST VACACIONAL", "BONO POST VACACIONAL"),
            ("511010136", "PRE-EMPLEO", "PRE-EMPLEO"),
            (
                "511010137",
                "BONIFICACION ESPECIAL ART. 92. ART. 94",
                "BONIFICACION ESPECIAL ART. 92. ART. 94",
            ),
            ("512010101", "COSTO MEDICINAS", "COSTO MEDICINAS"),
            ("512010102", "COSTO MATERIAL MEDICO", "COSTO MATERIAL MEDICO"),
            (
                "512010104",
                "COSTO REACTIVOS Y MATERIAL DE LABORATORIO",
                "COSTO REACTIVOS Y MATERIAL DE LABORATORIO",
            ),
            (
                "512020101",
                "SERV.CONTRAT.ALIMENTACION HOSPITALIZACION",
                "SERV.CONTRAT.ALIMENTACION HOSPITALIZACION",
            ),
            (
                "512020102",
                "SERV.CONTRAT.ALIMENTACION HOSPITALIZACION - CONSUM",
                "SERV.CONTRAT.ALIMENTACION HOSPITALIZACION - CONSUM",
            ),
            ("512020106", "ARREND.EQUIPO MEDICO", "ARREND.EQUIPO MEDICO"),
            ("512020107", "GASES MEDICINALES", "GASES MEDICINALES"),
            (
                "513010101",
                "PAPELERIA Y ARTICULOS DE OFICINA",
                "PAPELERIA Y ARTICULOS DE OFICINA",
            ),
            ("513010102", "REPUESTOS Y ACCESORIOS", "REPUESTOS Y ACCESORIOS"),
            ("513010103", "MATERIAL Y ARTIC.LIMPIEZA", "MATERIAL Y ARTIC.LIMPIEZA"),
            ("513010105", "EQUIPOS MENORES", "EQUIPOS MENORES"),
            ("514010100", "HONORARIOS PROFESIONALES", "HONORARIOS PROFESIONALES"),
            (
                "514010101",
                "HONORARIOS MÉDICOS Y TÉCNICOS",
                "HONORARIOS MÉDICOS Y TÉCNICOS",
            ),
            ("514010103", "HONORARIOS ABOGADOS", "HONORARIOS ABOGADOS"),
            ("514010105", "ASIGNACION JUNTA DIRECTIVA", "ASIGNACION JUNTA DIRECTIVA"),
            ("515010101", "ELECTRICIDAD", "ELECTRICIDAD"),
            ("515010102", "ASEO", "ASEO"),
            ("515010103", "TELÉFONO", "TELÉFONO"),
            ("515010104", "AGUA", "AGUA"),
            ("515010105", "INTERNET", "INTERNET"),
            ("516010102", "SERVICIO DE AMBULANCIA", "SERVICIO DE AMBULANCIA"),
            ("516010103", "SERVICIO DE LAVANDERIA", "SERVICIO DE LAVANDERIA"),
            ("516010105", "SUSC.REVISTAS Y ASOCIAC.", "SUSC.REVISTAS Y ASOCIAC."),
            ("516010106", "FLETE.MENS.Y TRANSP.", "FLETE.MENS.Y TRANSP."),
            ("516010107", "SERVICIO FUMIGACIONES", "SERVICIO FUMIGACIONES"),
            ("516010108", "SERVICIO REPROD.Y FOTOCOP.", "SERVICIO REPROD.Y FOTOCOP."),
            ("516010110", "SERV.ILUST.DISEÑO.PUBLIC.", "SERV.ILUST.DISEÑO.PUBLIC."),
            ("516010111", "SERVICIO CONTRATADOS OTROS", "SERVICIO CONTRATADOS OTROS"),
            ("516010113", "GASTOS POR EVENTOS", "GASTOS POR EVENTOS"),
            (
                "517010101",
                "MTTO.DE EDIF.Y/O AREAS VERDES",
                "MTTO.DE EDIF.Y/O AREAS VERDES",
            ),
            ("517010102", "REPARACIÓN Y  MTTO.EQ.MED.", "REPARACIÓN Y  MTTO.EQ.MED."),
            (
                "517010103",
                "REPARACIÓN Y MTTO.MAQ.EQ.OF.",
                "REPARACIÓN Y MTTO.MAQ.EQ.OF.",
            ),
            (
                "517010104",
                "REPARACIÓN Y MTTO. OTROS EQ.",
                "REPARACIÓN Y MTTO. OTROS EQ.",
            ),
            ("518010101", "ARREND.EQUIPO FOTOCOPIADO", "ARREND.EQUIPO FOTOCOPIADO"),
            (
                "518010105",
                "ARRENDAMIENTO AREAS TORRE HOSPITALIZACIÓN",
                "ARRENDAMIENTO AREAS TORRE HOSPITALIZACIÓN",
            ),
            (
                "519010101",
                "GTOS REFRIGERIOS Y OBSEQUIOS.",
                "GTOS REFRIGERIOS Y OBSEQUIOS.",
            ),
            (
                "519010103",
                "ASIGNACION VEHICULOS Y TAXIS",
                "ASIGNACION VEHICULOS Y TAXIS",
            ),
            (
                "520010101",
                "IMPUESTOS MUNICIPALES.(PATENTE Y DER.FRE)",
                "IMPUESTOS MUNICIPALES.(PATENTE Y DER.FRE)",
            ),
            ("520010103", "MULTAS Y SANCIONES", "MULTAS Y SANCIONES"),
            ("521010101", "ASOCIACION ASISTENCIALES", "ASOCIACION ASISTENCIALES"),
            ("521010103", "SEGURO PATRIMONIALES", "SEGURO PATRIMONIALES"),
            (
                "522010199",
                "ORDENES INTERNAS TRANSITORIA",
                "ORDENES INTERNAS TRANSITORIA",
            ),
            ("523010101", "COSTOS MTTO. GENERAL", "COSTOS MTTO. GENERAL"),
            ("523010102", "COSTOS MTTO. INTERNO", "COSTOS MTTO. INTERNO"),
            ("525010101", "GASTOS INTERES BANCARIO", "GASTOS INTERES BANCARIO"),
            ("525010102", "GASTOS POR INTERESES", "GASTOS POR INTERESES"),
            ("525010103", "COMISIONES BANCARIAS", "COMISIONES BANCARIAS"),
            (
                "525010104",
                "IMPUESTOS TRANSACCIONES FINANCIERAS",
                "IMPUESTOS TRANSACCIONES FINANCIERAS",
            ),
            ("526010101", "DESCUENTO POR PRONTO PAGO", "DESCUENTO POR PRONTO PAGO"),
            ("526010102", "PERDIDA EN CAMBIO", "PERDIDA EN CAMBIO"),
            (
                "526010104",
                "PERDIDA DESINCORPORACION.ACTIVO",
                "PERDIDA DESINCORPORACION.ACTIVO",
            ),
            (
                "526010105",
                "PROGRAMA ASIST.SERV. MEDICOS INTRAMURO",
                "PROGRAMA ASIST.SERV. MEDICOS INTRAMURO",
            ),
            (
                "526010106",
                "PROGRAMA ASIST SUMINISTROS INTRAMURO",
                "PROGRAMA ASIST SUMINISTROS INTRAMURO",
            ),
            (
                "526010107",
                "PROGRAMA ASIST.SERVICIOS CONTR.INTRAMURO",
                "PROGRAMA ASIST.SERVICIOS CONTR.INTRAMURO",
            ),
            (
                "526010108",
                "PROGRAMA ASIST.SERV. MEDICOS EXTRA",
                "PROGRAMA ASIST.SERV. MEDICOS EXTRA",
            ),
            (
                "526010109",
                "PROGRAMA ASIST SUMINISTROS EXTRA",
                "PROGRAMA ASIST SUMINISTROS EXTRA",
            ),
            (
                "526010110",
                "PROGRAMA ASIST.SERVICIOS CONTR.EXTR",
                "PROGRAMA ASIST.SERVICIOS CONTR.EXTR",
            ),
            ("526010118", "OTROS EGRESOS", "OTROS EGRESOS"),
            ("526010119", "GASTOS NO DEDUCIBLES", "GASTOS NO DEDUCIBLES"),
            (
                "526010120",
                "DESCUENTO ESPECIAL MEDICINA COMUNITARIA",
                "DESCUENTO ESPECIAL MEDICINA COMUNITARIA",
            ),
            ("SIN_DETALLE_GASTO", "NO CLASIFICADO", "Movimiento de Banco sin Desglose"),
        ]

        gastos_creados = 0
        for cuenta, cat, sub_cat in gastos_iniciales:
            #  Usamos get_or_create para no sobreescribir tus cambios en admin
            obj, created = ClasificacionGasto.objects.get_or_create(
                cuenta_gasto=cuenta,
                defaults={"categoria": cat, "sub_categoria": sub_cat},
            )
            if created:
                gastos_creados += 1

        # -------------------------------------------------------------------
        # 2. CARGA DE MAPEOS DE CAMPOS
        # -------------------------------------------------------------------
        self.stdout.write("Cargando Mapeos de Campos SAP...")

        mapeos_iniciales = [
            ("Partida", "Bukrs", "bukrs", "TEXTO"),
            ("Partida", "Belnr", "belnr", "TEXTO"),
            ("Partida", "Gjahr", "gjahr", "TEXTO"),
            ("Partida", "Blart", "blart", "TEXTO"),
            ("Partida", "Bktxt", "bktxt", "TEXTO"),
            ("Partida", "Bldat", "bldat", "FECHA"),
            ("Partida", "Budat", "budat", "FECHA"),
            ("PartidaPosicion", "Bukrs", "bukrs", "TEXTO"),
            ("PartidaPosicion", "Docnr", "docnr", "TEXTO"),
            ("PartidaPosicion", "Ryear", "ryear", "TEXTO"),
            ("PartidaPosicion", "Docln", "docln", "TEXTO"),
            ("PartidaPosicion", "Ractt", "ractt", "TEXTO"),
            ("PartidaPosicion", "Wsl", "wsl", "DECIMAL"),
            ("PartidaPosicion", "Drcrk", "drcrk", "TEXTO"),
            ("PartidaPosicion", "Rwcur", "rwcur", "TEXTO"),
            ("PartidaPosicion", "Lifnr", "lifnr", "TEXTO"),
            ("PartidaPosicion", "Kunnr", "kunnr", "TEXTO"),
            ("PartidaPosicion", "Koart", "koart", "TEXTO"),
            ("PartidaPosicion", "Augbl", "augbl", "TEXTO"),
            ("PartidaPosicion", "Zuonr", "zuonr", "TEXTO"),
            ("PartidaPosicion", "Budat", "budat", "FECHA"),
            ("PartidaPosicionFiltro", "Bukrs", "bukrs", "TEXTO"),
            ("PartidaPosicionFiltro", "Docnr", "docnr", "TEXTO"),
            ("PartidaPosicionFiltro", "Ryear", "ryear", "TEXTO"),
            ("PartidaPosicionFiltro", "Docln", "docln", "TEXTO"),
            ("PartidaPosicionFiltro", "Ractt", "ractt", "TEXTO"),
            ("PartidaPosicionFiltro", "Budat", "budat", "FECHA"),
            ("Compensacion", "Bukrs", "bukrs", "TEXTO"),
            ("Compensacion", "Belnr", "belnr", "TEXTO"),
            ("Compensacion", "Gjahr", "gjahr", "TEXTO"),
            ("Compensacion", "Buzei", "buzei", "TEXTO"),
            ("Compensacion", "Shkzg", "shkzg", "TEXTO"),
            ("Compensacion", "Dmbtr", "dmbtr", "DECIMAL"),
            ("Compensacion", "Wrbtr", "wrbtr", "DECIMAL"),
            ("Compensacion", "Pswbt", "pswbt", "DECIMAL"),
            ("Compensacion", "Pswsl", "pswsl", "TEXTO"),
            ("Compensacion", "Zuonr", "zuonr", "TEXTO"),
            ("Compensacion", "Sgtxt", "sgtxt", "TEXTO"),
            ("Compensacion", "Saknr", "saknr", "TEXTO"),
            ("Compensacion", "Hkont", "hkont", "TEXTO"),
            ("Compensacion", "Kunnr", "kunnr", "TEXTO"),
            ("Compensacion", "Lifnr", "lifnr", "TEXTO"),
            ("Compensacion", "Augdt", "augdt", "FECHA"),
            ("Compensacion", "Augcp", "augcp", "FECHA"),
            ("Compensacion", "Augbl", "augbl", "TEXTO"),
            ("Compensacion", "Bschl", "bschl", "TEXTO"),
            ("Compensacion", "Koart", "koart", "TEXTO"),
        ]

        mapeos_creados = 0
        for modelo, campo_sap, campo_django, tipo_dato in mapeos_iniciales:
            # ⚡ FIX: Cambiado a get_or_create para no pisar si lo desactivas en admin
            obj, created = MapeoCampo.objects.get_or_create(
                modelo_destino=modelo,
                campo_sap=campo_sap,
                defaults={
                    "campo_django": campo_django,
                    "tipo_dato": tipo_dato,
                    "activo": True,
                },
            )
            if created:
                mapeos_creados += 1

        # -------------------------------------------------------------------
        # 3. CARGA DE ROLES POR TRANSACCIÓN
        # -------------------------------------------------------------------
        self.stdout.write("Cargando Configuración de Roles por Transacción...")

        transacciones_iniciales = [
            ("SE16N", "ADMINISTRADOR", 10),
            ("FBL1N", "ANALISTA", 5),
            ("FBL3N", "ANALISTA", 5),
            ("VF01", "AUDITOR", 1),
            ("FB03", "AUDITOR", 1),
        ]

        transacciones_creadas = 0
        for tcode, rol, jerarquia in transacciones_iniciales:
            obj, created = TransaccionSAP.objects.get_or_create(
                tcode=tcode, defaults={"rol_asociado": rol, "jerarquia": jerarquia}
            )
            if created:
                transacciones_creadas += 1

        # -------------------------------------------------------------------
        # 4. CARGA DE COLUMNAS PARA EL DRILL-DOWN (NUEVO)
        # -------------------------------------------------------------------
        self.stdout.write("Cargando Columnas del Modal Drill-Down...")

        columnas_drilldown = [
            # campo_bd, etiqueta, tipo, orden, es_buscable, abre_documento
            ("documento_primario", "Doc. Banco (ZR)", "TEXTO", 1, True, True),
            ("documento_secundario", "Doc. Origen (ZP/Fac)", "TEXTO", 2, True, True),
            ("fecha_contabilizacion", "Fecha", "FECHA", 3, False, False),
            ("cuenta_contable", "Cuenta Banco", "TEXTO", 4, True, False),
            ("cuenta_gasto", "Cuenta Gasto", "TEXTO", 5, True, False),
            ("lifnr", "Socio (LIFNR/KUNNR)", "TEXTO", 6, True, False),
            ("referencia", "Referencia", "TEXTO", 7, True, False),
            ("monto_total", "Monto", "MONTO", 8, False, False),
        ]

        columnas_creadas = 0
        for campo, etiqueta, tipo, orden, buscable, abre in columnas_drilldown:
            obj, created = ColumnaDrillDown.objects.get_or_create(
                campo_bd=campo,
                defaults={
                    "etiqueta": etiqueta,
                    "tipo_dato": tipo,
                    "orden": orden,
                    "es_buscable": buscable,
                    "abre_documento": abre,
                    "activo": True,
                },
            )
            if created:
                columnas_creadas += 1

        # --- MENSAJE FINAL ---
        self.stdout.write(
            self.style.SUCCESS(
                f"Finalizado: {cuentas_creadas} cuentas | "
                f"{gastos_creados} Mapeos de Gasto | "
                f"{mapeos_creados} mapeos SAP | "
                f"{transacciones_creadas} roles | "
                f"{columnas_creadas} columnas drill-down."
            )
        )
