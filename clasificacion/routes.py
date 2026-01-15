# -*- coding: utf-8 -*-
"""
Rutas del módulo de Clasificación de SKUs
"""

import json
from flask import render_template, request, jsonify
from datetime import datetime
from config import CANALES_CLASIFICACION
from database import get_fresh_data
from clasificacion.blueprint import bp
from clasificacion.services import (
    obtener_meses_disponibles,
    calcular_clasificacion_skus,
    agrupar_clasificaciones_para_tabla,
    resumen_clasificaciones_con_participacion
)


@bp.route("/clasificacion", methods=["GET", "POST"])
def clasificacion():
    """
    Ruta principal para la pestaña de Clasificación

    Muestra resumen de clasificación de SKUs usando los 8 canales específicos
    """
    try:
        # Cargar datos frescos en cada request
        print("INFO: Cargando datos frescos para clasificacion...")
        df, channels_disponibles, warehouses_disponibles = get_fresh_data()

        # Verificar si el DataFrame está cargado
        if df is None or df.empty:
            print("DEBUG: DataFrame no disponible en /clasificacion")
            return render_template("clasificacion.html",
                                 clasificaciones=[],
                                 clasificaciones_agrupadas=[],
                                 resumen_clasificaciones=[],
                                 meses_disponibles=[],
                                 channels=[],
                                 selected_clasificacion_mes="",
                                 selected_clasificacion_canal="todos",
                                 active_tab="clasificacion",
                                 error="No hay datos disponibles")

        # Obtener meses disponibles
        meses_disponibles_clasificacion = obtener_meses_disponibles(df)
        print(f"DEBUG: Meses disponibles para clasificación: {len(meses_disponibles_clasificacion)}")

        # Solo usar los 8 canales específicos para clasificación
        channels = CANALES_CLASIFICACION

        # Valores por defecto
        clasificaciones = []
        clasificaciones_agrupadas = []
        resumen_clasificaciones_list = []
        selected_clasificacion_mes = ""
        selected_clasificacion_canal = "todos"

        if meses_disponibles_clasificacion:
            selected_clasificacion_mes = meses_disponibles_clasificacion[0]['valor']
            año_clasificacion = meses_disponibles_clasificacion[0]['año']
            mes_clasificacion = meses_disponibles_clasificacion[0]['mes']

            # Aplicar filtro de canales específicos para clasificación
            df_filtrado = df[df["Channel"].isin(CANALES_CLASIFICACION)]

            # Calcular clasificaciones por defecto
            clasificaciones = calcular_clasificacion_skus(df_filtrado, año_clasificacion, mes_clasificacion, "todos")

            if clasificaciones:
                clasificaciones_agrupadas = agrupar_clasificaciones_para_tabla(clasificaciones)
                resumen_clasificaciones_list = resumen_clasificaciones_con_participacion(clasificaciones)

        return render_template("clasificacion.html",
                             clasificaciones=clasificaciones,
                             clasificaciones_agrupadas=clasificaciones_agrupadas,
                             resumen_clasificaciones=resumen_clasificaciones_list,
                             meses_disponibles=meses_disponibles_clasificacion,
                             channels=channels,
                             selected_clasificacion_mes=selected_clasificacion_mes,
                             selected_clasificacion_canal=selected_clasificacion_canal,
                             active_tab="clasificacion")

    except Exception as e:
        print(f"Error en /clasificacion: {e}")
        return render_template("clasificacion.html",
                             clasificaciones=[],
                             clasificaciones_agrupadas=[],
                             resumen_clasificaciones=[],
                             meses_disponibles=[],
                             channels=[],
                             selected_clasificacion_mes="",
                             selected_clasificacion_canal="todos",
                             active_tab="clasificacion",
                             error=str(e))


@bp.route("/clasificacion-skus", methods=["GET", "POST"])
def clasificacion_skus():
    """
    Ruta para análisis detallado de clasificación de SKUs

    Permite filtrar por mes, canal y warehouse para análisis específico
    """
    # Cargar datos frescos en cada request
    print("INFO: Cargando datos frescos para clasificacion-skus...")
    df, channels_disponibles, warehouses_disponibles = get_fresh_data()

    # Obtener meses disponibles (solo últimos 12 meses)
    meses_disponibles = obtener_meses_disponibles(df)

    # Verificar si hay datos
    if df.empty or not meses_disponibles:
        return render_template("clasificacion_skus.html",
                             clasificaciones=[],
                             clasificaciones_agrupadas=[],
                             resumen_clasificaciones=[],
                             indicadores_clasificacion={'productos_catalogo': 0, 'comerciales_activos': 0, 'productos_comerciales': 0},
                             active_tab="clasificacion",
                             channels=channels_disponibles,
                             warehouses=warehouses_disponibles,
                             meses_disponibles=[],
                             selected_channels=[],
                             selected_warehouses=[],
                             selected_año=datetime.now().year,
                             selected_mes=datetime.now().month,
                             mes_comparacion="",
                             error="No se pudieron cargar los datos o no hay datos en los últimos 12 meses")

    clasificaciones = []
    clasificaciones_agrupadas = []
    resumen_clasificaciones_list = []
    # MANTENER FILTROS
    selected_channels = []
    selected_warehouses = []
    # MANTENER FECHA SELECCIONADA (año y mes) - Por defecto el mes más reciente
    selected_año = meses_disponibles[0]['año'] if meses_disponibles else datetime.now().year
    selected_mes = meses_disponibles[0]['mes'] if meses_disponibles else datetime.now().month
    mes_comparacion = ""

    if request.method == "POST":
        try:
            # Obtener parámetros del formulario
            año = int(request.form.get("año", selected_año))
            mes = int(request.form.get("mes", selected_mes))
            channel = request.form.getlist("channel")
            warehouse = request.form.getlist("warehouse")

            # CAPTURAR FILTROS Y FECHA SELECCIONADA
            selected_channels = channel
            selected_warehouses = warehouse
            selected_año = año
            selected_mes = mes

            # Aplicar filtros al DataFrame completo
            df_filtrado = df.copy()

            if channel:
                df_filtrado = df_filtrado[df_filtrado["Channel"].isin(channel)]

            if warehouse:
                df_filtrado = df_filtrado[df_filtrado["Warehouse"].isin(warehouse)]

            # Calcular clasificaciones para el mes específico
            print(f"=== INICIANDO ANÁLISIS DE CLASIFICACIÓN ===")
            print(f"Mes seleccionado: {mes}/{año}")
            print(f"Filtros aplicados - Channels: {channel}, Warehouses: {warehouse}")

            clasificaciones = calcular_clasificacion_skus(df_filtrado, año, mes, "todos")

            # Agrupar clasificaciones para la tabla de resumen
            clasificaciones_agrupadas = agrupar_clasificaciones_para_tabla(clasificaciones)

            # Generar resumen con porcentajes de participación
            resumen_clasificaciones_list = resumen_clasificaciones_con_participacion(clasificaciones)

            # Ya no necesitamos información del mes anterior
            mes_comparacion = ""

            print(f"=== ANÁLISIS COMPLETADO ===")
            print(f"Total clasificaciones: {len(clasificaciones)}")
            print(f"Agrupaciones: {len(clasificaciones_agrupadas)}")
            print(f"Comparando con: {mes_comparacion}")

        except Exception as e:
            print(f"Error procesando clasificación de SKUs: {e}")
            import traceback
            traceback.print_exc()
            return render_template("clasificacion_skus.html",
                                 clasificaciones=[],
                                 clasificaciones_agrupadas=[],
                                 resumen_clasificaciones=[],
                                 active_tab="clasificacion",
                                 channels=channels_disponibles,
                                 warehouses=warehouses_disponibles,
                                 meses_disponibles=meses_disponibles,
                                 selected_channels=selected_channels,
                                 selected_warehouses=selected_warehouses,
                                 selected_año=selected_año,
                                 selected_mes=selected_mes,
                                 mes_comparacion=mes_comparacion,
                                 error=f"Error procesando datos: {str(e)}")

    return render_template("clasificacion_skus.html",
                         clasificaciones=clasificaciones,
                         clasificaciones_agrupadas=clasificaciones_agrupadas,
                         resumen_clasificaciones=resumen_clasificaciones_list,
                         active_tab="clasificacion",
                         channels=channels_disponibles,
                         warehouses=warehouses_disponibles,
                         meses_disponibles=meses_disponibles,
                         selected_channels=selected_channels,
                         selected_warehouses=selected_warehouses,
                         selected_año=selected_año,
                         selected_mes=selected_mes,
                         mes_comparacion=mes_comparacion)


@bp.route("/clasificacion-ajax", methods=["POST"])
def clasificacion_ajax():
    """
    Ruta AJAX para actualizar dinámicamente la sección de clasificación

    Permite actualizar solo partes de la página sin recargar completamente
    """
    try:
        # Cargar datos frescos en cada request AJAX
        print("INFO: Cargando datos frescos para clasificacion-ajax...")
        df, channels_disponibles, warehouses_disponibles = get_fresh_data()

        # Obtener parámetros del request
        clasificacion_mes = request.form.get("clasificacion_mes", "")
        clasificacion_canal = request.form.get("clasificacion_canal", "todos")
        channels_seleccionados = request.form.getlist("channel")
        warehouses_seleccionados = request.form.getlist("warehouse")
        solo_tabla = request.form.get("solo_tabla", "false").lower() == "true"

        print(f"DEBUG AJAX: mes={clasificacion_mes}, canal={clasificacion_canal}, solo_tabla={solo_tabla}")

        # Validar que hay datos
        if df.empty:
            return json.dumps({"error": "No hay datos disponibles"}), 400, {'Content-Type': 'application/json'}

        # Obtener meses disponibles
        meses_disponibles = obtener_meses_disponibles(df)

        if not meses_disponibles:
            return json.dumps({"error": "No hay meses disponibles"}), 400, {'Content-Type': 'application/json'}

        # Si no se especifica mes, usar el más reciente
        if not clasificacion_mes:
            clasificacion_mes = meses_disponibles[0]['valor']

        # Parsear año y mes
        if "-" in clasificacion_mes:
            año_clasificacion, mes_clasificacion = map(int, clasificacion_mes.split("-"))
        else:
            año_clasificacion = meses_disponibles[0]['año']
            mes_clasificacion = meses_disponibles[0]['mes']

        # Aplicar filtros al DataFrame
        df_filtrado = df.copy()

        # SIEMPRE aplicar filtro de canales específicos para clasificación
        df_filtrado = df_filtrado[df_filtrado["Channel"].isin(CANALES_CLASIFICACION)]

        # Si hay channels_seleccionados adicionales, intersectar con los canales de clasificación
        if channels_seleccionados:
            canales_interseccion = list(set(channels_seleccionados) & set(CANALES_CLASIFICACION))
            if canales_interseccion:
                df_filtrado = df_filtrado[df_filtrado["Channel"].isin(canales_interseccion)]

        if warehouses_seleccionados:
            df_filtrado = df_filtrado[df_filtrado["Warehouse"].isin(warehouses_seleccionados)]

        # Calcular clasificaciones con filtro de canal
        clasificaciones = calcular_clasificacion_skus(df_filtrado, año_clasificacion, mes_clasificacion, clasificacion_canal)

        if solo_tabla:
            # Solo actualizar la tabla detallada de SKUs
            # Renderizar template AJAX
            tabla_html = render_template("_clasificacion_tabla.html", clasificaciones=clasificaciones)

            # Si el template devuelve vacío (sin clasificaciones), mostrar mensaje
            if not tabla_html.strip():
                tabla_html = '''
                    <tr>
                        <td colspan="12" class="text-center py-4">
                            <div class="alert alert-info mb-0">
                                <i class="bi bi-info-circle me-2"></i>
                                <strong>No se han registrado ventas para el canal seleccionado</strong>
                            </div>
                        </td>
                    </tr>
                '''

            return json.dumps({
                "success": True,
                "tabla_html": tabla_html,
                "selected_mes": clasificacion_mes,
                "selected_canal": clasificacion_canal
            }), 200, {'Content-Type': 'application/json'}
        else:
            # Actualizar todo el contenido de clasificación
            clasificaciones_agrupadas = agrupar_clasificaciones_para_tabla(clasificaciones)

            # Generar resumen con porcentajes de participación
            resumen_clasificaciones_list = resumen_clasificaciones_con_participacion(clasificaciones)

            # Ya no necesitamos información del mes anterior
            mes_comparacion = ""

            # Solo usar los 8 canales específicos para clasificación
            channels = CANALES_CLASIFICACION

            # Renderizar solo la parte de clasificación
            html_clasificacion = render_template("_clasificacion_content.html",
                                               clasificaciones=clasificaciones,
                                               clasificaciones_agrupadas=clasificaciones_agrupadas,
                                               resumen_clasificaciones=resumen_clasificaciones_list,
                                               mes_comparacion=mes_comparacion,
                                               selected_clasificacion_mes=clasificacion_mes,
                                               selected_clasificacion_canal=clasificacion_canal,
                                               channels=channels)

            return json.dumps({
                "success": True,
                "html": html_clasificacion,
                "selected_mes": clasificacion_mes,
                "selected_canal": clasificacion_canal
            }), 200, {'Content-Type': 'application/json'}

    except Exception as e:
        print(f"Error en clasificacion_ajax: {e}")
        import traceback
        traceback.print_exc()
        return json.dumps({"error": str(e)}), 500, {'Content-Type': 'application/json'}
