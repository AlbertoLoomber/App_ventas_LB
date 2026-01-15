# -*- coding: utf-8 -*-
"""
Rutas del módulo de Análisis de Ventas
Dashboard principal con métricas comparativas y rankings
"""

import json
import pandas as pd
from flask import render_template, request, redirect, url_for
from datetime import datetime, timedelta
from config import MAZATLAN_TZ, CANALES_CLASIFICACION
from database import get_fresh_data
from utils import agrupar_por, agrupar_condicional, formato_rango, NumpyEncoder
from radar_comercial.services import get_specific_skus_with_descriptions
from analisis_ventas.blueprint import bp
from analisis_ventas.services import (
    calcular_top_skus,
    unificar_productos_para_ranking,
    resumen_periodo
)


@bp.route("/", methods=["GET", "POST"])
def index():
    """
    Ruta principal del dashboard - Análisis de Ventas

    Muestra:
    - Métricas comparativas entre períodos
    - Gráficos de evolución temporal
    - Ranking de SKUs más vendidos
    - Clasificación de productos
    """
    # Si es GET sin parámetros (primera carga), redirigir a Control de Ingreso Real
    if request.method == "GET" and not request.args:
        print("INFO: Primera carga detectada - redirigiendo a Control de Ingreso Real...")
        return redirect(url_for('analisis_rentabilidad.analisis_rentabilidad'))

    # OPTIMIZACIÓN: Si viene de navbar con tab=analisis, mostrar formulario SIN cargar todo el año
    if request.method == "GET" and request.args.get('tab') == 'analisis':
        print("INFO: GET con tab=analisis - Mostrando formulario con listas de filtros pero SIN cargar datos...")

        # Cargar SOLO las listas de filtros (channels, warehouses, skus) sin cargar todo el DataFrame
        from database import get_db_connection

        # Query SOLO para obtener canales únicos (muy rápido)
        try:
            connection = get_db_connection()

            # Canales únicos
            channels_query = "SELECT DISTINCT Channel FROM Silver.RPT_Ventas_Con_Costo_Prueba WHERE toYear(Fecha) = year(now()) ORDER BY Channel"
            channels_result = connection.query(channels_query)
            channels_disponibles = [row[0] for row in channels_result.result_rows if row[0]]

            # Warehouses únicos
            warehouses_query = "SELECT DISTINCT Warehouse FROM Silver.RPT_Ventas_Con_Costo_Prueba WHERE toYear(Fecha) = year(now()) ORDER BY Warehouse"
            warehouses_result = connection.query(warehouses_query)
            warehouses_disponibles = [row[0] for row in warehouses_result.result_rows if row[0]]

            print(f"✅ Cargados {len(channels_disponibles)} canales y {len(warehouses_disponibles)} warehouses SIN cargar DataFrame completo")

        except Exception as e:
            print(f"Error cargando listas de filtros: {e}")
            channels_disponibles = []
            warehouses_disponibles = []

        # Canales por defecto (Ecommerce)
        canales_ecommerce = ['Mercado Libre', 'Doto', 'Yuhu', 'Aliexpress', 'Coppel', 'Liverpool', 'Shein', 'CrediTienda', 'Walmart', 'TikTok Shop']

        return render_template("index.html",
                             resumen=[],
                             labels=[],
                             datasets={},
                             unidad="",
                             comparacion="",
                             active_tab="analisis",
                             channels=channels_disponibles,
                             warehouses=warehouses_disponibles,
                             skus=[],  # SKUs vacío porque requiere procesar DataFrame completo
                             selected_channels=canales_ecommerce,  # Filtro por defecto
                             selected_warehouses=[],
                             selected_skus=[],
                             selected_preset_main="7",
                             selected_preset_compare="anterior",
                             selected_main_range="",
                             selected_compare_range="",
                             top_skus=[],
                             clasificaciones=[],
                             clasificaciones_agrupadas=[],
                             resumen_clasificaciones=[],
                             mes_comparacion="",
                             meses_disponibles=[],
                             selected_clasificacion_mes="",
                             selected_clasificacion_canal="todos")

    # Para POST o GET con otros parámetros, cargar datos
    print("INFO: Cargando Análisis de Ventas...")
    df, channels_disponibles, warehouses_disponibles = get_fresh_data()
    skus_disponibles = get_specific_skus_with_descriptions(df) if not df.empty else []

    # Variables de clasificación vacías (no se usan en este template)
    clasificaciones = []
    clasificaciones_agrupadas = []
    resumen_clasificaciones_list = []
    mes_comparacion = ""
    selected_clasificacion_mes = ""
    selected_clasificacion_canal = "todos"
    meses_disponibles_clasificacion = []

    # Verificar si hay datos ANTES de obtener meses disponibles
    if df.empty:
        print("DEBUG: DataFrame está vacío en index()")
        return render_template("index.html",
                             resumen=[],
                             labels=[],
                             datasets={},
                             unidad="",
                             comparacion="",
                             active_tab="analisis",
                             channels=channels_disponibles,
                             warehouses=warehouses_disponibles,
                             skus=skus_disponibles,
                             selected_channels=[],
                             selected_warehouses=[],
                             selected_skus=[],
                             selected_preset_main="7",
                             selected_preset_compare="anterior",
                             selected_main_range="",
                             selected_compare_range="",
                             top_skus=[],
                             clasificaciones=clasificaciones,
                             clasificaciones_agrupadas=clasificaciones_agrupadas,
                             resumen_clasificaciones=resumen_clasificaciones_list,
                             mes_comparacion=mes_comparacion,
                             meses_disponibles=[],
                             selected_clasificacion_mes="",
                             selected_clasificacion_canal="todos",
                             error="No se pudieron cargar los datos")


    resumen, labels, unidad, comparacion, datasets = [], [], "", "", {}
    top_skus = []

    # MANTENER FILTROS - Por defecto Canales Ecommerce
    canales_ecommerce = ['Mercado Libre', 'Doto', 'Yuhu', 'Aliexpress', 'Coppel', 'Liverpool', 'Shein', 'CrediTienda', 'Walmart', 'TikTok Shop']
    selected_channels = canales_ecommerce
    selected_warehouses = []
    selected_skus = []

    # MANTENER FECHAS SELECCIONADAS
    selected_preset_main = "7"
    selected_preset_compare = "anterior"
    selected_main_range = ""
    selected_compare_range = ""

    if request.method == "POST":
        try:
            hoy = datetime.now(MAZATLAN_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
            preset_main = request.form.get("preset_main")
            preset_compare = request.form.get("preset_compare")
            channel = request.form.getlist("channel")
            warehouse = request.form.getlist("warehouse")
            sku = request.form.getlist("sku")

            # CAPTURAR FILTROS Y FECHAS SELECCIONADAS
            selected_channels = channel if channel else canales_ecommerce
            selected_warehouses = warehouse
            selected_skus = sku
            selected_preset_main = preset_main
            selected_preset_compare = preset_compare
            selected_main_range = request.form.get("main_range", "")
            selected_compare_range = request.form.get("compare_range", "")

            # CAPTURAR TAMBIÉN LOS FILTROS DE CLASIFICACIÓN
            clasificacion_mes = request.form.get("clasificacion_mes", "")
            clasificacion_canal = request.form.get("clasificacion_canal", "todos")

            # Si no se envió clasificacion_mes, usar el anterior o el por defecto
            if not clasificacion_mes:
                clasificacion_mes_anterior = request.form.get("clasificacion_mes_anterior", "")
                if clasificacion_mes_anterior:
                    clasificacion_mes = clasificacion_mes_anterior
                elif meses_disponibles_clasificacion:
                    clasificacion_mes = meses_disponibles_clasificacion[0]['valor']

            # Si no se envió clasificacion_canal, usar el anterior o el por defecto
            if not clasificacion_canal:
                clasificacion_canal_anterior = request.form.get("clasificacion_canal_anterior", "todos")
                clasificacion_canal = clasificacion_canal_anterior

            selected_clasificacion_mes = clasificacion_mes
            selected_clasificacion_canal = clasificacion_canal

            print(f"DEBUG: clasificacion_mes seleccionado: {clasificacion_mes}")

            # Procesar clasificacion_mes si existe
            if clasificacion_mes and "-" in clasificacion_mes:
                año_clasificacion, mes_clasificacion = map(int, clasificacion_mes.split("-"))
            else:
                # Usar mes más reciente disponible como default
                if meses_disponibles_clasificacion:
                    año_clasificacion = meses_disponibles_clasificacion[0]['año']
                    mes_clasificacion = meses_disponibles_clasificacion[0]['mes']
                    selected_clasificacion_mes = meses_disponibles_clasificacion[0]['valor']
                else:
                    hoy_mazatlan = datetime.now(MAZATLAN_TZ)
                    año_clasificacion = hoy_mazatlan.year
                    mes_clasificacion = hoy_mazatlan.month
                    selected_clasificacion_mes = f"{año_clasificacion}-{mes_clasificacion:02d}"

            # Determinar fechas del período principal
            if preset_main == "hoy":
                f1 = hoy
                f2 = hoy + timedelta(days=1)
            elif preset_main in ["7", "15", "30"]:
                dias = int(preset_main)
                f1 = hoy - timedelta(days=dias)
                f2 = hoy + timedelta(days=1)
            elif preset_main == "personalizado":
                rango = request.form.get("main_range")
                if rango:
                    if " to " in rango:
                        f1_str, f2_str = rango.split(" to ")
                        f1 = MAZATLAN_TZ.localize(datetime.strptime(f1_str.strip(), "%Y-%m-%d"))
                        f2_temp = MAZATLAN_TZ.localize(datetime.strptime(f2_str.strip(), "%Y-%m-%d"))
                        f2 = f2_temp + timedelta(days=1)
                    else:
                        f1 = MAZATLAN_TZ.localize(datetime.strptime(rango.strip(), "%Y-%m-%d"))
                        f2 = f1 + timedelta(days=1)
                else:
                    raise ValueError("Rango personalizado inválido")
            else:
                raise ValueError("Preset principal inválido")

            # Determinar fechas del período de comparación
            delta = f2 - f1
            if preset_compare == "anterior":
                fc1 = (f1 - pd.DateOffset(months=1)).to_pydatetime()
                fc2 = fc1 + delta
            elif preset_compare == "anual":
                fc1 = f1.replace(year=f1.year - 1)
                fc2 = fc1 + delta
            elif preset_compare == "personalizado":
                rango_comp = request.form.get("compare_range")
                if rango_comp:
                    if " to " in rango_comp:
                        fc1_str, fc2_str = rango_comp.split(" to ")
                        fc1 = MAZATLAN_TZ.localize(datetime.strptime(fc1_str.strip(), "%Y-%m-%d"))
                        fc2_temp = MAZATLAN_TZ.localize(datetime.strptime(fc2_str.strip(), "%Y-%m-%d"))
                        fc2 = fc2_temp + timedelta(days=1)
                    else:
                        fc1 = MAZATLAN_TZ.localize(datetime.strptime(rango_comp.strip(), "%Y-%m-%d"))
                        fc2 = fc1 + timedelta(days=1)
                else:
                    fc2 = f1
                    fc1 = fc2 - delta
            else:
                fc2 = f1
                fc1 = fc2 - delta

            # Filtrar datos por fechas SOLAMENTE
            df_main_solo_fechas = df[(df["Fecha"] >= f1) & (df["Fecha"] < f2)]
            df_compare_solo_fechas = df[(df["Fecha"] >= fc1) & (df["Fecha"] < fc2)]

            # *** CALCULAR RANKING COMPLETO DE SKUs CON FILTROS ***
            top_skus = calcular_top_skus(df_main_solo_fechas, channels=channel, warehouses=warehouse, skus=sku)
            top_skus = unificar_productos_para_ranking(top_skus)

            # Eliminado .copy() innecesario - Pandas maneja vistas eficientemente
            df_main = df_main_solo_fechas
            df_compare = df_compare_solo_fechas

            if channel:
                df_main = df_main[df_main["Channel"].isin(channel)]
                df_compare = df_compare[df_compare["Channel"].isin(channel)]
            else:
                df_main = df_main[df_main["Channel"].isin(canales_ecommerce)]
                df_compare = df_compare[df_compare["Channel"].isin(canales_ecommerce)]
                selected_channels = canales_ecommerce

            if warehouse:
                df_main = df_main[df_main["Warehouse"].isin(warehouse)]
                df_compare = df_compare[df_compare["Warehouse"].isin(warehouse)]

            if sku:
                df_main = df_main[df_main["sku"].isin(sku)]
                df_compare = df_compare[df_compare["sku"].isin(sku)]

            # Determinar granularidad
            granularidad = "hora" if (f2 - f1).days <= 1 else "dia"
            unidad = "Hora" if granularidad == "hora" else "Día"

            # Calcular límite de hora para "hoy"
            limite_hora = None
            if preset_main == "hoy" and granularidad == "hora" and not df_main.empty:
                hora_actual = datetime.now(MAZATLAN_TZ).hour
                ultima_hora_datos = df_main["Fecha"].dt.hour.max()
                limite_hora = min(hora_actual, ultima_hora_datos)

            # GENERAR ETIQUETAS SEPARADAS
            labels_main = list(agrupar_por(df_main, granularidad, limite_hora).index)
            labels_compare = list(agrupar_por(df_compare, granularidad, limite_hora).index)

            # Generar datasets
            datasets = {
                "Ventas brutas": {
                    "main": list(agrupar_por(df_main, granularidad, limite_hora).values),
                    "compare": list(agrupar_por(df_compare, granularidad, limite_hora).values),
                    "labels_main": labels_main,
                    "labels_compare": labels_compare
                },
                "Cancelaciones": {
                    "main": list(agrupar_condicional(df_main, granularidad, "cancelado", limite_hora).values),
                    "compare": list(agrupar_condicional(df_compare, granularidad, "cancelado", limite_hora).values),
                    "labels_main": labels_main,
                    "labels_compare": labels_compare
                },
                "Ingreso Neto": {
                    "main": list(agrupar_condicional(df_main, granularidad, "neto", limite_hora).values),
                    "compare": list(agrupar_condicional(df_compare, granularidad, "neto", limite_hora).values),
                    "labels_main": labels_main,
                    "labels_compare": labels_compare
                }
            }

            labels = labels_main
            resumen = resumen_periodo(df_main, df_compare, granularidad)
            comparacion = formato_rango(fc1, fc2 - timedelta(seconds=1))

        except Exception as e:
            print(f"Error procesando datos: {e}")
            import traceback
            traceback.print_exc()
            return render_template("index.html",
                                 resumen=[],
                                 labels=[],
                                 datasets={},
                                 unidad="",
                                 comparacion="",
                                 active_tab="analisis",
                                 channels=channels_disponibles,
                                 warehouses=warehouses_disponibles,
                                 skus=skus_disponibles,
                                 selected_channels=selected_channels,
                                 selected_warehouses=selected_warehouses,
                                 selected_skus=selected_skus,
                                 selected_preset_main=selected_preset_main,
                                 selected_preset_compare=selected_preset_compare,
                                 selected_main_range=selected_main_range,
                                 selected_compare_range=selected_compare_range,
                                 top_skus=[],
                                 clasificaciones=[],
                                 clasificaciones_agrupadas=[],
                                 resumen_clasificaciones=[],
                                 mes_comparacion="",
                                 meses_disponibles=meses_disponibles_clasificacion,
                                 selected_clasificacion_mes="",
                                 selected_clasificacion_canal="todos",
                                 error=f"Error procesando datos: {str(e)}")
    else:
        # GET request - Primera carga con datos por defecto
        try:
            hoy = datetime.now(MAZATLAN_TZ).replace(hour=0, minute=0, second=0, microsecond=0)

            # Valores por defecto (últimos 7 días)
            f1 = hoy - timedelta(days=7)
            f2 = hoy + timedelta(days=1)
            fc1 = f1 - timedelta(days=7)
            fc2 = f1

            # Filtrar datos
            df_main = df[(df["Fecha"] >= f1) & (df["Fecha"] < f2)]
            df_compare = df[(df["Fecha"] >= fc1) & (df["Fecha"] < fc2)]

            # Aplicar filtro de Ecommerce
            df_main = df_main[df_main["Channel"].isin(canales_ecommerce)]
            df_compare = df_compare[df_compare["Channel"].isin(canales_ecommerce)]

            # Granularidad
            granularidad = "dia"
            unidad = "Día"

            # Generar datos
            labels_main = list(agrupar_por(df_main, granularidad).keys())
            labels_compare = list(agrupar_por(df_compare, granularidad).keys())
            labels = labels_main if len(labels_main) >= len(labels_compare) else labels_compare

            datasets = {
                "Ventas brutas": {
                    "main": list(agrupar_por(df_main, granularidad).values()),
                    "compare": list(agrupar_por(df_compare, granularidad).values()),
                    "color_main": "#007bff",
                    "color_compare": "#28a745"
                }
            }

            resumen = resumen_periodo(df_main, df_compare)
            comparacion = formato_rango(f1, f2)

            # Top SKUs
            top_skus = calcular_top_skus(df_main, channels=canales_ecommerce)
            top_skus = unificar_productos_para_ranking(top_skus)


        except Exception as e:
            print(f"Error generando datos por defecto: {e}")
            import traceback
            traceback.print_exc()

    return render_template("index.html",
                         resumen=resumen,
                         labels=labels,
                         datasets=json.loads(json.dumps(datasets, cls=NumpyEncoder)),
                         unidad=unidad,
                         comparacion=comparacion,
                         active_tab="analisis",
                         channels=channels_disponibles,
                         warehouses=warehouses_disponibles,
                         skus=skus_disponibles,
                         selected_channels=selected_channels,
                         selected_warehouses=selected_warehouses,
                         selected_skus=selected_skus,
                         selected_preset_main=selected_preset_main,
                         selected_preset_compare=selected_preset_compare,
                         selected_main_range=selected_main_range,
                         selected_compare_range=selected_compare_range,
                         top_skus=top_skus,
                         clasificaciones=clasificaciones,
                         clasificaciones_agrupadas=clasificaciones_agrupadas,
                         resumen_clasificaciones=resumen_clasificaciones_list,
                         mes_comparacion=mes_comparacion,
                         meses_disponibles=meses_disponibles_clasificacion,
                         selected_clasificacion_mes=selected_clasificacion_mes,
                         selected_clasificacion_canal=selected_clasificacion_canal)
