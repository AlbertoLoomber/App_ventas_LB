# -*- coding: utf-8 -*-
"""
Rutas del módulo de Cumplimiento de Metas
Maneja las rutas y endpoints relacionados con el análisis de cumplimiento de metas por canal
"""

from flask import request, render_template, jsonify, send_file
from datetime import datetime, timedelta
import pandas as pd
from io import BytesIO

from cumplimiento_metas.blueprint import bp
from config import MAZATLAN_TZ as mazatlan_tz, CANALES_CLASIFICACION
from database import get_fresh_data, get_fresh_metas, obtener_mes_actual
from utils import formato_periodo_texto, clean_data_for_json
from cumplimiento_metas.services import (
    calcular_cumplimiento_metas,
    get_default_resumen_general
)


@bp.route("/cumplimiento-metas", methods=["GET", "POST"])
def cumplimiento_metas():
    """Nueva pestaña para análisis de cumplimiento de metas por canal"""

    # Obtener filtro de mes global (GET) - Por defecto el mes actual del sistema
    mes_actual = obtener_mes_actual()  # Detecta el mes actual automáticamente
    mes_filtro = request.args.get('mes_filtro', str(mes_actual))
    mes_seleccionado = int(mes_filtro) if mes_filtro and mes_filtro.isdigit() else mes_actual

    # Cargar datos frescos con filtro optimizado
    print(f"INFO: Cargando datos frescos para cumplimiento (mes {mes_seleccionado})...")
    df, channels_disponibles, warehouses_disponibles = get_fresh_data(mes_seleccionado)
    df_metas = get_fresh_metas()

    # DEBUG: Información de datos ya filtrados
    print(f"DEBUG: Registros cargados (ya filtrados por mes {mes_seleccionado}): {len(df)}")
    if not df.empty:
        print(f"DEBUG: Meses disponibles en datos: {sorted(df['Fecha'].dt.month.unique())}")
        print(f"DEBUG: Fechas min-max: {df['Fecha'].min()} a {df['Fecha'].max()}")
    else:
        print(f"WARNING: No hay datos para el mes {mes_seleccionado}")

    # Verificar si hay datos DE VENTAS
    if df.empty:
        # Mensaje específico según el mes
        meses_nombres = {1: "enero", 2: "febrero", 3: "marzo", 4: "abril", 5: "mayo", 6: "junio",
                        7: "julio", 8: "agosto", 9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"}
        mes_nombre = meses_nombres.get(mes_seleccionado, f"mes {mes_seleccionado}")

        if mes_seleccionado == 9:
            mensaje_error = f"Aún no hay datos de ventas disponibles para {mes_nombre}. Las metas para septiembre estarán disponibles próximamente."
        elif mes_seleccionado == 8:
            mensaje_error = f"No se encontraron datos de ventas para {mes_nombre}. Verifica la conexión a la base de datos."
        else:
            mensaje_error = f"No hay datos disponibles para el mes seleccionado ({mes_nombre})."

        # Inicializar TODAS las variables requeridas por el template
        default_resumen = get_default_resumen_general()

        return render_template("cumplimiento_metas.html",
                             # Variables principales
                             cumplimiento_data=[],
                             resumen_general=default_resumen,
                             # Variables de los 4 tipos de meta (requeridas por el template)
                             cumplimiento_data_ventas=[],
                             resumen_general_ventas=default_resumen,
                             cumplimiento_data_costo=[],
                             resumen_general_costo=default_resumen,
                             cumplimiento_data_ingreso=[],
                             resumen_general_ingreso=default_resumen,
                             cumplimiento_data_ingreso_nominal=[],
                             resumen_general_ingreso_nominal=default_resumen,
                             # HTML precalculado
                             html_ventas="",
                             html_costo="",
                             html_ingreso_nominal="",
                             # Variables de configuración
                             periodo_texto="",
                             active_tab="cumplimiento-metas",
                             selected_preset_main="mes_actual",
                             selected_main_range="",
                             mes_seleccionado=mes_seleccionado,
                             error=mensaje_error)

    # CALCULAR ANÁLISIS DE RENTABILIDAD SIEMPRE QUE HAY DATOS DE VENTAS
    # (Independiente de si hay metas o no)
    print("INFO: Hay datos de ventas, calculando Análisis de Rentabilidad...")

    # Inicializar TODAS las variables por defecto para evitar Undefined en template
    cumplimiento_data = []
    cumplimiento_data_ventas = []
    resumen_general_ventas = get_default_resumen_general()
    cumplimiento_data_costo = []
    resumen_general_costo = get_default_resumen_general()
    cumplimiento_data_ingreso = []
    resumen_general_ingreso = get_default_resumen_general()
    cumplimiento_data_ingreso_nominal = []
    resumen_general_ingreso_nominal = get_default_resumen_general()
    html_ventas = ""
    html_costo = ""
    html_ingreso_nominal = ""
    periodo_texto = ""
    # Variables de configuración también
    selected_preset_main = "mes_actual"
    selected_main_range = ""

    # Siempre calcular datos (tanto en GET como en POST)
    try:
        hoy = datetime.now(mazatlan_tz).replace(hour=0, minute=0, second=0, microsecond=0)

        # Obtener parámetros según el método
        if request.method == "POST":
            preset_main = request.form.get("preset_main", "mes_actual")
            selected_preset_main = preset_main
            selected_main_range = request.form.get("main_range", "")
        else:  # GET - valores por defecto
            preset_main = "mes_actual"
            selected_preset_main = "mes_actual"
            selected_main_range = ""

        # Determinar fechas según el preset (para ambos GET y POST)
        if preset_main == "hoy":
            f1 = hoy
            f2 = hoy + timedelta(days=1)
        elif preset_main in ["7", "15"]:
            dias = int(preset_main)
            f1 = hoy - timedelta(days=dias)
            f2 = hoy + timedelta(days=1)
        elif preset_main == "mes_actual":
            # Primer día del mes actual hasta hoy
            f1 = hoy.replace(day=1)
            f2 = hoy + timedelta(days=1)
        elif preset_main == "mes_completo":
            # Mes anterior completo
            primer_dia_mes_anterior = (hoy.replace(day=1) - timedelta(days=1)).replace(day=1)
            ultimo_dia_mes_anterior = hoy.replace(day=1) - timedelta(days=1)
            f1 = primer_dia_mes_anterior
            f2 = ultimo_dia_mes_anterior + timedelta(days=1)
        elif preset_main == "personalizado":
            rango = request.form.get("main_range") if request.method == "POST" else ""
            if rango:
                if " to " in rango:
                    # Rango de fechas
                    f1_str, f2_str = rango.split(" to ")
                    f1 = mazatlan_tz.localize(datetime.strptime(f1_str.strip(), "%Y-%m-%d"))
                    f2_temp = mazatlan_tz.localize(datetime.strptime(f2_str.strip(), "%Y-%m-%d"))
                    f2 = f2_temp + timedelta(days=1)
                else:
                    # Un solo día
                    f1 = mazatlan_tz.localize(datetime.strptime(rango.strip(), "%Y-%m-%d"))
                    f2 = f1 + timedelta(days=1)
            else:
                # Si no hay rango personalizado, usar mes actual por defecto
                f1 = hoy.replace(day=1)
                f2 = hoy + timedelta(days=1)
        else:
            # Default a mes actual
            f1 = hoy.replace(day=1)
            f2 = hoy + timedelta(days=1)

        # ✅ OPTIMIZACIÓN: Pre-procesar datos UNA sola vez para los 3 tipos de meta
        print("INFO: Pre-procesando datos compartidos...")

        # PASO 1: Procesar datos UNA sola vez (lo que antes se hacía 3 veces)
        df_ventas_naive = df.copy()
        df_ventas_naive["Fecha"] = df_ventas_naive["Fecha"].dt.tz_localize(None)

        f1_naive = pd.to_datetime(f1).tz_localize(None) if hasattr(f1, 'tzinfo') and f1.tzinfo else pd.to_datetime(f1)
        f2_naive = pd.to_datetime(f2).tz_localize(None) if hasattr(f2, 'tzinfo') and f2.tzinfo else pd.to_datetime(f2)

        # Filtrar ventas período UNA vez
        ventas_periodo_compartido = df_ventas_naive[
            df_ventas_naive["estado"] != "Cancelado"
        ].copy()

        # Filtrar por canales oficiales UNA vez
        ventas_periodo_compartido = ventas_periodo_compartido[
            ventas_periodo_compartido['Channel'].isin(CANALES_CLASIFICACION)
        ].copy()

        # Convertir cantidad UNA vez
        if 'cantidad' in ventas_periodo_compartido.columns:
            ventas_periodo_compartido['cantidad'] = pd.to_numeric(
                ventas_periodo_compartido['cantidad'], errors='coerce'
            ).fillna(0)

        print(f"✅ Datos compartidos procesados: {len(ventas_periodo_compartido)} registros")

        # PASO 2: Calcular los 3 tipos de meta usando datos pre-procesados
        print("INFO: Calculando 3 tipos de meta con datos pre-procesados...")

        # Calcular metas de ventas
        cumplimiento_data_ventas, resumen_general_ventas = calcular_cumplimiento_metas(
            ventas_periodo_compartido, df_metas, f1, f2, "ventas", skip_preprocessing=True
        )

        # Calcular metas de costo
        cumplimiento_data_costo, resumen_general_costo = calcular_cumplimiento_metas(
            ventas_periodo_compartido, df_metas, f1, f2, "costo", skip_preprocessing=True
        )

        # Calcular metas de ingreso real
        cumplimiento_data_ingreso, resumen_general_ingreso = calcular_cumplimiento_metas(
            ventas_periodo_compartido, df_metas, f1, f2, "ingreso_real", skip_preprocessing=True
        )

        # ✅ NUEVO: Calcular metas de ingreso real nominal
        cumplimiento_data_ingreso_nominal, resumen_general_ingreso_nominal = calcular_cumplimiento_metas(
            ventas_periodo_compartido, df_metas, f1, f2, "ingreso_real_nominal", skip_preprocessing=True
        )

        # OPTIMIZACIÓN: Generar HTML renderizado para cada tipo de meta
        html_ventas = render_template('cumplimiento_metas_partial_simple.html',
                                    cumplimiento_data=cumplimiento_data_ventas,
                                    resumen_general=resumen_general_ventas,
                                    periodo_texto=formato_periodo_texto(preset_main, f1, f2),
                                    tipo_meta="ventas")

        html_costo = render_template('cumplimiento_metas_partial_simple.html',
                                   cumplimiento_data=cumplimiento_data_costo,
                                   resumen_general=resumen_general_costo,
                                   periodo_texto=formato_periodo_texto(preset_main, f1, f2),
                                   tipo_meta="costo")

        # ✅ HTML para ingreso real nominal
        html_ingreso_nominal = render_template('cumplimiento_metas_partial_simple.html',
                                              cumplimiento_data=cumplimiento_data_ingreso_nominal,
                                              resumen_general=resumen_general_ingreso_nominal,
                                              periodo_texto=formato_periodo_texto(preset_main, f1, f2),
                                              tipo_meta="ingreso_real_nominal")

        # Por defecto mostrar metas de ventas
        cumplimiento_data = cumplimiento_data_ventas
        resumen_general = resumen_general_ventas

        # Generar texto del período
        periodo_texto = formato_periodo_texto(preset_main, f1, f2)

        print("OK: Precalculo completado con HTML renderizado")

    except Exception as e:
        print(f"Error procesando cumplimiento de metas: {e}")
        import traceback
        traceback.print_exc()

        # Asegurar que todas las variables estén definidas para el template de error
        default_resumen = get_default_resumen_general()
        return render_template("cumplimiento_metas.html",
                                 # Variables principales
                                 cumplimiento_data=[],
                                 resumen_general=default_resumen,
                                 # Variables de los 4 tipos de meta (requeridas por el template)
                                 cumplimiento_data_ventas=[],
                                 resumen_general_ventas=default_resumen,
                                 cumplimiento_data_costo=[],
                                 resumen_general_costo=default_resumen,
                                 cumplimiento_data_ingreso=[],
                                 resumen_general_ingreso=default_resumen,
                                 cumplimiento_data_ingreso_nominal=[],
                                 resumen_general_ingreso_nominal=default_resumen,
                                 # HTML precalculado
                                 html_ventas="",
                                 html_costo="",
                                 html_ingreso_nominal="",
                                 # Variables de configuración
                                 periodo_texto="",
                                 active_tab="cumplimiento-metas",
                                 selected_preset_main=selected_preset_main,
                                 selected_main_range=selected_main_range,
                                 mes_seleccionado=mes_seleccionado,
                                 error=f"Error procesando datos: {str(e)}")

    # DEBUGGING: Verificar todas las variables antes del render
    print("=== DEBUGGING VARIABLES ANTES DE RENDER ===")
    print(f"cumplimiento_data type: {type(cumplimiento_data)}, len: {len(cumplimiento_data) if isinstance(cumplimiento_data, list) else 'N/A'}")
    print(f"resumen_general type: {type(resumen_general)}")
    print(f"cumplimiento_data_ventas type: {type(cumplimiento_data_ventas)}, len: {len(cumplimiento_data_ventas) if isinstance(cumplimiento_data_ventas, list) else 'N/A'}")
    print(f"html_ventas type: {type(html_ventas)}, len: {len(html_ventas) if isinstance(html_ventas, str) else 'N/A'}")

    # Limpiar datos con logging detallado
    clean_cumplimiento_data = clean_data_for_json(cumplimiento_data, "cumplimiento_data")
    clean_resumen_general = clean_data_for_json(resumen_general, "resumen_general")
    clean_cumplimiento_data_ventas = clean_data_for_json(cumplimiento_data_ventas, "cumplimiento_data_ventas")
    clean_resumen_general_ventas = clean_data_for_json(resumen_general_ventas, "resumen_general_ventas")
    clean_cumplimiento_data_costo = clean_data_for_json(cumplimiento_data_costo, "cumplimiento_data_costo")
    clean_resumen_general_costo = clean_data_for_json(resumen_general_costo, "resumen_general_costo")
    clean_cumplimiento_data_ingreso = clean_data_for_json(cumplimiento_data_ingreso, "cumplimiento_data_ingreso")
    clean_resumen_general_ingreso = clean_data_for_json(resumen_general_ingreso, "resumen_general_ingreso")
    clean_cumplimiento_data_ingreso_nominal = clean_data_for_json(cumplimiento_data_ingreso_nominal, "cumplimiento_data_ingreso_nominal")
    clean_resumen_general_ingreso_nominal = clean_data_for_json(resumen_general_ingreso_nominal, "resumen_general_ingreso_nominal")

    return render_template("cumplimiento_metas.html",
                         # Datos por defecto (ventas) - limpiados para JSON
                         cumplimiento_data=clean_cumplimiento_data,
                         resumen_general=clean_resumen_general,
                         # OPTIMIZACIÓN: Datos precalculados de los 4 tipos de meta - limpiados para JSON
                         cumplimiento_data_ventas=clean_cumplimiento_data_ventas,
                         resumen_general_ventas=clean_resumen_general_ventas,
                         cumplimiento_data_costo=clean_cumplimiento_data_costo,
                         resumen_general_costo=clean_resumen_general_costo,
                         cumplimiento_data_ingreso=clean_cumplimiento_data_ingreso,
                         resumen_general_ingreso=clean_resumen_general_ingreso,
                         cumplimiento_data_ingreso_nominal=clean_cumplimiento_data_ingreso_nominal,
                         resumen_general_ingreso_nominal=clean_resumen_general_ingreso_nominal,
                         # OPTIMIZACIÓN: HTML precalculado para cambio instantáneo
                         html_ventas=html_ventas,
                         html_costo=html_costo,
                         html_ingreso_nominal=html_ingreso_nominal,
                         # Datos de configuración
                         periodo_texto=periodo_texto,
                         active_tab="cumplimiento-metas",
                         selected_preset_main=selected_preset_main,
                         selected_main_range=selected_main_range,
                         mes_seleccionado=mes_seleccionado)


@bp.route("/cumplimiento-metas-actualizar", methods=["POST"])
def cumplimiento_metas_actualizar():
    """OPTIMIZACIÓN: Endpoint para recalcular los 3 tipos de meta con nuevo período"""
    try:
        # Cargar datos frescos
        df, channels_disponibles, warehouses_disponibles = get_fresh_data()
        df_metas = get_fresh_metas()

        # Aplicar filtro de mes global - Por defecto el mes actual del sistema
        mes_actual = obtener_mes_actual()
        mes_filtro = request.args.get('mes_filtro', str(mes_actual))
        mes_seleccionado = int(mes_filtro) if mes_filtro and mes_filtro.isdigit() else mes_actual
        df = df[df["Fecha"].dt.month == mes_seleccionado].copy()

        if df.empty:
            return jsonify({
                'success': False,
                'error': 'No se encontraron datos para el filtro aplicado'
            })

        # Obtener parámetros del formulario
        hoy = datetime.now(mazatlan_tz).replace(hour=0, minute=0, second=0, microsecond=0)
        preset_main = request.form.get("preset_main", "mes_actual")

        # Determinar fechas según el preset (misma lógica que la función principal)
        if preset_main == "hoy":
            f1 = hoy
            f2 = hoy + timedelta(days=1)
        elif preset_main in ["7", "15"]:
            dias = int(preset_main)
            f1 = hoy - timedelta(days=dias)
            f2 = hoy + timedelta(days=1)
        elif preset_main == "mes_actual":
            f1 = hoy.replace(day=1)
            f2 = hoy + timedelta(days=1)
        elif preset_main == "mes_completo":
            primer_dia_mes_anterior = (hoy.replace(day=1) - timedelta(days=1)).replace(day=1)
            ultimo_dia_mes_anterior = hoy.replace(day=1) - timedelta(days=1)
            f1 = primer_dia_mes_anterior
            f2 = ultimo_dia_mes_anterior + timedelta(days=1)
        elif preset_main == "personalizado":
            rango = request.form.get("main_range", "")
            if rango:
                if " to " in rango:
                    f1_str, f2_str = rango.split(" to ")
                    f1 = mazatlan_tz.localize(datetime.strptime(f1_str.strip(), "%Y-%m-%d"))
                    f2_temp = mazatlan_tz.localize(datetime.strptime(f2_str.strip(), "%Y-%m-%d"))
                    f2 = f2_temp + timedelta(days=1)
                else:
                    f1 = mazatlan_tz.localize(datetime.strptime(rango.strip(), "%Y-%m-%d"))
                    f2 = f1 + timedelta(days=1)
            else:
                f1 = hoy.replace(day=1)
                f2 = hoy + timedelta(days=1)
        else:
            f1 = hoy.replace(day=1)
            f2 = hoy + timedelta(days=1)

        print(f"INFO: Recalculando los 3 tipos de meta para periodo {f1} - {f2}")

        # RECALCULAR LOS 3 TIPOS DE META con el nuevo período
        cumplimiento_data_ventas, resumen_general_ventas = calcular_cumplimiento_metas(
            df, df_metas, f1, f2, "ventas"
        )

        cumplimiento_data_costo, resumen_general_costo = calcular_cumplimiento_metas(
            df, df_metas, f1, f2, "costo"
        )

        cumplimiento_data_ingreso, resumen_general_ingreso = calcular_cumplimiento_metas(
            df, df_metas, f1, f2, "ingreso_real"
        )

        # Generar texto del período
        periodo_texto = formato_periodo_texto(preset_main, f1, f2)

        # GENERAR HTML ACTUALIZADO PARA LOS 3 TIPOS
        html_ventas = render_template('cumplimiento_metas_partial_simple.html',
                                    cumplimiento_data=cumplimiento_data_ventas,
                                    resumen_general=resumen_general_ventas,
                                    periodo_texto=periodo_texto,
                                    tipo_meta="ventas")

        html_costo = render_template('cumplimiento_metas_partial_simple.html',
                                   cumplimiento_data=cumplimiento_data_costo,
                                   resumen_general=resumen_general_costo,
                                   periodo_texto=periodo_texto,
                                   tipo_meta="costo")

        html_ingreso = render_template('cumplimiento_metas_partial_simple.html',
                                     cumplimiento_data=cumplimiento_data_ingreso,
                                     resumen_general=resumen_general_ingreso,
                                     periodo_texto=periodo_texto,
                                     tipo_meta="ingreso_real")

        # Obtener qué tipo de meta está activo para mostrar
        tipo_meta_activo = request.form.get("tipo_meta_activo", "ventas")

        # Determinar qué HTML mostrar según el tipo activo
        if tipo_meta_activo == "costo":
            html_mostrar = html_costo
        elif tipo_meta_activo == "ingreso_real":
            html_mostrar = html_ingreso
        else:
            html_mostrar = html_ventas

        # Preparar configuraciones de gauges si es necesario - SOLO FILAS PRINCIPALES
        gauge_configs = []
        if tipo_meta_activo == "costo":
            gauge_configs = [canal.get('gauge_config') for canal in cumplimiento_data_costo if canal.get('gauge_config') and canal.get('es_fila_principal') == True]
        elif tipo_meta_activo == "ingreso_real":
            gauge_configs = [canal.get('gauge_config') for canal in cumplimiento_data_ingreso if canal.get('gauge_config') and canal.get('es_fila_principal') == True]

        print(f"OK: Datos recalculados exitosamente")

        return jsonify({
            'success': True,
            'html': html_mostrar,
            'gauge_configs': gauge_configs,
            'tipo_meta': tipo_meta_activo,
            # DATOS ACTUALIZADOS PARA EL FRONTEND - limpios para JSON
            'datos_actualizados': {
                'ventas': {
                    'cumplimiento_data': clean_data_for_json(cumplimiento_data_ventas),
                    'resumen_general': clean_data_for_json(resumen_general_ventas),
                    'html': html_ventas
                },
                'costo': {
                    'cumplimiento_data': clean_data_for_json(cumplimiento_data_costo),
                    'resumen_general': clean_data_for_json(resumen_general_costo),
                    'html': html_costo
                },
                'ingreso_real': {
                    'cumplimiento_data': clean_data_for_json(cumplimiento_data_ingreso),
                    'resumen_general': clean_data_for_json(resumen_general_ingreso),
                    'html': html_ingreso
                },
                'periodo_texto': periodo_texto
            }
        })

    except Exception as e:
        print(f"Error actualizando cumplimiento de metas: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': f'Error procesando datos: {str(e)}'
        })


@bp.route("/cumplimiento-metas-ajax", methods=["POST"])
def cumplimiento_metas_ajax():
    """Endpoint AJAX para actualizar solo la sección de cumplimiento de metas"""
    try:
        # Cargar datos frescos
        df, channels_disponibles, warehouses_disponibles = get_fresh_data()
        df_metas = get_fresh_metas()

        # Aplicar filtro de mes global - Por defecto el mes actual del sistema
        mes_actual = obtener_mes_actual()
        mes_filtro = request.args.get('mes_filtro', str(mes_actual))
        mes_seleccionado = int(mes_filtro) if mes_filtro and mes_filtro.isdigit() else mes_actual
        df = df[df["Fecha"].dt.month == mes_seleccionado].copy()

        if df.empty:
            return jsonify({
                'success': False,
                'error': 'No se encontraron datos para el filtro aplicado'
            })

        # Obtener parámetros del formulario AJAX
        hoy = datetime.now(mazatlan_tz).replace(hour=0, minute=0, second=0, microsecond=0)
        preset_main = request.form.get("preset_main", "mes_actual")
        tipo_meta = request.form.get("tipo_meta", "ventas")  # ventas, costo, o ingreso_real

        print(f"🎯 AJAX Cumplimiento - Período: {preset_main}, Tipo Meta: {tipo_meta}")

        # Determinar fechas según el preset (misma lógica que la función principal)
        if preset_main == "hoy":
            f1 = hoy
            f2 = hoy + timedelta(days=1)
        elif preset_main in ["7", "15"]:
            dias = int(preset_main)
            f1 = hoy - timedelta(days=dias)
            f2 = hoy + timedelta(days=1)
        elif preset_main == "mes_actual":
            f1 = hoy.replace(day=1)
            f2 = hoy + timedelta(days=1)
        elif preset_main == "mes_completo":
            primer_dia_mes_anterior = (hoy.replace(day=1) - timedelta(days=1)).replace(day=1)
            ultimo_dia_mes_anterior = hoy.replace(day=1) - timedelta(days=1)
            f1 = primer_dia_mes_anterior
            f2 = ultimo_dia_mes_anterior + timedelta(days=1)
        elif preset_main == "personalizado":
            rango = request.form.get("main_range")
            if rango:
                if " to " in rango:
                    f1_str, f2_str = rango.split(" to ")
                    f1 = mazatlan_tz.localize(datetime.strptime(f1_str.strip(), "%Y-%m-%d"))
                    f2_temp = mazatlan_tz.localize(datetime.strptime(f2_str.strip(), "%Y-%m-%d"))
                    f2 = f2_temp + timedelta(days=1)
                else:
                    f1 = mazatlan_tz.localize(datetime.strptime(rango.strip(), "%Y-%m-%d"))
                    f2 = f1 + timedelta(days=1)
            else:
                raise ValueError("Rango personalizado inválido")
        else:
            raise ValueError("Preset inválido")

        # Calcular cumplimiento
        cumplimiento_data, resumen_general = calcular_cumplimiento_metas(df, df_metas, f1, f2, tipo_meta)

        # Generar texto del período
        periodo_texto = formato_periodo_texto(preset_main, f1, f2)

        # Renderizar template parcial usando archivo separado (versión simple)
        html_content = render_template('cumplimiento_metas_partial_simple.html',
                                     cumplimiento_data=cumplimiento_data,
                                     resumen_general=resumen_general,
                                     periodo_texto=periodo_texto,
                                     tipo_meta=tipo_meta)

        # Extraer configuraciones de gauges de Plotly para ejecutar desde contexto global - SOLO FILAS PRINCIPALES
        gauge_configs = []
        for canal in cumplimiento_data:
            if (canal.get('tipo_meta') in ['costo', 'ingreso_real'] and
                canal.get('gauge_config') is not None and
                canal.get('es_fila_principal') == True and
                not canal.get('es_subfila', False)):  # Filtro adicional de seguridad
                gauge_configs.append(canal['gauge_config'])

        return jsonify({
            'success': True,
            'html': html_content,
            'gauge_configs': clean_data_for_json(gauge_configs),  # Configuraciones limpias para JSON
            'tipo_meta': tipo_meta  # Para debugging
        })

    except Exception as e:
        print(f"Error en AJAX cumplimiento metas: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': f'Error procesando datos: {str(e)}'
        })


@bp.route("/cumplimiento-metas-detalle-diario", methods=["POST"])
def cumplimiento_metas_detalle_diario():
    """Endpoint para obtener detalle diario de metas vs ventas reales"""
    try:
        from datetime import datetime, date

        # Obtener parámetros
        mes_filtro = request.form.get('mes_filtro', str(obtener_mes_actual()))
        mes_seleccionado = int(mes_filtro) if mes_filtro and mes_filtro.isdigit() else obtener_mes_actual()
        canal_filtro = request.form.get('canal_filtro', 'TODOS')  # 'TODOS' o nombre específico del canal
        tipo_meta = request.form.get('tipo_meta', 'ventas')  # ventas, costo, ingreso_real_nominal

        # Obtener filtros de fecha
        fecha_inicio_str = request.form.get('fecha_inicio', '')
        fecha_fin_str = request.form.get('fecha_fin', '')

        # Convertir strings a objetos date
        fecha_inicio = None
        fecha_fin = None

        if fecha_inicio_str:
            try:
                fecha_inicio = datetime.strptime(fecha_inicio_str, '%Y-%m-%d').date()
            except:
                fecha_inicio = None

        if fecha_fin_str:
            try:
                fecha_fin = datetime.strptime(fecha_fin_str, '%Y-%m-%d').date()
            except:
                fecha_fin = None

        print(f"INFO: Cargando detalle diario - Mes: {mes_seleccionado}, Canal: {canal_filtro}, Tipo: {tipo_meta}, Fechas: {fecha_inicio} a {fecha_fin}")

        # Cargar datos frescos
        df_ventas, _, _ = get_fresh_data(mes_seleccionado)
        df_metas = get_fresh_metas()

        if df_ventas.empty:
            return jsonify({
                'success': False,
                'error': f'No hay datos de ventas disponibles para el mes {mes_seleccionado}'
            })

        if df_metas.empty:
            return jsonify({
                'success': False,
                'error': 'No hay metas configuradas en la base de datos'
            })

        # Filtrar metas por mes
        df_metas_mes = df_metas[df_metas['Fecha'].dt.month == mes_seleccionado].copy()

        if df_metas_mes.empty:
            return jsonify({
                'success': False,
                'error': f'No hay metas configuradas para el mes {mes_seleccionado}'
            })

        # Preparar datos de ventas (sin cancelados, solo canales oficiales)
        df_ventas_naive = df_ventas.copy()
        df_ventas_naive["Fecha"] = df_ventas_naive["Fecha"].dt.tz_localize(None)

        ventas_procesadas = df_ventas_naive[
            (df_ventas_naive["estado"] != "Cancelado") &
            (df_ventas_naive['Channel'].isin(CANALES_CLASIFICACION))
        ].copy()

        # Asegurarse que cantidad sea numérica
        if 'cantidad' in ventas_procesadas.columns:
            ventas_procesadas['cantidad'] = pd.to_numeric(ventas_procesadas['cantidad'], errors='coerce').fillna(0)

        # DEBUG: Verificar ventas procesadas
        print(f"DEBUG: Total registros en ventas_procesadas: {len(ventas_procesadas)}")
        print(f"DEBUG: Canales únicos en ventas_procesadas: {sorted(ventas_procesadas['Channel'].unique())}")

        # Determinar qué campo de meta usar según tipo_meta
        if tipo_meta == 'ventas':
            campo_meta = 'Meta_Diaria'
            campo_ventas = 'Total'
        elif tipo_meta == 'ingreso_real_nominal':
            campo_meta = 'Meta_Ingreso_Real_Diaria'
            campo_ventas = 'Ingreso real'
        elif tipo_meta == 'nominal':  # NUEVO: Ingreso Nominal
            campo_meta = 'Meta_Diaria'
            campo_ventas = 'Total'
        elif tipo_meta == 'real':  # NUEVO: Ingreso Real
            campo_meta = 'Meta_Ingreso_Real_Diaria'
            campo_ventas = 'Ingreso real'
        else:  # costo
            campo_meta = 'Meta_Diaria'
            campo_ventas = 'Costo de venta'

        # DEBUG: Verificar que el campo existe y tiene datos
        if campo_ventas in ventas_procesadas.columns:
            total_ventas_campo = ventas_procesadas[campo_ventas].sum()
            print(f"DEBUG: Campo '{campo_ventas}' existe. Total: ${total_ventas_campo:,.0f}")
            print(f"DEBUG: Registros con {campo_ventas} > 0: {len(ventas_procesadas[ventas_procesadas[campo_ventas] > 0])}")
        else:
            print(f"ERROR: Campo '{campo_ventas}' NO existe en ventas_procesadas")
            print(f"DEBUG: Columnas disponibles: {ventas_procesadas.columns.tolist()}")

        # Agrupar ventas por día y canal
        ventas_diarias = ventas_procesadas.groupby([
            ventas_procesadas['Fecha'].dt.date,
            'Channel'
        ]).agg({
            campo_ventas: 'sum',
            'estado': 'count'  # número de transacciones
        }).reset_index()

        ventas_diarias.columns = ['Fecha', 'Canal', 'Ventas_Reales', 'Num_Transacciones']

        # NORMALIZACIÓN: Estandarizar nombre de "Aliexpress" a "AliExpress" para que coincida con metas
        # Mapeo de nombres de canales para corregir diferencias de mayúsculas/minúsculas
        nombre_canal_map = {
            'Aliexpress': 'AliExpress',
            'aliexpress': 'AliExpress',
            'ALIEXPRESS': 'AliExpress'
        }

        # Aplicar normalización a ventas_diarias
        ventas_diarias['Canal'] = ventas_diarias['Canal'].replace(nombre_canal_map)

        # DEBUG: Mostrar canales en ventas
        print(f"DEBUG: Canales únicos en ventas_diarias (después de normalización): {sorted(ventas_diarias['Canal'].unique())}")
        print(f"DEBUG: Total filas en ventas_diarias: {len(ventas_diarias)}")
        if len(ventas_diarias) > 0:
            print(f"DEBUG: Muestra de ventas_diarias:")
            print(ventas_diarias.head(10))

        # Preparar metas por día y canal
        df_metas_mes['Fecha_Simple'] = df_metas_mes['Fecha'].dt.date
        metas_diarias = df_metas_mes[['Fecha_Simple', 'Canal', campo_meta]].copy()
        metas_diarias.columns = ['Fecha', 'Canal', 'Meta_Diaria']

        # DEBUG: Mostrar canales en metas
        print(f"DEBUG: Canales únicos en metas_diarias: {sorted(metas_diarias['Canal'].unique())}")
        print(f"DEBUG: Total filas en metas_diarias: {len(metas_diarias)}")

        # Combinar metas con ventas
        detalle_completo = pd.merge(
            metas_diarias,
            ventas_diarias,
            on=['Fecha', 'Canal'],
            how='left'
        )

        # DEBUG: Verificar resultado del merge
        print(f"DEBUG: Total filas después del merge: {len(detalle_completo)}")
        print(f"DEBUG: Filas con ventas > 0: {len(detalle_completo[detalle_completo['Ventas_Reales'] > 0])}")
        if len(detalle_completo[detalle_completo['Ventas_Reales'] > 0]) > 0:
            print(f"DEBUG: Muestra de filas con ventas:")
            print(detalle_completo[detalle_completo['Ventas_Reales'] > 0].head(10))

        # Rellenar ventas vacías con 0
        detalle_completo['Ventas_Reales'] = detalle_completo['Ventas_Reales'].fillna(0)
        detalle_completo['Num_Transacciones'] = detalle_completo['Num_Transacciones'].fillna(0).astype(int)

        # Calcular variación
        detalle_completo['Variacion_Absoluta'] = detalle_completo['Ventas_Reales'] - detalle_completo['Meta_Diaria']
        detalle_completo['Variacion_Porcentual'] = (
            (detalle_completo['Ventas_Reales'] / detalle_completo['Meta_Diaria'] * 100)
            - 100
        ).fillna(0)
        detalle_completo['Cumplimiento'] = (
            detalle_completo['Ventas_Reales'] / detalle_completo['Meta_Diaria'] * 100
        ).fillna(0)

        # Calcular acumulado por canal
        detalle_completo = detalle_completo.sort_values(['Canal', 'Fecha'])
        detalle_completo['Meta_Acumulada'] = detalle_completo.groupby('Canal')['Meta_Diaria'].cumsum()
        detalle_completo['Ventas_Acumuladas'] = detalle_completo.groupby('Canal')['Ventas_Reales'].cumsum()
        detalle_completo['Cumplimiento_Acumulado'] = (
            detalle_completo['Ventas_Acumuladas'] / detalle_completo['Meta_Acumulada'] * 100
        ).fillna(0)

        # Filtrar por canal si no es TODOS
        if canal_filtro != 'TODOS':
            detalle_completo = detalle_completo[detalle_completo['Canal'] == canal_filtro]

        # IMPORTANTE: Filtrar por rango de fechas
        hoy = date.today()

        # Si se proporcionan fechas de filtro, usarlas; sino, usar hasta hoy
        if fecha_inicio and fecha_fin:
            detalle_hasta_hoy = detalle_completo[
                (detalle_completo['Fecha'] >= fecha_inicio) &
                (detalle_completo['Fecha'] <= fecha_fin)
            ].copy()
        elif fecha_inicio:
            # Solo fecha inicio proporcionada
            detalle_hasta_hoy = detalle_completo[
                (detalle_completo['Fecha'] >= fecha_inicio) &
                (detalle_completo['Fecha'] <= hoy)
            ].copy()
        elif fecha_fin:
            # Solo fecha fin proporcionada
            detalle_hasta_hoy = detalle_completo[
                detalle_completo['Fecha'] <= fecha_fin
            ].copy()
        else:
            # Sin filtros de fecha, usar hasta hoy
            detalle_hasta_hoy = detalle_completo[detalle_completo['Fecha'] <= hoy].copy()

        # Calcular totales SOLO con días transcurridos
        total_meta = float(detalle_hasta_hoy['Meta_Diaria'].sum())
        total_ventas = float(detalle_hasta_hoy['Ventas_Reales'].sum())
        total_variacion = total_ventas - total_meta
        total_cumplimiento = (total_ventas / total_meta * 100) if total_meta > 0 else 0

        # Convertir fechas a string para JSON (usar SOLO días hasta hoy)
        detalle_hasta_hoy['Fecha'] = detalle_hasta_hoy['Fecha'].astype(str)

        # Preparar datos para la tabla (SOLO días transcurridos)
        tabla_datos = detalle_hasta_hoy.to_dict('records')

        # Preparar datos para el gráfico ACUMULADO (agrupado por fecha si es TODOS los canales)
        if canal_filtro == 'TODOS':
            # Agrupar por fecha para el gráfico y luego acumular
            grafico_datos = detalle_hasta_hoy.groupby('Fecha').agg({
                'Meta_Diaria': 'sum',
                'Ventas_Reales': 'sum'
            }).reset_index()
            # Calcular acumulados para el gráfico
            grafico_datos['Meta_Acumulada'] = grafico_datos['Meta_Diaria'].cumsum()
            grafico_datos['Ventas_Acumuladas'] = grafico_datos['Ventas_Reales'].cumsum()
        else:
            # Para un canal específico, usar directamente los acumulados ya calculados
            grafico_datos = detalle_hasta_hoy[['Fecha', 'Meta_Acumulada', 'Ventas_Acumuladas']].copy()

        # Seleccionar solo las columnas necesarias para el gráfico (acumulados)
        grafico_datos = grafico_datos[['Fecha', 'Meta_Acumulada', 'Ventas_Acumuladas']].to_dict('records')

        # NUEVO: Preparar datos por canal para barómetros (cuando es TODOS)
        datos_por_canal = []
        detalle_por_canal = {}  # NUEVO: Detalle día por día para cada canal

        if canal_filtro == 'TODOS':
            # Obtener el último registro acumulado por cada canal
            ultimo_por_canal = detalle_hasta_hoy.sort_values('Fecha').groupby('Canal').tail(1)

            # Mostrar todos los canales de manera individual (sin agrupación)
            for _, row in ultimo_por_canal.iterrows():
                datos_por_canal.append({
                    'Canal': row['Canal'],
                    'Ventas_Acumuladas': float(row['Ventas_Acumuladas']),
                    'Meta_Acumulada': float(row['Meta_Acumulada']),
                    'Cumplimiento': float(row['Cumplimiento_Acumulado']),
                    'Fecha': row['Fecha']
                })

                # Obtener detalle diario para este canal (TODOS los días del mes, no solo hasta hoy)
                detalle_canal_completo = detalle_completo[detalle_completo['Canal'] == row['Canal']].copy()
                # Convertir fecha a string para JSON
                detalle_canal_completo['Fecha'] = detalle_canal_completo['Fecha'].astype(str)
                detalle_canal = detalle_canal_completo[['Fecha', 'Meta_Diaria', 'Ventas_Reales', 'Cumplimiento']].to_dict('records')
                detalle_por_canal[row['Canal']] = detalle_canal

        # Obtener lista de canales disponibles para el filtro
        canales_disponibles = sorted(df_metas_mes['Canal'].unique().tolist())

        return jsonify({
            'success': True,
            'datos_tabla': tabla_datos,
            'datos_grafico': grafico_datos,
            'datos_por_canal': datos_por_canal,  # NUEVO: Datos individuales por canal para barómetros
            'detalle_por_canal': detalle_por_canal,  # NUEVO: Detalle diario por cada canal
            'totales': {
                'meta': total_meta,
                'ventas': total_ventas,
                'variacion': total_variacion,
                'cumplimiento': total_cumplimiento
            },
            'canales_disponibles': canales_disponibles,
            'canal_seleccionado': canal_filtro,
            'mes_seleccionado': mes_seleccionado,
            'tipo_meta': tipo_meta
        })

    except Exception as e:
        print(f"Error en detalle diario: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': f'Error procesando detalle diario: {str(e)}'
        })


@bp.route("/cumplimiento-metas-dia-actual", methods=["POST"])
def cumplimiento_metas_dia_actual():
    """Endpoint para obtener cumplimiento del día actual por canal"""
    try:
        # Obtener parámetros
        mes_filtro = request.form.get('mes_filtro', str(obtener_mes_actual()))
        mes_seleccionado = int(mes_filtro) if mes_filtro and mes_filtro.isdigit() else obtener_mes_actual()
        tipo_meta = request.form.get('tipo_meta', 'nominal')  # nominal o real

        print(f"INFO: Cargando cumplimiento día actual - Mes: {mes_seleccionado}, Tipo: {tipo_meta}")

        # Cargar datos frescos
        df_ventas, _, _ = get_fresh_data(mes_seleccionado)
        df_metas = get_fresh_metas()

        if df_ventas.empty:
            return jsonify({
                'success': False,
                'error': f'No hay datos de ventas disponibles para el mes {mes_seleccionado}'
            })

        if df_metas.empty:
            return jsonify({
                'success': False,
                'error': 'No hay metas configuradas en la base de datos'
            })

        # Obtener fecha actual (HOY)
        from datetime import date
        hoy = date.today()

        # Filtrar metas para HOY
        df_metas_hoy = df_metas[df_metas['Fecha'].dt.date == hoy].copy()

        if df_metas_hoy.empty:
            return jsonify({
                'success': False,
                'error': f'No hay metas configuradas para hoy ({hoy.strftime("%d/%m/%Y")})'
            })

        # Preparar datos de ventas (sin cancelados, solo canales oficiales)
        df_ventas_naive = df_ventas.copy()
        df_ventas_naive["Fecha"] = df_ventas_naive["Fecha"].dt.tz_localize(None)

        ventas_procesadas = df_ventas_naive[
            (df_ventas_naive["estado"] != "Cancelado") &
            (df_ventas_naive['Channel'].isin(CANALES_CLASIFICACION))
        ].copy()

        # Asegurarse que cantidad sea numérica
        if 'cantidad' in ventas_procesadas.columns:
            ventas_procesadas['cantidad'] = pd.to_numeric(ventas_procesadas['cantidad'], errors='coerce').fillna(0)

        # Determinar qué campo usar según tipo_meta
        if tipo_meta == 'nominal':
            campo_meta = 'Meta_Diaria'
            campo_ventas = 'Total'
        else:  # real
            campo_meta = 'Meta_Ingreso_Real_Diaria'
            campo_ventas = 'Ingreso real'

        # Filtrar ventas de HOY
        ventas_hoy = ventas_procesadas[ventas_procesadas['Fecha'].dt.date == hoy].copy()

        # Agrupar ventas de hoy por canal
        if not ventas_hoy.empty:
            ventas_hoy_por_canal = ventas_hoy.groupby('Channel').agg({
                campo_ventas: 'sum',
                'estado': 'count'
            }).reset_index()
            ventas_hoy_por_canal.columns = ['Canal', 'Ventas_Reales', 'Num_Transacciones']
        else:
            ventas_hoy_por_canal = pd.DataFrame(columns=['Canal', 'Ventas_Reales', 'Num_Transacciones'])

        # Normalizar nombres de canales
        nombre_canal_map = {
            'Aliexpress': 'AliExpress',
            'aliexpress': 'AliExpress',
            'ALIEXPRESS': 'AliExpress'
        }
        ventas_hoy_por_canal['Canal'] = ventas_hoy_por_canal['Canal'].replace(nombre_canal_map)

        # Combinar metas con ventas
        detalle_hoy = pd.merge(
            df_metas_hoy[['Canal', campo_meta]],
            ventas_hoy_por_canal,
            on='Canal',
            how='left'
        )

        # Rellenar ventas vacías con 0
        detalle_hoy['Ventas_Reales'] = detalle_hoy['Ventas_Reales'].fillna(0)
        detalle_hoy['Num_Transacciones'] = detalle_hoy['Num_Transacciones'].fillna(0).astype(int)

        # Calcular métricas
        detalle_hoy['Variacion'] = detalle_hoy['Ventas_Reales'] - detalle_hoy[campo_meta]
        detalle_hoy['Cumplimiento'] = (
            (detalle_hoy['Ventas_Reales'] / detalle_hoy[campo_meta] * 100)
        ).fillna(0)

        # Calcular totales globales
        total_meta = float(detalle_hoy[campo_meta].sum())
        total_ventas = float(detalle_hoy['Ventas_Reales'].sum())
        total_variacion = total_ventas - total_meta
        total_cumplimiento = (total_ventas / total_meta * 100) if total_meta > 0 else 0

        # Preparar datos para la tabla
        tabla_datos = detalle_hoy[[
            'Canal', campo_meta, 'Ventas_Reales', 'Variacion', 'Cumplimiento', 'Num_Transacciones'
        ]].copy()

        # Renombrar columna de meta para la respuesta
        tabla_datos.rename(columns={campo_meta: 'Meta_Diaria'}, inplace=True)

        # Ordenar por ventas reales descendente
        tabla_datos = tabla_datos.sort_values('Ventas_Reales', ascending=False)

        return jsonify({
            'success': True,
            'fecha': hoy.strftime('%d/%m/%Y'),
            'fecha_completa': hoy.strftime('%A, %d de %B de %Y'),
            'totales': {
                'meta': total_meta,
                'ventas': total_ventas,
                'variacion': total_variacion,
                'cumplimiento': total_cumplimiento
            },
            'datos_tabla': tabla_datos.to_dict('records'),
            'tipo_meta': tipo_meta
        })

    except Exception as e:
        print(f"Error en cumplimiento día actual: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': f'Error procesando datos: {str(e)}'
        })


@bp.route("/cumplimiento-metas-exportar-excel", methods=["POST"])
def cumplimiento_metas_exportar_excel():
    """Endpoint para exportar detalle diario a Excel"""
    try:
        # Obtener parámetros (mismo código que el endpoint anterior)
        mes_filtro = request.form.get('mes_filtro', str(obtener_mes_actual()))
        mes_seleccionado = int(mes_filtro) if mes_filtro and mes_filtro.isdigit() else obtener_mes_actual()
        canal_filtro = request.form.get('canal_filtro', 'TODOS')
        tipo_meta = request.form.get('tipo_meta', 'ventas')

        print(f"INFO: Exportando a Excel - Mes: {mes_seleccionado}, Canal: {canal_filtro}, Tipo: {tipo_meta}")

        # Cargar datos frescos (mismo procesamiento que antes)
        df_ventas, _, _ = get_fresh_data(mes_seleccionado)
        df_metas = get_fresh_metas()

        if df_ventas.empty or df_metas.empty:
            return jsonify({'success': False, 'error': 'No hay datos disponibles para exportar'})

        # Filtrar metas por mes
        df_metas_mes = df_metas[df_metas['Fecha'].dt.month == mes_seleccionado].copy()

        # Preparar datos de ventas
        df_ventas_naive = df_ventas.copy()
        df_ventas_naive["Fecha"] = df_ventas_naive["Fecha"].dt.tz_localize(None)

        ventas_procesadas = df_ventas_naive[
            (df_ventas_naive["estado"] != "Cancelado") &
            (df_ventas_naive['Channel'].isin(CANALES_CLASIFICACION))
        ].copy()

        if 'cantidad' in ventas_procesadas.columns:
            ventas_procesadas['cantidad'] = pd.to_numeric(ventas_procesadas['cantidad'], errors='coerce').fillna(0)

        # Determinar campo según tipo de meta
        if tipo_meta == 'ventas':
            campo_meta = 'Meta_Diaria'
            campo_ventas = 'Total'
            nombre_tipo = 'Ventas'
        elif tipo_meta == 'ingreso_real_nominal':
            campo_meta = 'Meta_Ingreso_Real_Diaria'
            campo_ventas = 'Ingreso real'
            nombre_tipo = 'Ingreso Real Nominal'
        elif tipo_meta == 'nominal':
            # Ingreso Nominal desde el modal
            campo_meta = 'Meta_Diaria'
            campo_ventas = 'Total'
            nombre_tipo = 'Ingreso Nominal'
        elif tipo_meta == 'real':
            # Ingreso Real desde el modal
            campo_meta = 'Meta_Ingreso_Real_Diaria'
            campo_ventas = 'Ingreso real'
            nombre_tipo = 'Ingreso Real'
        else:
            campo_meta = 'Meta_Diaria'
            campo_ventas = 'Costo de venta'
            nombre_tipo = 'Costo'

        # Agrupar ventas por día y canal
        ventas_diarias = ventas_procesadas.groupby([
            ventas_procesadas['Fecha'].dt.date,
            'Channel'
        ]).agg({
            campo_ventas: 'sum',
            'estado': 'count'
        }).reset_index()

        ventas_diarias.columns = ['Fecha', 'Canal', 'Ventas_Reales', 'Num_Transacciones']

        # NORMALIZACIÓN: Estandarizar nombre de "Aliexpress" a "AliExpress" (igual que en detalle diario)
        nombre_canal_map = {
            'Aliexpress': 'AliExpress',
            'aliexpress': 'AliExpress',
            'ALIEXPRESS': 'AliExpress'
        }
        ventas_diarias['Canal'] = ventas_diarias['Canal'].replace(nombre_canal_map)

        # Preparar metas
        df_metas_mes['Fecha_Simple'] = df_metas_mes['Fecha'].dt.date
        metas_diarias = df_metas_mes[['Fecha_Simple', 'Canal', campo_meta]].copy()
        metas_diarias.columns = ['Fecha', 'Canal', 'Meta_Diaria']

        # Combinar
        detalle_completo = pd.merge(
            metas_diarias,
            ventas_diarias,
            on=['Fecha', 'Canal'],
            how='left'
        )

        detalle_completo['Ventas_Reales'] = detalle_completo['Ventas_Reales'].fillna(0)
        detalle_completo['Num_Transacciones'] = detalle_completo['Num_Transacciones'].fillna(0).astype(int)

        # Calcular métricas
        detalle_completo['Variacion_Absoluta'] = detalle_completo['Ventas_Reales'] - detalle_completo['Meta_Diaria']
        detalle_completo['Variacion_Porcentual'] = (
            (detalle_completo['Ventas_Reales'] / detalle_completo['Meta_Diaria'] * 100) - 100
        ).fillna(0)
        detalle_completo['Cumplimiento'] = (
            detalle_completo['Ventas_Reales'] / detalle_completo['Meta_Diaria'] * 100
        ).fillna(0)

        # Acumulados
        detalle_completo = detalle_completo.sort_values(['Canal', 'Fecha'])
        detalle_completo['Meta_Acumulada'] = detalle_completo.groupby('Canal')['Meta_Diaria'].cumsum()
        detalle_completo['Ventas_Acumuladas'] = detalle_completo.groupby('Canal')['Ventas_Reales'].cumsum()
        detalle_completo['Cumplimiento_Acumulado'] = (
            detalle_completo['Ventas_Acumuladas'] / detalle_completo['Meta_Acumulada'] * 100
        ).fillna(0)

        # Filtrar por canal
        if canal_filtro != 'TODOS':
            detalle_completo = detalle_completo[detalle_completo['Canal'] == canal_filtro]

        # IMPORTANTE: Filtrar solo días transcurridos (hasta hoy) - igual que en la vista
        from datetime import date
        hoy = date.today()
        detalle_completo = detalle_completo[detalle_completo['Fecha'] <= hoy].copy()

        # Renombrar columnas para Excel (solo las columnas que mostramos en la tabla)
        df_export = detalle_completo[[
            'Fecha', 'Canal', 'Meta_Diaria', 'Ventas_Reales', 'Variacion_Absoluta',
            'Cumplimiento', 'Num_Transacciones'
        ]].copy()

        df_export.columns = [
            'Fecha', 'Canal', 'Meta Diaria', f'{nombre_tipo} Reales', 'Variación ($)',
            'Cumplimiento (%)', 'Transacciones'
        ]

        # ==================== CREAR DOS PESTAÑAS: INGRESO NOMINAL E INGRESO REAL ====================
        # Preparar datos para Ingreso Nominal (Total)
        ventas_diarias_nominal = ventas_procesadas.groupby([
            ventas_procesadas['Fecha'].dt.date,
            'Channel'
        ]).agg({
            'Total': 'sum',
            'estado': 'count'
        }).reset_index()
        ventas_diarias_nominal.columns = ['Fecha', 'Canal', 'Ventas_Reales', 'Num_Transacciones']
        ventas_diarias_nominal['Canal'] = ventas_diarias_nominal['Canal'].replace({
            'Aliexpress': 'AliExpress',
            'aliexpress': 'AliExpress',
            'ALIEXPRESS': 'AliExpress'
        })

        # Preparar metas para Ingreso Nominal
        # Crear Fecha_Simple si no existe ya
        if 'Fecha_Simple' not in df_metas_mes.columns:
            df_metas_mes['Fecha_Simple'] = df_metas_mes['Fecha'].dt.date
        metas_diarias_nominal = df_metas_mes[['Fecha_Simple', 'Canal', 'Meta_Diaria']].copy()
        metas_diarias_nominal.columns = ['Fecha', 'Canal', 'Meta_Diaria']

        # Combinar para Ingreso Nominal
        detalle_nominal = pd.merge(
            metas_diarias_nominal,
            ventas_diarias_nominal,
            on=['Fecha', 'Canal'],
            how='left'
        )
        detalle_nominal['Ventas_Reales'] = detalle_nominal['Ventas_Reales'].fillna(0)
        detalle_nominal['Num_Transacciones'] = detalle_nominal['Num_Transacciones'].fillna(0).astype(int)
        detalle_nominal['Variacion_Absoluta'] = detalle_nominal['Ventas_Reales'] - detalle_nominal['Meta_Diaria']
        detalle_nominal['Cumplimiento'] = (
            detalle_nominal['Ventas_Reales'] / detalle_nominal['Meta_Diaria'] * 100
        ).fillna(0)

        # Filtrar por canal y fecha
        if canal_filtro != 'TODOS':
            detalle_nominal = detalle_nominal[detalle_nominal['Canal'] == canal_filtro]
        detalle_nominal = detalle_nominal[detalle_nominal['Fecha'] <= hoy].copy()

        # DataFrame para exportar Ingreso Nominal
        df_export_nominal = detalle_nominal[[
            'Fecha', 'Canal', 'Meta_Diaria', 'Ventas_Reales', 'Variacion_Absoluta',
            'Cumplimiento', 'Num_Transacciones'
        ]].copy()
        df_export_nominal.columns = [
            'Fecha', 'Canal', 'Meta Diaria', 'Ingreso Nominal', 'Variación ($)',
            'Cumplimiento (%)', 'Transacciones'
        ]

        # Preparar datos para Ingreso Real
        ventas_diarias_real = ventas_procesadas.groupby([
            ventas_procesadas['Fecha'].dt.date,
            'Channel'
        ]).agg({
            'Ingreso real': 'sum',
            'estado': 'count'
        }).reset_index()
        ventas_diarias_real.columns = ['Fecha', 'Canal', 'Ventas_Reales', 'Num_Transacciones']
        ventas_diarias_real['Canal'] = ventas_diarias_real['Canal'].replace({
            'Aliexpress': 'AliExpress',
            'aliexpress': 'AliExpress',
            'ALIEXPRESS': 'AliExpress'
        })

        # Preparar metas para Ingreso Real
        metas_diarias_real = df_metas_mes[['Fecha_Simple', 'Canal', 'Meta_Ingreso_Real_Diaria']].copy()
        metas_diarias_real.columns = ['Fecha', 'Canal', 'Meta_Diaria']

        # Combinar para Ingreso Real
        detalle_real = pd.merge(
            metas_diarias_real,
            ventas_diarias_real,
            on=['Fecha', 'Canal'],
            how='left'
        )
        detalle_real['Ventas_Reales'] = detalle_real['Ventas_Reales'].fillna(0)
        detalle_real['Num_Transacciones'] = detalle_real['Num_Transacciones'].fillna(0).astype(int)
        detalle_real['Variacion_Absoluta'] = detalle_real['Ventas_Reales'] - detalle_real['Meta_Diaria']
        detalle_real['Cumplimiento'] = (
            detalle_real['Ventas_Reales'] / detalle_real['Meta_Diaria'] * 100
        ).fillna(0)

        # Filtrar por canal y fecha
        if canal_filtro != 'TODOS':
            detalle_real = detalle_real[detalle_real['Canal'] == canal_filtro]
        detalle_real = detalle_real[detalle_real['Fecha'] <= hoy].copy()

        # DataFrame para exportar Ingreso Real
        df_export_real = detalle_real[[
            'Fecha', 'Canal', 'Meta_Diaria', 'Ventas_Reales', 'Variacion_Absoluta',
            'Cumplimiento', 'Num_Transacciones'
        ]].copy()
        df_export_real.columns = [
            'Fecha', 'Canal', 'Meta Diaria', 'Ingreso Real', 'Variación ($)',
            'Cumplimiento (%)', 'Transacciones'
        ]

        # Crear archivo Excel en memoria con DOS pestañas
        output = BytesIO()

        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Pestaña 1: Ingreso Nominal
            df_export_nominal.to_excel(writer, sheet_name='Ingreso Nominal', index=False)
            worksheet_nominal = writer.sheets['Ingreso Nominal']

            # Ajustar ancho de columnas para Ingreso Nominal
            for column in worksheet_nominal.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet_nominal.column_dimensions[column_letter].width = adjusted_width

            # Pestaña 2: Ingreso Real
            df_export_real.to_excel(writer, sheet_name='Ingreso Real', index=False)
            worksheet_real = writer.sheets['Ingreso Real']

            # Ajustar ancho de columnas para Ingreso Real
            for column in worksheet_real.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet_real.column_dimensions[column_letter].width = adjusted_width

        output.seek(0)

        # Nombre del archivo
        meses_nombres = {8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"}
        mes_nombre = meses_nombres.get(mes_seleccionado, f"Mes{mes_seleccionado}")
        canal_nombre = canal_filtro if canal_filtro != 'TODOS' else 'TodosCanales'
        filename = f"Detalle_Metas_{mes_nombre}_{canal_nombre}.xlsx"

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )

    except Exception as e:
        print(f"Error exportando a Excel: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': f'Error exportando: {str(e)}'
        })
