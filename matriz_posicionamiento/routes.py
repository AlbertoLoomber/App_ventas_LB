"""
Rutas de Matriz de Posicionamiento
Endpoints Flask

HUB-COMPATIBLE MODULE
"""

from flask import render_template, request, jsonify, current_app
from datetime import datetime
import time
import sys

# ============================================================================
# IMPORT BLUEPRINT (HUB-COMPATIBLE)
# ============================================================================

from matriz_posicionamiento.blueprint import bp
# Alias para compatibilidad con código existente
matriz_bp = bp

# ============================================================================
# IMPORT SERVICES (HUB-COMPATIBLE)
# ============================================================================

try:
    from matriz_posicionamiento.services import (
        generar_datos_matriz,
        generar_datos_matriz_clasificacion,
        generar_datos_matriz_categorias,
        generar_datos_matriz_clasificacion_con_rango_dias,
        obtener_lista_skus
    )
    SERVICES_AVAILABLE = True
    print("✅ [ROUTES] Services imported successfully")
except ImportError as e:
    print(f"❌ [ROUTES] Error importing services: {e}")
    SERVICES_AVAILABLE = False
    raise  # No podemos continuar sin services

# ============================================================================
# HELPER FUNCTIONS - HUB COMPATIBILITY
# ============================================================================

def get_data_and_config():
    """
    Helper function para obtener datos y configuración
    Compatible con Hub y app.py legacy

    Returns:
        tuple: (df, channels, warehouses, CANALES_CLASIFICACION)
    """
    # Importar database manager del módulo
    from matriz_posicionamiento.database import db_manager, CANALES_CLASIFICACION as DEFAULT_CANALES

    # Cargar datos (ya tiene fallback automático a app.py o query directa)
    df, channels, warehouses = db_manager.cargar_acumulado_mensual()

    # Intentar obtener canales del Hub, si no usar los del módulo
    try:
        from database import get_canales_clasificacion
        CANALES_CLASIFICACION = get_canales_clasificacion()
        print("✅ [ROUTES] Using CANALES from Hub")
    except ImportError:
        # Usar canales por defecto del módulo
        CANALES_CLASIFICACION = DEFAULT_CANALES
        print("✅ [ROUTES] Using CANALES from module")

    return df, channels, warehouses, CANALES_CLASIFICACION


@matriz_bp.route("/", methods=["GET", "POST"])
def index():
    """
    Página principal de Matriz de Posicionamiento
    % Ingreso Real vs % ROI por Canal
    """
    tiempo_inicio_total = time.time()
    print(f"\n[PERFORMANCE] INICIO - Matriz de Posicionamiento")
    sys.stdout.flush()

    try:
        # Cargar datos y configuración (Hub-compatible)
        df, channels, warehouses, CANALES_CLASIFICACION = get_data_and_config()

        # Determinar mes actual
        fecha_hoy = datetime.now()
        mes_actual = fecha_hoy.month
        año_actual = fecha_hoy.year
        mes_actual_str = f"{año_actual}{mes_actual:02d}"  # Formato YYYYMM (ej: 202410)
        mes_nombre = fecha_hoy.strftime('%B %Y')

        # Nombres de meses en español
        meses_español = {
            1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
            5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
            9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
        }

        # Generar lista de meses disponibles (últimos 6 meses)
        meses_disponibles = []
        fecha_actual = datetime.now()

        for i in range(6):
            mes = fecha_actual.month - i
            año = fecha_actual.year

            # Ajustar año si el mes es negativo
            while mes <= 0:
                mes += 12
                año -= 1

            meses_disponibles.append({
                'valor': f"{año}{mes:02d}",  # Formato YYYYMM (ej: 202410)
                'nombre': meses_español[mes],
                'año': año
            })

        # Generar datos para la matriz de posicionamiento
        matriz_data = generar_datos_matriz(
            df,
            mes_filtro=mes_actual,
            marca_filtro='Ambos',  # Por defecto mostrar ambas marcas
            canales_clasificacion=CANALES_CLASIFICACION
        )

        # Generar datos para la matriz de categorías
        matriz_cat_data = generar_datos_matriz_categorias(
            df,
            mes_filtro=mes_actual,
            canales_clasificacion=CANALES_CLASIFICACION
        )

        # Obtener listas únicas de canales y categorías para los filtros
        if not df.empty and 'Fecha' in df.columns:
            df_mes_actual = df[df['Fecha'].dt.month == mes_actual].copy() if mes_actual else df.copy()
        else:
            df_mes_actual = df.copy()

        canales_disponibles = sorted(CANALES_CLASIFICACION)

        # Obtener categorías únicas (excluyendo nulos y vacíos)
        categorias_disponibles = []
        if not df_mes_actual.empty and 'Categoria' in df_mes_actual.columns:
            cats = df_mes_actual['Categoria'].dropna().unique()
            categorias_disponibles = sorted([c for c in cats if c and str(c).strip() != '' and str(c).strip() != 'Sin Categoría'])

        print(f"📋 [MATRIZ] Canales disponibles: {canales_disponibles}")
        print(f"📋 [MATRIZ] Categorías disponibles: {categorias_disponibles}")

        # Renderizar template
        resultado = render_template(
            'matriz.html',  # Nombre único para evitar conflictos con templates/index.html
            titulo="Matriz de Posicionamiento",
            subtitulo=f"% Ingreso Real vs ROI - {mes_nombre}",
            active_tab="matriz-posicionamiento",
            matriz_data=matriz_data,
            matriz_cat_data=matriz_cat_data,
            mes_actual=mes_actual,
            mes_actual_str=mes_actual_str,
            mes_nombre=mes_nombre,
            meses_disponibles=meses_disponibles,
            canales_disponibles=canales_disponibles,
            categorias_disponibles=categorias_disponibles
        )

        tiempo_fin_total = time.time()
        print(f"✅ [PERFORMANCE] TIEMPO TOTAL Matriz: {tiempo_fin_total - tiempo_inicio_total:.3f}s")
        sys.stdout.flush()

        return resultado

    except Exception as e:
        print(f"❌ ERROR en Matriz de Posicionamiento: {e}")
        import traceback
        traceback.print_exc()

        # Retornar página de error con valores por defecto completos
        default_estadisticas = {
            'total_skus': 0,
            'total_ventas': 0,
            'ingreso_real_total': 0,
            'roi_promedio': 0,
            'ingreso_real_pct_promedio': 0
        }

        return render_template(
            'matriz.html',  # Nombre único para evitar conflictos con templates/index.html
            titulo="Matriz de Posicionamiento",
            active_tab="matriz-posicionamiento",
            error=str(e),
            matriz_data={'datasets': [], 'canales': [], 'estadisticas': default_estadisticas},
            matriz_cat_data={'datasets': [], 'categorias': [], 'estadisticas': default_estadisticas},
            mes_actual=None,
            mes_actual_str='',
            mes_nombre='',
            meses_disponibles=[],
            canales_disponibles=[],
            categorias_disponibles=[]
        )


@matriz_bp.route("/actualizar", methods=["POST"])
def actualizar():
    """
    Endpoint AJAX para actualizar la matriz con diferentes filtros
    """
    try:
        # Cargar datos y configuración (Hub-compatible)
        df, _, _, CANALES_CLASIFICACION = get_data_and_config()
        # Obtener parámetros
        mes_filtro = request.json.get('mes', None)
        marca_filtro = request.json.get('marca', 'Ambos')  # Loomber, Otros, Ambos
        nivel_detalle = request.json.get('nivel', 'canal')  # canal, sku, categoria

        print(f"📥 [AJAX] Filtros recibidos - Mes: {mes_filtro}, Marca: {marca_filtro}")

        # Generar datos según nivel de detalle
        if nivel_detalle == 'canal':
            matriz_data = generar_datos_matriz(
                df,
                mes_filtro=mes_filtro,
                marca_filtro=marca_filtro,
                canales_clasificacion=CANALES_CLASIFICACION
            )
        else:
            # TODO: Implementar niveles de detalle por SKU y Categoría
            matriz_data = {'datasets': [], 'canales': [], 'estadisticas': {}}

        return jsonify({
            'success': True,
            'data': matriz_data
        })

    except Exception as e:
        print(f"❌ ERROR actualizando matriz: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@matriz_bp.route("/actualizar-categorias", methods=["POST"])
def actualizar_categorias():
    """
    Endpoint AJAX para actualizar la matriz de categorías con diferentes filtros
    Soporta filtros por: mes, canales (array), categorías (array)
    """
    try:
        # Cargar datos y configuración (Hub-compatible)
        df, _, _, CANALES_CLASIFICACION = get_data_and_config()
        # Obtener parámetros del request
        mes_filtro = request.json.get('mes', None)
        canales_filtro = request.json.get('canales', [])  # Array de canales
        categorias_filtro = request.json.get('categorias', [])  # Array de categorías

        print(f"📥 [AJAX DEBUG] Request JSON completo: {request.json}")
        print(f"📥 [AJAX DEBUG] Canales ANTES de procesar: {canales_filtro} (tipo: {type(canales_filtro)}, len: {len(canales_filtro) if canales_filtro else 0})")
        print(f"📥 [AJAX DEBUG] Categorías ANTES de procesar: {categorias_filtro} (tipo: {type(categorias_filtro)}, len: {len(categorias_filtro) if categorias_filtro else 0})")

        # Si están vacíos, usar None
        if not canales_filtro or len(canales_filtro) == 0:
            canales_filtro = None
        if not categorias_filtro or len(categorias_filtro) == 0:
            categorias_filtro = None

        print(f"📥 [AJAX] Filtros recibidos - Mes: {mes_filtro}, Canales: {canales_filtro}, Categorías: {categorias_filtro}")

        # Generar datos con filtros aplicados (usar nombres correctos de parámetros)
        matriz_data = generar_datos_matriz_categorias(
            df,
            mes_filtro=mes_filtro,
            canales_clasificacion=CANALES_CLASIFICACION,
            canales_filtro=canales_filtro,
            categorias_filtro=categorias_filtro
        )

        return jsonify({
            'success': True,
            'data': matriz_data
        })

    except Exception as e:
        print(f"❌ ERROR actualizando matriz categorías: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@matriz_bp.route("/obtener-skus", methods=["POST"])
def obtener_skus():
    """
    Endpoint AJAX para obtener la lista de SKUs disponibles según el mes
    """
    try:
        # Cargar datos (Hub-compatible)
        df, _, _, _ = get_data_and_config()
        # Obtener parámetros
        mes_filtro = request.json.get('mes', None)

        print(f"📥 [AJAX] Obteniendo SKUs para mes: {mes_filtro}")

        # Obtener lista de SKUs
        skus_lista = obtener_lista_skus(df, mes_filtro=mes_filtro)

        return jsonify({
            'success': True,
            'skus': skus_lista
        })

    except Exception as e:
        print(f"❌ ERROR obteniendo SKUs: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@matriz_bp.route("/actualizar-clasificacion", methods=["POST"])
def actualizar_clasificacion():
    """
    Endpoint AJAX para actualizar la matriz de clasificación con diferentes filtros
    Soporta filtros por: mes, canal, skus (array)
    """
    try:
        # Cargar datos (Hub-compatible)
        df, _, _, _ = get_data_and_config()
        # Obtener parámetros del request
        mes_filtro = request.json.get('mes', None)
        canales_filtro = request.json.get('canales', ['Todos'])  # Array de canales
        skus_filtro = request.json.get('skus', [])  # Array de SKUs

        # Si está vacío, usar None
        if not skus_filtro or len(skus_filtro) == 0:
            skus_filtro = None

        # Convertir array de canales a string para la función (temporalmente)
        # Si tiene "Todos" o está vacío, usar "Todos"
        if not canales_filtro or 'Todos' in canales_filtro:
            canal_filtro = 'Todos'
        elif len(canales_filtro) == 1:
            canal_filtro = canales_filtro[0]
        else:
            # Múltiples canales: pasar como lista
            canal_filtro = canales_filtro

        print(f"📥 [AJAX] Filtros recibidos - Mes: {mes_filtro}, Canales: {canales_filtro}, SKUs: {skus_filtro}")

        # Generar datos con filtros aplicados
        matriz_data = generar_datos_matriz_clasificacion(
            df,
            mes_filtro=mes_filtro,
            canal_filtro=canal_filtro,
            skus_seleccionados=skus_filtro
        )

        return jsonify({
            'success': True,
            'data': matriz_data
        })

    except Exception as e:
        print(f"❌ ERROR actualizando matriz clasificación: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@matriz_bp.route("/comparar-3-meses", methods=["POST"])
def comparar_3_meses():
    """
    Endpoint AJAX para comparar SKUs en los últimos 3 meses
    Retorna datos comparativos con tendencias
    """
    from dateutil.relativedelta import relativedelta

    try:
        # Cargar datos (Hub-compatible)
        df, _, _, _ = get_data_and_config()
        # Obtener parámetros
        mes_actual_str = request.json.get('mes', None)  # Formato: YYYYMM
        skus_filtro = request.json.get('skus', [])
        canales_filtro = request.json.get('canales', ['Todos'])  # Array de canales

        # Convertir array de canales a string para la función (temporalmente)
        if not canales_filtro or 'Todos' in canales_filtro:
            canal_filtro = 'Todos'
        elif len(canales_filtro) == 1:
            canal_filtro = canales_filtro[0]
        else:
            canal_filtro = canales_filtro

        print(f"📥 [COMPARAR] Parámetros recibidos - mes_actual_str: '{mes_actual_str}', skus: {skus_filtro}, canales: {canales_filtro}")

        if not mes_actual_str or not skus_filtro:
            return jsonify({
                'success': False,
                'error': 'Se requiere mes y SKUs'
            }), 400

        # Validar formato de mes
        mes_actual_str = str(mes_actual_str).strip()
        if len(mes_actual_str) < 6:
            return jsonify({
                'success': False,
                'error': f'Formato de mes inválido: {mes_actual_str}. Debe ser YYYYMM (ej: 202410)'
            }), 400

        # Convertir mes_actual a fecha
        try:
            año_actual = int(mes_actual_str[:4])
            mes_actual = int(mes_actual_str[4:6])
        except ValueError as e:
            return jsonify({
                'success': False,
                'error': f'Error al procesar mes: {mes_actual_str} - {str(e)}'
            }), 400
        fecha_actual = datetime(año_actual, mes_actual, 1)

        # Calcular meses anteriores
        fecha_mes_1 = fecha_actual - relativedelta(months=1)
        fecha_mes_2 = fecha_actual - relativedelta(months=2)

        mes_2 = int(fecha_mes_2.strftime('%Y%m'))
        mes_1 = int(fecha_mes_1.strftime('%Y%m'))
        mes_0 = int(fecha_actual.strftime('%Y%m'))

        print(f"📅 [COMPARAR] Meses: {mes_2}, {mes_1}, {mes_0}")
        print(f"📦 [COMPARAR] SKUs: {skus_filtro}")

        # Calcular el día máximo del mes actual para comparación justa
        # Si hoy es 3 de octubre, comparar días 1-3 de cada mes
        from datetime import datetime as dt_now
        import pytz
        tz = pytz.timezone('America/Los_Angeles')
        hoy = dt_now.now(tz)

        # Si el mes actual coincide con el mes_0, usar el día de hoy
        if hoy.year == año_actual and hoy.month == mes_actual:
            dia_maximo = hoy.day
            print(f"📅 [COMPARAR] Comparación con mismo rango: Días 1-{dia_maximo} de cada mes")
        else:
            # Si es un mes pasado, comparar mes completo
            dia_maximo = None
            print(f"📅 [COMPARAR] Comparación de meses completos (mes histórico)")

        # Obtener datos de cada mes con MISMO rango de días
        from .services import generar_datos_matriz_clasificacion_con_rango_dias
        datos_mes_2 = generar_datos_matriz_clasificacion_con_rango_dias(df, mes_2, canal_filtro, skus_filtro, dia_maximo)
        datos_mes_1 = generar_datos_matriz_clasificacion_con_rango_dias(df, mes_1, canal_filtro, skus_filtro, dia_maximo)
        datos_mes_0 = generar_datos_matriz_clasificacion_con_rango_dias(df, mes_0, canal_filtro, skus_filtro, dia_maximo)

        # Procesar comparación
        comparacion = procesar_comparacion_3_meses(
            datos_mes_2, datos_mes_1, datos_mes_0,
            mes_2, mes_1, mes_0
        )

        return jsonify({
            'success': True,
            'data': comparacion
        })

    except Exception as e:
        print(f"❌ ERROR en comparar-3-meses: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


def procesar_comparacion_3_meses(datos_mes_2, datos_mes_1, datos_mes_0, mes_2, mes_1, mes_0):
    """
    Procesa los datos de 3 meses y genera datasets comparativos con líneas de tendencia
    """
    from datetime import datetime

    # Crear diccionario para mapear SKU-Canal a datos de cada mes
    skus_data = {}

    # Procesar mes -2
    for sku_info in datos_mes_2.get('skus', []):
        key = f"{sku_info['sku']}_{sku_info['canal']}"
        if key not in skus_data:
            skus_data[key] = {
                'sku': sku_info['sku'],
                'canal': sku_info['canal'],
                'canal_abrev': sku_info['canal_abrev'],
                'color_canal': sku_info['color_canal'],
                'descripcion': sku_info.get('descripcion', ''),
                'meses': {}
            }
        skus_data[key]['meses'][mes_2] = {
            'ingreso_real_pct': sku_info['ingreso_real_pct'],
            'roi_pct': sku_info['roi_pct'],
            'ingreso_real': sku_info.get('ingreso_real', 0),
            'zona': sku_info['zona'],
            'clasificacion': sku_info['clasificacion'],
            'ventas': sku_info['ventas'],
            'color_zona': sku_info.get('color_zona', '#e9ecef'),
            'color_texto': sku_info.get('color_texto', '#000'),
            'icono': sku_info.get('icono', '⚪')
        }

    # Procesar mes -1
    for sku_info in datos_mes_1.get('skus', []):
        key = f"{sku_info['sku']}_{sku_info['canal']}"
        if key not in skus_data:
            skus_data[key] = {
                'sku': sku_info['sku'],
                'canal': sku_info['canal'],
                'canal_abrev': sku_info['canal_abrev'],
                'color_canal': sku_info['color_canal'],
                'descripcion': sku_info.get('descripcion', ''),
                'meses': {}
            }
        skus_data[key]['meses'][mes_1] = {
            'ingreso_real_pct': sku_info['ingreso_real_pct'],
            'roi_pct': sku_info['roi_pct'],
            'ingreso_real': sku_info.get('ingreso_real', 0),
            'zona': sku_info['zona'],
            'clasificacion': sku_info['clasificacion'],
            'ventas': sku_info['ventas'],
            'color_zona': sku_info.get('color_zona', '#e9ecef'),
            'color_texto': sku_info.get('color_texto', '#000'),
            'icono': sku_info.get('icono', '⚪')
        }

    # Procesar mes actual
    for sku_info in datos_mes_0.get('skus', []):
        key = f"{sku_info['sku']}_{sku_info['canal']}"
        if key not in skus_data:
            skus_data[key] = {
                'sku': sku_info['sku'],
                'canal': sku_info['canal'],
                'canal_abrev': sku_info['canal_abrev'],
                'color_canal': sku_info['color_canal'],
                'descripcion': sku_info.get('descripcion', ''),
                'meses': {}
            }
        skus_data[key]['meses'][mes_0] = {
            'ingreso_real_pct': sku_info['ingreso_real_pct'],
            'roi_pct': sku_info['roi_pct'],
            'ingreso_real': sku_info.get('ingreso_real', 0),
            'zona': sku_info['zona'],
            'clasificacion': sku_info['clasificacion'],
            'ventas': sku_info['ventas'],
            'color_zona': sku_info.get('color_zona', '#e9ecef'),
            'color_texto': sku_info.get('color_texto', '#000'),
            'icono': sku_info.get('icono', '⚪')
        }

    # Generar datasets para Chart.js (líneas conectadas)
    datasets = []
    skus_comparativos = []
    count_mejoraron = 0
    count_empeoraron = 0
    count_estable = 0

    for key, sku_data in skus_data.items():
        # Solo procesar si tiene datos en los 3 meses
        if len(sku_data['meses']) >= 2:  # Al menos 2 meses para comparar
            puntos = []

            # Crear puntos para cada mes con toda la información
            for mes in sorted(sku_data['meses'].keys()):
                mes_data = sku_data['meses'][mes]
                puntos.append({
                    'x': mes_data['ingreso_real_pct'],
                    'y': mes_data['roi_pct'],
                    'ingreso_real': mes_data.get('ingreso_real', 0),
                    'zona': mes_data.get('zona', 'Sin Clasificar'),
                    'clasificacion': mes_data.get('clasificacion', 'Sin Clasificar'),
                    'ventas': mes_data.get('ventas', 0),
                    'color_zona': mes_data.get('color_zona', '#e9ecef'),
                    'color_texto': mes_data.get('color_texto', '#000'),
                    'icono': mes_data.get('icono', '⚪')
                })

            # Crear dataset con línea conectada
            datasets.append({
                'label': f"{sku_data['canal_abrev']} - {sku_data['sku']}",
                'data': puntos,
                'borderColor': sku_data['color_canal'],
                'backgroundColor': sku_data['color_canal'],
                'showLine': True,  # Mostrar línea conectando puntos
                'pointRadius': 6,
                'pointStyle': 'cross',
                'fill': False,
                '_sku': sku_data['sku'],
                '_canal': sku_data['canal'],
                '_descripcion': sku_data['descripcion']
            })

            # Calcular tendencia
            tendencia = calcular_tendencia(sku_data['meses'], mes_2, mes_1, mes_0)

            # Información comparativa para la tabla
            skus_comparativos.append({
                'sku': sku_data['sku'],
                'canal': sku_data['canal'],
                'canal_abrev': sku_data['canal_abrev'],
                'color_canal': sku_data['color_canal'],
                'descripcion': sku_data['descripcion'],
                'meses_data': sku_data['meses'],
                'tendencia': tendencia
            })

            # Contar por tendencia
            if tendencia == 'mejoro':
                count_mejoraron += 1
            elif tendencia == 'empeoro':
                count_empeoraron += 1
            else:
                count_estable += 1

    # Formatear nombres de meses
    def format_mes(mes_int):
        fecha = datetime.strptime(str(mes_int), '%Y%m')
        meses_esp = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun',
                     'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']
        return meses_esp[fecha.month - 1]

    # Calcular ROI máximo considerando LOS 3 MESES para ajustar eje Y dinámicamente
    all_roi_values = []
    for dataset in datasets:
        for point in dataset['data']:
            all_roi_values.append(point['y'])  # y = ROI %

    roi_max = max(all_roi_values) if all_roi_values else 100
    import math
    eje_y_max = max(100, math.ceil(roi_max * 1.1 / 10) * 10)

    print(f"📊 [COMPARAR] ROI máximo en 3 meses: {roi_max:.1f}%")
    print(f"📊 [COMPARAR] Eje Y ajustado a: 0% - {eje_y_max}%")

    # Actualizar estadísticas con el eje Y calculado para los 3 meses
    estadisticas = datos_mes_0.get('estadisticas', {}).copy()
    estadisticas['eje_y_max'] = eje_y_max

    return {
        'datasets': datasets,
        'skus': skus_comparativos,
        'meses': [mes_2, mes_1, mes_0],
        'meses_labels': [format_mes(mes_2), format_mes(mes_1), format_mes(mes_0)],
        'resumen': {
            'mejoraron': count_mejoraron,
            'empeoraron': count_empeoraron,
            'estable': count_estable
        },
        'estadisticas': estadisticas
    }


def calcular_tendencia(meses_data, mes_2, mes_1, mes_0):
    """
    Calcula si el SKU mejoró, empeoró o se mantuvo estable
    """
    # Obtener datos del primer y último mes disponible
    meses_disponibles = sorted(meses_data.keys())

    if len(meses_disponibles) < 2:
        return 'estable'

    primer_mes = meses_data[meses_disponibles[0]]
    ultimo_mes = meses_data[meses_disponibles[-1]]

    # Calcular cambios
    cambio_ir = ultimo_mes['ingreso_real_pct'] - primer_mes['ingreso_real_pct']
    cambio_roi = ultimo_mes['roi_pct'] - primer_mes['roi_pct']

    # Determinar tendencia (umbral de 2 puntos porcentuales)
    if cambio_ir > 2 or cambio_roi > 5:
        return 'mejoro'
    elif cambio_ir < -2 or cambio_roi < -5:
        return 'empeoro'
    else:
        return 'estable'
