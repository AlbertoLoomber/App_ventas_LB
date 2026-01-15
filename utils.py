# -*- coding: utf-8 -*-
"""
Utilidades compartidas
Funciones auxiliares para formateo, serialización y procesamiento de datos
"""

import json
import math
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from config import MAZATLAN_TZ, MESES_ESPANOL_LOWER


# ====== SERIALIZACIÓN JSON ======

class NumpyEncoder(json.JSONEncoder):
    """Encoder personalizado para convertir tipos NumPy a tipos JSON serializables"""

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


def clean_data_for_json(data, path=""):
    """
    Limpia los datos para serialización JSON, reemplazando NaN, inf, Undefined y otros tipos problemáticos

    Args:
        data: Datos a limpiar (puede ser dict, list, número, etc.)
        path: Path interno para debugging (opcional)

    Returns:
        Datos limpios serializables a JSON
    """
    from jinja2.runtime import Undefined

    try:
        # Verificar si es un objeto Undefined de Jinja2
        if isinstance(data, Undefined):
            print(f"WARNING: Encontrado objeto Undefined en path '{path}', reemplazando con None")
            return None

        if data is None:
            return None
        elif isinstance(data, list):
            return [clean_data_for_json(item, f"{path}[{i}]") for i, item in enumerate(data)]
        elif isinstance(data, dict):
            cleaned_dict = {}
            for key, value in data.items():
                try:
                    cleaned_dict[key] = clean_data_for_json(value, f"{path}.{key}")
                except Exception as e:
                    print(f"ERROR limpiando clave '{key}' en path '{path}': {e}")
                    cleaned_dict[key] = None
            return cleaned_dict
        elif isinstance(data, (np.integer, int)):
            return int(data)
        elif isinstance(data, (np.floating, float)):
            if math.isnan(data) or math.isinf(data):
                print(f"WARNING: Valor NaN/inf encontrado en path '{path}', reemplazando con 0")
                return 0
            return float(data)
        elif hasattr(data, 'item'):  # Para tipos numpy
            return clean_data_for_json(data.item(), path)
        elif str(type(data)) == "<class 'jinja2.runtime.Undefined'>":
            print(f"WARNING: Objeto Undefined detectado por string en path '{path}', reemplazando con None")
            return None
        else:
            # Verificar si se puede serializar a JSON
            try:
                json.dumps(data)
                return data
            except (TypeError, ValueError) as e:
                print(f"WARNING: Objeto no serializable en path '{path}': {type(data)} - {e}, reemplazando con string")
                return str(data)

    except Exception as e:
        print(f"ERROR en clean_data_for_json en path '{path}': {e}")
        return None


# ====== FORMATEO DE FECHAS Y PERÍODOS ======

def formato_periodo_texto(preset_main, fecha_inicio, fecha_fin):
    """
    Genera texto descriptivo del período analizado

    Args:
        preset_main: Tipo de preset ("hoy", "7", "15", "mes_actual", "mes_completo", "personalizado")
        fecha_inicio: Fecha de inicio del período
        fecha_fin: Fecha de fin del período

    Returns:
        str: Descripción textual del período
    """
    if preset_main == "hoy":
        return f"Hoy ({fecha_inicio.strftime('%d/%m/%Y')})"
    elif preset_main == "7":
        return "Últimos 7 días"
    elif preset_main == "15":
        return "Últimos 15 días"
    elif preset_main == "mes_actual":
        mes_nombre = MESES_ESPANOL_LOWER[fecha_inicio.month]
        return f"Mes actual hasta hoy ({mes_nombre} {fecha_inicio.year})"
    elif preset_main == "mes_completo":
        mes_nombre = MESES_ESPANOL_LOWER[fecha_inicio.month]
        return f"Mes completo ({mes_nombre} {fecha_inicio.year})"
    else:
        # Para personalizado, restar 1 día a fecha_fin para mostrar la fecha real
        fecha_fin_display = fecha_fin - timedelta(days=1)
        return f"{fecha_inicio.strftime('%d/%m/%Y')} - {fecha_fin_display.strftime('%d/%m/%Y')}"


def formato_rango(f1, f2):
    """
    Formatea un rango de fechas de manera compacta

    Args:
        f1: Fecha de inicio
        f2: Fecha de fin

    Returns:
        str: Rango formateado (ej: "24 Jun 2025" o "24 Jun al 26 Jun 2025")
    """
    # Verificar si es un solo día (mismo día de inicio y fin)
    if f1.date() == f2.date():
        # Mostrar solo un día: "24 Jun 2025"
        return f"{f1.day} {f1.strftime('%b')} {f1.year}"
    else:
        # Mostrar rango de días: "24 Jun al 26 Jun 2025"
        dia1 = f1.day
        mes1 = f1.strftime('%b')
        dia2 = f2.day
        mes2 = f2.strftime('%b')
        anio = f2.year
        return f"{dia1} {mes1} al {dia2} {mes2} {anio}"


def formato_inverso(label, val_main, val_compare, tipo="$"):
    """
    Formatea valores con delta inverso (útil para métricas donde menos es mejor, como cancelaciones)

    Args:
        label: Etiqueta del valor
        val_main: Valor principal
        val_compare: Valor de comparación
        tipo: Tipo de formato ("$", "%", u otro)

    Returns:
        dict: Diccionario con label, valor, diferencia y delta
    """
    delta = val_main - val_compare
    pct = (delta / val_compare * 100) if val_compare else 0
    # Para cancelaciones: si bajan (pct negativo) = bueno = verde
    # Si suben (pct positivo) = malo = rojo
    simbolo = "▲" if pct >= 0 else "▼"
    color_delta = -pct  # Invertir: negativo se vuelve positivo (verde)

    if tipo == "$":
        formato_val = f"${val_main:,.0f}"
    elif tipo == "%":
        formato_val = f"{val_main:.1f} %"
    else:
        formato_val = f"{val_main:,.0f}"

    return {
        "label": label,
        "valor": formato_val,
        "diferencia": f"{simbolo} {abs(pct):.1f} %",
        "delta": color_delta  # Delta invertido para el color
    }


def obtener_mes_actual():
    """
    Obtiene el mes actual según timezone de Mazatlán

    Returns:
        int: Número del mes actual (1-12)
    """
    return datetime.now(MAZATLAN_TZ).month


# ====== AGRUPACIÓN Y PROCESAMIENTO DE DATOS ======

def agrupar_por(df, granularidad, limite_hora=None):
    """
    Agrupa un DataFrame por granularidad temporal

    Args:
        df: DataFrame con columna 'Fecha' y 'Total'
        granularidad: "hora" o "dia"
        limite_hora: Límite de hora para mostrar (opcional)

    Returns:
        Series: Datos agrupados por la granularidad especificada
    """
    if granularidad == "hora":
        if not df.empty and limite_hora is not None:
            # Solo mostrar hasta la última hora con datos
            hora_max = min(df["Fecha"].dt.hour.max(), limite_hora)
            horas_rango = range(0, hora_max + 1)
        else:
            # Mostrar todas las 24 horas
            horas_rango = range(24)
        return df.groupby(df["Fecha"].dt.hour)["Total"].sum().reindex(horas_rango, fill_value=0)
    else:
        return df.groupby(df["Fecha"].dt.strftime("%d-%b"))["Total"].sum().sort_index(
            key=lambda x: pd.to_datetime(x, format="%d-%b")
        )


def agrupar_condicional(df, granularidad, condicion, limite_hora=None):
    """
    Agrupa un DataFrame aplicando una condición antes de agrupar

    Args:
        df: DataFrame con columna 'Fecha', 'Total' y 'estado'
        granularidad: "hora" o "dia"
        condicion: "cancelado", "neto", u otra
        limite_hora: Límite de hora para mostrar (opcional)

    Returns:
        Series: Datos agrupados con la condición aplicada
    """
    if condicion == "cancelado":
        df = df[df["estado"] == "Cancelado"]
    elif condicion == "neto":
        df = df[df["estado"] != "Cancelado"]
    return agrupar_por(df, granularidad, limite_hora)


def resumen_periodo(df_periodo, df_comparado, granularidad):
    """
    Genera un resumen comparativo entre dos períodos

    Args:
        df_periodo: DataFrame del período principal
        df_comparado: DataFrame del período de comparación
        granularidad: "hora" o "dia"

    Returns:
        dict: Resumen con métricas comparativas
    """
    # Calcular métricas del período principal
    ventas_main = df_periodo[df_periodo["estado"] != "Cancelado"]["Total"].sum() if not df_periodo.empty else 0
    cancelaciones_main = df_periodo[df_periodo["estado"] == "Cancelado"]["Total"].sum() if not df_periodo.empty else 0

    # Calcular métricas del período de comparación
    ventas_compare = df_comparado[df_comparado["estado"] != "Cancelado"]["Total"].sum() if not df_comparado.empty else 0
    cancelaciones_compare = df_comparado[df_comparado["estado"] == "Cancelado"]["Total"].sum() if not df_comparado.empty else 0

    # Calcular deltas
    delta_ventas = ((ventas_main - ventas_compare) / ventas_compare * 100) if ventas_compare else 0
    delta_cancelaciones = ((cancelaciones_main - cancelaciones_compare) / cancelaciones_compare * 100) if cancelaciones_compare else 0

    return {
        "ventas_main": ventas_main,
        "ventas_compare": ventas_compare,
        "delta_ventas": delta_ventas,
        "cancelaciones_main": cancelaciones_main,
        "cancelaciones_compare": cancelaciones_compare,
        "delta_cancelaciones": delta_cancelaciones
    }


# ====== VALORES POR DEFECTO ======

def get_default_resumen_general():
    """
    Retorna un diccionario con valores por defecto para resumen_general

    Returns:
        dict: Estructura de resumen_general con valores iniciales
    """
    return {
        'ventas_totales': 0,
        'costo_venta_porcentaje': 0,
        'evolucion_costo': [],
        'evolucion_ventas': [],
        'evolucion_ingreso': [],
        'evolucion_roi': [],
        'top_canales': [],
        'top_skus': [],
        'metricas_comparativas': {
            'ventas': {'actual': 0, 'anterior': 0, 'delta': 0},
            'costo': {'actual': 0, 'anterior': 0, 'delta': 0},
            'ingreso': {'actual': 0, 'anterior': 0, 'delta': 0}
        }
    }
def crear_gauge_costo_config(porcentaje_actual, canal="Canal"):
    """
    Crear configuración JSON de Plotly para gauge de costo de venta
    Configuración específica para Metas de Costo con zona amarilla MÁXIMAMENTE CENTRADA:
    - Rango visual: 45%-57% (12 puntos totales) para máxima expansión de zona objetivo
    - Verde (45%-48%): 3 puntos = 25% del espacio visual
    - Amarillo (48%-54%): 6 puntos = 50% del espacio visual - DOMINANCIA TOTAL
    - Rojo (54%-57%): 3 puntos = 25% del espacio visual
    - Zona amarilla ocupa LA MITAD del gráfico con máxima granularidad y centrado perfecto
    """
    try:
        # Validar entrada
        if porcentaje_actual is None or porcentaje_actual < 0:
            print(f"WARN: porcentaje_actual inválido: {porcentaje_actual}, usando 50 por defecto")
            porcentaje_actual = 50.0
        gauge_config = {
            'data': [
                # Indicador principal
                {
                    'type': 'indicator',
                    'mode': 'gauge+number',
                    'value': porcentaje_actual,
                    'title': {'text': ""},  # Sin título
                    'number': {
                        'font': {'size': 28, 'color': '#1a1a1a', 'family': 'Inter, -apple-system, sans-serif', 'weight': 700}, 
                        'suffix': "%",
                        'valueformat': '.1f'
                    },
                    'domain': {'x': [0, 1], 'y': [0, 1]},
                    'gauge': {
                        'axis': {
                            'range': [45, 57], 
                            'tickmode': 'array',
                            'tickvals': [45, 57],  # Solo extremos
                            'ticktext': ['45', '57'],  # Solo etiquetas extremas
                            'tickwidth': 2, 
                            'tickcolor': "#666", 
                            'ticklen': 10,
                            'tickfont': {'size': 9, 'color': '#4a5568', 'family': 'Inter, -apple-system, sans-serif'}
                        },
                        'bar': {'color': "transparent", 'thickness': 0},  # Sin línea de progreso
                        'bgcolor': "rgba(248, 250, 252, 0.9)",
                        'borderwidth': 0,
                        'steps': [
                            # Verde intenso (45%-47%): Excelente (costo muy bajo)
                            {'range': [45, 46], 'color': "#20c997"},    # Verde teal puro
                            {'range': [46, 47], 'color': "#24b386"},    # Transición teal→verde
                            
                            # Verde claro (47%-48%): Muy bueno (costo bajo)
                            {'range': [47, 48], 'color': "#28a745"},    # Verde éxito puro
                            
                            # DEGRADADO AMARILLO (48%-54%): De débil a fuerte hacia el rojo
                            {'range': [48, 48.5], 'color': "#fff3cd"},  # Amarillo muy claro (cerca del verde)
                            {'range': [48.5, 49], 'color': "#ffecb3"},  # Amarillo suave
                            {'range': [49, 49.5], 'color': "#ffe082"},  # Amarillo claro
                            {'range': [49.5, 50], 'color': "#ffd54f"},  # Amarillo medio-claro
                            {'range': [50, 50.5], 'color': "#ffc107"},  # Amarillo estándar (centro)
                            {'range': [50.5, 51], 'color': "#ffb300"},  # Amarillo medio-fuerte
                            {'range': [51, 51.5], 'color': "#ffa000"},  # Amarillo fuerte
                            {'range': [51.5, 52], 'color': "#ff9800"},  # Amarillo-naranja suave
                            {'range': [52, 52.5], 'color': "#ff8f00"},  # Amarillo-naranja medio
                            {'range': [52.5, 53], 'color': "#ff8a65"},  # Amarillo-naranja fuerte
                            {'range': [53, 53.5], 'color': "#ff7043"},  # Casi naranja
                            {'range': [53.5, 54], 'color': "#ff6f00"},  # Naranja-amarillo (cerca del rojo)
                            
                            # Rojo (54%-57%): Alto Riesgo - después del rango objetivo
                            {'range': [54, 55.5], 'color': "#dc3545"},  # Rojo puro
                            {'range': [55.5, 57], 'color': "#c82333"}   # Rojo intenso final
                        ],
                        'threshold': {
                            'line': {'color': "#1f2937", 'width': 6},  # Aguja principal más gruesa y elegante
                            'thickness': 0.85,
                            'value': porcentaje_actual
                        },
                        'shape': "angular"
                    }
                },
                # Mini aguja marcadora en 48% 
                {
                    'type': 'indicator',
                    'mode': 'gauge',
                    'value': 48,  # Valor fijo en 48%
                    'domain': {'x': [0, 1], 'y': [0, 1]},
                    'gauge': {
                        'axis': {'range': [45, 57], 'visible': False},  # Mismo rango, eje invisible
                        'bar': {'color': "transparent", 'thickness': 0},
                        'bgcolor': "transparent",
                        'borderwidth': 0,
                        'threshold': {
                            'line': {'color': "#ffc107", 'width': 4},  # Aguja amarilla más gruesa
                            'thickness': 0.8,  # Más larga que antes
                            'value': 48
                        }
                    }
                },
                # Mini aguja marcadora en 54%
                {
                    'type': 'indicator', 
                    'mode': 'gauge',
                    'value': 54,  # Valor fijo en 54%
                    'domain': {'x': [0, 1], 'y': [0, 1]},
                    'gauge': {
                        'axis': {'range': [45, 57], 'visible': False},  # Mismo rango, eje invisible
                        'bar': {'color': "transparent", 'thickness': 0},
                        'bgcolor': "transparent",
                        'borderwidth': 0,
                        'threshold': {
                            'line': {'color': "#ffc107", 'width': 4},  # Aguja amarilla más gruesa (mismo color)
                            'thickness': 0.8,  # Más larga que antes
                            'value': 54
                        }
                    }
                }
            ],
            'layout': {
                'height': 240,
                'margin': {'l': 30, 'r': 30, 't': 50, 'b': 30},
                'paper_bgcolor': 'rgba(0,0,0,0)',
                'plot_bgcolor': 'rgba(0,0,0,0)',
                'font': {'color': '#374151', 'family': 'Inter, -apple-system, BlinkMacSystemFont, sans-serif', 'size': 12},
                'showlegend': False,
                'hovermode': False
            },
            'config': {'displayModeBar': False, 'responsive': True},
            'div_id': f"gauge-costo-{canal.lower().replace(' ', '-').replace('_', '-')}"
        }
        
        return gauge_config
        
    except Exception as e:
        print(f"Error creando configuración de gauge de costo: {e}")
        return None

def crear_gauge_ingreso_config(porcentaje_actual, canal="Canal"):
    """
    Crear configuración JSON de Plotly para gauge de ingreso real
    Configuración específica para Metas de Ingreso Real con zona amarilla CENTRADA:
    - Rango visual: 5%-20% (15 puntos totales) para centrar zona objetivo
    - Rojo (5%-10%): Rentabilidad baja
    - Amarillo (10%-15%): Zona objetivo con degradado inteligente (12 segmentos)
    - Verde (15%-20%): Excelente rentabilidad
    - MISMO DISEÑO VISUAL que metas de costo pero adaptado a rangos de ingreso real
    """
    try:
        # Validar entrada
        if porcentaje_actual is None or porcentaje_actual < 0:
            print(f"WARN: porcentaje_actual inválido: {porcentaje_actual}, usando 12 por defecto")
            porcentaje_actual = 12.0
        gauge_config = {
            'data': [
                # Indicador principal
                {
                    'type': 'indicator',
                    'mode': 'gauge+number',
                    'value': porcentaje_actual,
                    'title': {'text': ""},  # Sin título
                    'number': {
                        'font': {'size': 28, 'color': '#1a1a1a', 'family': 'Inter, -apple-system, sans-serif', 'weight': 700}, 
                        'suffix': "%",
                        'valueformat': '.1f'
                    },
                    'domain': {'x': [0, 1], 'y': [0, 1]},
                    'gauge': {
                        'axis': {
                            'range': [5, 20], 
                            'tickmode': 'array',
                            'tickvals': [5, 20],  # Solo extremos
                            'ticktext': ['5', '20'],  # Solo etiquetas extremas
                            'tickwidth': 2, 
                            'tickcolor': "#666", 
                            'ticklen': 10,
                            'tickfont': {'size': 9, 'color': '#4a5568', 'family': 'Inter, -apple-system, sans-serif'}
                        },
                        'bar': {'color': "transparent", 'thickness': 0},  # Sin línea de progreso
                        'bgcolor': "rgba(248, 250, 252, 0.9)",
                        'borderwidth': 0,
                        'steps': [
                            # Rojo (5%-10%): Rentabilidad baja
                            {'range': [5, 7], 'color': "#dc3545"},    # Rojo puro
                            {'range': [7, 10], 'color': "#c82333"},   # Rojo intenso
                            
                            # DEGRADADO AMARILLO (10%-15%): De fuerte hacia verde
                            {'range': [10, 10.4], 'color': "#ff6f00"},  # Naranja-amarillo (cerca del rojo)
                            {'range': [10.4, 10.8], 'color': "#ff7043"},  # Casi naranja
                            {'range': [10.8, 11.2], 'color': "#ff8a65"},  # Amarillo-naranja fuerte
                            {'range': [11.2, 11.6], 'color': "#ff8f00"},  # Amarillo-naranja medio
                            {'range': [11.6, 12], 'color': "#ffa000"},    # Amarillo fuerte
                            {'range': [12, 12.4], 'color': "#ffb300"},    # Amarillo medio-fuerte
                            {'range': [12.4, 12.8], 'color': "#ffc107"},  # Amarillo estándar (centro)
                            {'range': [12.8, 13.2], 'color': "#ffd54f"},  # Amarillo medio-claro
                            {'range': [13.2, 13.6], 'color': "#ffe082"},  # Amarillo claro
                            {'range': [13.6, 14], 'color': "#ffecb3"},    # Amarillo suave
                            {'range': [14, 14.4], 'color': "#fff3cd"},    # Amarillo muy claro
                            {'range': [14.4, 15], 'color': "#fff8e1"},    # Amarillo muy claro (cerca del verde)
                            
                            # Verde (15%-20%): Excelente rentabilidad
                            {'range': [15, 17.5], 'color': "#28a745"},   # Verde éxito puro
                            {'range': [17.5, 20], 'color': "#20c997"}     # Verde teal puro
                        ],
                        'threshold': {
                            'line': {'color': "#1f2937", 'width': 6},  # Aguja principal más gruesa y elegante
                            'thickness': 0.85,
                            'value': porcentaje_actual
                        },
                        'shape': "angular"
                    }
                },
                # Mini aguja marcadora en 10% 
                {
                    'type': 'indicator',
                    'mode': 'gauge',
                    'value': 10,  # Valor fijo en 10%
                    'domain': {'x': [0, 1], 'y': [0, 1]},
                    'gauge': {
                        'axis': {'range': [5, 20], 'visible': False},  # Mismo rango, eje invisible
                        'bar': {'color': "transparent", 'thickness': 0},
                        'bgcolor': "transparent",
                        'borderwidth': 0,
                        'threshold': {
                            'line': {'color': "#ffc107", 'width': 4},  # Aguja amarilla más gruesa
                            'thickness': 0.8,  # Más larga que antes
                            'value': 10
                        }
                    }
                },
                # Mini aguja marcadora en 15%
                {
                    'type': 'indicator', 
                    'mode': 'gauge',
                    'value': 15,  # Valor fijo en 15%
                    'domain': {'x': [0, 1], 'y': [0, 1]},
                    'gauge': {
                        'axis': {'range': [5, 20], 'visible': False},  # Mismo rango, eje invisible
                        'bar': {'color': "transparent", 'thickness': 0},
                        'bgcolor': "transparent",
                        'borderwidth': 0,
                        'threshold': {
                            'line': {'color': "#ffc107", 'width': 4},  # Aguja amarilla más gruesa (mismo color)
                            'thickness': 0.8,  # Más larga que antes
                            'value': 15
                        }
                    }
                }
            ],
            'layout': {
                'height': 240,
                'margin': {'l': 30, 'r': 30, 't': 50, 'b': 30},
                'paper_bgcolor': 'rgba(0,0,0,0)',
                'plot_bgcolor': 'rgba(0,0,0,0)',
                'font': {'color': '#374151', 'family': 'Inter, -apple-system, BlinkMacSystemFont, sans-serif', 'size': 12},
                'showlegend': False,
                'hovermode': False
            },
            'config': {'displayModeBar': False, 'responsive': True},
            'div_id': f"gauge-ingreso-{canal.lower().replace(' ', '-').replace('_', '-')}"
        }
        
        return gauge_config
        
    except Exception as e:
        print(f"Error creando configuración de gauge de ingreso real: {e}")
        return None

