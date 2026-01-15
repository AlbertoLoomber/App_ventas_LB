# -*- coding: utf-8 -*-
"""
Servicios del módulo de Distribución de Inventario
Lógica de negocio para distribuir inventario entre canales
"""

import pandas as pd
from database import get_distribucion_inventario, get_distribucion_semanal_inventario


def obtener_meses_disponibles():
    """
    Retorna la lista de meses disponibles para filtrar

    Returns:
        list: Lista de diccionarios con value y label del mes
    """
    meses = [
        {'value': 'Diciembre 2025', 'label': 'Diciembre 2025'},
        {'value': 'Enero 2026', 'label': 'Enero 2026'},
        {'value': 'Febrero 2026', 'label': 'Febrero 2026'},
        {'value': 'Marzo 2026', 'label': 'Marzo 2026'},
        {'value': 'Abril 2026', 'label': 'Abril 2026'},
    ]
    return meses


def procesar_distribucion_inventario(mes_nombre='Diciembre 2025'):
    """
    Procesa los datos de distribución de inventario para un mes específico

    Args:
        mes_nombre: Nombre del mes a filtrar (ej: 'Diciembre 2025')

    Returns:
        dict: Diccionario con datos procesados incluyendo:
              - datos_tabla: Lista de diccionarios con datos para la tabla
              - resumen_general: Resumen con totales
              - canales: Lista de canales únicos
    """
    # Obtener datos de distribución
    df = get_distribucion_inventario(mes_nombre)

    if df.empty:
        return {
            'datos_tabla': [],
            'resumen_general': {
                'total_skus': 0,
                'total_disponible': 0,
                'total_asignado': 0,
                'total_canales': 0
            },
            'canales': []
        }

    # Convertir a lista de diccionarios para el template
    datos_tabla = df.to_dict('records')

    # Calcular resumen general
    skus_unicos = df['sku'].nunique()
    canales_unicos = sorted(df['Channel'].unique().tolist())
    total_disponible = df.groupby('sku')['Disponible_Para_Vender'].first().sum()
    total_asignado = df['Disponible_Para_Vender_Canal_FINAL'].sum()

    resumen_general = {
        'total_skus': int(skus_unicos),
        'total_disponible': float(total_disponible),
        'total_asignado': float(total_asignado),
        'total_canales': len(canales_unicos)
    }

    return {
        'datos_tabla': datos_tabla,
        'resumen_general': resumen_general,
        'canales': canales_unicos
    }


def obtener_resumen_por_canal(mes_nombre='Diciembre 2025'):
    """
    Obtiene resumen de distribución agrupado por canal

    Args:
        mes_nombre: Nombre del mes a filtrar

    Returns:
        list: Lista de diccionarios con resumen por canal
    """
    df = get_distribucion_inventario(mes_nombre)

    if df.empty:
        return []

    # Agrupar por canal
    resumen_canal = df.groupby('Channel').agg({
        'sku': 'nunique',
        'Disponible_Para_Vender_Canal_FINAL': 'sum',
        'peso_combinado_normalizado': 'mean'
    }).reset_index()

    resumen_canal.columns = ['canal', 'total_skus', 'total_asignado', 'peso_promedio']

    # Ordenar por total asignado descendente
    resumen_canal = resumen_canal.sort_values('total_asignado', ascending=False)

    return resumen_canal.to_dict('records')


def obtener_distribucion_por_sku(sku, mes_nombre='Diciembre 2025'):
    """
    Obtiene la distribución de un SKU específico entre todos sus canales

    Args:
        sku: SKU a consultar
        mes_nombre: Nombre del mes a filtrar

    Returns:
        dict: Información del SKU con su distribución por canal
    """
    df = get_distribucion_inventario(mes_nombre)

    if df.empty:
        return None

    # Filtrar por SKU
    df_sku = df[df['sku'] == sku].copy()

    if df_sku.empty:
        return None

    # Información general del SKU
    primera_fila = df_sku.iloc[0]

    info_sku = {
        'sku': sku,
        'descripcion': primera_fila['descripcion'],
        'disponible_total': float(primera_fila['Disponible_Para_Vender']),
        'forecast_mes': float(primera_fila['Forecast_Mes']),
        'distribucion_canales': []
    }

    # Distribución por canal
    for _, row in df_sku.iterrows():
        info_sku['distribucion_canales'].append({
            'canal': row['Channel'],
            'peso': float(row['peso_combinado_normalizado']),
            'venta_promedio_2m': float(row['venta_promedio_mensual_2m']),
            'capacidad_maxima': int(row['capacidad_maxima_canal']),
            'tipo_asignacion': row['tipo_asignacion'],
            'asignacion_final': int(row['Disponible_Para_Vender_Canal_FINAL']),
            'porcentaje_asignado': round(row['Disponible_Para_Vender_Canal_FINAL'] / primera_fila['Disponible_Para_Vender'] * 100, 1) if primera_fila['Disponible_Para_Vender'] > 0 else 0
        })

    return info_sku


def procesar_distribucion_semanal(mes_nombre='Diciembre 2025'):
    """
    Procesa los datos de distribución semanal de inventario para un mes específico

    Args:
        mes_nombre: Nombre del mes a filtrar (ej: 'Diciembre 2025')

    Returns:
        dict: Diccionario con datos procesados incluyendo:
              - datos_tabla: Lista de diccionarios con datos semanales
              - resumen_semanal: Resumen con totales por semana
              - semanas: Lista de semanas únicas
    """
    # Obtener datos de distribución semanal
    df = get_distribucion_semanal_inventario(mes_nombre)

    if df.empty:
        return {
            'datos_tabla': [],
            'resumen_semanal': [],
            'semanas': [],
            'canales': []
        }

    # Convertir a lista de diccionarios para el template
    datos_tabla = df.to_dict('records')

    # Calcular resumen por semana
    # IMPORTANTE: inventario_fisico y asignacion_semana se repiten por canal
    # Solo debemos contar UNA VEZ por SKU

    # Para inventario físico: tomar solo un registro por SKU-semana
    inventario_por_semana = df.groupby(['semana', 'sku']).first()[['inventario_fisico']].groupby('semana').sum().reset_index()

    # Para asignación: sumar asignacion_canal (ya es específica por canal)
    asignacion_por_semana = df.groupby('semana')['asignacion_canal'].sum().reset_index()

    # Para ventas: sumar ventas_reales_informativas (dato visual, no afecta cálculos)
    ventas_por_semana = df.groupby('semana')['ventas_reales_informativas'].sum().reset_index()

    # SKUs únicos por semana
    skus_por_semana = df.groupby('semana')['sku'].nunique().reset_index()

    # Combinar todo
    resumen_semanal = inventario_por_semana.merge(asignacion_por_semana, on='semana')
    resumen_semanal = resumen_semanal.merge(ventas_por_semana, on='semana')
    resumen_semanal = resumen_semanal.merge(skus_por_semana, on='semana')

    # Calcular cumplimiento por semana (usando ventas informativas)
    resumen_semanal['cumplimiento_pct'] = (
        resumen_semanal['ventas_reales_informativas'] / resumen_semanal['asignacion_canal'] * 100
    ).fillna(0)

    resumen_semanal.columns = [
        'semana', 'inventario_fisico_total', 'asignacion_total',
        'ventas_totales', 'total_skus', 'cumplimiento_pct'
    ]

    # Ordenar por semana
    resumen_semanal = resumen_semanal.sort_values('semana')

    # Listas únicas
    semanas_unicas = sorted(df['semana'].unique().tolist())
    canales_unicos = sorted(df['canal'].unique().tolist())

    return {
        'datos_tabla': datos_tabla,
        'resumen_semanal': resumen_semanal.to_dict('records'),
        'semanas': semanas_unicas,
        'canales': canales_unicos
    }
