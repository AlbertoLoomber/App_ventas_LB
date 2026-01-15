# -*- coding: utf-8 -*-
"""
Servicios del módulo de Cumplimiento BF
Lógica de negocio para calcular cumplimiento de metas de SKUs
"""

import pandas as pd
from datetime import datetime, timedelta
from database import (
    get_catalogo_productos_bf,
    get_inventario_bf,
    get_ventas_producto_compra_mes_actual,
    get_nombre_almacen,
    get_ventas_individual_vs_combo_periodo
)


def obtener_catalogo_bf():
    """
    Retorna el catálogo completo de productos BF desde la base de datos

    Returns:
        DataFrame: Catálogo con columnas: sku, descripcion, categoria, producto_relevante, producto_nuevo, remate
    """
    return get_catalogo_productos_bf()


def obtener_categorias_bf():
    """
    Retorna la lista de categorías disponibles en el catálogo BF

    Returns:
        list: Lista de categorías únicas
    """
    catalogo = get_catalogo_productos_bf()

    if catalogo.empty:
        return []

    categorias = sorted(catalogo['categoria'].unique().tolist())
    return categorias


def obtener_skus_bf(filtro_tipo=None, filtro_categoria=None):
    """
    Retorna la lista de SKUs que son parte del seguimiento BF

    Args:
        filtro_tipo: Tipo de filtro a aplicar (None, 'relevante', 'nuevo', 'remate')
        filtro_categoria: Categoría específica o None para todas

    Returns:
        list: Lista de SKUs
    """
    catalogo = get_catalogo_productos_bf()

    if catalogo.empty:
        print("WARNING: Catálogo BF vacío, no hay SKUs para filtrar")
        return []

    # Aplicar filtro según el tipo
    if filtro_tipo == 'relevante':
        df_filtrado = catalogo[catalogo['producto_relevante'] == 1]
    elif filtro_tipo == 'nuevo':
        df_filtrado = catalogo[catalogo['producto_nuevo'] == 1]
    elif filtro_tipo == 'remate':
        df_filtrado = catalogo[catalogo['remate'] == 1]
    else:
        # Sin filtro, todos los SKUs
        df_filtrado = catalogo

    # Aplicar filtro de categoría si se proporciona
    if filtro_categoria and filtro_categoria != 'todas':
        df_filtrado = df_filtrado[df_filtrado['categoria'] == filtro_categoria]

    skus = df_filtrado['sku'].unique().tolist()
    print(f"INFO: SKUs BF obtenidos: {len(skus)} (tipo: {filtro_tipo or 'todos'}, categoría: {filtro_categoria or 'todas'})")

    return skus


def calcular_cumplimiento_skus(df_ventas, df_metas_skus, fecha_inicio, fecha_fin, filtro_tipo=None, filtro_canal=None, filtro_categoria=None):
    """
    Calcula el cumplimiento de metas para SKUs específicos usando el catálogo BF
    INCLUYE DESGLOSE DE VENTAS INDIVIDUALES VS COMBO

    Args:
        df_ventas: DataFrame con datos de ventas (de Silver.RPT_Ventas_Con_Costo_Prueba)
        df_metas_skus: DataFrame con metas por SKU (opcional)
        fecha_inicio: Fecha de inicio del período
        fecha_fin: Fecha fin del período
        filtro_tipo: Tipo de filtro ('relevante', 'nuevo', 'remate', o None para todos)
        filtro_canal: Canal específico o None para todos
        filtro_categoria: Categoría específica o None para todas

    Returns:
        tuple: (datos_cumplimiento, resumen_general)
        Los datos_cumplimiento incluyen filas adicionales con tipo_fila='combo' para cada SKU
    """

    # Canales permitidos para BF
    CANALES_BF = ['Mercado Libre', 'CrediTienda', 'Walmart', 'Shein', 'Yuhu', 'Liverpool', 'AliExpress', 'Aliexpress']

    # Si no hay datos, retornar estructuras vacías
    if df_ventas.empty:
        return [], {
            'total_meta': 0,
            'total_real': 0,
            'cumplimiento_porcentaje': 0,
            'diferencia': 0,
            'total_cantidad': 0,
            'total_skus': 0,
            'total_ingreso_real': 0,
            'total_costo': 0,
            'total_gastos_directos': 0,
            'roi_promedio': 0
        }

    # Cargar catálogo BF
    catalogo_bf = obtener_catalogo_bf()

    if catalogo_bf.empty:
        print("WARNING: Catálogo BF vacío, no se puede calcular cumplimiento")
        return [], {
            'total_meta': 0,
            'total_real': 0,
            'cumplimiento_porcentaje': 0,
            'diferencia': 0,
            'total_cantidad': 0,
            'total_skus': 0,
            'total_ingreso_real': 0,
            'total_costo': 0,
            'total_gastos_directos': 0,
            'roi_promedio': 0
        }

    # Obtener lista de SKUs según filtros de tipo y categoría
    skus_bf = obtener_skus_bf(filtro_tipo, filtro_categoria)

    if not skus_bf:
        print("WARNING: No hay SKUs BF después de aplicar filtros")
        return [], {
            'total_meta': 0,
            'total_real': 0,
            'cumplimiento_porcentaje': 0,
            'diferencia': 0,
            'total_cantidad': 0,
            'total_skus': 0,
            'total_ingreso_real': 0,
            'total_costo': 0,
            'total_gastos_directos': 0,
            'roi_promedio': 0
        }

    # ========================================
    # NUEVA LÓGICA: Obtener desglose individual vs combo desde Gold.RPT_Ventas
    # ========================================
    print(f"INFO: Obteniendo desglose individual vs combo para período {fecha_inicio} - {fecha_fin}")
    # La función ya aplica el filtro de canal internamente
    df_individual_combo = get_ventas_individual_vs_combo_periodo(fecha_inicio, fecha_fin, filtro_canal)

    # Filtrar solo SKUs BF
    df_individual_combo = df_individual_combo[df_individual_combo['Sku_Primario'].isin(skus_bf)].copy()

    print(f"DEBUG: Desglose individual/combo cargado: {len(df_individual_combo)} registros")

    # ========================================
    # Procesar ventas desde Silver.RPT_Ventas_Con_Costo_Prueba para costos/gastos
    # ========================================

    # Filtrar ventas por canales permitidos BF
    df_ventas_filtrado = df_ventas[df_ventas['Channel'].isin(CANALES_BF)].copy()
    print(f"DEBUG: Total ventas después de filtrar por canales BF: {len(df_ventas_filtrado)} registros")

    # FILTRO CRÍTICO: Aplicar filtro de fechas
    if hasattr(df_ventas_filtrado['Fecha'].dtype, 'tz') and df_ventas_filtrado['Fecha'].dt.tz is not None:
        df_ventas_filtrado = df_ventas_filtrado[
            (df_ventas_filtrado['Fecha'] >= fecha_inicio) &
            (df_ventas_filtrado['Fecha'] < fecha_fin)
        ].copy()
    else:
        fecha_inicio_sin_tz = fecha_inicio.replace(tzinfo=None) if hasattr(fecha_inicio, 'tzinfo') else fecha_inicio
        fecha_fin_sin_tz = fecha_fin.replace(tzinfo=None) if hasattr(fecha_fin, 'tzinfo') else fecha_fin
        df_ventas_filtrado = df_ventas_filtrado[
            (df_ventas_filtrado['Fecha'] >= fecha_inicio_sin_tz) &
            (df_ventas_filtrado['Fecha'] < fecha_fin_sin_tz)
        ].copy()
    print(f"DEBUG: Total ventas después de filtrar por fechas: {len(df_ventas_filtrado)} registros")

    # Aplicar filtro de canal específico si se proporciona
    if filtro_canal and filtro_canal != 'todos':
        df_ventas_filtrado = df_ventas_filtrado[df_ventas_filtrado['Channel'] == filtro_canal].copy()
        print(f"DEBUG: Total ventas después de filtrar por canal '{filtro_canal}': {len(df_ventas_filtrado)} registros")

    # Filtrar ventas por SKUs BF
    df_ventas_filtrado = df_ventas_filtrado[df_ventas_filtrado['sku'].isin(skus_bf)].copy()
    print(f"DEBUG: Total ventas después de filtrar por SKUs BF: {len(df_ventas_filtrado)} registros")

    # Agrupar para obtener costos/gastos totales por SKU
    costos_por_sku = df_ventas_filtrado.groupby('sku').agg({
        'cantidad': 'sum',
        'Ingreso real': 'sum',
        'Costo de venta': 'sum',
        'Gastos_directos': 'sum',
        'descripcion': 'first'
    }).reset_index()

    costos_por_sku.columns = ['sku', 'Cantidad_Total', 'Ingreso_Real_Total', 'Costo_Total', 'Gastos_Total', 'descripcion']

    # ========================================
    # Combinar datos individual/combo con costos
    # ========================================

    # Pivotar df_individual_combo para tener Individual y Combo como columnas
    df_pivot = df_individual_combo.pivot_table(
        index='Sku_Primario',
        columns='Tipo_Venta',
        values=['Cantidad_Vendida', 'Total_Ventas'],
        aggfunc='sum',
        fill_value=0
    ).reset_index()

    # Aplanar columnas
    df_pivot.columns = ['_'.join(col).strip('_') if col[1] else col[0] for col in df_pivot.columns.values]

    # Renombrar columnas para claridad
    df_pivot.rename(columns={'Sku_Primario': 'sku'}, inplace=True)

    # Asegurar que existan columnas de Individual y Combo (pueden no existir si no hay datos)
    if 'Cantidad_Vendida_Individual' not in df_pivot.columns:
        df_pivot['Cantidad_Vendida_Individual'] = 0
    if 'Total_Ventas_Individual' not in df_pivot.columns:
        df_pivot['Total_Ventas_Individual'] = 0
    if 'Cantidad_Vendida_Combo' not in df_pivot.columns:
        df_pivot['Cantidad_Vendida_Combo'] = 0
    if 'Total_Ventas_Combo' not in df_pivot.columns:
        df_pivot['Total_Ventas_Combo'] = 0

    print(f"DEBUG: SKUs con desglose individual/combo: {len(df_pivot)}")
    print(f"DEBUG: SKUs con costos en Silver: {len(costos_por_sku)}")

    # Combinar con costos - usar LEFT JOIN para mantener TODOS los SKUs con ventas
    # incluso si no tienen registro en Silver (vendidos solo en combos)
    datos_completos = pd.merge(
        df_pivot,
        costos_por_sku,
        on='sku',
        how='left'  # LEFT JOIN mantiene todos los SKUs de df_pivot
    )

    # Marcar SKUs sin costos en Silver (vendidos solo en combos)
    datos_completos['tiene_costos_silver'] = ~datos_completos['Cantidad_Total'].isna()

    # Rellenar valores faltantes con 0 para SKUs sin costos en Silver
    datos_completos['Cantidad_Total'] = datos_completos['Cantidad_Total'].fillna(0)
    datos_completos['Ingreso_Real_Total'] = datos_completos['Ingreso_Real_Total'].fillna(0)
    datos_completos['Costo_Total'] = datos_completos['Costo_Total'].fillna(0)
    datos_completos['Gastos_Total'] = datos_completos['Gastos_Total'].fillna(0)
    datos_completos['descripcion'] = datos_completos['descripcion'].fillna('')

    # Log de SKUs sin costos
    skus_sin_costos = datos_completos[~datos_completos['tiene_costos_silver']]['sku'].tolist()
    if skus_sin_costos:
        print(f"INFO: {len(skus_sin_costos)} SKUs vendidos solo en combos (sin costos en Silver): {skus_sin_costos[:5]}{'...' if len(skus_sin_costos) > 5 else ''}")

    print(f"DEBUG: SKUs después de combinar con costos: {len(datos_completos)}")

    # Combinar con catálogo - INNER JOIN porque TODOS deben estar en el catálogo BF
    datos_completos = pd.merge(
        datos_completos,
        catalogo_bf[['sku', 'descripcion', 'categoria', 'producto_relevante', 'producto_nuevo', 'remate']],
        on='sku',
        how='inner',
        suffixes=('_costos', '_catalogo')
    )

    # Si descripcion_costos está vacía, usar descripcion_catalogo
    if 'descripcion_costos' in datos_completos.columns and 'descripcion_catalogo' in datos_completos.columns:
        datos_completos['descripcion'] = datos_completos.apply(
            lambda x: x['descripcion_catalogo'] if (pd.isna(x['descripcion_costos']) or x['descripcion_costos'] == '') else x['descripcion_costos'],
            axis=1
        )
        # Eliminar columnas auxiliares
        datos_completos.drop(['descripcion_costos', 'descripcion_catalogo'], axis=1, inplace=True)
    elif 'descripcion_catalogo' in datos_completos.columns:
        datos_completos['descripcion'] = datos_completos['descripcion_catalogo']
        datos_completos.drop('descripcion_catalogo', axis=1, inplace=True)

    print(f"DEBUG: SKUs después de combinar con catálogo: {len(datos_completos)}")

    if datos_completos.empty:
        print("WARNING: No hay SKUs con ventas después de aplicar filtros")
        return [], {
            'total_meta': 0,
            'total_real': 0,
            'cumplimiento_porcentaje': 0,
            'diferencia': 0,
            'total_cantidad': 0,
            'total_skus': 0,
            'total_ingreso_real': 0,
            'total_costo': 0,
            'total_gastos_directos': 0,
            'roi_promedio': 0
        }

    # ========================================
    # Calcular precio promedio y ajustar ventas combo
    # ========================================

    # Precio promedio = SOLO de ventas individuales (Total_Ventas_Individual / Cantidad_Vendida_Individual)
    datos_completos['Cantidad_Total_Vendida'] = (
        datos_completos['Cantidad_Vendida_Individual'] + datos_completos['Cantidad_Vendida_Combo']
    )

    datos_completos['Ventas_Totales_Real'] = (
        datos_completos['Total_Ventas_Individual'] + datos_completos['Total_Ventas_Combo']
    )

    datos_completos['Precio_Promedio'] = (
        datos_completos['Total_Ventas_Individual'] / datos_completos['Cantidad_Vendida_Individual']
    ).fillna(0).replace([float('inf'), float('-inf')], 0)

    # Ventas combo ajustadas = Cantidad_Combo × Precio_Promedio × 0.90
    datos_completos['Ventas_Combo_Ajustadas'] = (
        datos_completos['Cantidad_Vendida_Combo'] * datos_completos['Precio_Promedio'] * 0.90
    )

    # Ventas totales finales = Individual + Combo_Ajustadas
    datos_completos['Ventas_Reales'] = (
        datos_completos['Total_Ventas_Individual'] + datos_completos['Ventas_Combo_Ajustadas']
    )

    print(f"DEBUG: Cálculo de precios promedio y ventas ajustadas completado")

    # ========================================
    # Calcular costos y gastos proporcionales para individual y combo
    # ========================================

    # Costo unitario y gasto unitario - USAR CANTIDAD_TOTAL DE SILVER, NO LA DESCOMPUESTA DE GOLD
    # Cantidad_Total viene de Silver y representa solo las unidades vendidas directamente (sin descomponer combos)
    datos_completos['Costo_Unitario'] = (
        datos_completos['Costo_Total'] / datos_completos['Cantidad_Total']
    ).fillna(0).replace([float('inf'), float('-inf')], 0)

    datos_completos['Gasto_Unitario'] = (
        datos_completos['Gastos_Total'] / datos_completos['Cantidad_Total']
    ).fillna(0).replace([float('inf'), float('-inf')], 0)

    # Para ventas individuales
    datos_completos['Costo_Individual'] = (
        datos_completos['Cantidad_Vendida_Individual'] * datos_completos['Costo_Unitario']
    )
    datos_completos['Gastos_Individual'] = (
        datos_completos['Cantidad_Vendida_Individual'] * datos_completos['Gasto_Unitario']
    )
    datos_completos['Ingreso_Real_Individual'] = (
        datos_completos['Total_Ventas_Individual'] -
        datos_completos['Costo_Individual'] -
        datos_completos['Gastos_Individual']
    )
    datos_completos['ROI_Individual'] = (
        (datos_completos['Ingreso_Real_Individual'] / datos_completos['Costo_Individual'] * 100)
        .fillna(0).replace([float('inf'), float('-inf')], 0)
    )

    # Para ventas combo
    datos_completos['Costo_Combo'] = (
        datos_completos['Cantidad_Vendida_Combo'] * datos_completos['Costo_Unitario']
    )
    datos_completos['Gastos_Combo'] = (
        datos_completos['Cantidad_Vendida_Combo'] * datos_completos['Gasto_Unitario']
    )
    datos_completos['Ingreso_Real_Combo'] = (
        datos_completos['Ventas_Combo_Ajustadas'] -
        datos_completos['Costo_Combo'] -
        datos_completos['Gastos_Combo']
    )
    datos_completos['ROI_Combo'] = (
        (datos_completos['Ingreso_Real_Combo'] / datos_completos['Costo_Combo'] * 100)
        .fillna(0).replace([float('inf'), float('-inf')], 0)
    )

    # ========================================
    # Si hay metas, combinarlas
    # ========================================
    if not df_metas_skus.empty:
        datos_completos = pd.merge(
            datos_completos,
            df_metas_skus[['sku', 'Meta']],
            on='sku',
            how='left'
        )
        datos_completos['Meta'] = datos_completos['Meta'].fillna(0)
    else:
        datos_completos['Meta'] = 0

    # Agregar etiquetas
    datos_completos['Es_Relevante'] = datos_completos['producto_relevante'] == 1
    datos_completos['Es_Nuevo'] = datos_completos['producto_nuevo'] == 1
    datos_completos['Es_Remate'] = datos_completos['remate'] == 1

    # Ordenar por ventas reales descendente
    datos_completos = datos_completos.sort_values('Ventas_Reales', ascending=False)

    # ========================================
    # Crear lista de datos con filas principales y secundarias
    # ========================================
    datos_cumplimiento = []

    for _, row in datos_completos.iterrows():
        # FILA PRINCIPAL - Solo ventas individuales
        fila_principal = {
            'sku': row['sku'],
            'descripcion': row['descripcion'],
            'categoria': row['categoria'],
            'Cantidad_Vendida': int(row['Cantidad_Vendida_Individual']),
            'Cantidad_Vendida_Individual': int(row['Cantidad_Vendida_Individual']),  # Para verificación en template
            'Cantidad_Vendida_Combo': int(row['Cantidad_Vendida_Combo']),  # Para verificación en template
            'Ventas_Reales': float(row['Total_Ventas_Individual']),
            'Costo_Venta': float(row['Costo_Individual']),
            'Gastos_Directos': float(row['Gastos_Individual']),
            'Ingreso_Real': float(row['Ingreso_Real_Individual']),
            'ROI': float(row['ROI_Individual']),
            'Meta': float(row['Meta']),
            'Es_Relevante': row['Es_Relevante'],
            'Es_Nuevo': row['Es_Nuevo'],
            'Es_Remate': row['Es_Remate'],
            'tipo_fila': 'individual',  # Identificador para el template
            'tiene_combo': row['Cantidad_Vendida_Combo'] > 0,  # Indicador si hay fila combo
            'tiene_costos_silver': row.get('tiene_costos_silver', True)  # Indicador si tiene costos en Silver
        }
        datos_cumplimiento.append(fila_principal)

        # FILA SECUNDARIA - Solo ventas combo (si existen)
        if row['Cantidad_Vendida_Combo'] > 0:
            fila_combo = {
                'sku': row['sku'],
                'descripcion': 'Cantidad vendida en combo',
                'categoria': row['categoria'],
                'Cantidad_Vendida': int(row['Cantidad_Vendida_Combo']),
                'Ventas_Reales': float(row['Ventas_Combo_Ajustadas']),
                'Costo_Venta': float(row['Costo_Combo']),
                'Gastos_Directos': float(row['Gastos_Combo']),
                'Ingreso_Real': float(row['Ingreso_Real_Combo']),
                'ROI': float(row['ROI_Combo']),
                'Meta': 0,  # Sin meta para fila combo
                'Es_Relevante': row['Es_Relevante'],
                'Es_Nuevo': row['Es_Nuevo'],
                'Es_Remate': row['Es_Remate'],
                'tipo_fila': 'combo'  # Identificador para el template
            }
            datos_cumplimiento.append(fila_combo)

    # ========================================
    # Calcular resumen general (solo filas principales)
    # ========================================
    total_meta = float(datos_completos['Meta'].sum())
    total_real = float(datos_completos['Ventas_Reales'].sum())
    total_cantidad = int(datos_completos['Cantidad_Total_Vendida'].sum())
    total_skus = len(datos_completos)
    cumplimiento_porcentaje = (total_real / total_meta * 100) if total_meta > 0 else 0
    total_ingreso_real = float(datos_completos['Ingreso_Real_Total'].sum())
    total_costo = float(datos_completos['Costo_Total'].sum())
    total_gastos_directos = float(datos_completos['Gastos_Total'].sum())
    roi_promedio = (total_ingreso_real / total_costo * 100) if total_costo > 0 else 0

    resumen_general = {
        'total_meta': total_meta,
        'total_real': total_real,
        'cumplimiento_porcentaje': cumplimiento_porcentaje,
        'diferencia': total_real - total_meta,
        'total_cantidad': total_cantidad,
        'total_skus': total_skus,
        'total_ingreso_real': total_ingreso_real,
        'total_costo': total_costo,
        'total_gastos_directos': total_gastos_directos,
        'roi_promedio': roi_promedio
    }

    print(f"OK: Cumplimiento calculado con desglose individual/combo: {total_skus} SKUs, ${total_real:,.0f} en ventas")
    print(f"   - Filas totales (incluyendo combo): {len(datos_cumplimiento)}")

    return datos_cumplimiento, resumen_general


def obtener_grafico_cumplimiento_diario(df_ventas, df_metas_skus, fecha_inicio, fecha_fin, filtro_tipo=None, filtro_canal=None, filtro_categoria=None):
    """
    Genera datos para el gráfico de cumplimiento diario

    Args:
        df_ventas: DataFrame con datos de ventas
        df_metas_skus: DataFrame con metas por SKU
        fecha_inicio: Fecha de inicio
        fecha_fin: Fecha fin
        filtro_tipo: Tipo de filtro ('relevante', 'nuevo', 'remate', o None)
        filtro_canal: Canal específico o None para todos
        filtro_categoria: Categoría específica o None para todas

    Returns:
        dict: Datos formateados para el gráfico
    """

    # Canales permitidos para BF
    CANALES_BF = ['Mercado Libre', 'CrediTienda', 'Walmart', 'Shein', 'Yuhu', 'Liverpool', 'AliExpress', 'Aliexpress']

    if df_ventas.empty:
        return {
            'fechas': [],
            'metas': [],
            'ventas': []
        }

    # Filtrar por SKUs BF según tipo y categoría
    skus_bf = obtener_skus_bf(filtro_tipo, filtro_categoria)

    if not skus_bf:
        return {
            'fechas': [],
            'metas': [],
            'ventas': []
        }

    # Filtrar por canales BF
    df_ventas_filtrado = df_ventas[df_ventas['Channel'].isin(CANALES_BF)].copy()

    # Aplicar filtro de canal específico si se proporciona
    if filtro_canal and filtro_canal != 'todos':
        df_ventas_filtrado = df_ventas_filtrado[df_ventas_filtrado['Channel'] == filtro_canal].copy()

    # Filtrar ventas por SKUs BF (usando columna 'sku')
    df_ventas_filtrado = df_ventas_filtrado[df_ventas_filtrado['sku'].isin(skus_bf)].copy()

    if df_ventas_filtrado.empty:
        return {
            'fechas': [],
            'metas': [],
            'ventas': []
        }

    # Asegurarse que la columna Fecha sea datetime sin timezone
    if hasattr(df_ventas_filtrado['Fecha'].dtype, 'tz') and df_ventas_filtrado['Fecha'].dt.tz is not None:
        df_ventas_filtrado['Fecha'] = df_ventas_filtrado['Fecha'].dt.tz_localize(None)

    # Agrupar por día
    ventas_diarias = df_ventas_filtrado.groupby(
        df_ventas_filtrado['Fecha'].dt.date
    )['Total'].sum().reset_index()

    ventas_diarias.columns = ['Fecha', 'Ventas']

    # Crear acumulado
    ventas_diarias = ventas_diarias.sort_values('Fecha')
    ventas_diarias['Ventas_Acumuladas'] = ventas_diarias['Ventas'].cumsum()

    # Preparar datos para el gráfico
    grafico_data = {
        'fechas': [str(fecha) for fecha in ventas_diarias['Fecha'].tolist()],
        'ventas': ventas_diarias['Ventas_Acumuladas'].tolist(),
        'metas': []  # Por ahora sin metas acumuladas
    }

    return grafico_data


def obtener_inventario_ventas_bf(filtro_tipo=None, filtro_categoria=None, filtro_canal=None, fecha_inicio=None, fecha_fin=None):
    """
    Obtiene datos de inventario y ventas para productos BF

    IMPORTANTE: Las existencias son GENERALES (no se filtran por canal)
                Las ventas SÍ se filtran por canal si se especifica

    Args:
        filtro_tipo: Tipo de filtro ('relevante', 'nuevo', 'remate', o None)
        filtro_categoria: Categoría específica o None para todas
        filtro_canal: Canal específico o None para todos (SOLO filtra ventas, NO existencias)
        fecha_inicio: Fecha de inicio del período (datetime), None para mes actual
        fecha_fin: Fecha fin del período (datetime), None para hasta hoy

    Returns:
        list: Lista de diccionarios con datos de inventario y ventas por SKU
    """
    from database import get_ventas_producto_compra_periodo

    # Cargar catálogo BF
    catalogo_bf = obtener_catalogo_bf()

    if catalogo_bf.empty:
        print("WARNING: Catálogo BF vacío")
        return []

    # Aplicar filtros de tipo
    if filtro_tipo == 'relevante':
        catalogo_bf = catalogo_bf[catalogo_bf['producto_relevante'] == 1]
    elif filtro_tipo == 'nuevo':
        catalogo_bf = catalogo_bf[catalogo_bf['producto_nuevo'] == 1]
    elif filtro_tipo == 'remate':
        catalogo_bf = catalogo_bf[catalogo_bf['remate'] == 1]

    # Aplicar filtro de categoría
    if filtro_categoria and filtro_categoria != 'todas':
        catalogo_bf = catalogo_bf[catalogo_bf['categoria'] == filtro_categoria]

    if catalogo_bf.empty:
        print("WARNING: No hay productos BF después de aplicar filtros")
        return []

    # Obtener inventario (SIEMPRE es general, no se filtra por canal)
    df_inventario = get_inventario_bf()

    # Obtener ventas del período especificado (AQUÍ SÍ aplicamos filtro de canal)
    if fecha_inicio:
        df_ventas = get_ventas_producto_compra_periodo(fecha_inicio, fecha_fin, filtro_canal)
    else:
        df_ventas = get_ventas_producto_compra_mes_actual(filtro_canal)

    # Filtrar inventario solo para SKUs del catálogo BF
    skus_bf = catalogo_bf['sku'].unique().tolist()
    df_inventario_filtrado = df_inventario[df_inventario['sku'].isin(skus_bf)].copy()

    if df_inventario_filtrado.empty:
        print("WARNING: No hay inventario para los SKUs BF filtrados")
        return []

    # Agrupar inventario por SKU para obtener existencia total y almacenes
    inventario_por_sku = df_inventario_filtrado.groupby('sku').agg({
        'Existencia': 'first',  # La existencia total ya está calculada en la query
        'descripcion': 'first'
    }).reset_index()

    # Crear diccionario de almacenes por SKU con nombres completos
    almacenes_dict = {}
    for sku in df_inventario_filtrado['sku'].unique():
        almacenes = df_inventario_filtrado[df_inventario_filtrado['sku'] == sku][['almacen', 'cantidad_libre_de_usar']].to_dict('records')
        # Convertir códigos de almacén a nombres completos
        for almacen in almacenes:
            almacen['almacen'] = get_nombre_almacen(almacen['almacen'])
        almacenes_dict[sku] = almacenes

    # Combinar con catálogo
    datos_completos = pd.merge(
        inventario_por_sku,
        catalogo_bf[['sku', 'categoria', 'producto_relevante', 'producto_nuevo', 'remate']],
        on='sku',
        how='inner'
    )

    # Combinar con ventas
    if not df_ventas.empty:
        datos_completos = pd.merge(
            datos_completos,
            df_ventas.rename(columns={'Sku_Primario': 'sku'}),
            on='sku',
            how='left'
        )
        datos_completos['Venta_Mes_Actual'] = datos_completos['Venta_Mes_Actual'].fillna(0)
    else:
        datos_completos['Venta_Mes_Actual'] = 0

    # Agregar información de almacenes
    datos_completos['almacenes'] = datos_completos['sku'].apply(lambda x: almacenes_dict.get(x, []))

    # Agregar etiquetas
    datos_completos['Es_Relevante'] = datos_completos['producto_relevante'] == 1
    datos_completos['Es_Nuevo'] = datos_completos['producto_nuevo'] == 1
    datos_completos['Es_Remate'] = datos_completos['remate'] == 1

    # Ordenar por existencia descendente
    datos_completos = datos_completos.sort_values('Existencia', ascending=False)

    # Convertir a lista de diccionarios
    resultado = datos_completos.to_dict('records')

    print(f"OK: Datos de inventario y ventas procesados: {len(resultado)} SKUs")

    return resultado


def agrupar_inventario_por_tipo(filtro_tipo=None, filtro_categoria=None, filtro_canal=None, fecha_inicio=None, fecha_fin=None):
    """
    Agrupa datos de inventario y ventas por tipo de producto (Relevante, Nuevo, Remate)

    Args:
        filtro_tipo: Tipo de filtro ('relevante', 'nuevo', 'remate', o None)
        filtro_categoria: Categoría específica o None para todas
        filtro_canal: Canal específico o None para todos (SOLO para filtrar ventas, NO existencias)
        fecha_inicio: Fecha de inicio del período (datetime), None para mes actual
        fecha_fin: Fecha fin del período (datetime), None para hasta hoy

    Returns:
        dict: Diccionario con resumen por tipo y productos detallados
    """

    # Obtener todos los productos (las existencias son generales, las ventas se filtran por canal)
    productos = obtener_inventario_ventas_bf(filtro_tipo, filtro_categoria, filtro_canal, fecha_inicio, fecha_fin)

    if not productos:
        return {
            'resumen': [],
            'productos_por_tipo': {}
        }

    # Convertir a DataFrame para facilitar agrupación
    df = pd.DataFrame(productos)

    # Crear estructura de datos agrupados
    tipos_info = []
    productos_por_tipo = {}

    # Definir tipos y sus propiedades
    tipos_config = [
        {
            'nombre': 'Relevante',
            'campo': 'Es_Relevante',
            'icono': 'bi-star-fill',
            'color': '#6f42c1',
            'gradient': 'linear-gradient(135deg, #6f42c1, #5a32a3)'
        },
        {
            'nombre': 'Nuevo',
            'campo': 'Es_Nuevo',
            'icono': 'bi-sparkles',
            'color': '#0dcaf0',
            'gradient': 'linear-gradient(135deg, #0dcaf0, #0aa2c0)'
        },
        {
            'nombre': 'Remate',
            'campo': 'Es_Remate',
            'icono': 'bi-tag-fill',
            'color': '#fd7e14',
            'gradient': 'linear-gradient(135deg, #fd7e14, #dc6502)'
        }
    ]

    # Procesar cada tipo
    for tipo_config in tipos_config:
        # Filtrar productos de este tipo
        df_tipo = df[df[tipo_config['campo']] == True].copy()

        if not df_tipo.empty:
            # Calcular totales
            total_skus = len(df_tipo)
            total_existencia = int(df_tipo['Existencia'].sum())
            total_venta = int(df_tipo['Venta_Mes_Actual'].sum())

            # Agregar resumen del tipo
            tipos_info.append({
                'nombre': tipo_config['nombre'],
                'icono': tipo_config['icono'],
                'color': tipo_config['color'],
                'gradient': tipo_config['gradient'],
                'total_skus': total_skus,
                'total_existencia': total_existencia,
                'total_venta': total_venta
            })

            # Ordenar productos por cantidad vendida (descendente) y guardar
            df_tipo_ordenado = df_tipo.sort_values('Venta_Mes_Actual', ascending=False)
            productos_por_tipo[tipo_config['nombre']] = df_tipo_ordenado.to_dict('records')

    resultado = {
        'resumen': tipos_info,
        'productos_por_tipo': productos_por_tipo
    }

    print(f"OK: Inventario agrupado por tipo: {len(tipos_info)} tipos con productos")

    return resultado


def agrupar_inventario_por_tipo_desde_skus(skus_data, filtro_tipo=None, filtro_categoria=None):
    """
    Agrupa datos de inventario y ventas por tipo usando los datos ya calculados en skus_data
    Esta función usa las ventas YA FILTRADAS POR FECHA que vienen en skus_data

    Args:
        skus_data: Lista de diccionarios con datos de SKUs ya procesados (con ventas filtradas por fecha)
        filtro_tipo: Tipo de filtro ('relevante', 'nuevo', 'remate', o None)
        filtro_categoria: Categoría específica o None para todas

    Returns:
        dict: Diccionario con resumen por tipo y productos detallados
    """

    if not skus_data:
        return {
            'resumen': [],
            'productos_por_tipo': {}
        }

    # Obtener inventario completo (existencias) - esto NO depende de fechas
    inventario_completo = obtener_inventario_ventas_bf(filtro_tipo, filtro_categoria)

    if not inventario_completo:
        return {
            'resumen': [],
            'productos_por_tipo': {}
        }

    # Crear diccionario de ventas del período desde skus_data (ya filtrado por fechas)
    ventas_periodo = {}
    for sku_info in skus_data:
        ventas_periodo[sku_info['sku']] = int(sku_info.get('Cantidad_Vendida', 0))

    # Convertir inventario a DataFrame
    df = pd.DataFrame(inventario_completo)

    # Reemplazar Venta_Mes_Actual con las ventas del período seleccionado
    df['Venta_Periodo'] = df['sku'].apply(lambda sku: ventas_periodo.get(sku, 0))

    # Crear estructura de datos agrupados
    tipos_info = []
    productos_por_tipo = {}

    # Definir tipos y sus propiedades
    tipos_config = [
        {
            'nombre': 'Relevante',
            'campo': 'Es_Relevante',
            'icono': 'bi-star-fill',
            'color': '#6f42c1',
            'gradient': 'linear-gradient(135deg, #6f42c1, #5a32a3)'
        },
        {
            'nombre': 'Nuevo',
            'campo': 'Es_Nuevo',
            'icono': 'bi-sparkles',
            'color': '#0dcaf0',
            'gradient': 'linear-gradient(135deg, #0dcaf0, #0aa2c0)'
        },
        {
            'nombre': 'Remate',
            'campo': 'Es_Remate',
            'icono': 'bi-tag-fill',
            'color': '#fd7e14',
            'gradient': 'linear-gradient(135deg, #fd7e14, #dc6502)'
        }
    ]

    # Procesar cada tipo
    for tipo_config in tipos_config:
        # Filtrar productos de este tipo
        df_tipo = df[df[tipo_config['campo']] == True].copy()

        if not df_tipo.empty:
            # Calcular totales
            total_skus = len(df_tipo)
            total_existencia = int(df_tipo['Existencia'].sum())
            total_venta = int(df_tipo['Venta_Periodo'].sum())  # Usar Venta_Periodo en lugar de Venta_Mes_Actual

            # Agregar resumen del tipo
            tipos_info.append({
                'nombre': tipo_config['nombre'],
                'icono': tipo_config['icono'],
                'color': tipo_config['color'],
                'gradient': tipo_config['gradient'],
                'total_skus': total_skus,
                'total_existencia': total_existencia,
                'total_venta': total_venta
            })

            # Reemplazar Venta_Mes_Actual con Venta_Periodo para cada producto
            df_tipo_records = df_tipo.to_dict('records')
            for producto in df_tipo_records:
                producto['Venta_Mes_Actual'] = producto['Venta_Periodo']  # Renombrar para compatibilidad con template

            # Guardar productos de este tipo
            productos_por_tipo[tipo_config['nombre']] = df_tipo_records

    resultado = {
        'resumen': tipos_info,
        'productos_por_tipo': productos_por_tipo
    }

    print(f"OK: Inventario agrupado por tipo desde skus_data: {len(tipos_info)} tipos con productos")

    return resultado
