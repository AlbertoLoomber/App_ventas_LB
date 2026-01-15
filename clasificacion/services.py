# -*- coding: utf-8 -*-
"""
Servicios para el módulo de Clasificación de SKUs
Lógica de negocio para clasificación y análisis de productos
"""

import pandas as pd
from datetime import date
import calendar
from config import (
    MESES_ESPANOL, UMBRALES_CLASIFICACION,
    COLORES_CLASIFICACION, ORDEN_CLASIFICACION,
    MAPEO_CLASIFICACIONES, CANALES_CLASIFICACION
)
from database import get_db_connection


# ====== FUNCIONES DE CLASIFICACIÓN ======

def clasificar_sku_por_ventas(cantidad_mensual, año_mes=None):
    """
    Clasifica un SKU basado en su cantidad de ventas mensuales
    Clasificación actualizada:
    - Estrella: ≥ 500 ventas/mes
    - Prometedores: 100-499 ventas/mes
    - Potenciales: 30-99 ventas/mes
    - Revisión: 10-29 ventas/mes
    - Remover: < 10 ventas/mes

    Si año_mes es proporcionado y es el mes actual, ajusta las metas proporcionalmente

    Args:
        cantidad_mensual: Cantidad de unidades vendidas en el mes
        año_mes: Tupla (año, mes) para ajustar metas si es mes actual

    Returns:
        tuple: (clasificacion, color, orden)
    """
    # Metas base
    meta_estrella = UMBRALES_CLASIFICACION['estrella']
    meta_prometedores = UMBRALES_CLASIFICACION['prometedores']
    meta_potenciales = UMBRALES_CLASIFICACION['potenciales']
    meta_revision = UMBRALES_CLASIFICACION['revision']

    # Ajustar metas si es el mes actual y no ha terminado
    if año_mes:
        año, mes = año_mes
        hoy = date.today()

        # Solo ajustar si es el mes actual
        if hoy.year == año and hoy.month == mes:
            dias_mes = calendar.monthrange(año, mes)[1]
            dia_actual = hoy.day

            # Si estamos muy al inicio del mes, usar mínimo 3 días
            if dia_actual < 3:
                dia_actual = 3

            factor = dia_actual / dias_mes
            meta_estrella = meta_estrella * factor
            meta_prometedores = meta_prometedores * factor
            meta_potenciales = meta_potenciales * factor
            meta_revision = meta_revision * factor

            print(f"DEBUG: Ajuste proporcional - día {dia_actual}/{dias_mes}, factor: {factor:.3f}")

    # Clasificar con las metas (ajustadas o normales)
    if cantidad_mensual >= meta_estrella:
        return "Estrella", COLORES_CLASIFICACION['Estrella'], ORDEN_CLASIFICACION['Estrella']
    elif cantidad_mensual >= meta_prometedores:
        return "Prometedores", COLORES_CLASIFICACION['Prometedores'], ORDEN_CLASIFICACION['Prometedores']
    elif cantidad_mensual >= meta_potenciales:
        return "Potenciales", COLORES_CLASIFICACION['Potenciales'], ORDEN_CLASIFICACION['Potenciales']
    elif cantidad_mensual >= meta_revision:
        return "Revisión", COLORES_CLASIFICACION['Revisión'], ORDEN_CLASIFICACION['Revisión']
    else:
        return "Remover", COLORES_CLASIFICACION['Remover'], ORDEN_CLASIFICACION['Remover']


def obtener_meses_disponibles(df):
    """
    Obtiene los últimos 12 meses disponibles desde los datos

    Args:
        df: DataFrame con columna 'Fecha' y 'estado'

    Returns:
        list: Lista de diccionarios con información de meses disponibles
    """
    if df.empty:
        print("DataFrame está vacío")
        return []

    try:
        # Filtrar datos válidos primero
        df_valido = df[df["estado"] == "Orden de Venta"].copy()

        if df_valido.empty:
            print("No hay órdenes de venta válidas")
            return []

        # Obtener meses únicos de los datos
        df_valido['año_mes'] = df_valido['Fecha'].dt.to_period('M')
        meses_unicos = df_valido['año_mes'].unique()

        # Ordenar descendente (más reciente primero)
        meses_unicos = sorted(meses_unicos, reverse=True)

        # Tomar solo los últimos 12 meses
        meses_unicos = meses_unicos[:12]

        meses_disponibles = []

        for periodo in meses_unicos:
            año = periodo.year
            mes = periodo.month

            meses_disponibles.append({
                'año': año,
                'mes': mes,
                'nombre_mes': MESES_ESPANOL[mes],
                'fecha_completa': f"{MESES_ESPANOL[mes]} {año}",
                'valor': f"{año}-{mes:02d}"
            })

        return meses_disponibles

    except Exception as e:
        print(f"Error en obtener_meses_disponibles: {e}")
        return []


def calcular_cuartiles_precios_sku(df_sku_ventas, canal_filtro=None):
    """
    Calcula los cuartiles de precios para un SKU específico usando promedio ponderado por rangos de órdenes

    NUEVA LÓGICA:
    - Q1: Promedio ponderado de órdenes en el primer 25%
    - Q2: Promedio ponderado de órdenes entre 26%-50%
    - Q3: Promedio ponderado de órdenes entre 51%-75%
    - MAX: Promedio ponderado de órdenes entre 76%-100%
    - MIN: Mantiene precio mínimo real
    - Ticket Promedio: Promedio ponderado de todas las órdenes

    Args:
        df_sku_ventas: DataFrame con todas las ventas individuales de un SKU
        canal_filtro: Filtro de canal aplicado ("todos" o canal específico)

    Returns:
        dict: Cuartiles con promedios ponderados e ingresos reales por rangos
    """
    if df_sku_ventas.empty:
        return {
            'min_precio': 0, 'min_ingreso': 0, 'min_porcentaje': 0,
            'max_precio': 0, 'max_ingreso': 0, 'max_porcentaje': 0,
            'q1_precio': 0, 'q1_ingreso': 0, 'q1_porcentaje': 0,
            'q2_precio': 0, 'q2_ingreso': 0, 'q2_porcentaje': 0,
            'q3_precio': 0, 'q3_ingreso': 0, 'q3_porcentaje': 0,
            'q4_precio': 0, 'q4_ingreso': 0, 'q4_porcentaje': 0
        }

    # Convertir datos a numéricos
    df_sku_ventas = df_sku_ventas.copy()
    df_sku_ventas['cantidad'] = pd.to_numeric(df_sku_ventas['cantidad'], errors='coerce')
    df_sku_ventas['Total'] = pd.to_numeric(df_sku_ventas['Total'], errors='coerce')
    df_sku_ventas = df_sku_ventas.dropna(subset=['cantidad', 'Total'])

    if df_sku_ventas.empty:
        return {
            'min_precio': 0, 'min_ingreso': 0, 'min_porcentaje': 0,
            'max_precio': 0, 'max_ingreso': 0, 'max_porcentaje': 0,
            'q1_precio': 0, 'q1_ingreso': 0, 'q1_porcentaje': 0,
            'q2_precio': 0, 'q2_ingreso': 0, 'q2_porcentaje': 0,
            'q3_precio': 0, 'q3_ingreso': 0, 'q3_porcentaje': 0,
            'q4_precio': 0, 'q4_ingreso': 0, 'q4_porcentaje': 0
        }

    # Calcular precio unitario para cada orden
    df_sku_ventas['precio_unitario'] = df_sku_ventas['Total'] / df_sku_ventas['cantidad']

    # FILTRAR POR CANAL según parámetro
    if canal_filtro and canal_filtro != "todos" and canal_filtro.strip():
        # Solo incluir órdenes del canal específico
        if 'Channel' in df_sku_ventas.columns:
            df_sku_ventas = df_sku_ventas[df_sku_ventas['Channel'] == canal_filtro].copy()
        else:
            print(f"WARNING: Columna 'Channel' no encontrada para filtrar por {canal_filtro}")
    # Si canal_filtro es "todos" o None, incluir todas las órdenes (no filtrar)

    # Verificar que aún tengamos datos después del filtrado
    if df_sku_ventas.empty:
        return {
            'min_precio': 0, 'min_ingreso': 0, 'min_porcentaje': 0,
            'max_precio': 0, 'max_ingreso': 0, 'max_porcentaje': 0,
            'q1_precio': 0, 'q1_ingreso': 0, 'q1_porcentaje': 0,
            'q2_precio': 0, 'q2_ingreso': 0, 'q2_porcentaje': 0,
            'q3_precio': 0, 'q3_ingreso': 0, 'q3_porcentaje': 0,
            'q4_precio': 0, 'q4_ingreso': 0, 'q4_porcentaje': 0,
            'precio_promedio_precio': 0, 'precio_promedio_ingreso': 0, 'precio_promedio_porcentaje': 0
        }

    # Ordenar órdenes por precio unitario de menor a mayor
    df_ordenado = df_sku_ventas.sort_values('precio_unitario').reset_index(drop=True)

    total_ordenes = len(df_ordenado)

    # Si solo hay una orden, todos los cuartiles serán iguales
    if total_ordenes == 1:
        precio_unico = float(df_ordenado.iloc[0]['precio_unitario'])

        # Calcular ingreso real usando la misma lógica que los grupos
        if 'Ingreso real' in df_ordenado.columns:
            ingreso_real_unico = pd.to_numeric(df_ordenado.iloc[0]['Ingreso real'], errors='coerce')
            if not pd.isna(ingreso_real_unico) and ingreso_real_unico >= 0:
                cantidad_unica = float(df_ordenado.iloc[0]['cantidad'])
                ingreso_real_unitario = ingreso_real_unico / cantidad_unica if cantidad_unica > 0 else 0
                porcentaje_unico = (ingreso_real_unitario / precio_unico * 100) if precio_unico > 0 else 0
                porcentaje_unico = max(0, porcentaje_unico)
                ingreso_unico = float(ingreso_real_unitario)
            else:
                porcentaje_unico = 20.0  # Default fallback
                ingreso_unico = float(precio_unico * 0.2)
        else:
            porcentaje_unico = 20.0  # Default fallback
            ingreso_unico = float(precio_unico * 0.2)

        return {
            'min_precio': precio_unico, 'min_ingreso': ingreso_unico, 'min_porcentaje': porcentaje_unico,
            'max_precio': precio_unico, 'max_ingreso': ingreso_unico, 'max_porcentaje': porcentaje_unico,
            'q1_precio': precio_unico, 'q1_ingreso': ingreso_unico, 'q1_porcentaje': porcentaje_unico,
            'q2_precio': precio_unico, 'q2_ingreso': ingreso_unico, 'q2_porcentaje': porcentaje_unico,
            'q3_precio': precio_unico, 'q3_ingreso': ingreso_unico, 'q3_porcentaje': porcentaje_unico,
            'q4_precio': precio_unico, 'q4_ingreso': ingreso_unico, 'q4_porcentaje': porcentaje_unico,
            'precio_promedio_precio': precio_unico, 'precio_promedio_ingreso': ingreso_unico,
            'precio_promedio_porcentaje': porcentaje_unico
        }

    # Dividir órdenes en 4 cuartiles del 25% cada uno
    cuartil_size = total_ordenes / 4

    # Definir rangos de órdenes para cada cuartil
    q1_start = 0
    q1_end = max(1, int(cuartil_size))

    q2_start = q1_end
    q2_end = max(q2_start + 1, int(cuartil_size * 2))

    q3_start = q2_end
    q3_end = max(q3_start + 1, int(cuartil_size * 3))

    q4_start = q3_end
    q4_end = total_ordenes

    def calcular_promedio_ponderado_grupo(df_grupo, grupo_nombre="grupo"):
        """Calcula el promedio ponderado de un grupo de órdenes usando datos directos de ClickHouse"""
        if df_grupo.empty:
            return 0.0, 0.0, 0.0

        # Limpiar datos numéricos
        df_grupo_clean = df_grupo.copy()
        df_grupo_clean['cantidad'] = pd.to_numeric(df_grupo_clean['cantidad'], errors='coerce')
        df_grupo_clean['Total'] = pd.to_numeric(df_grupo_clean['Total'], errors='coerce')
        df_grupo_clean['Ingreso real'] = pd.to_numeric(df_grupo_clean['Ingreso real'], errors='coerce')

        df_grupo_clean = df_grupo_clean.dropna(subset=['cantidad', 'Total', 'Ingreso real'])

        if df_grupo_clean.empty:
            return 0.0, 0.0, 20.0

        total_cantidad = df_grupo_clean['cantidad'].sum()

        if total_cantidad == 0:
            return 0.0, 0.0, 0.0

        # PROMEDIO PONDERADO DE PRECIOS: Σ(precio_unitario × cantidad) / Σ(cantidad)
        precio_promedio = (df_grupo_clean['precio_unitario'] * df_grupo_clean['cantidad']).sum() / total_cantidad

        # PROMEDIO PONDERADO DE INGRESO REAL: Σ(ingreso_real_unitario × cantidad) / Σ(cantidad)
        df_grupo_clean['ingreso_real_unitario'] = df_grupo_clean['Ingreso real'] / df_grupo_clean['cantidad']
        ingreso_real_promedio = (df_grupo_clean['ingreso_real_unitario'] * df_grupo_clean['cantidad']).sum() / total_cantidad

        # PORCENTAJE DE INGRESO REAL DIRECTO
        porcentaje_ingreso = (ingreso_real_promedio / precio_promedio * 100) if precio_promedio > 0 else 0
        porcentaje_ingreso = max(0, porcentaje_ingreso)  # Asegurar que no sea negativo

        return float(precio_promedio), float(ingreso_real_promedio), float(porcentaje_ingreso)

    # Calcular promedio ponderado para cada cuartil
    q1_precio, q1_ingreso, q1_porcentaje = calcular_promedio_ponderado_grupo(df_ordenado.iloc[q1_start:q1_end], "Q1")
    q2_precio, q2_ingreso, q2_porcentaje = calcular_promedio_ponderado_grupo(df_ordenado.iloc[q2_start:q2_end], "Q2")
    q3_precio, q3_ingreso, q3_porcentaje = calcular_promedio_ponderado_grupo(df_ordenado.iloc[q3_start:q3_end], "Q3")
    max_precio, max_ingreso, max_porcentaje = calcular_promedio_ponderado_grupo(df_ordenado.iloc[q4_start:q4_end], "MAX")

    # MIN: Mantener como precio mínimo real (según requerimiento)
    min_precio = float(df_ordenado.iloc[0]['precio_unitario'])

    # Calcular ingreso real para MIN usando la misma lógica
    df_min = df_ordenado.iloc[0:1]  # Primera fila como DataFrame
    _, min_ingreso, min_porcentaje = calcular_promedio_ponderado_grupo(df_min, "MIN")

    # Calcular ticket promedio de todas las órdenes
    precio_promedio_precio, precio_promedio_ingreso, precio_promedio_porcentaje = calcular_promedio_ponderado_grupo(df_ordenado, "precio_promedio")

    # REDISTRIBUCIÓN: Llenar cuartiles vacíos con el último cuartil válido disponible
    # Esto soluciona el problema cuando un canal tiene pocas órdenes (1-3 órdenes)

    # 1. Llenar MAX si está vacío
    if max_precio == 0.0 or max_porcentaje == 0.0:
        if q3_precio > 0.0 and q3_porcentaje > 0.0:
            max_precio, max_ingreso, max_porcentaje = q3_precio, q3_ingreso, q3_porcentaje
        elif q2_precio > 0.0 and q2_porcentaje > 0.0:
            max_precio, max_ingreso, max_porcentaje = q2_precio, q2_ingreso, q2_porcentaje
        elif q1_precio > 0.0 and q1_porcentaje > 0.0:
            max_precio, max_ingreso, max_porcentaje = q1_precio, q1_ingreso, q1_porcentaje

    # 2. Llenar Q3 si está vacío
    if q3_precio == 0.0 or q3_porcentaje == 0.0:
        if q2_precio > 0.0 and q2_porcentaje > 0.0:
            q3_precio, q3_ingreso, q3_porcentaje = q2_precio, q2_ingreso, q2_porcentaje
        elif q1_precio > 0.0 and q1_porcentaje > 0.0:
            q3_precio, q3_ingreso, q3_porcentaje = q1_precio, q1_ingreso, q1_porcentaje

    # 3. Llenar Q2 si está vacío
    if q2_precio == 0.0 or q2_porcentaje == 0.0:
        if q1_precio > 0.0 and q1_porcentaje > 0.0:
            q2_precio, q2_ingreso, q2_porcentaje = q1_precio, q1_ingreso, q1_porcentaje

    return {
        # Valores MIN (precio mínimo real)
        'min_precio': min_precio, 'min_ingreso': min_ingreso, 'min_porcentaje': min_porcentaje,
        # Valores MAX (promedio ponderado del cuarto cuartil)
        'max_precio': max_precio, 'max_ingreso': max_ingreso, 'max_porcentaje': max_porcentaje,
        # Cuartiles (promedios ponderados por rangos)
        'q1_precio': q1_precio, 'q1_ingreso': q1_ingreso, 'q1_porcentaje': q1_porcentaje,
        'q2_precio': q2_precio, 'q2_ingreso': q2_ingreso, 'q2_porcentaje': q2_porcentaje,
        'q3_precio': q3_precio, 'q3_ingreso': q3_ingreso, 'q3_porcentaje': q3_porcentaje,
        'q4_precio': max_precio, 'q4_ingreso': max_ingreso, 'q4_porcentaje': max_porcentaje,  # Q4 es igual a MAX
        # Ticket promedio de todas las órdenes
        'precio_promedio_precio': precio_promedio_precio,
        'precio_promedio_ingreso': precio_promedio_ingreso,
        'precio_promedio_porcentaje': precio_promedio_porcentaje
    }


def calcular_clasificacion_skus(df_filtrado, año, mes, canal_filtro=None):
    """
    Calcula la clasificación de SKUs para un mes específico con análisis de cuartiles

    Args:
        df_filtrado: DataFrame filtrado con datos de ventas
        año: Año a analizar
        mes: Mes a analizar (1-12)
        canal_filtro: Filtro de canal (opcional)

    Returns:
        list: Lista de clasificaciones con datos completos por SKU
    """
    if df_filtrado.empty:
        print("DEBUG: DataFrame está vacío")
        return []

    print(f"=== CALCULANDO CLASIFICACIÓN PARA {mes}/{año} ===")
    print(f"DataFrame original tiene {len(df_filtrado)} filas")
    if canal_filtro:
        print(f"Filtro de canal aplicado: {canal_filtro}")

    # Filtrar solo órdenes de venta (excluir cancelados)
    df_activo = df_filtrado[df_filtrado["estado"] == "Orden de Venta"].copy()

    # Aplicar filtro de canal si se especifica (evitar filtrar por cadenas vacías)
    if canal_filtro and canal_filtro != "todos" and canal_filtro.strip():
        df_activo = df_activo[df_activo["Channel"] == canal_filtro].copy()
        print(f"Después del filtro de canal '{canal_filtro}': {len(df_activo)} filas")

    if df_activo.empty:
        print("DEBUG: No hay órdenes de venta después del filtro")
        return []

    print(f"Después del filtro de estado: {len(df_activo)} filas")

    # Filtrar por año y mes específicos
    df_mes = df_activo[
        (df_activo["Fecha"].dt.year == año) &
        (df_activo["Fecha"].dt.month == mes)
    ].copy()

    if df_mes.empty:
        print(f"DEBUG: No hay datos para {mes}/{año}")
        return []

    print(f"Datos para {mes}/{año}: {len(df_mes)} filas")

    # Convertir cantidad a numérico y limpiar datos
    df_mes['cantidad'] = pd.to_numeric(df_mes['cantidad'], errors='coerce')
    df_mes = df_mes.dropna(subset=['cantidad'])

    # Agrupar por SKU y descripción, sumar cantidades y totales
    skus_mes = df_mes.groupby(['sku', 'descripcion']).agg({
        'cantidad': 'sum',      # Total unidades vendidas en el mes
        'Total': 'sum'          # Total monto vendido en el mes
    }).reset_index()

    print(f"SKUs únicos en el mes: {len(skus_mes)}")

    # Aplicar clasificación a cada SKU
    clasificaciones = []
    contadores_clasificacion = {
        "Estrella": 0, "Prometedores": 0, "Potenciales": 0,
        "Revisión": 0, "Remover": 0
    }

    for _, row in skus_mes.iterrows():
        sku = row['sku']
        cantidad_mensual = int(row['cantidad'])
        clasificacion, color, orden = clasificar_sku_por_ventas(cantidad_mensual, (año, mes))

        # Calcular cuartiles de precios para este SKU
        df_sku_ventas = df_mes[df_mes['sku'] == sku].copy()
        cuartiles = calcular_cuartiles_precios_sku(df_sku_ventas, canal_filtro)

        # Usar el porcentaje de ingreso promedio calculado en los cuartiles (ya incluye la lógica híbrida)
        porcentaje_ingreso_promedio = cuartiles.get('precio_promedio_porcentaje', 0.0)

        # Contar por clasificación
        contadores_clasificacion[clasificacion] += 1

        clasificaciones.append({
            'sku': sku,
            'descripcion': row['descripcion'],
            'cantidad_mensual': cantidad_mensual,
            'monto_mensual': float(row['Total']),
            'clasificacion': clasificacion,
            'color': color,
            'orden': orden,
            'porcentaje_ingreso_promedio': float(porcentaje_ingreso_promedio),
            # Agregar datos de cuartiles
            **cuartiles
        })

    # Ordenar por cantidad mensual descendente
    clasificaciones.sort(key=lambda x: x['cantidad_mensual'], reverse=True)

    print(f"=== RESUMEN DE CLASIFICACIÓN ===")
    for clasificacion, cantidad in contadores_clasificacion.items():
        print(f"{clasificacion}: {cantidad} SKUs")

    print(f"Clasificaciones generadas: {len(clasificaciones)}")
    if clasificaciones:
        print(f"Top 3 SKUs por ventas:")
        for i, item in enumerate(clasificaciones[:3]):
            print(f"{i+1}. {item['sku']}: {item['cantidad_mensual']} unidades - {item['clasificacion']}")

    return clasificaciones


def agrupar_clasificaciones_para_tabla(clasificaciones):
    """
    Agrupa las clasificaciones por tipo para mostrar en tabla resumen

    Args:
        clasificaciones: Lista de clasificaciones individuales por SKU

    Returns:
        list: Lista de agrupaciones por clasificación
    """
    if not clasificaciones:
        return []

    # Agrupar por clasificación
    agrupaciones = {}
    for item in clasificaciones:
        clasificacion = item['clasificacion']
        if clasificacion not in agrupaciones:
            agrupaciones[clasificacion] = {
                'clasificacion': clasificacion,
                'color': item['color'],
                'orden': item['orden'],
                'cantidad_skus': 0,
                'total_unidades': 0,
                'total_monto': 0,
                'skus': []
            }

        agrupaciones[clasificacion]['cantidad_skus'] += 1
        agrupaciones[clasificacion]['total_unidades'] += item['cantidad_mensual']
        agrupaciones[clasificacion]['total_monto'] += item['monto_mensual']
        agrupaciones[clasificacion]['skus'].append(item)

    # Convertir a lista y ordenar por orden de clasificación
    resultado = list(agrupaciones.values())
    resultado.sort(key=lambda x: x['orden'])

    return resultado


def resumen_clasificaciones_con_participacion(clasificaciones_actual):
    """
    Genera resumen de clasificaciones con porcentaje de participación en ventas

    Args:
        clasificaciones_actual: Lista de clasificaciones del período actual

    Returns:
        list: Lista de resúmenes por clasificación con porcentajes
    """
    # Agrupar por clasificación y calcular totales
    agrupaciones = {}
    total_ventas_mes = 0

    for item in clasificaciones_actual:
        clasificacion = item['clasificacion']
        monto_mensual = item['monto_mensual']

        if clasificacion not in agrupaciones:
            agrupaciones[clasificacion] = {
                'clasificacion': clasificacion,
                'cantidad_skus': 0,
                'total_monto': 0
            }

        agrupaciones[clasificacion]['cantidad_skus'] += 1
        agrupaciones[clasificacion]['total_monto'] += monto_mensual
        total_ventas_mes += monto_mensual

    # Orden de clasificaciones
    orden_clasificaciones = ["Estrella", "Prometedores", "Potenciales", "Revisión", "Remover"]

    resumen = []
    total_porcentaje_verificacion = 0

    for clasificacion in orden_clasificaciones:
        if clasificacion in agrupaciones:
            data = agrupaciones[clasificacion]
            porcentaje = (data['total_monto'] / total_ventas_mes * 100) if total_ventas_mes > 0 else 0
            total_porcentaje_verificacion += porcentaje

            resumen.append({
                'clasificacion': clasificacion,
                'color': COLORES_CLASIFICACION.get(clasificacion, '#6c757d'),
                'cantidad_actual': data['cantidad_skus'],
                'porcentaje_participacion': porcentaje,
                'diferencia': f"{porcentaje:.1f}%",  # Ahora muestra el porcentaje de participación
                'delta': porcentaje  # Usar el porcentaje para el color (todos serán positivos)
            })
        else:
            # Si no hay SKUs en esta clasificación
            resumen.append({
                'clasificacion': clasificacion,
                'color': COLORES_CLASIFICACION.get(clasificacion, '#6c757d'),
                'cantidad_actual': 0,
                'porcentaje_participacion': 0,
                'diferencia': "0.0%",
                'delta': 0
            })

    print(f"=== VERIFICACIÓN PORCENTAJES ===")
    print(f"Total ventas del mes: ${total_ventas_mes:,.0f}")
    print(f"Suma de porcentajes: {total_porcentaje_verificacion:.1f}%")

    return resumen


def calcular_mes_anterior(año, mes):
    """
    Calcula el año y mes anterior

    Args:
        año: Año actual
        mes: Mes actual (1-12)

    Returns:
        tuple: (año_anterior, mes_anterior)
    """
    if mes == 1:
        return año - 1, 12
    else:
        return año, mes - 1


# ====== FUNCIONES PARA CONSULTAS CLICKHOUSE ======

def obtener_clasificaciones_desde_clickhouse(año=None, mes=None, clasificaciones_filtro=None):
    """
    Consulta la vista Silver.Clasificacion_SKU optimizada desde ClickHouse

    Args:
        año: Año específico (opcional, default: año actual)
        mes: Mes específico (opcional, default: mes actual)
        clasificaciones_filtro: Lista como ['Estrellas', 'Prometedores'] (opcional)

    Returns:
        Lista de SKUs clasificados con estructura compatible con Python actual
    """
    from datetime import datetime
    import time

    # Valores por defecto
    if año is None:
        año = datetime.now().year
    if mes is None:
        mes = datetime.now().month

    try:
        tiempo_inicio = time.time()
        print(f"=== CONSULTA SÚPER OPTIMIZADA ClickHouse PARA {mes}/{año} ===")
        print(f"🎯 Filtros: clasificaciones={clasificaciones_filtro}, límite=100 SKUs")

        # Construir filtros de consulta
        anio_mes = f"{año}-{mes:02d}"

        # Query base optimizada con LIMIT para mejor performance
        query = f"""
        SELECT
            sku,
            descripcion,
            marca,
            categoria,
            canal_principal,
            cantidad_vendida,
            numero_ordenes,
            clasificacion,
            umbral_aplicado,
            es_mes_actual,
            dias_transcurridos
        FROM Silver.Clasificacion_SKU
        WHERE anio_mes = '{anio_mes}'
        """

        # Agregar filtro de clasificaciones si se especifica
        if clasificaciones_filtro:
            clasificaciones_str = "', '".join(clasificaciones_filtro)
            query += f" AND clasificacion IN ('{clasificaciones_str}')"

        # Ordenar por cantidad vendida descendente y limitar a top 100
        query += " ORDER BY cantidad_vendida DESC LIMIT 100"

        print(f"Ejecutando query: {query}")

        # Ejecutar consulta
        connection = get_db_connection()
        result = connection.query(query)
        rows = result.result_rows

        tiempo_query = time.time()
        print(f"⏱️  Query ClickHouse ejecutado en: {tiempo_query - tiempo_inicio:.3f} segundos")
        print(f"📊 SKUs obtenidos desde ClickHouse: {len(rows)}")

        # Convertir a formato compatible con Python
        clasificaciones = []
        contadores_clasificacion = {
            "Estrella": 0, "Prometedores": 0, "Potenciales": 0,
            "Revisión": 0, "Remover": 0
        }

        for row in rows:
            # Mapear clasificación de ClickHouse a Python
            clasificacion_clickhouse = row[7]  # clasificacion
            clasificacion_python = MAPEO_CLASIFICACIONES.get(clasificacion_clickhouse, clasificacion_clickhouse)

            # Obtener color y orden
            color = COLORES_CLASIFICACION.get(clasificacion_python, '#6c757d')
            orden = ORDEN_CLASIFICACION.get(clasificacion_python, 6)

            # Contar por clasificación
            if clasificacion_python in contadores_clasificacion:
                contadores_clasificacion[clasificacion_python] += 1

            clasificaciones.append({
                'sku': row[0],                          # sku
                'descripcion': row[1],                  # descripcion
                'marca': row[2],                        # marca
                'categoria': row[3],                    # categoria
                'canal_principal': row[4],              # canal_principal
                'cantidad_mensual': int(row[5]),        # cantidad_vendida
                'numero_ordenes': int(row[6]),          # numero_ordenes
                'clasificacion': clasificacion_python,  # clasificacion mapeada
                'color': color,
                'orden': orden,
                'umbral_aplicado': float(row[8]),       # umbral_aplicado
                'es_mes_actual': bool(row[9]),          # es_mes_actual
                'dias_transcurridos': int(row[10])      # dias_transcurridos
            })

        tiempo_final = time.time()
        print(f"⏱️  Procesamiento total: {tiempo_final - tiempo_inicio:.3f} segundos")

        # Mostrar resumen
        print(f"=== RESUMEN OPTIMIZADO ClickHouse ===")
        for clasificacion, cantidad in contadores_clasificacion.items():
            if cantidad > 0:
                print(f"{clasificacion}: {cantidad} SKUs")

        # Mostrar top 3 si hay datos
        if clasificaciones:
            print(f"Top 3 SKUs por ventas:")
            for i, item in enumerate(clasificaciones[:3]):
                print(f"{i+1}. {item['sku']}: {item['cantidad_mensual']} unidades - {item['clasificacion']}")

        return clasificaciones

    except Exception as e:
        print(f"❌ ERROR consultando Silver.Clasificacion_SKU: {e}")
        print(f"Parámetros: año={año}, mes={mes}, filtros={clasificaciones_filtro}")
        # Fallback: retornar lista vacía en caso de error
        return []
def obtener_skus_por_clasificacion_clickhouse(clasificacion, año=None, mes=None):
    """
    Función optimizada para obtener SKUs de una clasificación específica desde ClickHouse

    Args:
        clasificacion: 'Estrella', 'Prometedores', 'Potenciales', etc.
        año: Año específico (opcional, default: año actual)
        mes: Mes específico (opcional, default: mes actual)

    Returns:
        Lista de SKUs de la clasificación solicitada
    """
    # Mapear clasificación Python -> ClickHouse
    MAPEO_INVERSO = {
        'Estrella': 'Estrellas',
        'Prometedores': 'Prometedores',
        'Potenciales': 'Potenciales',
        'Revisión': 'Revision',
        'Remover': 'Remover'
    }

    clasificacion_clickhouse = MAPEO_INVERSO.get(clasificacion, clasificacion)

    # Consultar ClickHouse con filtro específico
    resultados = obtener_clasificaciones_desde_clickhouse(
        año=año,
        mes=mes,
        clasificaciones_filtro=[clasificacion_clickhouse]
    )

    print(f"📊 SKUs {clasificacion} obtenidos desde ClickHouse: {len(resultados)}")
    return resultados

