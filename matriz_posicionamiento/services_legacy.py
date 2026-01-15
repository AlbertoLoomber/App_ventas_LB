"""
Servicios de Matriz de Posicionamiento
Lógica de negocio y cálculos
"""

import pandas as pd
import time


def clasificar_zona(ingreso_real_pct, roi_pct):
    """
    Clasifica un punto en una de las 4 zonas de la matriz

    Zonas:
    - Crítico: IR 0-20%, ROI 0-40% (Bajo ingreso + Bajo ROI)
    - Eficiente: IR 0-20%, ROI 40-100% (Bajo ingreso pero buen ROI)
    - A Desarrollar: IR 20-50%, ROI 0-40% (Buen ingreso pero bajo ROI)
    - Ideal: IR 20-50%, ROI 40-100% (Alto ingreso + Alto ROI)

    Args:
        ingreso_real_pct (float): % Ingreso Real (0-100)
        roi_pct (float): % ROI (0-100)

    Returns:
        tuple: (nombre_zona, color_fondo, color_texto, icono)
    """
    if ingreso_real_pct < 20:
        if roi_pct < 40:
            return 'Crítico', '#ffcccc', '#dc3545', '🔴'
        else:
            return 'Eficiente', '#cce5ff', '#0056b3', '🔵'
    else:  # ingreso_real_pct >= 20
        if roi_pct < 40:
            return 'A Desarrollar', '#e6e6e6', '#6c757d', '🟡'
        else:
            return 'Ideal', '#d4edda', '#28a745', '🟢'


def calcular_metricas_canal(df_canal):
    """
    Calcula métricas para un canal específico

    Args:
        df_canal: DataFrame filtrado para un canal

    Returns:
        dict: Métricas calculadas
    """
    ventas_reales = float(df_canal['Total'].sum())
    costo_venta = float(df_canal['Costo de venta'].sum())
    gastos_directos = float(df_canal['Gastos_directos'].sum())
    ingreso_real = float(df_canal['Ingreso real'].sum())
    num_transacciones = int(len(df_canal))

    # Calcular porcentajes
    ingreso_real_pct = (ingreso_real / ventas_reales * 100) if ventas_reales > 0 else 0
    roi_pct = (ingreso_real / costo_venta * 100) if costo_venta > 0 else 0

    return {
        'ventas_reales': ventas_reales,
        'costo_venta': costo_venta,
        'gastos_directos': gastos_directos,
        'ingreso_real': ingreso_real,
        'ingreso_real_pct': ingreso_real_pct,
        'roi_pct': roi_pct,
        'num_transacciones': num_transacciones
    }


def escalar_radio_burbuja(ventas, min_ventas, max_ventas, radio_min=12, radio_max=28):
    """
    Escala el tamaño del radio de la burbuja usando escala LOGARÍTMICA.

    Ideal para datos con diferencias extremas de ventas (>100x).
    La escala logarítmica comprime las diferencias grandes manteniendo
    todas las burbujas visibles y distinguibles.

    Ejemplo con datos reales:
    - Shein ($2.9M): 27-28px (grande)
    - TikTok ($1.2M): 25px (mediano-grande)
    - Aliexpress ($154K): 20px (mediano)
    - Coppel ($8K): 12px (pequeño pero VISIBLE)

    Args:
        ventas: Ventas del canal/categoría
        min_ventas: Ventas mínimas de todos los canales
        max_ventas: Ventas máximas de todos los canales
        radio_min: Radio mínimo de burbuja (px) - Aumentado de 8 a 12
        radio_max: Radio máximo de burbuja (px) - Reducido de 30 a 28

    Returns:
        float: Radio escalado entre radio_min y radio_max
    """
    if max_ventas > min_ventas and min_ventas > 0:
        # Importar math si no está disponible
        import math

        # Aplicar logaritmo natural para comprimir diferencias extremas
        # +1 para evitar log(0) en casos edge
        log_ventas = math.log(ventas + 1)
        log_min = math.log(min_ventas + 1)
        log_max = math.log(max_ventas + 1)

        # Normalizar proporción logarítmica (0 a 1)
        proporcion = (log_ventas - log_min) / (log_max - log_min)

        # Calcular radio final
        return radio_min + proporcion * (radio_max - radio_min)
    else:
        # Si todas las ventas son iguales o min_ventas=0, usar tamaño medio
        return (radio_min + radio_max) / 2


def filtrar_por_mes(df, mes_filtro):
    """
    Filtra DataFrame por mes, manejando formato YYYYMM (202410) o entero (10)

    Args:
        df: DataFrame con columna 'Fecha'
        mes_filtro: Mes en formato YYYYMM (ej: 202410) o entero (1-12)

    Returns:
        DataFrame filtrado
    """
    mes_filtro_str = str(mes_filtro)

    if len(mes_filtro_str) == 6:  # Formato YYYYMM (202410)
        año = int(mes_filtro_str[:4])
        mes = int(mes_filtro_str[4:6])
        return df[(df['Fecha'].dt.year == año) & (df['Fecha'].dt.month == mes)].copy()
    else:  # Formato antiguo (1-12)
        mes = int(mes_filtro_str)
        return df[df['Fecha'].dt.month == mes].copy()


def filtrar_por_rango_dias(df, mes_filtro, dia_maximo=None):
    """
    Filtra DataFrame por mes y mismo rango de días
    Si dia_maximo es None, toma todos los días del mes
    Si dia_maximo es 5, filtra del día 1 al 5 del mes

    Args:
        df: DataFrame con columna 'Fecha'
        mes_filtro: Mes en formato YYYYMM (ej: 202410)
        dia_maximo: Día máximo a incluir (1-31), None para todo el mes

    Returns:
        DataFrame filtrado
    """
    mes_filtro_str = str(mes_filtro)

    if len(mes_filtro_str) != 6:
        # Si no es formato YYYYMM, usar filtro normal
        return filtrar_por_mes(df, mes_filtro)

    año = int(mes_filtro_str[:4])
    mes = int(mes_filtro_str[4:6])

    # Filtrar por año y mes
    df_mes = df[(df['Fecha'].dt.year == año) & (df['Fecha'].dt.month == mes)].copy()

    # Si no se especifica día máximo, devolver todo el mes
    if dia_maximo is None:
        return df_mes

    # Filtrar por rango de días (1 hasta dia_maximo)
    df_filtrado = df_mes[df_mes['Fecha'].dt.day <= dia_maximo].copy()

    print(f"📅 [FILTRO] Mes {mes_filtro}: Días 1-{dia_maximo} → {len(df_filtrado)} registros")

    return df_filtrado


def obtener_lista_skus(df, mes_filtro=None):
    """
    Obtiene la lista de SKUs disponibles con su descripción y clasificación

    Args:
        df: DataFrame con datos de ventas
        mes_filtro: Mes a filtrar en formato YYYYMM (ej: 202410) o entero (1-12), None para todos

    Returns:
        list: Lista de dicts con {sku, descripcion, clasificacion}
    """
    print(f"🔍 [CLASIFICACION] Obteniendo lista de SKUs para mes {mes_filtro}...")

    if df.empty:
        return []

    # Filtrar por mes si se especifica
    if mes_filtro:
        df = filtrar_por_mes(df, mes_filtro)
        print(f"🔍 [CLASIFICACION] Filtrado por mes {mes_filtro}: {len(df)} registros")

    # Verificar si existe la columna Clasificacion
    if 'Clasificacion' not in df.columns:
        print(f"⚠️  [CLASIFICACION] Columna 'Clasificacion' NO encontrada en el DataFrame")
        print(f"📋 [CLASIFICACION] Columnas disponibles: {df.columns.tolist()}")
    else:
        print(f"✅ [CLASIFICACION] Columna 'Clasificacion' encontrada")
        clasificaciones_unicas = df['Clasificacion'].unique()
        print(f"📊 [CLASIFICACION] Clasificaciones únicas: {clasificaciones_unicas}")

    # Tomar el último registro por SKU para obtener la info más reciente
    df_sorted = df.sort_values('Fecha', ascending=False)
    df_ultimo = df_sorted.groupby('sku').first().reset_index()

    # Crear lista de SKUs
    skus_lista = []
    for _, row in df_ultimo.iterrows():
        # Intentar obtener clasificación de diferentes formas
        clasificacion = None
        if 'Clasificacion' in row:
            clasificacion = row['Clasificacion']

        # Si el valor es None, vacío o NaN, usar 'Sin Clasificar'
        if clasificacion is None or pd.isna(clasificacion) or clasificacion == '':
            clasificacion = 'Sin Clasificar'

        # Debug: imprimir las primeras 3 clasificaciones
        if len(skus_lista) < 3:
            print(f"🔍 [CLASIFICACION DEBUG] SKU: {row['sku']}, Clasificacion: '{clasificacion}'")

        skus_lista.append({
            'sku': row['sku'],
            'descripcion': row['Descripcion'],
            'clasificacion': clasificacion
        })

    # Definir orden de clasificaciones
    orden_clasificacion = {
        'Estrellas': 1,
        'Prometedores': 2,
        'Potenciales': 3,
        'Revision': 4,
        'Remover': 5,
        'Sin Clasificar': 6
    }

    # Ordenar por clasificación (según orden definido) y luego por SKU alfabéticamente
    skus_lista = sorted(skus_lista, key=lambda x: (orden_clasificacion.get(x['clasificacion'], 999), x['sku']))

    # Contar clasificaciones
    from collections import Counter
    clasificaciones_count = Counter([s['clasificacion'] for s in skus_lista])
    print(f"✅ [CLASIFICACION] {len(skus_lista)} SKUs encontrados")
    print(f"📊 [CLASIFICACION] Distribución: {dict(clasificaciones_count)}")

    return skus_lista


def generar_datos_matriz_clasificacion_con_rango_dias(df, mes_filtro=None, canal_filtro=None, skus_seleccionados=None, dia_maximo=None):
    """
    Genera datos para la matriz de clasificación con filtro por rango de días
    IGUAL que generar_datos_matriz_clasificacion pero con filtro por días

    Args:
        df: DataFrame con datos de ventas
        mes_filtro: Mes a filtrar (1-12), None para todos
        canal_filtro: Canal a filtrar, None o "Todos" para todos
        skus_seleccionados: Lista de SKUs a mostrar, None para vacío
        dia_maximo: Día máximo a incluir (1-31), None para todo el mes

    Returns:
        dict: {datasets, skus, estadisticas}
    """
    tiempo_inicio = time.time()
    print(f"🔍 [CLASIFICACION-RANGO] Generando datos - Mes: {mes_filtro}, Canal: {canal_filtro}, SKUs: {skus_seleccionados}, Día máximo: {dia_maximo}")

    if df.empty:
        return {
            'datasets': [],
            'skus': [],
            'estadisticas': {
                'total_skus': 0,
                'ventas_totales': 0,
                'ingreso_promedio': 0,
                'roi_promedio': 0,
                'eje_y_max': 100
            }
        }

    # Si no hay SKUs seleccionados, devolver datos vacíos
    if not skus_seleccionados or len(skus_seleccionados) == 0:
        print(f"⚠️  [CLASIFICACION-RANGO] No hay SKUs seleccionados, retornando vacío")
        return {
            'datasets': [],
            'skus': [],
            'estadisticas': {
                'total_skus': 0,
                'ventas_totales': 0,
                'ingreso_promedio': 0,
                'roi_promedio': 0,
                'eje_y_max': 100
            }
        }

    # Filtrar por mes Y rango de días si se especifica
    if mes_filtro:
        df = filtrar_por_rango_dias(df, mes_filtro, dia_maximo)
        print(f"📊 [CLASIFICACION-RANGO] Registros después de filtrar por mes {mes_filtro} (días 1-{dia_maximo or 'fin'}): {len(df)}")

    # Filtrar por canal si se especifica (y no es "Todos")
    if canal_filtro and canal_filtro != 'Todos':
        # Soportar tanto string como lista
        if isinstance(canal_filtro, list):
            df = df[df['Channel'].isin(canal_filtro)].copy()
            print(f"📊 [CLASIFICACION-RANGO] Registros después de filtrar por canales {canal_filtro}: {len(df)}")
        else:
            df = df[df['Channel'] == canal_filtro].copy()
            print(f"📊 [CLASIFICACION-RANGO] Registros después de filtrar por canal {canal_filtro}: {len(df)}")

    # Filtrar por SKUs seleccionados
    df = df[df['sku'].isin(skus_seleccionados)].copy()
    print(f"📊 [CLASIFICACION-RANGO] Registros después de filtrar por SKUs: {len(df)}")

    # Filtrar por estado
    df_filtrado = df[df['estado'] != 'Cancelado'].copy()

    if df_filtrado.empty:
        print(f"⚠️  [CLASIFICACION-RANGO] No hay datos después de aplicar filtros")
        return {
            'datasets': [],
            'skus': [],
            'estadisticas': {
                'total_skus': 0,
                'ventas_totales': 0,
                'ingreso_promedio': 0,
                'roi_promedio': 0,
                'eje_y_max': 100
            }
        }

    # IMPORTANTE: Tomar el último registro de cada SKU + Channel (datos acumulados)
    df_filtrado = df_filtrado.sort_values('Fecha', ascending=False)
    df_ultimo_registro = df_filtrado.groupby(['sku', 'Channel']).first().reset_index()

    print(f"📊 [CLASIFICACION-RANGO] Registros después de tomar último por SKU-Canal: {len(df_ultimo_registro)}")

    # Llamar a la función original con df ya filtrado (sin mes_filtro para evitar doble filtro)
    return generar_datos_matriz_clasificacion(df_ultimo_registro, mes_filtro=None, canal_filtro=None, skus_seleccionados=skus_seleccionados)


def generar_datos_matriz_clasificacion(df, mes_filtro=None, canal_filtro=None, skus_seleccionados=None):
    """
    Genera datos para la matriz de clasificación (% Ingreso Real vs % ROI por SKU-Canal)

    Args:
        df: DataFrame con datos de ventas
        mes_filtro: Mes a filtrar (1-12), None para todos
        canal_filtro: Canal a filtrar, None o "Todos" para todos
        skus_seleccionados: Lista de SKUs a mostrar, None para vacío

    Returns:
        dict: {
            'datasets': Lista de datasets para Chart.js,
            'skus': Lista de información de SKU-Canal,
            'estadisticas': Estadísticas generales
        }
    """
    tiempo_inicio = time.time()
    print(f"🔍 [CLASIFICACION] Generando datos para matriz de clasificación...")
    print(f"📥 [CLASIFICACION] Filtros - Mes: {mes_filtro}, Canal: {canal_filtro}, SKUs: {skus_seleccionados}")

    if df.empty:
        return {
            'datasets': [],
            'skus': [],
            'estadisticas': {
                'total_skus': 0,
                'ventas_totales': 0,
                'ingreso_promedio': 0,
                'roi_promedio': 0,
                'eje_y_max': 100
            }
        }

    # Si no hay SKUs seleccionados, devolver datos vacíos
    if not skus_seleccionados or len(skus_seleccionados) == 0:
        print(f"⚠️  [CLASIFICACION] No hay SKUs seleccionados, retornando vacío")
        return {
            'datasets': [],
            'skus': [],
            'estadisticas': {
                'total_skus': 0,
                'ventas_totales': 0,
                'ingreso_promedio': 0,
                'roi_promedio': 0,
                'eje_y_max': 100
            }
        }

    # Filtrar por mes si se especifica
    if mes_filtro:
        df = filtrar_por_mes(df, mes_filtro)
        print(f"📊 [CLASIFICACION] Registros después de filtrar por mes {mes_filtro}: {len(df)}")

    # Filtrar por canal si se especifica (y no es "Todos")
    if canal_filtro and canal_filtro != 'Todos':
        # Soportar tanto string como lista
        if isinstance(canal_filtro, list):
            df = df[df['Channel'].isin(canal_filtro)].copy()
            print(f"📊 [CLASIFICACION] Registros después de filtrar por canales {canal_filtro}: {len(df)}")
        else:
            df = df[df['Channel'] == canal_filtro].copy()
            print(f"📊 [CLASIFICACION] Registros después de filtrar por canal {canal_filtro}: {len(df)}")

    # Filtrar por SKUs seleccionados
    df = df[df['sku'].isin(skus_seleccionados)].copy()
    print(f"📊 [CLASIFICACION] Registros después de filtrar por SKUs: {len(df)}")

    # Filtrar por estado
    df_filtrado = df[df['estado'] != 'Cancelado'].copy()

    if df_filtrado.empty:
        print(f"⚠️  [CLASIFICACION] No hay datos después de aplicar filtros")
        return {
            'datasets': [],
            'skus': [],
            'estadisticas': {
                'total_skus': 0,
                'ventas_totales': 0,
                'ingreso_promedio': 0,
                'roi_promedio': 0,
                'eje_y_max': 100
            }
        }

    # IMPORTANTE: Tomar el último registro de cada SKU + Channel (datos acumulados)
    df_filtrado = df_filtrado.sort_values('Fecha', ascending=False)
    df_ultimo_registro = df_filtrado.groupby(['sku', 'Channel']).first().reset_index()

    print(f"📊 [CLASIFICACION] Registros después de tomar último por SKU-Canal: {len(df_ultimo_registro)}")

    # Diccionario de abreviaciones de canales
    abreviaciones_canales = {
        'Mercado Libre': 'ML',
        'Walmart': 'WM',
        'Liverpool': 'LV',
        'Shein': 'SH',
        'CrediTienda': 'CT',
        'Yuhu': 'YH',
        'Aliexpress': 'AE',
        'Coppel': 'CP',
        'TikTok Shop': 'TT',
        'Temu': 'TM'
    }

    # Colores por canal (PRINCIPAL - usado para graficar)
    colores_canales = {
        'Mercado Libre': '#FFE135',  # Amarillo
        'Walmart': '#0071CE',        # Azul
        'Liverpool': '#E4002B',      # Rojo
        'Shein': '#FF6B35',          # Naranja vibrante (diferenciado de ML)
        'CrediTienda': '#00A650',    # Verde
        'Yuhu': '#9B59B6',           # Morado
        'Aliexpress': '#E62129',     # Rojo oscuro
        'Coppel': '#003DA5',         # Azul oscuro
        'TikTok Shop': '#25F4EE',    # Cyan/Turquesa (color distintivo de TikTok)
        'Temu': '#FF6C00'            # Naranja Temu (color oficial de la marca)
    }

    # Colores por clasificación (solo para información adicional en tooltips)
    colores_clasificacion = {
        'Estrellas': '#FFD700',      # Dorado
        'Prometedores': '#28a745',   # Verde
        'Potenciales': '#17a2b8',    # Azul
        'Revision': '#ffc107',       # Amarillo/Naranja
        'Remover': '#dc3545',        # Rojo
        'Sin Clasificar': '#6c757d'  # Gris
    }

    # Procesar cada SKU-Canal
    datasets = []
    skus_info = []
    ventas_list = []

    print(f"📋 [CLASIFICACION] Columnas disponibles: {df_ultimo_registro.columns.tolist()}")

    for idx, row in df_ultimo_registro.iterrows():
        try:
            sku = row['sku']
            descripcion = row['Descripcion']
            marca = row['Marca']
            categoria = row['Categoria']
            canal = row['Channel']

            # Obtener abreviación del canal
            canal_abrev = abreviaciones_canales.get(canal, canal[:2].upper())

            # Obtener clasificación
            clasificacion = row.get('Clasificacion', 'Sin Clasificar')
            # Si es None o NaN, usar 'Sin Clasificar'
            if pd.isna(clasificacion) or clasificacion == '' or clasificacion is None:
                clasificacion = 'Sin Clasificar'

            # Debug: imprimir las primeras 3
            if len(skus_info) < 3:
                print(f"🔍 [MATRIZ CLASIF DEBUG] SKU: {sku}, Canal: {canal}, Clasificacion: '{clasificacion}'")

            ventas = float(row['Total'])
            costo = float(row['Costo de venta'])
            ingreso_real = float(row['Ingreso real'])
        except Exception as e:
            print(f"❌ [CLASIFICACION] Error procesando fila {idx}: {e}")
            print(f"   Row keys: {row.index.tolist()}")
            import traceback
            traceback.print_exc()
            continue

        # Calcular porcentajes
        ingreso_real_pct = (ingreso_real / ventas * 100) if ventas > 0 else 0
        roi_pct = (ingreso_real / costo * 100) if costo > 0 else 0

        # Clasificar zona
        zona, color_fondo, color_texto, icono = clasificar_zona(ingreso_real_pct, roi_pct)

        # Guardar para escalar radios después
        ventas_list.append(ventas)

        # Información del SKU-Canal
        skus_info.append({
            'sku': sku,
            'descripcion': descripcion,
            'marca': marca,
            'categoria': categoria,
            'canal': canal,
            'canal_abrev': canal_abrev,  # Agregar abreviación
            'clasificacion': clasificacion,
            'ingreso_real_pct': round(ingreso_real_pct, 2),
            'roi_pct': round(roi_pct, 2),
            'ventas': ventas,
            'ingreso_real': ingreso_real,
            'zona': zona,
            'color_zona': color_fondo,
            'color_texto': color_texto,
            'icono': icono,
            'color_clasificacion': colores_clasificacion.get(clasificacion, '#6c757d'),
            'color_canal': colores_canales.get(canal, '#6c757d')
        })

    # Escalar radios
    min_ventas = min(ventas_list) if ventas_list else 0
    max_ventas = max(ventas_list) if ventas_list else 0

    # Crear datasets por clasificación (agrupar burbujas por color de clasificación)
    datasets_temp = []

    for sku_info in skus_info:
        radio = escalar_radio_burbuja(
            sku_info['ventas'],
            min_ventas,
            max_ventas
        )

        # Dataset para Chart.js (un punto por SKU-Canal)
        # IMPORTANTE: Usar color de CANAL, no de clasificación
        # Label formato: "ABREV - SKU" (ej: "ML - 2000005")
        datasets_temp.append({
            'label': f"{sku_info['canal_abrev']} - {sku_info['sku']}",
            'data': [{
                'x': sku_info['ingreso_real_pct'],
                'y': sku_info['roi_pct'],
                'r': radio
            }],
            'backgroundColor': sku_info['color_canal'],  # Cambio: usar color de canal
            'borderColor': sku_info['color_canal'],      # Cambio: usar color de canal
            'borderWidth': 2,
            'pointStyle': 'cross',  # Cambio: usar 'cross' (X) en lugar de círculo
            'pointRadius': radio,   # Tamaño del punto
            '_radio': radio,  # Para ordenar
            '_sku': sku_info['sku'],
            '_descripcion': sku_info['descripcion'],
            '_canal': sku_info['canal'],
            '_clasificacion': sku_info['clasificacion'],
            '_ventas': sku_info['ventas'],
            '_ingreso_real': sku_info['ingreso_real']
        })

    # Ordenar datasets de menor a mayor radio
    datasets = sorted(datasets_temp, key=lambda d: d['_radio'], reverse=False)

    # Remover campos temporales
    for ds in datasets:
        del ds['_radio']

    # Calcular ROI máximo para ajustar el eje Y dinámicamente
    roi_max = max([s['roi_pct'] for s in skus_info]) if skus_info else 100
    import math
    eje_y_max = max(100, math.ceil(roi_max * 1.1 / 10) * 10)

    print(f"📊 [CLASIFICACION] ROI máximo encontrado: {roi_max:.1f}%")
    print(f"📊 [CLASIFICACION] Eje Y ajustado a: 0% - {eje_y_max}%")

    # Estadísticas generales
    total_ventas = sum(s['ventas'] for s in skus_info)
    total_ingreso_real = sum(s['ingreso_real'] for s in skus_info)
    ingreso_promedio = sum(s['ingreso_real_pct'] for s in skus_info) / len(skus_info) if skus_info else 0
    roi_promedio = sum(s['roi_pct'] for s in skus_info) / len(skus_info) if skus_info else 0

    estadisticas = {
        'total_skus': len(skus_info),
        'ventas_totales': total_ventas,
        'ingreso_real_total': total_ingreso_real,
        'ingreso_promedio': round(ingreso_promedio, 2),
        'roi_promedio': round(roi_promedio, 2),
        'critico': sum(1 for s in skus_info if s['zona'] == 'Crítico'),
        'eficiente': sum(1 for s in skus_info if s['zona'] == 'Eficiente'),
        'a_desarrollar': sum(1 for s in skus_info if s['zona'] == 'A Desarrollar'),
        'ideal': sum(1 for s in skus_info if s['zona'] == 'Ideal'),
        'eje_y_max': eje_y_max
    }

    tiempo_fin = time.time()
    print(f"✅ [CLASIFICACION] Datos generados: {len(datasets)} SKU-Canal en {tiempo_fin - tiempo_inicio:.3f}s")

    return {
        'datasets': datasets,
        'skus': sorted(skus_info, key=lambda x: x['ingreso_real'], reverse=True),  # Ordenar por $ IR
        'estadisticas': estadisticas
    }


def generar_datos_matriz(df, mes_filtro=None, marca_filtro='Ambos', canales_clasificacion=None):
    """
    Genera datos para la matriz de posicionamiento (% Ingreso Real vs % ROI)

    Args:
        df: DataFrame con datos de ventas
        mes_filtro: Mes a filtrar (1-12), None para todos
        marca_filtro: Filtro de marca ('Loomber', 'Otros', 'Ambos')
        canales_clasificacion: Lista de canales oficiales

    Returns:
        dict: {
            'datasets': Lista de datasets para Chart.js,
            'canales': Lista de información de canales,
            'estadisticas': Estadísticas generales
        }
    """
    tiempo_inicio = time.time()
    print(f"🔍 [MATRIZ] Generando datos para matriz de posicionamiento... (Marca: {marca_filtro})")

    if df.empty:
        return {
            'datasets': [],
            'canales': [],
            'estadisticas': {
                'total_canales': 0,
                'ventas_totales': 0,
                'ingreso_promedio': 0,
                'roi_promedio': 0
            }
        }

    # Filtrar por mes si se especifica
    if mes_filtro:
        df = filtrar_por_mes(df, mes_filtro)

    # Filtrar por marca si se especifica (y no es "Ambos")
    if marca_filtro and marca_filtro != 'Ambos':
        print(f"🏷️  [MATRIZ] Filtrando por marca: {marca_filtro}")
        df = df[df['Marca'] == marca_filtro].copy()
        print(f"📊 [MATRIZ] Registros después de filtrar por marca: {len(df)}")

    # Filtrar por canales oficiales y estado
    print(f"🔍 [MATRIZ DEBUG] Canales en clasificación: {canales_clasificacion}")
    print(f"🔍 [MATRIZ DEBUG] Canales únicos en DataFrame ANTES de filtrar: {sorted(df['Channel'].unique().tolist())}")

    df_filtrado = df[
        (df['estado'] != 'Cancelado') &
        (df['Channel'].isin(canales_clasificacion))
    ].copy()

    print(f"🔍 [MATRIZ DEBUG] Canales únicos DESPUÉS de filtrar: {sorted(df_filtrado['Channel'].unique().tolist())}")
    print(f"🔍 [MATRIZ DEBUG] Total registros filtrados: {len(df_filtrado)}")

    if df_filtrado.empty:
        return {'datasets': [], 'canales': [], 'estadisticas': {}}

    # IMPORTANTE: Como los datos son acumulados, solo tomar el último registro de cada SKU
    # Ordenar por fecha y tomar el último registro de cada SKU + Channel
    df_filtrado = df_filtrado.sort_values('Fecha', ascending=False)
    df_ultimo_registro = df_filtrado.groupby(['sku', 'Channel']).first().reset_index()

    print(f"📊 [MATRIZ] Registros después de tomar último por SKU: {len(df_ultimo_registro)} (antes: {len(df_filtrado)})")
    print(f"🔍 [MATRIZ DEBUG] Canales únicos en df_ultimo_registro: {sorted(df_ultimo_registro['Channel'].unique().tolist())}")

    # Contar registros por canal
    canales_count = df_ultimo_registro['Channel'].value_counts()
    print(f"🔍 [MATRIZ DEBUG] Distribución de registros por canal:")
    for canal, count in canales_count.items():
        print(f"   - {canal}: {count} registros")

    # Colores por canal
    colores_canales = {
        'Mercado Libre': '#FFE135',  # Amarillo
        'Walmart': '#0071CE',        # Azul
        'Liverpool': '#E4002B',      # Rojo
        'Shein': '#FF6B35',          # Naranja vibrante (diferenciado de ML)
        'CrediTienda': '#00A650',    # Verde
        'Yuhu': '#9B59B6',           # Morado
        'Aliexpress': '#E62129',     # Rojo oscuro
        'Coppel': '#003DA5',         # Azul oscuro
        'TikTok Shop': '#25F4EE',    # Cyan/Turquesa (color distintivo de TikTok)
        'Temu': '#FF6C00'            # Naranja Temu (color oficial de la marca)
    }

    # Agrupar por canal y calcular métricas
    datasets = []
    canales_info = []
    ventas_list = []

    for canal in df_ultimo_registro['Channel'].unique():
        df_canal = df_ultimo_registro[df_ultimo_registro['Channel'] == canal]

        # Calcular métricas (ahora suma los últimos registros de cada SKU)
        metricas = calcular_metricas_canal(df_canal)

        # Clasificar zona
        zona, color_fondo, color_texto, icono = clasificar_zona(
            metricas['ingreso_real_pct'],
            metricas['roi_pct']
        )

        # Guardar para escalar radios después
        ventas_list.append(metricas['ventas_reales'])

        # Información del canal
        canales_info.append({
            'canal': canal,
            'ingreso_real_pct': round(metricas['ingreso_real_pct'], 2),
            'roi_pct': round(metricas['roi_pct'], 2),
            'ventas': metricas['ventas_reales'],
            'ingreso_real': metricas['ingreso_real'],
            'costo_venta': metricas['costo_venta'],  # AGREGADO para cálculo correcto
            'zona': zona,
            'color_zona': color_fondo,
            'color_texto': color_texto,
            'icono': icono,
            'num_transacciones': metricas['num_transacciones'],
            'color_canal': colores_canales.get(canal, '#6c757d')
        })

    # Escalar radios
    min_ventas = min(ventas_list) if ventas_list else 0
    max_ventas = max(ventas_list) if ventas_list else 0

    # Lista temporal para ordenar por tamaño
    datasets_temp = []

    for canal_info in canales_info:
        radio = escalar_radio_burbuja(
            canal_info['ventas'],
            min_ventas,
            max_ventas
        )

        # Dataset para Chart.js (un punto por canal)
        datasets_temp.append({
            'label': canal_info['canal'],
            'data': [{
                'x': canal_info['ingreso_real_pct'],
                'y': canal_info['roi_pct'],
                'r': radio
            }],
            'backgroundColor': canal_info['color_canal'],
            'borderColor': canal_info['color_canal'],
            'borderWidth': 2,
            '_radio': radio  # Para ordenar
        })

    # Ordenar datasets de menor a mayor radio (burbujas pequeñas primero/atrás, grandes al final/adelante)
    # Esto asegura que las burbujas pequeñas siempre sean visibles encima de las grandes
    datasets = sorted(datasets_temp, key=lambda d: d['_radio'], reverse=False)

    # Remover el campo temporal
    for ds in datasets:
        del ds['_radio']

    # Estadísticas generales
    total_ventas = sum(c['ventas'] for c in canales_info)
    total_ingreso_real = sum(c['ingreso_real'] for c in canales_info)
    total_costo_venta = sum(c['costo_venta'] for c in canales_info)

    # CORREGIDO: Calcular porcentajes sobre totales, NO promedio de porcentajes
    ingreso_promedio = (total_ingreso_real / total_ventas * 100) if total_ventas > 0 else 0
    roi_promedio = (total_ingreso_real / total_costo_venta * 100) if total_costo_venta > 0 else 0

    # Calcular ROI máximo para ajustar el eje Y dinámicamente
    roi_max = max([c['roi_pct'] for c in canales_info]) if canales_info else 100
    # Calcular el máximo del eje Y: al menos 100, o ROI_max * 1.1 redondeado al siguiente múltiplo de 10
    import math
    eje_y_max = max(100, math.ceil(roi_max * 1.1 / 10) * 10)

    print(f"📊 [MATRIZ] ROI máximo encontrado: {roi_max:.1f}%")
    print(f"📊 [MATRIZ] Eje Y ajustado a: 0% - {eje_y_max}%")

    estadisticas = {
        'total_canales': len(canales_info),
        'ventas_totales': total_ventas,
        'ingreso_real_total': total_ingreso_real,
        'ingreso_promedio': round(ingreso_promedio, 2),
        'roi_promedio': round(roi_promedio, 2),
        'critico': sum(1 for c in canales_info if c['zona'] == 'Crítico'),
        'eficiente': sum(1 for c in canales_info if c['zona'] == 'Eficiente'),
        'a_desarrollar': sum(1 for c in canales_info if c['zona'] == 'A Desarrollar'),
        'ideal': sum(1 for c in canales_info if c['zona'] == 'Ideal'),
        'eje_y_max': eje_y_max  # ← NUEVO: Para usar en el frontend
    }

    tiempo_fin = time.time()
    print(f"✅ [MATRIZ] Datos generados: {len(datasets)} canales en {tiempo_fin - tiempo_inicio:.3f}s")

    return {
        'datasets': datasets,
        'canales': sorted(canales_info, key=lambda x: x['ingreso_real'], reverse=True),  # Ordenar por $ IR
        'estadisticas': estadisticas
    }


# ============================================================================
# MATRIZ DE CATEGORÍAS - Funciones movidas desde matriz_categorias/services.py
# ============================================================================

def calcular_metricas_categoria(df_grupo):
    """
    Calcula métricas para una combinación Canal + Categoría

    Args:
        df_grupo: DataFrame filtrado para un grupo

    Returns:
        dict: Métricas calculadas
    """
    ventas_reales = float(df_grupo['Total'].sum())
    costo_venta = float(df_grupo['Costo de venta'].sum())
    gastos_directos = float(df_grupo['Gastos_directos'].sum())
    ingreso_real = float(df_grupo['Ingreso real'].sum())
    num_transacciones = int(len(df_grupo))

    # Calcular porcentajes
    ingreso_real_pct = (ingreso_real / ventas_reales * 100) if ventas_reales > 0 else 0
    roi_pct = (ingreso_real / costo_venta * 100) if costo_venta > 0 else 0

    return {
        'ventas_reales': ventas_reales,
        'costo_venta': costo_venta,
        'gastos_directos': gastos_directos,
        'ingreso_real': ingreso_real,
        'ingreso_real_pct': ingreso_real_pct,
        'roi_pct': roi_pct,
        'num_transacciones': num_transacciones
    }


def escalar_tamano_marcador(ventas, min_ventas, max_ventas, tamano_min=8, tamano_max=20):
    """
    Escala el tamaño del marcador X proporcionalmente a las ventas

    Args:
        ventas: Ventas del grupo
        min_ventas: Ventas mínimas de todos los grupos
        max_ventas: Ventas máximas de todos los grupos
        tamano_min: Tamaño mínimo del marcador
        tamano_max: Tamaño máximo del marcador

    Returns:
        float: Tamaño escalado
    """
    if max_ventas > min_ventas:
        return tamano_min + ((ventas - min_ventas) / (max_ventas - min_ventas)) * (tamano_max - tamano_min)
    else:
        return (tamano_min + tamano_max) / 2


def generar_datos_matriz_categorias(df, mes_filtro=None, canales_clasificacion=None, canales_filtro=None, categorias_filtro=None):
    """
    Genera datos para la matriz de categorías (% Ingreso Real vs % ROI)
    Agrupado por Canal + Categoría

    Args:
        df: DataFrame con datos de ventas
        mes_filtro: Mes a filtrar (1-12), None para todos
        canales_clasificacion: Lista de canales oficiales
        canales_filtro: Lista de canales a filtrar (list), None para todos
        categorias_filtro: Lista de categorías a filtrar (list), None para todas

    Returns:
        dict: {
            'datasets': Lista de datasets para Chart.js,
            'categorias': Lista de información de categorías,
            'estadisticas': Estadísticas generales
        }
    """
    # Diccionario de abreviaciones de canales
    abreviaciones_canales = {
        'Mercado Libre': 'ML',
        'Walmart': 'WM',
        'Liverpool': 'LV',
        'Shein': 'SH',
        'CrediTienda': 'CT',
        'Yuhu': 'YH',
        'Aliexpress': 'AE',
        'Coppel': 'CP',
        'TikTok Shop': 'TT',
        'Temu': 'TM'
    }

    tiempo_inicio = time.time()
    print("🔍 [MATRIZ CAT] Generando datos para matriz de categorías...")
    print(f"📋 [MATRIZ CAT] Filtros aplicados - Canales: {canales_filtro}, Categorías: {categorias_filtro}")
    print(f"📋 [MATRIZ CAT] Total filas recibidas: {len(df)}")

    if df.empty:
        return {
            'datasets': [],
            'categorias': [],
            'estadisticas': {
                'total_combinaciones': 0,
                'ventas_totales': 0,
                'ingreso_promedio': 0,
                'roi_promedio': 0,
                'critico': 0,
                'eficiente': 0,
                'a_desarrollar': 0,
                'ideal': 0
            }
        }

    # Filtrar por mes si se especifica
    if mes_filtro:
        df = filtrar_por_mes(df, mes_filtro)

    # Verificar que exista la columna 'Categoria_Catalogo' y crear alias 'Categoria'
    if 'Categoria_Catalogo' in df.columns:
        df['Categoria'] = df['Categoria_Catalogo']
        print(f"✅ [MATRIZ CAT] Columna 'Categoria_Catalogo' encontrada y mapeada a 'Categoria'")
    elif 'Categoria' not in df.columns:
        print("⚠️ [MATRIZ CAT] No existe columna 'Categoria' ni 'Categoria_Catalogo', usando categoría genérica")
        df['Categoria'] = 'Sin Categoría'

    # Filtrar por canales oficiales y estado
    df_filtrado = df[
        (df['estado'] != 'Cancelado') &
        (df['Channel'].isin(canales_clasificacion))
    ].copy()

    if df_filtrado.empty:
        return {'datasets': [], 'categorias': [], 'estadisticas': {}}

    # Reemplazar valores vacíos o nulos en Categoria
    df_filtrado['Categoria'] = df_filtrado['Categoria'].fillna('Sin Categoría')
    df_filtrado.loc[df_filtrado['Categoria'].str.strip() == '', 'Categoria'] = 'Sin Categoría'

    # IMPORTANTE: Como los datos son acumulados, solo tomar el último registro de cada SKU
    df_filtrado = df_filtrado.sort_values('Fecha', ascending=False)
    df_ultimo_registro = df_filtrado.groupby(['sku', 'Channel']).first().reset_index()

    print(f"📊 [MATRIZ CAT] Registros después de tomar último por SKU: {len(df_ultimo_registro)}")

    # Aplicar filtros adicionales de Canales y/o Categorías (múltiples)
    if canales_filtro and len(canales_filtro) > 0:
        df_ultimo_registro = df_ultimo_registro[df_ultimo_registro['Channel'].isin(canales_filtro)].copy()
        print(f"🔍 [MATRIZ CAT] Filtrado por canales {canales_filtro}: {len(df_ultimo_registro)} registros")

    if categorias_filtro and len(categorias_filtro) > 0:
        df_ultimo_registro = df_ultimo_registro[df_ultimo_registro['Categoria'].isin(categorias_filtro)].copy()
        print(f"🔍 [MATRIZ CAT] Filtrado por categorías {categorias_filtro}: {len(df_ultimo_registro)} registros")

    if df_ultimo_registro.empty:
        print(f"⚠️ [MATRIZ CAT] No hay datos después de aplicar filtros")
        return {'datasets': [], 'categorias': [], 'estadisticas': {
            'total_combinaciones': 0,
            'ventas_totales': 0,
            'ingreso_promedio': 0,
            'roi_promedio': 0
        }}

    # Colores por canal (mismos que matriz de posicionamiento)
    colores_canales = {
        'Mercado Libre': '#FFE135',
        'Walmart': '#0071CE',
        'Liverpool': '#E4002B',
        'Shein': '#FF6B35',
        'CrediTienda': '#00A650',
        'Yuhu': '#9B59B6',
        'Aliexpress': '#E62129',
        'Coppel': '#003DA5',
        'TikTok Shop': '#25F4EE',
        'Temu': '#FF6C00'
    }

    # Agrupar por Canal + Categoría
    categorias_info = []
    ventas_list = []

    grupos = df_ultimo_registro.groupby(['Channel', 'Categoria'])

    for (canal, categoria), df_grupo in grupos:
        metricas = calcular_metricas_categoria(df_grupo)

        zona, color_fondo, color_texto, icono = clasificar_zona(
            metricas['ingreso_real_pct'],
            metricas['roi_pct']
        )

        ventas_list.append(metricas['ventas_reales'])
        canal_abrev = abreviaciones_canales.get(canal, canal)
        label = f"{canal_abrev} - {categoria}"

        categorias_info.append({
            'canal': canal,
            'canal_abrev': canal_abrev,
            'categoria': categoria,
            'label': label,
            'ingreso_real_pct': round(metricas['ingreso_real_pct'], 2),
            'roi_pct': round(metricas['roi_pct'], 2),
            'ventas': metricas['ventas_reales'],
            'ingreso_real': metricas['ingreso_real'],
            'costo_venta': metricas['costo_venta'],  # AGREGADO para cálculo correcto
            'zona': zona,
            'color_zona': color_fondo,
            'color_texto': color_texto,
            'icono': icono,
            'num_transacciones': metricas['num_transacciones'],
            'color_canal': colores_canales.get(canal, '#6c757d')
        })

    # Escalar radios
    min_ventas = min(ventas_list) if ventas_list else 0
    max_ventas = max(ventas_list) if ventas_list else 0

    datasets_temp = []

    for cat_info in categorias_info:
        tamano = escalar_tamano_marcador(
            cat_info['ventas'],
            min_ventas,
            max_ventas
        )

        datasets_temp.append({
            'label': cat_info['label'],
            'data': [{
                'x': cat_info['ingreso_real_pct'],
                'y': cat_info['roi_pct']
            }],
            'backgroundColor': cat_info['color_canal'],
            'borderColor': cat_info['color_canal'],
            'borderWidth': 2,
            'pointRadius': tamano,
            'pointHoverRadius': tamano + 3,
            'pointStyle': 'crossRot',
            '_tamano': tamano
        })

    datasets = sorted(datasets_temp, key=lambda d: d['_tamano'], reverse=False)

    for ds in datasets:
        del ds['_tamano']

    # Estadísticas generales
    total_ventas = sum(c['ventas'] for c in categorias_info)
    total_ingreso_real = sum(c['ingreso_real'] for c in categorias_info)
    total_costo_venta = sum(c['costo_venta'] for c in categorias_info)
    # CORREGIDO: Calcular porcentajes sobre totales, NO promedio de porcentajes
    ingreso_promedio = (total_ingreso_real / total_ventas * 100) if total_ventas > 0 else 0
    roi_promedio = (total_ingreso_real / total_costo_venta * 100) if total_costo_venta > 0 else 0

    roi_max = max([c['roi_pct'] for c in categorias_info]) if categorias_info else 100
    import math
    eje_y_max = max(100, math.ceil(roi_max * 1.1 / 10) * 10)

    estadisticas = {
        'total_combinaciones': len(categorias_info),
        'ventas_totales': total_ventas,
        'ingreso_real_total': total_ingreso_real,
        'ingreso_promedio': round(ingreso_promedio, 2),
        'roi_promedio': round(roi_promedio, 2),
        'critico': sum(1 for c in categorias_info if c['zona'] == 'Crítico'),
        'eficiente': sum(1 for c in categorias_info if c['zona'] == 'Eficiente'),
        'a_desarrollar': sum(1 for c in categorias_info if c['zona'] == 'A Desarrollar'),
        'ideal': sum(1 for c in categorias_info if c['zona'] == 'Ideal'),
        'eje_y_max': eje_y_max
    }

    # Calcular el $ IR total por canal para ordenar
    ingreso_real_por_canal = {}
    for cat_info in categorias_info:
        canal = cat_info['canal']
        if canal not in ingreso_real_por_canal:
            ingreso_real_por_canal[canal] = 0
        ingreso_real_por_canal[canal] += cat_info['ingreso_real']

    # Ordenar categorías
    categorias_ordenadas = sorted(
        categorias_info,
        key=lambda x: (
            -ingreso_real_por_canal[x['canal']],
            -x['ingreso_real']
        )
    )

    tiempo_fin = time.time()
    print(f"✅ [MATRIZ CAT] Datos generados: {len(datasets)} combinaciones en {tiempo_fin - tiempo_inicio:.3f}s")

    return {
        'datasets': datasets,
        'categorias': categorias_ordenadas,
        'estadisticas': estadisticas
    }
