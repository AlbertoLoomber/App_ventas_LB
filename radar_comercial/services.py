# -*- coding: utf-8 -*-
"""
Servicios para el módulo de Radar Comercial
Lógica de negocio para análisis de competencia y comparación de precios
"""

import pandas as pd
from database import get_radar_comercial_data, get_analisis_competencia_ml


def get_specific_skus_with_descriptions(df):
    """
    Obtiene los SKUs específicos con sus descripciones

    Args:
        df: DataFrame con columnas 'sku' y 'descripcion'

    Returns:
        list: Lista de diccionarios con SKU y descripción
    """
    # SKUs que queremos mostrar en el filtro
    target_skus = ['2000005', '9900157', '2000013', '9900021', '9900027',
                   '2000040', '2000020', '2000002', '2000026', '2000032', '9900023']

    try:
        # Filtrar el dataframe para obtener solo estos SKUs
        df_skus = df[df['sku'].isin(target_skus)].copy()

        if df_skus.empty:
            print("DEBUG: No se encontraron los SKUs especificados en los datos")
            return []

        # Obtener SKU y descripción únicos
        skus_with_desc = df_skus.groupby('sku')['descripcion'].first().reset_index()

        # Crear lista de diccionarios con SKU y descripción
        skus_disponibles = []
        for _, row in skus_with_desc.iterrows():
            skus_disponibles.append({
                'sku': row['sku'],
                'descripcion': row['descripcion'][:60] + '...' if len(row['descripcion']) > 60 else row['descripcion']
            })

        # Ordenar por SKU
        skus_disponibles.sort(key=lambda x: x['sku'])

        print(f"=== SKUs ESPECÍFICOS ENCONTRADOS ===")
        for sku_info in skus_disponibles:
            print(f"SKU: {sku_info['sku']} - {sku_info['descripcion']}")

        return skus_disponibles

    except Exception as e:
        print(f"Error obteniendo SKUs específicos: {e}")
        return []


def calcular_indicadores(df_main, df_compare):
    """
    Calcula los indicadores clave comparando dos períodos

    Args:
        df_main: DataFrame del período principal
        df_compare: DataFrame del período de comparación

    Returns:
        list: Lista de diccionarios con los indicadores formateados
    """
    # Calcular valores del período principal
    v_main = df_main["Total"].sum()
    c_main = df_main[df_main["estado"] == "Cancelado"]["Total"].sum()
    n_main = df_main[df_main["estado"] != "Cancelado"]["Total"].sum()
    p_main = (c_main / v_main * 100) if v_main else 0
    t_main = (v_main / len(df_main)) if len(df_main) else 0
    n_tx_main = len(df_main)

    # Calcular valores del período de comparación
    v_comp = df_compare["Total"].sum()
    c_comp = df_compare[df_compare["estado"] == "Cancelado"]["Total"].sum()
    n_comp = df_compare[df_compare["estado"] != "Cancelado"]["Total"].sum()
    p_comp = (c_comp / v_comp * 100) if v_comp else 0
    t_comp = (v_comp / len(df_compare)) if len(df_compare) else 0
    n_tx_comp = len(df_compare)

    def formato(label, val_main, val_comp, tipo="$"):
        """Formatea un indicador normal (más es mejor)"""
        delta = val_main - val_comp
        pct = (delta / val_comp * 100) if val_comp else 0
        simbolo = "▲" if pct >= 0 else "▼"

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
            "delta": pct
        }

    def formato_inverso(label, val_main, val_compare, tipo="$"):
        """Formatea un indicador inverso (menos es mejor, como cancelaciones)"""
        delta = val_main - val_compare
        pct = (delta / val_compare * 100) if val_compare else 0
        simbolo = "▲" if pct >= 0 else "▼"
        color_delta = -pct  # Invertir el signo para colores

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
            "delta": color_delta  # Usar el delta invertido
        }

    indicadores = [
        formato("Ventas brutas", v_main, v_comp, "$"),
        formato_inverso("Cancelaciones", c_main, c_comp, "$"),
        formato("Ingreso neto", n_main, n_comp, "$"),
        formato_inverso("% Cancelaciones", p_main, p_comp, "%"),
        formato("Ticket promedio", t_main, t_comp, "$"),
        formato("Transacciones", n_tx_main, n_tx_comp, "n")
    ]

    return indicadores


def parsear_fechas_request(preset_main, preset_compare, main_range=None, compare_range=None):
    """
    Parsea las fechas desde los parámetros del request

    Args:
        preset_main: Preset del período principal ("hoy", "7", "15", "30", "personalizado")
        preset_compare: Preset del período de comparación ("anterior", "anual", "personalizado")
        main_range: Rango personalizado para el período principal (opcional)
        compare_range: Rango personalizado para el período de comparación (opcional)

    Returns:
        tuple: (f1, f2, fc1, fc2) - Fechas de inicio y fin de ambos períodos
    """
    hoy = datetime.now(MAZATLAN_TZ).replace(hour=0, minute=0, second=0, microsecond=0)

    # Determinar fechas del período principal
    if preset_main == "hoy":
        f1 = hoy
        f2 = hoy + timedelta(days=1)
    elif preset_main in ["7", "15", "30"]:
        dias = int(preset_main)
        f1 = hoy - timedelta(days=dias)
        f2 = hoy + timedelta(days=1)
    elif preset_main == "personalizado":
        if main_range:
            if " to " in main_range:
                # Rango de fechas (dos fechas)
                f1_str, f2_str = main_range.split(" to ")
                f1 = MAZATLAN_TZ.localize(datetime.strptime(f1_str.strip(), "%Y-%m-%d"))
                f2_temp = MAZATLAN_TZ.localize(datetime.strptime(f2_str.strip(), "%Y-%m-%d"))
                f2 = f2_temp + timedelta(days=1)
            else:
                # Un solo día seleccionado
                f1 = MAZATLAN_TZ.localize(datetime.strptime(main_range.strip(), "%Y-%m-%d"))
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
        if compare_range:
            if " to " in compare_range:
                # Rango de fechas (dos fechas)
                fc1_str, fc2_str = compare_range.split(" to ")
                fc1 = MAZATLAN_TZ.localize(datetime.strptime(fc1_str.strip(), "%Y-%m-%d"))
                fc2_temp = MAZATLAN_TZ.localize(datetime.strptime(fc2_str.strip(), "%Y-%m-%d"))
                fc2 = fc2_temp + timedelta(days=1)
            else:
                # Un solo día seleccionado
                fc1 = MAZATLAN_TZ.localize(datetime.strptime(compare_range.strip(), "%Y-%m-%d"))
                fc2 = fc1 + timedelta(days=1)
        else:
            fc2 = f1
            fc1 = fc2 - delta
    else:
        fc2 = f1
        fc1 = fc2 - delta

    return f1, f2, fc1, fc2


def aplicar_filtros(df_main, df_compare, channels=None, warehouses=None, skus=None):
    """
    Aplica filtros de canales, almacenes y SKUs a los DataFrames

    Args:
        df_main: DataFrame del período principal
        df_compare: DataFrame del período de comparación
        channels: Lista de canales a filtrar (opcional)
        warehouses: Lista de almacenes a filtrar (opcional)
        skus: Lista de SKUs a filtrar (opcional)

    Returns:
        tuple: (df_main_filtrado, df_compare_filtrado, selected_channels)
    """
    # Si no hay canales seleccionados, usar Ecommerce por defecto
    if not channels:
        canales_ecommerce = ['Mercado Libre', 'Doto', 'Yuhu', 'Aliexpress',
                             'Coppel', 'Liverpool', 'Shein', 'CrediTienda', 'Walmart', 'TikTok Shop']
        df_main = df_main[df_main["Channel"].isin(canales_ecommerce)]
        df_compare = df_compare[df_compare["Channel"].isin(canales_ecommerce)]
        selected_channels = canales_ecommerce
    else:
        df_main = df_main[df_main["Channel"].isin(channels)]
        df_compare = df_compare[df_compare["Channel"].isin(channels)]
        selected_channels = channels

    # Aplicar filtros de warehouse y SKU si existen
    if warehouses:
        df_main = df_main[df_main["Warehouse"].isin(warehouses)]
        df_compare = df_compare[df_compare["Warehouse"].isin(warehouses)]

    if skus:
        df_main = df_main[df_main["sku"].isin(skus)]
        df_compare = df_compare[df_compare["sku"].isin(skus)]

    return df_main, df_compare, selected_channels


def procesar_datos_radar():
    """
    Obtiene y procesa los datos del radar comercial para la visualización

    Returns:
        tuple: (productos_procesados, estadisticas_generales)
    """
    # Obtener datos de ClickHouse
    df = get_radar_comercial_data()

    if df.empty:
        return [], {}

    productos = []

    for _, row in df.iterrows():
        # Extraer datos básicos
        producto = {
            'sku': row['sku'],
            'descripcion': row['descripcion']
        }

        # Procesar cada canal
        canales = ['ML', 'CT', 'WM', 'SH', 'TK', 'LP', 'YH']
        nombres_canales = {
            'ML': 'Mercado Libre',
            'CT': 'CrediTienda',
            'WM': 'Walmart',
            'SH': 'Shein',
            'TK': 'TikTok Shop',
            'LP': 'Liverpool',
            'YH': 'Yuhu'
        }

        precios = []
        irs = []

        for canal in canales:
            precio = row.get(f'precio_{canal}')
            ir_str = row.get(f'%IR_{canal}')
            dias_precio = row.get(f'dias_precio_{canal}')
            inv_asignado = row.get(f'inv_asignado_{canal}')

            # Procesar precio
            if pd.notna(precio):
                producto[f'precio_{canal}'] = float(precio)
                precios.append(float(precio))
            else:
                producto[f'precio_{canal}'] = None

            # Procesar IR (viene como string con %)
            if pd.notna(ir_str) and ir_str:
                try:
                    ir_value = float(str(ir_str).replace('%', '').strip())
                    producto[f'ir_{canal}'] = ir_value
                    producto[f'ir_{canal}_str'] = f"{ir_value:.1f}%"
                    irs.append(ir_value)
                except:
                    producto[f'ir_{canal}'] = None
                    producto[f'ir_{canal}_str'] = 'N/A'
            else:
                producto[f'ir_{canal}'] = None
                producto[f'ir_{canal}_str'] = 'N/A'

            # Procesar días de precio activo
            if pd.notna(dias_precio):
                producto[f'dias_precio_{canal}'] = int(dias_precio)
            else:
                producto[f'dias_precio_{canal}'] = None

            # Procesar inventario asignado
            if pd.notna(inv_asignado):
                producto[f'inv_asignado_{canal}'] = float(inv_asignado)
            else:
                producto[f'inv_asignado_{canal}'] = 0

            # Nombre del canal para display
            producto[f'nombre_{canal}'] = nombres_canales[canal]

        # Procesar conversión de Mercado Libre (solo para ML)
        conv_ml_str = row.get('%Conv_ML')
        if pd.notna(conv_ml_str) and conv_ml_str:
            try:
                producto['conv_ML_str'] = conv_ml_str
                # Extraer valor numérico para análisis
                conv_value = float(str(conv_ml_str).replace('%', '').strip())
                producto['conv_ML'] = conv_value
            except:
                producto['conv_ML_str'] = 'N/A'
                producto['conv_ML'] = None
        else:
            producto['conv_ML_str'] = 'N/A'
            producto['conv_ML'] = None

        # Calcular estadísticas del producto
        if precios:
            producto['precio_min'] = min(precios)
            producto['precio_max'] = max(precios)
            producto['precio_promedio'] = sum(precios) / len(precios)
            producto['diferencia_precio'] = producto['precio_max'] - producto['precio_min']
            producto['diferencia_precio_pct'] = (producto['diferencia_precio'] / producto['precio_min'] * 100) if producto['precio_min'] > 0 else 0
        else:
            producto['precio_min'] = None
            producto['precio_max'] = None
            producto['precio_promedio'] = None
            producto['diferencia_precio'] = None
            producto['diferencia_precio_pct'] = None

        if irs:
            producto['ir_promedio'] = sum(irs) / len(irs)
            producto['ir_min'] = min(irs)
            producto['ir_max'] = max(irs)
        else:
            producto['ir_promedio'] = None
            producto['ir_min'] = None
            producto['ir_max'] = None

        # Contar presencia en canales
        producto['canales_activos'] = sum(1 for p in [producto[f'precio_{c}'] for c in canales] if p is not None)

        productos.append(producto)

    # Calcular estadísticas generales
    stats = obtener_estadisticas_generales(productos)

    return productos, stats


def filtrar_productos(productos, filtro_texto=None):
    """
    Filtra la lista de productos según texto de búsqueda

    Args:
        productos: Lista de productos procesados
        filtro_texto: Texto para buscar en SKU o descripción

    Returns:
        list: Lista filtrada de productos
    """
    if not productos:
        return []

    if not filtro_texto:
        return productos

    filtro_texto = filtro_texto.lower()
    return [
        p for p in productos
        if filtro_texto in str(p['sku']).lower() or
           filtro_texto in str(p['descripcion']).lower()
    ]


def obtener_estadisticas_generales(productos):
    """
    Calcula estadísticas generales del radar comercial

    Args:
        productos: Lista de productos procesados

    Returns:
        dict: Diccionario con estadísticas generales
    """
    if not productos:
        return {
            'total_productos': 0,
            'productos_en_ml': 0,
            'productos_en_ct': 0,
            'productos_en_wm': 0,
            'productos_en_sh': 0,
            'productos_multicanal': 0,
            'ir_promedio_general': 0
        }

    stats = {
        'total_productos': len(productos),
        'productos_en_ml': sum(1 for p in productos if p.get('precio_ML') is not None),
        'productos_en_ct': sum(1 for p in productos if p.get('precio_CT') is not None),
        'productos_en_wm': sum(1 for p in productos if p.get('precio_WM') is not None),
        'productos_en_sh': sum(1 for p in productos if p.get('precio_SH') is not None),
        'productos_multicanal': sum(1 for p in productos if p.get('canales_activos', 0) > 1)
    }

    # IR promedio general
    irs_validos = [p['ir_promedio'] for p in productos if p.get('ir_promedio') is not None]
    stats['ir_promedio_general'] = sum(irs_validos) / len(irs_validos) if irs_validos else 0

    return stats


def clasificar_ir(ir_value):
    """
    Clasifica el IR en categorías para colorear

    Args:
        ir_value: Valor de IR (0-100)

    Returns:
        str: Clase CSS para colorear (excelente, bueno, regular, bajo)
    """
    if ir_value is None:
        return 'sin-datos'

    if ir_value >= 30:
        return 'excelente'
    elif ir_value >= 20:
        return 'bueno'
    elif ir_value >= 10:
        return 'regular'
    else:
        return 'bajo'


def procesar_analisis_competencia():
    """
    Obtiene y procesa el análisis de competencia de Mercado Libre
    Formato: tabla horizontal con Loomber + top 3 competidores por score

    Posiciones basadas en score_competitividad_total (mayor score = 1er lugar)

    Returns:
        list: Lista de diccionarios con SKU, posición Loomber, precio Loomber,
              y top 3 competidores con sus posiciones
    """
    df = get_analisis_competencia_ml()

    if df.empty:
        return []

    # Obtener datos de radar para precios de Loomber
    df_radar = get_radar_comercial_data()

    # Lista para almacenar filas de la tabla
    tabla_competencia = []

    for sku in df['sku'].unique():
        df_sku = df[df['sku'] == sku].copy()

        if df_sku.empty:
            continue

        # Información del producto
        primera_fila = df_sku.iloc[0]
        producto = primera_fila['producto']

        # Crear lista con todos los proveedores (incluyendo Loomber si aparece)
        # IMPORTANTE: Cuando Loomber aparece en la tabla, ESE es nuestro dato con score
        todos_proveedores = []
        loomber_encontrado_en_tabla = False

        # Procesar todos los proveedores de la tabla de competencia
        for _, row in df_sku.iterrows():
            if pd.notna(row['precio']) and row['precio'] > 0:
                score = row['score_competitividad_total']
                if pd.notna(score):
                    score = float(score)
                else:
                    score = 0

                es_loomber = row['nombre_proveedor'].strip().lower() == 'loomber'

                if es_loomber:
                    loomber_encontrado_en_tabla = True

                todos_proveedores.append({
                    'nombre': row['nombre_proveedor'],
                    'precio': float(row['precio']),
                    'url': row['url'] if pd.notna(row['url']) else None,
                    'es_loomber': es_loomber,
                    'score_total': score
                })

        # Si Loomber NO aparece en la tabla, usar el precio del radar con score 0
        if not loomber_encontrado_en_tabla:
            precio_loomber_radar = None
            if not df_radar.empty:
                df_loomber = df_radar[df_radar['sku'] == sku]
                if not df_loomber.empty:
                    precio_ml = df_loomber.iloc[0].get('precio_ML')
                    if pd.notna(precio_ml):
                        precio_loomber_radar = float(precio_ml)

            # Si encontramos precio en el radar, agregarlo
            if precio_loomber_radar is not None:
                todos_proveedores.append({
                    'nombre': 'Loomber',
                    'precio': precio_loomber_radar,
                    'url': None,
                    'es_loomber': True,
                    'score_total': 0
                })

        # Si no hay datos de Loomber (ni en tabla ni en radar), saltar este SKU
        if not any(p['es_loomber'] for p in todos_proveedores):
            continue

        # Ordenar todos por score_total (mayor a menor) para determinar posiciones
        # Mayor score = 1er lugar = más competitivo
        todos_proveedores.sort(key=lambda x: x['score_total'], reverse=True)

        # Asignar posiciones (1er lugar = mayor score)
        for idx, proveedor in enumerate(todos_proveedores, start=1):
            proveedor['posicion'] = idx

        # Encontrar posición de Loomber
        loomber_data = next((p for p in todos_proveedores if p['es_loomber']), None)

        if not loomber_data:
            continue

        # Obtener top 3 competidores por score (excluyendo Loomber)
        competidores = [p for p in todos_proveedores if not p['es_loomber']]
        top_3_competidores = competidores[:3]  # Los 3 con mayor score

        # Construir fila de la tabla
        fila = {
            'sku': sku,
            'producto': producto,
            'loomber_precio': loomber_data['precio'],
            'loomber_posicion': loomber_data['posicion'],
            'loomber_url': loomber_data.get('url'),
            'competidores': []
        }

        # Agregar competidores (máximo 3)
        for comp in top_3_competidores:
            fila['competidores'].append({
                'nombre': comp['nombre'],
                'precio': comp['precio'],
                'posicion': comp['posicion'],
                'url': comp['url'],
                'score_total': comp['score_total']
            })

        tabla_competencia.append(fila)

    return tabla_competencia


def procesar_datos_semanales(df_semanal):
    """
    Procesa datos semanales de inventario y ventas para formato JSON

    Args:
        df_semanal: DataFrame con columnas: sku, canal, inv_asignado_semana, ventas_semana

    Returns:
        dict: Diccionario con estructura {sku: {canal: {inv, ventas}}}
    """
    if df_semanal.empty:
        print("WARN: [PROCESAR SEMANAL] DataFrame vacío")
        return {}

    print(f"INFO: [PROCESAR SEMANAL] Procesando {len(df_semanal)} registros")
    print(f"DEBUG: [PROCESAR SEMANAL] SKUs únicos: {df_semanal['sku'].nunique()}")
    print(f"DEBUG: [PROCESAR SEMANAL] Canales únicos: {df_semanal['canal'].unique()}")

    # Mapeo de nombres de canales a códigos
    mapeo_canales = {
        'Mercado Libre': 'ML',
        'CrediTienda': 'CT',
        'Walmart': 'WM',
        'Shein': 'SH',
        'TikTok Shop': 'TK',
        'Liverpool': 'LP',
        'Yuhu': 'YH'
    }

    datos_procesados = {}

    for idx, row in df_semanal.iterrows():
        sku = row['sku']
        canal_nombre = row['canal']
        inv_asignado = float(row['inv_asignado_semana']) if pd.notna(row['inv_asignado_semana']) else 0
        ventas = float(row['ventas_semana']) if pd.notna(row['ventas_semana']) else 0

        # Obtener código del canal
        canal_codigo = mapeo_canales.get(canal_nombre, canal_nombre)

        # Inicializar diccionario para el SKU si no existe
        if sku not in datos_procesados:
            datos_procesados[sku] = {}

        # Agregar datos del canal
        datos_procesados[sku][canal_codigo] = {
            'inv': inv_asignado,
            'ventas': ventas
        }

    print(f"INFO: [PROCESAR SEMANAL] Datos procesados para {len(datos_procesados)} SKUs")

    return datos_procesados
