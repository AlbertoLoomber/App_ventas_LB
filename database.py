# -*- coding: utf-8 -*-
"""
Módulo de acceso a base de datos
Funciones compartidas para conexión y carga de datos desde ClickHouse
"""

import pandas as pd
from datetime import datetime, date
import calendar
import clickhouse_connect
from config import CLICKHOUSE_CONFIG, MAZATLAN_TZ, CANALES_CLASIFICACION


def get_db_connection():
    """
    Establece conexión con ClickHouse

    Returns:
        clickhouse_connect.Client: Cliente de ClickHouse o None si falla
    """
    try:
        client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
        return client
    except Exception as e:
        print(f"Error conectando a la base de datos: {e}")
        return None


def load_data_improved(mes_filtro=None, incluir_comparacion=False, año_especifico=None):
    """
    Solución híbrida optimizada con validación y queries inteligentes

    Args:
        mes_filtro: Mes específico a cargar (1-12)
        incluir_comparacion: Si True, carga también el mes anterior
        año_especifico: Año específico (default: año actual)

    Returns:
        tuple: (DataFrame, channels_disponibles, warehouses_disponibles)
    """
    client = get_db_connection()
    if not client:
        return pd.DataFrame(), [], []

    try:
        # Primero probar conexión básica
        print("INFO: Probando conexion a ClickHouse...")
        test_query = "SELECT 1 as test"
        test_result = client.query(test_query)
        print(f"OK: Conexion exitosa: {test_result.result_rows}")

        # Determinar año dinámicamente basado en el mes
        if año_especifico:
            año_actual = año_especifico
        elif mes_filtro:
            # Si el mes seleccionado es mayor al mes actual, es del año anterior
            mes_actual_sistema = datetime.now().month
            año_actual_sistema = datetime.now().year
            if mes_filtro > mes_actual_sistema:
                año_actual = año_actual_sistema - 1
                print(f"INFO: Mes {mes_filtro} > mes actual {mes_actual_sistema}, usando año {año_actual}")
            else:
                año_actual = año_actual_sistema
        else:
            año_actual = datetime.now().year

        if incluir_comparacion and mes_filtro:
            # Cargar mes actual + anterior para comparaciones
            mes_anterior = mes_filtro - 1 if mes_filtro > 1 else 12
            año_anterior = año_actual if mes_filtro > 1 else año_actual - 1

            # Primero validar disponibilidad de datos
            count_query = f"""
            SELECT toMonth(Fecha) as mes, toYear(Fecha) as anio, count() as registros
            FROM Silver.RPT_Ventas_Con_Costo_Prueba
            WHERE (toYear(Fecha) = {año_actual} AND toMonth(Fecha) = {mes_filtro})
               OR (toYear(Fecha) = {año_anterior} AND toMonth(Fecha) = {mes_anterior})
            GROUP BY mes, anio
            ORDER BY anio, mes
            """

            disponibilidad_result = client.query(count_query)
            disponibilidad = disponibilidad_result.result_rows
            print(f"=== VALIDACIÓN DE DISPONIBILIDAD ===")
            for row in disponibilidad:
                mes, anio, registros = row
                print(f"  {anio}-{mes:02d}: {registros:,} registros disponibles")

            # Query optimizada sin límites artificiales
            query = f"""
            SELECT * FROM Silver.RPT_Ventas_Con_Costo_Prueba
            WHERE (toYear(Fecha) = {año_actual} AND toMonth(Fecha) = {mes_filtro})
               OR (toYear(Fecha) = {año_anterior} AND toMonth(Fecha) = {mes_anterior})
            ORDER BY Fecha DESC
            """
            print(f"OPTIMIZACIÓN: Consulta de comparación {mes_filtro}/{año_actual} vs {mes_anterior}/{año_anterior} - SIN LÍMITES")

        elif mes_filtro:
            # Solo mes específico - validar primero
            count_query = f"""
            SELECT count() as total
            FROM Silver.RPT_Ventas_Con_Costo_Prueba
            WHERE toYear(Fecha) = {año_actual}
            AND toMonth(Fecha) = {mes_filtro}
            """

            total_result = client.query(count_query)
            total_records = total_result.result_rows[0][0]
            print(f"=== VALIDACIÓN DE VOLUMEN ===")
            print(f"Total registros disponibles para {mes_filtro}/{año_actual}: {total_records:,}")

            # Query específica sin límites
            query = f"""
            SELECT * FROM Silver.RPT_Ventas_Con_Costo_Prueba
            WHERE toYear(Fecha) = {año_actual}
            AND toMonth(Fecha) = {mes_filtro}
            ORDER BY Fecha DESC
            """
            print(f"OPTIMIZACIÓN: Consulta específica {mes_filtro}/{año_actual} - SIN LÍMITES")
        else:
            # Sin filtro específico - cargar año completo
            count_query = f"""
            SELECT count() as total
            FROM Silver.RPT_Ventas_Con_Costo_Prueba
            WHERE toYear(Fecha) = {año_actual}
            """

            total_result = client.query(count_query)
            total_records = total_result.result_rows[0][0]
            print(f"=== VALIDACIÓN DE VOLUMEN ANUAL ===")
            print(f"Total registros disponibles para {año_actual}: {total_records:,}")

            if total_records > 500000:  # Solo advertir si son muchos registros
                print(f"⚠️  ADVERTENCIA: {total_records:,} registros - considere usar filtro de mes")

            query = f"""
            SELECT * FROM Silver.RPT_Ventas_Con_Costo_Prueba
            WHERE toYear(Fecha) = {año_actual}
            ORDER BY Fecha DESC
            """
            print(f"OPTIMIZACIÓN: Consulta anual {año_actual} - SIN LÍMITES")

        print(f"Query ejecutada: {query}")
        result = client.query(query)
        df = pd.DataFrame(result.result_rows, columns=result.column_names)
        print(f"DATOS: Cargados {len(df)} registros exitosamente")

        # Convertir columna de fecha
        df["Fecha"] = pd.to_datetime(df["Fecha"])

        # DEBUGGING: Información detallada sobre los datos cargados
        if not df.empty:
            print(f"=== ANALISIS DE DATOS CARGADOS ===")
            print(f"Rango de fechas: {df['Fecha'].min()} a {df['Fecha'].max()}")
            print(f"Meses únicos: {sorted(df['Fecha'].dt.month.unique())}")
            print(f"Años únicos: {sorted(df['Fecha'].dt.year.unique())}")
            if 'Channel' in df.columns:
                print(f"Canales únicos ({df['Channel'].nunique()}): {df['Channel'].unique().tolist()}")
            print(f"Registros por mes:")
            for mes in sorted(df['Fecha'].dt.month.unique()):
                count = len(df[df['Fecha'].dt.month == mes])
                print(f"  Mes {mes}: {count:,} registros")

        # Limpiar y convertir columna cantidad
        print(f"=== LIMPIANDO COLUMNA CANTIDAD ===")
        print(f"Tipo original: {df['cantidad'].dtype}")
        print(f"Valores únicos (primeros 10): {df['cantidad'].unique()[:10]}")

        # Convertir cantidad a numérico
        df['cantidad'] = pd.to_numeric(df['cantidad'], errors='coerce')
        df = df.dropna(subset=['cantidad'])  # Eliminar filas con cantidad inválida

        print(f"Después de conversión: {df['cantidad'].dtype}")
        print(f"Filas restantes: {len(df)}")
        print(f"Rango de cantidades: {df['cantidad'].min()} - {df['cantidad'].max()}")

        # Filtros únicos dinámicos (filtrar cadenas vacías)
        channels_disponibles = sorted([c for c in df["Channel"].dropna().unique().tolist() if c and c.strip()])
        warehouses_disponibles = sorted(df["Warehouse"].dropna().unique().tolist())

        return df, channels_disponibles, warehouses_disponibles

    except Exception as e:
        print(f"Error cargando datos: {e}")
        return pd.DataFrame(), [], []


def load_data(mes_filtro=None):
    """Función de compatibilidad - redirige a load_data_improved"""
    return load_data_improved(mes_filtro)


def get_fresh_data(mes_filtro=None):
    """Carga datos frescos desde ClickHouse en cada consulta"""
    return load_data_improved(mes_filtro)


def load_metas_diarias():
    """Carga las metas diarias desde ClickHouse Silver.metas_diarias_canal"""
    client = get_db_connection()
    if not client:
        print("ERROR: No se pudo conectar a ClickHouse")
        return create_mock_metas()

    try:
        # Primero verificar si la tabla existe
        check_query = "EXISTS TABLE Silver.metas_diarias_canal"
        exists_result = client.query(check_query)
        table_exists = exists_result.result_rows[0][0] if exists_result.result_rows else False

        if not table_exists:
            print("ADVERTENCIA: Tabla Silver.metas_diarias_canal no existe. Usando datos mock.")
            return create_mock_metas()

        # Verificar estructura de la tabla
        describe_query = "DESCRIBE Silver.metas_diarias_canal"
        describe_result = client.query(describe_query)
        columns_info = describe_result.result_rows
        column_names = [row[0] for row in columns_info]

        print(f"Columnas disponibles en Silver.metas_diarias_canal: {column_names}")

        # Verificar si existe la columna Canal o M.Canal
        canal_column = None
        if 'Canal' in column_names:
            canal_column = 'Canal'
        elif 'M.Canal' in column_names:
            canal_column = 'M.Canal'

        if not canal_column:
            print("ERROR: Columna de canal no encontrada en la tabla. Estructura actual:")
            for col_info in columns_info:
                print(f"  - {col_info[0]}: {col_info[1]}")
            print("Usando datos mock...")
            return create_mock_metas()

        print(f"INFO: Usando columna de canal: {canal_column}")

        # Usar alias para normalizar el nombre de la columna
        query = f"""
        SELECT
            `{canal_column}` AS Canal,
            Fecha,
            Meta_Diaria,
            Meta_Acumulada,
            Meta_Mensual,
            Meta_Ingreso_Real_Diaria,
            Meta_Ingreso_Real_Acumulada,
            Meta_Ingreso_Real_Mensual,
            Modelo_Usado,
            dias_del_mes,
            Porcentaje_Semanal,
            Dias_DOW_en_Mes
        FROM Silver.metas_diarias_canal
        ORDER BY `{canal_column}`, Fecha
        """
        print(f"INFO: Ejecutando query: {query}")
        result = client.query(query)
        df_metas = pd.DataFrame(result.result_rows, columns=result.column_names)

        print(f"INFO: Query ejecutado exitosamente. Filas obtenidas: {len(df_metas)}")
        if not df_metas.empty:
            print(f"INFO: Primeras fechas: {df_metas['Fecha'].head(3).tolist()}")
            print(f"INFO: Canales encontrados: {df_metas['Canal'].unique().tolist()}")
            print(f"INFO: Suma total Meta_Diaria: ${df_metas['Meta_Diaria'].sum():,.0f}")

        if df_metas.empty:
            print("ERROR: Tabla Silver.metas_diarias_canal está vacía pero existe.")
            print("INFO: Verificando contenido de la tabla...")
            count_result = client.query("SELECT COUNT(*) FROM Silver.metas_diarias_canal")
            row_count = count_result.result_rows[0][0]
            print(f"INFO: La tabla tiene {row_count} registros en ClickHouse")
            print("USANDO DATOS MOCK como último recurso...")
            return create_mock_metas()

        # Convertir tipos
        df_metas["Fecha"] = pd.to_datetime(df_metas["Fecha"])
        df_metas["Meta_Diaria"] = pd.to_numeric(df_metas["Meta_Diaria"], errors='coerce')
        df_metas["Meta_Acumulada"] = pd.to_numeric(df_metas["Meta_Acumulada"], errors='coerce')
        df_metas["Meta_Mensual"] = pd.to_numeric(df_metas["Meta_Mensual"], errors='coerce')

        # Verificar que no hay valores NaN después de la conversión
        nan_count = df_metas["Meta_Diaria"].isna().sum()
        if nan_count > 0:
            print(f"WARNING: {nan_count} valores NaN encontrados en Meta_Diaria después de conversión")
            df_metas = df_metas.dropna(subset=['Meta_Diaria'])
            print(f"INFO: Registros después de limpiar NaN: {len(df_metas)}")

        print(f"=== METAS CARGADAS DESDE CLICKHOUSE EXITOSAMENTE ===")
        print(f"Registros de metas: {len(df_metas)}")
        print(f"Canales con metas: {df_metas['Canal'].nunique()}")
        print(f"Canales: {df_metas['Canal'].unique().tolist()}")
        print(f"Rango de fechas metas: {df_metas['Fecha'].min()} a {df_metas['Fecha'].max()}")
        print(f"Suma total Meta_Diaria (después de conversión): ${df_metas['Meta_Diaria'].sum():,.0f}")

        return df_metas

    except Exception as e:
        print(f"ERROR CRÍTICO cargando metas desde ClickHouse: {e}")
        print(f"Tipo de error: {type(e).__name__}")
        print("FALLBACK: Usando datos mock como último recurso...")
        print("RECOMENDACIÓN: Revisar conexión a ClickHouse y estructura de datos")
        return create_mock_metas()


def create_mock_metas():
    """Crea datos mock de metas para testing"""
    print("=== CREANDO DATOS MOCK DE METAS ===")

    # Obtener el mes actual y anterior
    hoy = date.today()
    primer_dia_mes_actual = hoy.replace(day=1)

    # Mes anterior
    if hoy.month == 1:
        mes_anterior = 12
        año_anterior = hoy.year - 1
    else:
        mes_anterior = hoy.month - 1
        año_anterior = hoy.year

    primer_dia_mes_anterior = date(año_anterior, mes_anterior, 1)
    dias_mes_anterior = calendar.monthrange(año_anterior, mes_anterior)[1]
    ultimo_dia_mes_anterior = date(año_anterior, mes_anterior, dias_mes_anterior)

    # Canales mock
    canales = ["CrediTienda", "Yuhu", "Walmart"]

    # Metas diarias mock
    metas_data = []

    for canal in canales:
        # Mes anterior completo
        fecha_actual = primer_dia_mes_anterior
        while fecha_actual <= ultimo_dia_mes_anterior:
            metas_data.append({
                'Canal': canal,
                'Fecha': fecha_actual,
                'Meta_Diaria': 10000,
                'Meta_Acumulada': 10000 * fecha_actual.day,
                'Meta_Mensual': 10000 * dias_mes_anterior,
                'Meta_Ingreso_Real_Diaria': 2000,
                'Meta_Ingreso_Real_Acumulada': 2000 * fecha_actual.day,
                'Meta_Ingreso_Real_Mensual': 2000 * dias_mes_anterior,
                'Modelo_Usado': 'Mock',
                'dias_del_mes': dias_mes_anterior,
                'Porcentaje_Semanal': 0.15,
                'Dias_DOW_en_Mes': 4
            })
            fecha_actual = fecha_actual + pd.Timedelta(days=1)

        # Mes actual hasta hoy
        fecha_actual = primer_dia_mes_actual
        while fecha_actual <= hoy:
            dias_mes_actual = calendar.monthrange(hoy.year, hoy.month)[1]
            metas_data.append({
                'Canal': canal,
                'Fecha': fecha_actual,
                'Meta_Diaria': 10000,
                'Meta_Acumulada': 10000 * fecha_actual.day,
                'Meta_Mensual': 10000 * dias_mes_actual,
                'Meta_Ingreso_Real_Diaria': 2000,
                'Meta_Ingreso_Real_Acumulada': 2000 * fecha_actual.day,
                'Meta_Ingreso_Real_Mensual': 2000 * dias_mes_actual,
                'Modelo_Usado': 'Mock',
                'dias_del_mes': dias_mes_actual,
                'Porcentaje_Semanal': 0.15,
                'Dias_DOW_en_Mes': 4
            })
            fecha_actual = fecha_actual + pd.Timedelta(days=1)

    df_metas_mock = pd.DataFrame(metas_data)
    print(f"OK: Creados {len(df_metas_mock)} registros mock de metas")
    return df_metas_mock


def get_fresh_metas():
    """Carga metas frescas desde ClickHouse en cada consulta"""
    return load_metas_diarias()


def get_catalogo_productos_bf():
    """
    Carga el catálogo de productos BF desde ClickHouse

    Returns:
        DataFrame: Catálogo con columnas: sku, descripcion, categoria, producto_relevante, producto_nuevo, remate
    """
    client = get_db_connection()
    if not client:
        print("ERROR: No se pudo conectar a la base de datos para catálogo BF")
        return pd.DataFrame()

    try:
        print("INFO: Cargando catálogo de productos BF desde ClickHouse...")

        query = """
        SELECT
            sku,
            descripcion,
            categoria,
            producto_relevante,
            producto_nuevo,
            remate,
            fecha_carga
        FROM Silver.catalogo_productos_BF
        ORDER BY categoria, sku
        """

        result = client.query(query)
        df = pd.DataFrame(result.result_rows, columns=result.column_names)

        print(f"OK: Catalogo BF cargado: {len(df)} productos")

        if not df.empty:
            print(f"   - Categorias: {df['categoria'].nunique()}")
            print(f"   - Productos relevantes: {df['producto_relevante'].sum()}")
            print(f"   - Productos nuevos: {df['producto_nuevo'].sum()}")
            print(f"   - Productos en remate: {df['remate'].sum()}")

        return df

    except Exception as e:
        print(f"ERROR al cargar catálogo BF: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def get_inventario_bf():
    """
    Obtiene el inventario de productos BF por almacén y total

    Returns:
        DataFrame: Inventario con columnas: sku, descripcion, almacen, cantidad_libre_de_usar, Existencia
    """
    client = get_db_connection()
    if not client:
        print("ERROR: No se pudo conectar a la base de datos para inventario BF")
        return pd.DataFrame()

    try:
        print("INFO: Cargando inventario BF desde ClickHouse...")

        query = """
        SELECT
            sku,
            descripcion,
            almacen,
            cantidad_libre_de_usar,
            sum(cantidad_libre_de_usar) OVER (PARTITION BY sku, descripcion) AS Existencia
        FROM Gold.RPT_Inventarios
        WHERE sku <> ''
        ORDER BY sku
        """

        result = client.query(query)
        df = pd.DataFrame(result.result_rows, columns=result.column_names)

        print(f"OK: Inventario BF cargado: {len(df)} registros")

        return df

    except Exception as e:
        print(f"ERROR al cargar inventario BF: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def get_nombre_almacen(codigo):
    """
    Convierte código de almacén a nombre completo

    Args:
        codigo: Código del almacén (ATS, MELI, etc.)

    Returns:
        str: Nombre completo del almacén
    """
    ALMACENES_NOMBRES = {
        'ATS': 'Astillero',
        'MELI': 'Meli Full',
        '1C': 'Astillero 1C',
        'MLPS': 'Manzanillo',
        'TIJ': 'Rosarito',
        'NAU': 'Nautica',
        'WFS': 'Walmart Full',
        'BEX': 'BEX',
        'HUMMA': 'Humma'
    }
    return ALMACENES_NOMBRES.get(codigo, codigo)


def get_ventas_producto_compra_mes_actual(filtro_canal=None):
    """
    Obtiene las ventas de producto de compra (sin combos) del mes actual
    Solo para los 7 canales BF (o canal específico si se filtra)

    Args:
        filtro_canal: Canal específico para filtrar, o None para todos los canales BF

    Returns:
        DataFrame: Ventas con columnas: Sku_Primario, Venta_Mes_Actual
    """
    from datetime import datetime
    primer_dia_mes = datetime.now().replace(day=1)
    return get_ventas_producto_compra_periodo(primer_dia_mes, None, filtro_canal)


def get_ventas_producto_compra_periodo(fecha_inicio, fecha_fin=None, filtro_canal=None):
    """
    Obtiene las ventas de producto de compra (sin combos) para un período específico
    Usa arrayJoin para descomponer producto_comercial correctamente
    Solo para los 7 canales BF (o canal específico si se filtra)

    Args:
        fecha_inicio: Fecha de inicio del período (datetime)
        fecha_fin: Fecha fin del período (datetime), None para hasta hoy
        filtro_canal: Canal específico para filtrar, o None para todos los canales BF

    Returns:
        DataFrame: Ventas con columnas: Sku_Primario, Venta_Mes_Actual
    """
    client = get_db_connection()
    if not client:
        print("ERROR: No se pudo conectar a la base de datos para ventas producto compra")
        return pd.DataFrame()

    try:
        # Formatear fechas
        fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d')

        # Construir condición de fecha
        if fecha_fin:
            fecha_fin_str = fecha_fin.strftime('%Y-%m-%d')
            condicion_fecha = f"Fecha >= '{fecha_inicio_str}' AND Fecha < '{fecha_fin_str}'"
            print(f"INFO: Cargando ventas de producto de compra del período {fecha_inicio_str} - {fecha_fin_str}...")
        else:
            condicion_fecha = f"Fecha >= '{fecha_inicio_str}'"
            print(f"INFO: Cargando ventas de producto de compra desde {fecha_inicio_str}...")

        # Construir condición de canal
        if filtro_canal and filtro_canal != 'todos':
            condicion_canal = f"Channel = '{filtro_canal}'"
            print(f"INFO: Filtrando ventas por canal: {filtro_canal}")
        else:
            condicion_canal = "Channel IN ('Shein', 'Mercado Libre', 'Yuhu', 'CrediTienda', 'Walmart', 'Liverpool', 'Aliexpress', 'Coppel', 'TikTok Shop')"

        query = f"""
        WITH separacion AS
        (
            SELECT
                cantidad,
                arrayJoin(producto_comercial) AS Producto,
                Producto.1 AS Sku_Primario,
                Producto.2 AS Cantidad,
                toFloat64(cantidad) * toFloat64(Cantidad) AS Cantidad_Total,
                Fecha,
                estado,
                Channel
            FROM Gold.RPT_Ventas
            WHERE
                estado = 'Orden de Venta'
                AND {condicion_canal}
                AND {condicion_fecha}
        ),
        VENTAS AS
        (
            SELECT
                Sku_Primario,
                SUM(Cantidad_Total) AS Venta_Mes_Actual
            FROM separacion
            GROUP BY Sku_Primario
        )
        SELECT * FROM VENTAS
        """

        result = client.query(query)
        df = pd.DataFrame(result.result_rows, columns=result.column_names)

        print(f"OK: Ventas producto compra cargadas: {len(df)} SKUs con venta")

        return df

    except Exception as e:
        print(f"ERROR al cargar ventas producto compra: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def analizar_estructura_tabla_fuente():
    """Función temporal para analizar la estructura completa de Silver.RPT_Ventas_Con_Costo_Prueba"""
    client = get_db_connection()
    if not client:
        return None

    try:
        print(" ANALIZANDO ESTRUCTURA DE TABLA Silver.RPT_Ventas_Con_Costo_Prueba...")

        # Consultar estructura de la tabla
        describe_query = "DESCRIBE Silver.RPT_Ventas_Con_Costo_Prueba"
        describe_result = client.query(describe_query)
        columns_info = describe_result.result_rows

        print(f"📊 CAMPOS DISPONIBLES EN Silver.RPT_Ventas_Con_Costo_Prueba:")
        print("=" * 70)
        for i, (column_name, column_type, default_value, *other) in enumerate(columns_info, 1):
            print(f"{i:2d}. {column_name:<25} | {column_type:<20}")
        print("=" * 70)

        # También obtener una muestra de datos para ver valores reales
        sample_query = "SELECT * FROM Silver.RPT_Ventas_Con_Costo_Prueba LIMIT 1"
        sample_result = client.query(sample_query)

        if sample_result.result_rows:
            print(f"\n📋 MUESTRA DE DATOS (primera fila):")
            print("-" * 70)
            sample_row = sample_result.result_rows[0]
            column_names = sample_result.column_names

            for i, (col_name, value) in enumerate(zip(column_names, sample_row), 1):
                print(f"{i:2d}. {col_name:<25} = {str(value)[:40]}")
            print("-" * 70)

        return columns_info, sample_result.column_names if sample_result.result_rows else []

    except Exception as e:
        print(f"ERROR: Error analizando estructura: {e}")
        return None


def obtener_mes_actual():
    """Obtiene el mes actual según timezone de Mazatlán"""
    return datetime.now(MAZATLAN_TZ).month
def cargar_ultimos_3_meses_rentabilidad():
    """
    SOLO PARA ANÁLISIS DE RENTABILIDAD
    Carga datos optimizados de los últimos 4 meses para desglose temporal.

    IMPORTANTE: Se cargan 4 meses (no 3) para evitar que el 3er punto de la evolución
    aparezca en 0% cuando el usuario filtra por un mes pasado.

    Ejemplo: Si estamos en Dic 2025, carga Sep, Oct, Nov, Dic 2025
             - Si usuario filtra Nov: evolución Nov, Oct, Sep (todos tienen datos)
             - Si usuario filtra Dic: evolución Dic, Nov, Oct (todos tienen datos)
    """
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    import time
    import sys

    tiempo_funcion_inicio = time.time()
    print(f"[PERFORMANCE] INICIO - cargar_ultimos_3_meses_rentabilidad() [CARGA 4 MESES]")
    sys.stdout.flush()

    tiempo_conexion_inicio = time.time()
    client = get_db_connection()
    tiempo_conexion_fin = time.time()
    print(f"  [PERFORMANCE] Conexión BD: {tiempo_conexion_fin - tiempo_conexion_inicio:.3f} segundos")
    sys.stdout.flush()

    if not client:
        return pd.DataFrame(), [], []

    try:
        hoy = datetime.now()
        print(f"INFO: [RENTABILIDAD] Cargando datos de últimos 4 meses desde {hoy.strftime('%Y-%m-%d')}")

        # Calcular los 4 meses a cargar (no 3)
        meses_condiciones = []
        meses_info = []

        for i in range(4):  # ✅ CAMBIADO: 3 → 4 meses
            fecha_mes = hoy.replace(day=1) - relativedelta(months=i)
            año = fecha_mes.year
            mes = fecha_mes.month

            meses_condiciones.append(f"(toYear(Fecha) = {año} AND toMonth(Fecha) = {mes})")
            meses_info.append(f"{año}-{mes:02d}")

        print(f"INFO: [RENTABILIDAD] Meses a cargar: {', '.join(meses_info)}")

        # Query optimizada para 4 meses específicos + canales oficiales
        tiempo_query_inicio = time.time()
        query = f"""
        SELECT * FROM Silver.RPT_Ventas_Con_Costo_Prueba
        WHERE ({' OR '.join(meses_condiciones)})
        AND Channel IN ({','.join([f"'{canal}'" for canal in CANALES_CLASIFICACION])})
        ORDER BY Fecha DESC
        """

        print(f"INFO: [RENTABILIDAD] Ejecutando query optimizada para 3 meses...")
        sys.stdout.flush()
        tiempo_ejecucion_inicio = time.time()
        result = client.query(query)
        tiempo_ejecucion_fin = time.time()
        print(f"  [PERFORMANCE] Ejecución query BD: {tiempo_ejecucion_fin - tiempo_ejecucion_inicio:.3f} segundos")
        sys.stdout.flush()

        if not result.result_rows:
            print("WARNING: [RENTABILIDAD] No se encontraron datos para los últimos 3 meses")
            return pd.DataFrame(), [], []

        # Convertir a DataFrame
        tiempo_dataframe_inicio = time.time()
        df = pd.DataFrame(result.result_rows, columns=result.column_names)
        df['Fecha'] = pd.to_datetime(df['Fecha'])

        # IMPORTANTE: Convertir cantidad a numérico desde el inicio
        if 'cantidad' in df.columns:
            print(f"DEBUG: Convirtiendo columna 'cantidad' a numérico en carga inicial")
            print(f"DEBUG: Tipo original: {df['cantidad'].dtype}")
            df['cantidad'] = pd.to_numeric(df['cantidad'], errors='coerce').fillna(0)
            print(f"DEBUG: Tipo después de conversión: {df['cantidad'].dtype}")
            print(f"DEBUG: Registros con cantidad válida: {(df['cantidad'] > 0).sum()} de {len(df)}")
        else:
            print(f"WARNING: Columna 'cantidad' no encontrada en los datos de ClickHouse")

        tiempo_dataframe_fin = time.time()
        print(f"  [PERFORMANCE] Conversión a DataFrame: {tiempo_dataframe_fin - tiempo_dataframe_inicio:.3f} segundos")
        sys.stdout.flush()

        # Obtener listas de canales y warehouses disponibles
        tiempo_listas_inicio = time.time()
        channels_disponibles = sorted(df['Channel'].unique().tolist())
        warehouses_disponibles = sorted(df['Warehouse'].unique().tolist())
        tiempo_listas_fin = time.time()
        print(f"  [PERFORMANCE] Generación listas: {tiempo_listas_fin - tiempo_listas_inicio:.3f} segundos")
        sys.stdout.flush()

        tiempo_query_fin = time.time()
        print(f"  [PERFORMANCE] Query TOTAL: {tiempo_query_fin - tiempo_query_inicio:.3f} segundos")

        tiempo_funcion_fin = time.time()
        print(f"[PERFORMANCE] FIN - cargar_ultimos_3_meses_rentabilidad(): {tiempo_funcion_fin - tiempo_funcion_inicio:.3f} segundos")
        sys.stdout.flush()

        print(f"SUCCESS: [RENTABILIDAD] Cargados {len(df):,} registros de {len(df['Fecha'].dt.to_period('M').unique())} meses")
        print(f"INFO: [RENTABILIDAD] Canales encontrados: {len(channels_disponibles)}")
        print(f"INFO: [RENTABILIDAD] Warehouses encontrados: {len(warehouses_disponibles)}")

        return df, channels_disponibles, warehouses_disponibles

    except Exception as e:
        print(f"ERROR: [RENTABILIDAD] Error cargando datos de 3 meses: {e}")
        return pd.DataFrame(), [], []

def filtrar_por_mes_actual_rentabilidad(df_completo, mes_seleccionado):
    """
    SOLO PARA ANÁLISIS DE RENTABILIDAD
    Filtra el DataFrame completo para obtener solo el mes actual
    Usado para métricas principales que no necesitan desglose temporal
    """
    if df_completo.empty:
        return df_completo

    # Filtrar por el mes específico
    df_filtrado = df_completo[df_completo['Fecha'].dt.month == mes_seleccionado].copy()

    print(f"INFO: [RENTABILIDAD] Filtrado {len(df_completo):,} → {len(df_filtrado):,} registros para mes {mes_seleccionado}")

    return df_filtrado


def cargar_inventario_disponible():
    """
    Carga el inventario disponible desde Gold.RPT_Inventarios

    Returns:
        dict: Diccionario con SKU como key y datos de inventario como value
              Formato: {
                  'sku': {
                      'total': float,  # Existencia total
                      'almacenes': [{'almacen': str, 'cantidad': float}, ...]
                  }
              }
    """
    import time
    import sys

    # Mapeo de códigos de almacén a nombres completos
    ALMACENES_NOMBRES = {
        'ATS': 'Astillero',
        'MELI': 'Meli Full',
        '1C': 'Astillero 1C',
        'MLPS': 'Manzanillo',
        'TIJ': 'Rosarito',
        'NAU': 'Nautica',
        'WFS': 'Walmart Full',
        'BEX': 'BEX',
        'HUMMA': 'Humma'
    }

    tiempo_inicio = time.time()
    print(f"[INVENTARIO] Cargando inventario disponible...")
    sys.stdout.flush()

    client = get_db_connection()
    if not client:
        print("ERROR: [INVENTARIO] No se pudo conectar a ClickHouse")
        return {}

    try:
        query = """
        SELECT
            toString(sku) AS sku,
            descripcion,
            almacen,
            cantidad_libre_de_usar,
            sum(cantidad_libre_de_usar) OVER (PARTITION BY sku, descripcion) AS Existencia
        FROM Gold.RPT_Inventarios
        WHERE sku <> ''
        ORDER BY sku, almacen
        """

        result = client.query(query)

        if not result.result_rows:
            print("WARNING: [INVENTARIO] No se encontraron datos de inventario")
            return {}

        # Procesar datos en estructura optimizada
        inventario_dict = {}

        for row in result.result_rows:
            sku, descripcion, almacen, cantidad_libre, existencia_total = row

            # Convertir a float y manejar valores None
            cantidad_libre = float(cantidad_libre) if cantidad_libre is not None else 0.0
            existencia_total = float(existencia_total) if existencia_total is not None else 0.0

            if sku not in inventario_dict:
                inventario_dict[sku] = {
                    'total': existencia_total,
                    'almacenes': []
                }

            # Solo agregar almacenes con cantidad > 0 para mantener tooltip compacto
            if cantidad_libre > 0:
                # Convertir código de almacén a nombre completo
                almacen_codigo = almacen.upper() if almacen else ''
                almacen_nombre = ALMACENES_NOMBRES.get(almacen_codigo, almacen)

                inventario_dict[sku]['almacenes'].append({
                    'almacen': almacen_nombre,
                    'cantidad': cantidad_libre
                })

        tiempo_fin = time.time()
        print(f"[INVENTARIO] Cargados {len(inventario_dict)} SKUs en {tiempo_fin - tiempo_inicio:.3f} segundos")
        sys.stdout.flush()

        return inventario_dict

    except Exception as e:
        print(f"ERROR: [INVENTARIO] Error cargando inventario: {e}")
        return {}


def cargar_inventario_en_transito():
    """
    Carga el inventario en tránsito desde Silver.Entregas_bodega
    Agrupa por SKU y rangos de fecha para tooltip compacto

    Returns:
        dict: Diccionario con SKU como key y datos de tránsito como value
              Formato: {
                  'sku': {
                      'total': int,
                      'esta_semana': int,
                      'proximas_semanas': int,
                      'envios': [{'cantidad': int, 'almacen': str, 'fecha': date}, ...]
                  }
              }
    """
    import time
    import sys
    from datetime import datetime, timedelta

    tiempo_inicio = time.time()
    print(f"[INVENTARIO] Cargando inventario en tránsito...")
    sys.stdout.flush()

    client = get_db_connection()
    if not client:
        print("ERROR: [INVENTARIO] No se pudo conectar a ClickHouse")
        return {}

    try:
        query = """
        SELECT
            toString(Sku) AS sku,
            Producto,
            Cantidades_paq AS Unidades,
            Bodega AS Almacen,
            Cita_descarga AS Fecha_Llegada
        FROM Silver.Entregas_bodega
        WHERE Estatus = 'En tránsito'
            AND toYear(Cita_descarga) > 2000
            AND Bodega <> ''
        ORDER BY sku, Cita_descarga
        """

        result = client.query(query)

        if not result.result_rows:
            print("WARNING: [INVENTARIO] No se encontraron datos de tránsito")
            return {}

        # Definir rangos de fecha
        hoy = datetime.now().date()
        fin_esta_semana = hoy + timedelta(days=7)

        # Procesar datos
        transito_dict = {}

        for row in result.result_rows:
            sku, producto, unidades, almacen, fecha_llegada = row

            # Convertir tipos
            unidades = int(unidades) if unidades is not None else 0

            # Convertir fecha si es necesario
            if isinstance(fecha_llegada, str):
                fecha_llegada = datetime.strptime(fecha_llegada, '%Y-%m-%d').date()
            elif isinstance(fecha_llegada, datetime):
                fecha_llegada = fecha_llegada.date()

            if sku not in transito_dict:
                transito_dict[sku] = {
                    'total': 0,
                    'esta_semana': 0,
                    'proximas_semanas': 0,
                    'envios': []
                }

            # Agregar al total
            transito_dict[sku]['total'] += unidades

            # Clasificar por rango de fecha
            if fecha_llegada <= fin_esta_semana:
                transito_dict[sku]['esta_semana'] += unidades
            else:
                transito_dict[sku]['proximas_semanas'] += unidades

            # Guardar detalle de envío
            transito_dict[sku]['envios'].append({
                'cantidad': unidades,
                'almacen': almacen,
                'fecha': fecha_llegada
            })

        tiempo_fin = time.time()
        print(f"[INVENTARIO] Cargados {len(transito_dict)} SKUs en tránsito en {tiempo_fin - tiempo_inicio:.3f} segundos")
        sys.stdout.flush()

        return transito_dict

    except Exception as e:
        print(f"ERROR: [INVENTARIO] Error cargando tránsito: {e}")
        return {}


def get_ventas_individual_vs_combo_periodo(fecha_inicio, fecha_fin=None, filtro_canal=None):
    """
    Obtiene ventas separadas en individuales vs combo para un período específico

    Identifica:
    - Venta Individual: sku vendido == sku en producto_comercial
    - Venta Combo: sku vendido != sku en producto_comercial

    Args:
        fecha_inicio: Fecha de inicio del período (datetime)
        fecha_fin: Fecha fin del período (datetime), None para hasta hoy
        filtro_canal: Canal específico para filtrar, o None para todos los canales BF

    Returns:
        DataFrame: Ventas con columnas: Sku_Primario, Tipo_Venta, Cantidad_Vendida, Total_Ventas
    """
    client = get_db_connection()
    if not client:
        print("ERROR: No se pudo conectar a la base de datos para ventas individual vs combo")
        return pd.DataFrame()

    try:
        # Formatear fechas
        fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d')

        # Construir condición de fecha
        if fecha_fin:
            fecha_fin_str = fecha_fin.strftime('%Y-%m-%d')
            condicion_fecha = f"Fecha >= '{fecha_inicio_str}' AND Fecha < '{fecha_fin_str}'"
            print(f"INFO: Cargando ventas individual vs combo del período {fecha_inicio_str} - {fecha_fin_str}...")
        else:
            condicion_fecha = f"Fecha >= '{fecha_inicio_str}'"
            print(f"INFO: Cargando ventas individual vs combo desde {fecha_inicio_str}...")

        # Construir condición de canal
        if filtro_canal and filtro_canal != 'todos':
            condicion_canal = f"Channel = '{filtro_canal}'"
            print(f"INFO: Filtrando desglose individual/combo por canal: {filtro_canal}")
        else:
            condicion_canal = "Channel IN ('Shein', 'Mercado Libre', 'Yuhu', 'CrediTienda', 'Walmart', 'Liverpool', 'Aliexpress', 'Coppel', 'TikTok Shop')"

        query = f"""
        WITH separacion AS
        (
            SELECT
                sku AS Sku_Vendido,
                cantidad,
                Total,
                arrayJoin(producto_comercial) AS Producto,
                Producto.1 AS Sku_Primario,
                Producto.2 AS Cantidad_Componente,
                toFloat64(cantidad) * toFloat64(Cantidad_Componente) AS Cantidad_Total,
                -- Identificar si es individual o combo
                if(Sku_Vendido = Sku_Primario, 'Individual', 'Combo') AS Tipo_Venta,
                Fecha,
                estado,
                Channel
            FROM Gold.RPT_Ventas
            WHERE
                estado = 'Orden de Venta'
                AND {condicion_canal}
                AND {condicion_fecha}
        ),
        VENTAS_AGRUPADAS AS
        (
            SELECT
                Sku_Primario,
                Tipo_Venta,
                SUM(Cantidad_Total) AS Cantidad_Vendida,
                SUM(Total) AS Total_Ventas
            FROM separacion
            GROUP BY Sku_Primario, Tipo_Venta
        )
        SELECT * FROM VENTAS_AGRUPADAS
        ORDER BY Sku_Primario, Tipo_Venta
        """

        result = client.query(query)
        df = pd.DataFrame(result.result_rows, columns=result.column_names)

        print(f"OK: Ventas individual vs combo cargadas: {len(df)} registros")

        if not df.empty:
            total_individual = len(df[df['Tipo_Venta'] == 'Individual'])
            total_combo = len(df[df['Tipo_Venta'] == 'Combo'])
            print(f"   - Individual: {total_individual} SKUs")
            print(f"   - Combo: {total_combo} SKUs")

        return df

    except Exception as e:
        print(f"ERROR al cargar ventas individual vs combo: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def get_radar_comercial_data():
    """
    Obtiene datos del radar comercial comparando precios e IR por canal

    Analiza productos relevantes y compara:
    - Precio de venta en cada canal
    - % de Ingreso Real (IR) por canal
    - Inventario asignado por canal

    Canales analizados: Mercado Libre, CrediTienda, Walmart, Shein

    Returns:
        DataFrame: Comparativa de precios con columnas:
                  sku, descripcion, precio_ML, %IR_ML, inv_asignado_ML, precio_CT, %IR_CT, inv_asignado_CT,
                  precio_WM, %IR_WM, inv_asignado_WM, precio_SH, %IR_SH, inv_asignado_SH
    """
    client = get_db_connection()
    if not client:
        print("ERROR: No se pudo conectar a la base de datos para radar comercial")
        return pd.DataFrame()

    try:
        print("INFO: [RADAR COMERCIAL] Ejecutando query de análisis de competencia...")

        # Obtener mes actual en formato "Mes YYYY" (ej: "Diciembre 2025")
        from datetime import datetime

        # Mapeo manual de meses en español (primera letra mayúscula)
        meses_es = {
            1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
            5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
            9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
        }

        ahora = datetime.now()
        mes_nombre = meses_es[ahora.month]
        año = ahora.year
        mes_actual = f"{mes_nombre} {año}"

        print(f"INFO: [RADAR COMERCIAL] Obteniendo inventario asignado para: {mes_actual}")

        query = f"""
        WITH lista_productos AS (
            SELECT sku, descripcion
            FROM Silver.catalogo_productos_BF
            WHERE (producto_relevante = 1 AND descripcion NOT LIKE '%Midea%')
               OR sku IN ('1000065', '1000066', '1000067', '1000068', '1000069','2000097','2000096')
        ),
        precio_actual AS (
            SELECT *
            FROM (
                SELECT
                    sku,
                    descripcion,
                    Channel,
                    Total,
                    (Total - `Costo de venta` - `Comision por venta` - gastos_destino - Ultima_milla) / Total AS IR,
                    Fecha,
                    row_number() OVER (PARTITION BY sku, Channel ORDER BY Fecha DESC) AS Rn
                FROM Silver.RPT_Ventas_Con_Costo_Prueba
                WHERE estado = 'Orden de Venta'
                  AND cantidad = 1
                  AND Channel <> ''
                  AND sku <> '2000090' AND sku <> '2000032'
                  AND sku IN (SELECT sku FROM lista_productos)
            )
            WHERE Rn = 1
        ),
        tiempo_precio_activo AS (
            -- Para cada SKU y canal, encontrar cuándo cambió el precio por última vez
            SELECT
                sku,
                Channel,
                Total AS precio_actual,
                Fecha AS fecha_precio_actual,
                -- Obtener el precio anterior diferente
                lagInFrame(Total) OVER (PARTITION BY sku, Channel ORDER BY Fecha) AS precio_anterior,
                lagInFrame(Fecha) OVER (PARTITION BY sku, Channel ORDER BY Fecha) AS fecha_precio_anterior
            FROM Silver.RPT_Ventas_Con_Costo_Prueba
            WHERE estado = 'Orden de Venta'
              AND cantidad = 1
              AND Channel <> ''
              AND sku IN (SELECT sku FROM lista_productos)
        ),
        ultimo_cambio_precio AS (
            -- Filtrar solo donde hubo cambio de precio
            SELECT
                sku,
                Channel,
                precio_actual,
                fecha_precio_actual,
                precio_anterior,
                fecha_precio_anterior,
                dateDiff('day', fecha_precio_anterior, fecha_precio_actual) AS dias_desde_cambio
            FROM tiempo_precio_activo
            WHERE precio_actual != precio_anterior
               OR precio_anterior IS NULL  -- Primera vez que aparece el producto
        ),
        precio_estable AS (
            -- Obtener el último cambio de precio para cada SKU-Canal
            SELECT *
            FROM (
                SELECT
                    sku,
                    Channel,
                    fecha_precio_actual,
                    dias_desde_cambio,
                    -- Calcular días desde el último cambio hasta hoy
                    dateDiff('day', fecha_precio_actual, today()) AS dias_precio_activo,
                    row_number() OVER (PARTITION BY sku, Channel ORDER BY fecha_precio_actual DESC) AS rn
                FROM ultimo_cambio_precio
            )
            WHERE rn = 1
        ),
        conversion_meli AS (
            WITH ultimo_dia AS (
                SELECT MAX(fecha) AS max_fecha
                FROM Gold.visitas_canales
                WHERE canal = 'Mercado Libre'
            ),
            ultima_hora AS (
                SELECT MAX(fecha_registro) AS max_fecha_registro
                FROM Gold.visitas_canales
                WHERE canal = 'Mercado Libre'
                  AND fecha = (SELECT max_fecha FROM ultimo_dia)
            )
            SELECT
                sku,
                SUM(visitas_unicas) AS visitas_totales,
                SUM(cantidad_ventas) AS ventas_totales,
                IF(SUM(visitas_unicas) > 0,
                   SUM(cantidad_ventas) / SUM(visitas_unicas),
                   0) AS conversion_rate
            FROM Gold.visitas_canales
            WHERE canal = 'Mercado Libre'
              AND fecha = (SELECT max_fecha FROM ultimo_dia)
              AND fecha_registro = (SELECT max_fecha_registro FROM ultima_hora)
              AND sku IN (SELECT sku FROM lista_productos)
            GROUP BY sku
        ),
        inventario_asignado AS (
            SELECT
                sku,
                Channel,
                cupo_manual AS inventario_asignado
            FROM Silver.Distribucion_Mensual_Canal_Manual
            WHERE mes = '{mes_actual}'
              AND activo = 1
              AND sku IN (SELECT sku FROM lista_productos)
        )
        SELECT
            p.sku AS sku,
            any(p.descripcion) AS descripcion,

            -- Mercado Libre
            MAX(IF(p.Channel = 'Mercado Libre', p.Total, NULL)) AS precio_ML,
            CONCAT(ROUND(MAX(IF(p.Channel = 'Mercado Libre', p.IR, NULL)) * 100, 1), '%') AS `%IR_ML`,
            CONCAT(ROUND(c.conversion_rate * 100, 2), '%') AS `%Conv_ML`,
            MAX(IF(p.Channel = 'Mercado Libre', pe.dias_precio_activo, NULL)) AS dias_precio_ML,
            COALESCE(MAX(IF(ia.Channel = 'Mercado Libre', ia.inventario_asignado, NULL)), 0) AS inv_asignado_ML,

            -- CrediTienda
            MAX(IF(p.Channel = 'CrediTienda', p.Total, NULL)) AS precio_CT,
            CONCAT(ROUND(MAX(IF(p.Channel = 'CrediTienda', p.IR, NULL)) * 100, 1), '%') AS `%IR_CT`,
            MAX(IF(p.Channel = 'CrediTienda', pe.dias_precio_activo, NULL)) AS dias_precio_CT,
            COALESCE(MAX(IF(ia.Channel = 'CrediTienda', ia.inventario_asignado, NULL)), 0) AS inv_asignado_CT,

            -- Walmart
            MAX(IF(p.Channel = 'Walmart', p.Total, NULL)) AS precio_WM,
            CONCAT(ROUND(MAX(IF(p.Channel = 'Walmart', p.IR, NULL)) * 100, 1), '%') AS `%IR_WM`,
            MAX(IF(p.Channel = 'Walmart', pe.dias_precio_activo, NULL)) AS dias_precio_WM,
            COALESCE(MAX(IF(ia.Channel = 'Walmart', ia.inventario_asignado, NULL)), 0) AS inv_asignado_WM,

            -- Shein
            MAX(IF(p.Channel = 'Shein', p.Total, NULL)) AS precio_SH,
            CONCAT(ROUND(MAX(IF(p.Channel = 'Shein', p.IR, NULL)) * 100, 1), '%') AS `%IR_SH`,
            MAX(IF(p.Channel = 'Shein', pe.dias_precio_activo, NULL)) AS dias_precio_SH,
            COALESCE(MAX(IF(ia.Channel = 'Shein', ia.inventario_asignado, NULL)), 0) AS inv_asignado_SH,

            -- TikTok Shop
            MAX(IF(p.Channel = 'TikTok Shop', p.Total, NULL)) AS precio_TK,
            CONCAT(ROUND(MAX(IF(p.Channel = 'TikTok Shop', p.IR, NULL)) * 100, 1), '%') AS `%IR_TK`,
            MAX(IF(p.Channel = 'TikTok Shop', pe.dias_precio_activo, NULL)) AS dias_precio_TK,
            COALESCE(MAX(IF(ia.Channel = 'TikTok Shop', ia.inventario_asignado, NULL)), 0) AS inv_asignado_TK,

            -- Liverpool
            MAX(IF(p.Channel = 'Liverpool', p.Total, NULL)) AS precio_LP,
            CONCAT(ROUND(MAX(IF(p.Channel = 'Liverpool', p.IR, NULL)) * 100, 1), '%') AS `%IR_LP`,
            MAX(IF(p.Channel = 'Liverpool', pe.dias_precio_activo, NULL)) AS dias_precio_LP,
            COALESCE(MAX(IF(ia.Channel = 'Liverpool', ia.inventario_asignado, NULL)), 0) AS inv_asignado_LP,

            -- Yuhu
            MAX(IF(p.Channel = 'Yuhu', p.Total, NULL)) AS precio_YH,
            CONCAT(ROUND(MAX(IF(p.Channel = 'Yuhu', p.IR, NULL)) * 100, 1), '%') AS `%IR_YH`,
            MAX(IF(p.Channel = 'Yuhu', pe.dias_precio_activo, NULL)) AS dias_precio_YH,
            COALESCE(MAX(IF(ia.Channel = 'Yuhu', ia.inventario_asignado, NULL)), 0) AS inv_asignado_YH

        FROM precio_actual p
        LEFT JOIN conversion_meli c ON p.sku = c.sku
        LEFT JOIN precio_estable pe ON p.sku = pe.sku AND p.Channel = pe.Channel
        LEFT JOIN inventario_asignado ia ON p.sku = ia.sku AND p.Channel = ia.Channel
        LEFT JOIN Silver.Ranking_Sku r ON p.sku = r.sku
        GROUP BY p.sku, c.conversion_rate, r.ranking_global
        ORDER BY r.ranking_global ASC NULLS LAST
        """

        result = client.query(query)
        df = pd.DataFrame(result.result_rows, columns=result.column_names)

        print(f"OK: [RADAR COMERCIAL] Datos cargados: {len(df)} productos analizados")

        if not df.empty:
            # Calcular estadísticas
            productos_en_ml = df['precio_ML'].notna().sum()
            productos_en_ct = df['precio_CT'].notna().sum()
            productos_en_wm = df['precio_WM'].notna().sum()
            productos_en_sh = df['precio_SH'].notna().sum()
            productos_en_tk = df['precio_TK'].notna().sum()
            productos_en_lp = df['precio_LP'].notna().sum()
            productos_en_yh = df['precio_YH'].notna().sum()

            print(f"   - Productos en Mercado Libre: {productos_en_ml}")
            print(f"   - Productos en CrediTienda: {productos_en_ct}")
            print(f"   - Productos en Walmart: {productos_en_wm}")
            print(f"   - Productos en Shein: {productos_en_sh}")
            print(f"   - Productos en TikTok Shop: {productos_en_tk}")
            print(f"   - Productos en Liverpool: {productos_en_lp}")
            print(f"   - Productos en Yuhu: {productos_en_yh}")

        return df

    except Exception as e:
        print(f"ERROR: [RADAR COMERCIAL] Error ejecutando query: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def get_radar_comercial_datos_semanales(mes_nombre=None, semana_num=None):
    """
    Obtiene datos semanales de inventario y ventas para el Radar Comercial

    Args:
        mes_nombre: Nombre del mes (ej: 'Diciembre 2025'). Si es None, usa mes actual
        semana_num: Número de semana (1-4). Si es None, usa semana actual

    Returns:
        DataFrame con columnas: sku, canal, inv_asignado_semana, ventas_semana
    """
    from datetime import datetime
    import pandas as pd

    try:
        # Determinar mes actual si no se especifica
        if mes_nombre is None:
            meses_es = {
                1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
                5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
                9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
            }
            ahora = datetime.now()
            mes_nombre = f"{meses_es[ahora.month]} {ahora.year}"

        print(f"INFO: [RADAR SEMANAL] Solicitando datos para mes: {mes_nombre}")

        # Obtener datos semanales completos (SIN FILTRAR - todos los SKUs)
        df_semanal = get_distribucion_semanal_inventario(mes_nombre)

        if df_semanal.empty:
            print(f"WARN: [RADAR SEMANAL] No hay datos semanales para {mes_nombre}")
            return pd.DataFrame()

        print(f"DEBUG: [RADAR SEMANAL] Datos totales obtenidos: {len(df_semanal)} registros, {df_semanal['sku'].nunique()} SKUs")

        # Determinar semana actual si no se especifica
        if semana_num is None:
            semana_num = 1

        print(f"INFO: [RADAR SEMANAL] Filtrando por Semana del mes {semana_num}")
        print(f"DEBUG: [RADAR SEMANAL] Semanas disponibles en datos: {sorted(df_semanal['semana'].unique())}")

        # MAPEAR semana del mes (1-4) a semana del año (según el mes)
        # Obtener las semanas únicas ordenadas
        semanas_disponibles = sorted(df_semanal['semana'].unique())

        if len(semanas_disponibles) == 0:
            print(f"ERROR: [RADAR SEMANAL] No hay semanas disponibles en los datos")
            return pd.DataFrame()

        # Mapear: semana 1 del mes = primera semana disponible, etc.
        if 1 <= semana_num <= len(semanas_disponibles):
            semana_real = semanas_disponibles[semana_num - 1]  # Índice 0-based
            print(f"INFO: [RADAR SEMANAL] Mapeando Semana {semana_num} del mes → Semana {semana_real} del año")
        else:
            print(f"ERROR: [RADAR SEMANAL] Semana {semana_num} fuera de rango (hay {len(semanas_disponibles)} semanas)")
            return pd.DataFrame()

        # Filtrar por la semana específica del año
        df_semana = df_semanal[df_semanal['semana'] == semana_real].copy()

        if df_semana.empty:
            print(f"WARN: [RADAR SEMANAL] No hay datos para semana {semana_num}")
            return pd.DataFrame()

        print(f"DEBUG: [RADAR SEMANAL] Registros en Semana {semana_num}: {len(df_semana)}, SKUs únicos: {df_semana['sku'].nunique()}")

        # Preparar datos en formato para el Radar Comercial
        df_resultado = df_semana[['sku', 'canal', 'asignacion_canal', 'ventas_reales_informativas']].copy()
        df_resultado.columns = ['sku', 'canal', 'inv_asignado_semana', 'ventas_semana']

        print(f"OK: [RADAR SEMANAL] Retornando {len(df_resultado)} registros para {df_resultado['sku'].nunique()} SKUs")
        print(f"DEBUG: [RADAR SEMANAL] Primeros SKUs: {sorted(df_resultado['sku'].unique())[:10]}")

        return df_resultado

    except Exception as e:
        print(f"ERROR: [RADAR SEMANAL] Error obteniendo datos semanales: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()






def get_analisis_competencia_ml():
    """
    Obtiene el análisis de competencia de Mercado Libre

    Analiza competidores por SKU calculando scores de:
    - Ventas
    - Precio
    - Stock
    - Ofertas
    - Almacenamiento
    - Score total de competitividad

    Returns:
        DataFrame: Análisis de competencia con todas las métricas
    """
    client = get_db_connection()
    if not client:
        print("ERROR: No se pudo conectar a la base de datos para análisis de competencia")
        return pd.DataFrame()

    try:
        print("INFO: [ANÁLISIS COMPETENCIA ML] Obteniendo datos de competidores...")

        query = """
        SELECT
            fecha_analisis,
            hora_analisis,
            sku,
            producto,
            nombre_proveedor,
            url,
            num_competidores,
            precio,
            stock_disponible,
            ventas_formato_original,
            cantidad_ventas,
            tipo_de_almacenamiento,
            tipo_de_oferta,
            porcentaje_descuento,
            mas_vendido,
            score_ventas,
            score_precio,
            score_stock,
            score_ofertas,
            score_almacenamiento,
            score_competitividad_total,
            clasificacion_competidor
        FROM Silver.analisis_competencia_ml
        ORDER BY sku ASC, score_competitividad_total DESC
        """

        result = client.query(query)
        df = pd.DataFrame(result.result_rows, columns=result.column_names)

        print(f"OK: [ANÁLISIS COMPETENCIA ML] Datos cargados: {len(df)} registros de competencia")

        if not df.empty:
            skus_unicos = df['sku'].nunique()
            print(f"   - SKUs analizados: {skus_unicos}")
            print(f"   - Total de competidores: {len(df)}")

        return df

    except Exception as e:
        print(f"ERROR: [ANÁLISIS COMPETENCIA ML] Error ejecutando query: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def get_distribucion_inventario(mes_nombre='Diciembre 2025'):
    """
    Obtiene la distribución de inventario por canal para un mes específico

    Calcula cómo distribuir el inventario disponible entre canales usando:
    - Venta promedio mensual (65%)
    - ROI promedio por canal (35%)
    - Límites de capacidad para ciertos canales
    - Redistribución de sobrantes

    IMPORTANTE:
    - Para meses PASADOS: Solo usa distribución manual (tiene mes específico)
    - Para mes ACTUAL: Usa automática + manual (tabla materializada no tiene mes)

    Args:
        mes_nombre: Nombre del mes (ej: 'Diciembre 2025', 'Enero 2026')

    Returns:
        DataFrame: Distribución de inventario con columnas:
                  sku, descripcion, Channel, Disponible_Para_Vender, Forecast_Mes,
                  peso_combinado_normalizado, venta_promedio_mensual_2m, capacidad_maxima_canal,
                  tipo_asignacion, asignacion_calculada, asignacion_con_limite, sobrante_total_sku,
                  unidades_redistribuidas, Disponible_Para_Vender_Canal_FINAL
    """
    from datetime import datetime

    client = get_db_connection()
    if not client:
        print("ERROR: No se pudo conectar a la base de datos para distribución de inventario")
        return pd.DataFrame()

    try:
        # Determinar si es mes actual o pasado
        # Parseamos el mes_nombre para extraer mes y año
        # Formato esperado: "Enero 2026", "Diciembre 2025"
        mes_año_dict = {
            'Enero': 1, 'Febrero': 2, 'Marzo': 3, 'Abril': 4, 'Mayo': 5, 'Junio': 6,
            'Julio': 7, 'Agosto': 8, 'Septiembre': 9, 'Octubre': 10, 'Noviembre': 11, 'Diciembre': 12
        }

        partes = mes_nombre.split()
        mes_texto = partes[0]
        año = int(partes[1]) if len(partes) > 1 else datetime.now().year
        mes_num = mes_año_dict.get(mes_texto, datetime.now().month)

        ahora = datetime.now()
        es_mes_actual = (mes_num == ahora.month and año == ahora.year)

        if es_mes_actual:
            print(f"INFO: [DISTRIBUCIÓN INVENTARIO] {mes_nombre} es el MES ACTUAL - Usando automática + manual")
        else:
            print(f"INFO: [DISTRIBUCIÓN INVENTARIO] {mes_nombre} es MES PASADO - Usando SOLO manual")

        # Para meses pasados, solo usar tabla manual
        if not es_mes_actual:
            query = f"""
            SELECT
                dm.sku AS sku,
                COALESCE(d.descripcion, 'Sin descripción') AS descripcion,
                dm.Channel AS Channel,
                argMax(dm.disponible_total_manual, dm.fecha_modificacion) OVER (PARTITION BY dm.sku) AS Disponible_Para_Vender,
                0 AS Forecast_Mes,
                dm.cupo_manual / argMax(dm.disponible_total_manual, dm.fecha_modificacion) OVER (PARTITION BY dm.sku) AS peso_combinado_normalizado,
                0 AS venta_promedio_mensual_2m,
                0 AS capacidad_maxima_canal,
                'Manual' AS tipo_asignacion,
                0 AS asignacion_calculada,
                0 AS asignacion_con_limite,
                0 AS sobrante_total_sku,
                0 AS unidades_redistribuidas,
                dm.cupo_manual AS Disponible_Para_Vender_Canal_FINAL,
                1 AS es_manual,
                dm.cupo_automatico AS cupo_automatico_original,
                dm.diferencia AS diferencia_manual,
                dm.fecha_modificacion AS fecha_modificacion,
                dm.usuario AS usuario_modificacion,
                dm.comentario AS comentario,
                argMax(dm.disponible_total_manual, dm.fecha_modificacion) OVER (PARTITION BY dm.sku) AS disponible_total_manual,
                argMax(dm.disponible_total_automatico, dm.fecha_modificacion) OVER (PARTITION BY dm.sku) AS disponible_total_automatico
            FROM Silver.Distribucion_Mensual_Canal_Manual dm
            LEFT JOIN (SELECT DISTINCT sku, any(descripcion) as descripcion FROM Silver.RPT_Ventas_Con_Costo_Prueba GROUP BY sku) d ON dm.sku = d.sku
            WHERE dm.mes = '{mes_nombre}'
              AND dm.activo = 1
              AND dm.cupo_manual > 0
            ORDER BY dm.sku, dm.cupo_manual DESC
            """
        else:
            # Mes actual: usar automática + manual (query original)
            query = f"""
            WITH
            -- Distribución automática desde tabla materializada
            dist_automatica AS (
                SELECT
                    sku,
                    descripcion,
                    Channel,
                    Disponible_Para_Vender,
                    Forecast_Mes,
                    peso_combinado_normalizado,
                    venta_promedio_mensual_2m,
                    capacidad_maxima_canal,
                    tipo_asignacion,
                    asignacion_calculada,
                    asignacion_con_limite,
                    sobrante_total_sku,
                    unidades_redistribuidas,
                    Disponible_Para_Vender_Canal_FINAL
                FROM Silver.Distribucion_Mensual_Canal_Materializada
                WHERE sku IS NOT NULL
                  AND Disponible_Para_Vender_Canal_FINAL > 0
            ),

        -- Distribución manual por canal (si existe)
        dist_manual AS (
            SELECT
                sku,
                Channel,
                cupo_manual,
                cupo_automatico,
                diferencia,
                fecha_modificacion,
                usuario,
                comentario
            FROM Silver.Distribucion_Mensual_Canal_Manual
            WHERE mes = '{mes_nombre}'
              AND activo = 1
        ),

        -- Disponible total manual por SKU (tomar el último valor modificado)
        disponible_total_sku AS (
            SELECT
                sku,
                argMax(disponible_total_manual, fecha_modificacion) as disponible_total_manual,
                argMax(disponible_total_automatico, fecha_modificacion) as disponible_total_automatico
            FROM Silver.Distribucion_Mensual_Canal_Manual
            WHERE mes = '{mes_nombre}'
              AND activo = 1
            GROUP BY sku
            HAVING disponible_total_manual > 0
        ),

        -- Canales agregados manualmente que NO existen en la distribución automática
        canales_solo_manual AS (
            SELECT
                dm.sku,
                dm.Channel
            FROM dist_manual dm
            LEFT JOIN dist_automatica da ON dm.sku = da.sku AND dm.Channel = da.Channel
            WHERE da.sku IS NULL  -- No existe en automática
        )

        -- Query final: combinar automático con manual (manual tiene prioridad)
        SELECT
            da.sku AS sku,
            da.descripcion AS descripcion,
            da.Channel AS Channel,
            da.Disponible_Para_Vender AS Disponible_Para_Vender,
            da.Forecast_Mes AS Forecast_Mes,
            da.peso_combinado_normalizado AS peso_combinado_normalizado,
            da.venta_promedio_mensual_2m AS venta_promedio_mensual_2m,
            da.capacidad_maxima_canal AS capacidad_maxima_canal,
            da.tipo_asignacion AS tipo_asignacion,
            da.asignacion_calculada AS asignacion_calculada,
            da.asignacion_con_limite AS asignacion_con_limite,
            da.sobrante_total_sku AS sobrante_total_sku,
            da.unidades_redistribuidas AS unidades_redistribuidas,
            -- Si existe distribución manual, usar cupo_manual; si no, usar el automático
            if(dm.sku != '', dm.cupo_manual, da.Disponible_Para_Vender_Canal_FINAL) AS Disponible_Para_Vender_Canal_FINAL,
            -- Indicadores de distribución manual
            if(dm.sku != '', 1, 0) AS es_manual,
            COALESCE(dm.cupo_automatico, 0) AS cupo_automatico_original,
            COALESCE(dm.diferencia, 0) AS diferencia_manual,
            COALESCE(dm.fecha_modificacion, toDateTime('1970-01-01 00:00:00')) AS fecha_modificacion,
            COALESCE(dm.usuario, '') AS usuario_modificacion,
            COALESCE(dm.comentario, '') AS comentario,
            COALESCE(dt.disponible_total_manual, 0) AS disponible_total_manual,
            COALESCE(dt.disponible_total_automatico, 0) AS disponible_total_automatico
        FROM dist_automatica da
        LEFT JOIN dist_manual dm ON da.sku = dm.sku AND da.Channel = dm.Channel
        LEFT JOIN disponible_total_sku dt ON da.sku = dt.sku

        UNION ALL

        -- Agregar canales que están SOLO en manual (agregados manualmente)
        SELECT
            dm.sku AS sku,
            COALESCE(da_desc.descripcion, 'Sin descripción') AS descripcion,
            dm.Channel AS Channel,
            COALESCE(dt2.disponible_total_manual, 0) AS Disponible_Para_Vender,
            0 AS Forecast_Mes,
            0 AS peso_combinado_normalizado,
            0 AS venta_promedio_mensual_2m,
            0 AS capacidad_maxima_canal,
            'Manual' AS tipo_asignacion,
            0 AS asignacion_calculada,
            0 AS asignacion_con_limite,
            0 AS sobrante_total_sku,
            0 AS unidades_redistribuidas,
            dm.cupo_manual AS Disponible_Para_Vender_Canal_FINAL,
            1 AS es_manual,
            dm.cupo_automatico AS cupo_automatico_original,
            dm.diferencia AS diferencia_manual,
            dm.fecha_modificacion AS fecha_modificacion,
            dm.usuario AS usuario_modificacion,
            dm.comentario AS comentario,
            COALESCE(dt2.disponible_total_manual, 0) AS disponible_total_manual,
            COALESCE(dt2.disponible_total_automatico, 0) AS disponible_total_automatico
        FROM canales_solo_manual csm
        INNER JOIN dist_manual dm ON csm.sku = dm.sku AND csm.Channel = dm.Channel
        LEFT JOIN disponible_total_sku dt2 ON dm.sku = dt2.sku
        LEFT JOIN (SELECT DISTINCT sku, descripcion FROM dist_automatica) da_desc ON dm.sku = da_desc.sku

        ORDER BY sku, peso_combinado_normalizado DESC
            """

        result = client.query(query)
        df = pd.DataFrame(result.result_rows, columns=result.column_names)

        print(f"OK: [DISTRIBUCIÓN INVENTARIO] Datos cargados: {len(df)} registros")

        if not df.empty:
            # Debug: mostrar nombres de columnas
            print(f"DEBUG: Columnas del DataFrame: {list(df.columns)}")

            skus_unicos = df['sku'].nunique()
            canales_unicos = df['Channel'].nunique()
            total_disponible = df.groupby('sku')['Disponible_Para_Vender'].first().sum()
            total_asignado = df['Disponible_Para_Vender_Canal_FINAL'].sum()

            print(f"   - SKUs: {skus_unicos}")
            print(f"   - Canales: {canales_unicos}")
            print(f"   - Total disponible: {total_disponible:,.0f} unidades")
            print(f"   - Total asignado: {total_asignado:,.0f} unidades")

        return df

    except Exception as e:
        print(f"ERROR: [DISTRIBUCIÓN INVENTARIO] Error ejecutando query: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def get_distribucion_semanal_inventario(mes_nombre='Diciembre 2025'):
    """
    Obtiene la distribución semanal de inventario para un mes específico

    Algoritmo secuencial que respeta:
    - Inventario físico disponible cada semana
    - Cupo mensual (Disponible_Para_Vender)
    - Distribución por canal usando pesos mensuales

    Args:
        mes_nombre: Nombre del mes (ej: 'Diciembre 2025')

    Returns:
        DataFrame: Distribución semanal con columnas:
                  sku, descripcion, semana_num, fecha_inicio, fecha_fin,
                  inventario_inicial, arribos_esperados, forecast_b2b,
                  inventario_fisico_disponible, cupo_mensual_restante,
                  asignacion_semana, ventas_reales_semana,
                  inventario_final_semana, Channel, peso_canal,
                  asignacion_canal_semana
    """
    client = get_db_connection()
    if not client:
        print("ERROR: No se pudo conectar a la base de datos para distribución semanal")
        return pd.DataFrame()

    try:
        print(f"INFO: [DISTRIBUCIÓN SEMANAL] Calculando distribución semanal para {mes_nombre}...")

        # Mapeo de mes a semanas y fechas con factores de estacionalidad
        meses_config = {
            'Diciembre 2025': {
                'semanas': [49, 50, 51, 52],
                'anio': 2025,
                'fechas_inicio': ['2025-11-30', '2025-12-07', '2025-12-14', '2025-12-21'],
                'fechas_fin': ['2025-12-06', '2025-12-13', '2025-12-20', '2025-12-27'],
                'fechas_inventario': ['2025-11-29', '2025-12-06', '2025-12-13', '2025-12-20'],  # Sábado anterior
                'factores_estacionalidad': {
                    49: 1.2832,
                    50: 1.4456,
                    51: 1.2287,
                    52: 1.3010
                }
            },
            'Enero 2026': {
                'semanas': [53, 1, 2, 3, 4],
                'anio': 2026,
                'fechas_inicio': ['2025-12-28', '2026-01-04', '2026-01-11', '2026-01-18', '2026-01-25'],
                'fechas_fin': ['2026-01-03', '2026-01-10', '2026-01-17', '2026-01-24', '2026-01-31'],
                'fechas_inventario': ['2025-12-27', '2026-01-03', '2026-01-10', '2026-01-17', '2026-01-24'],
                # ✅ ESPECIAL: Fechas diferentes SOLO para ventas (Semana 1 toma del 1 al 10 de enero)
                'fechas_ventas_inicio': ['2025-12-28', '2026-01-01', '2026-01-11', '2026-01-18', '2026-01-25'],
                'fechas_ventas_fin': ['2025-12-31', '2026-01-10', '2026-01-17', '2026-01-24', '2026-01-31'],
                'factores_estacionalidad': {
                    53: 1.0,
                    1: 1.0,
                    2: 1.0,
                    3: 1.0,
                    4: 1.0
                }
            }
        }

        if mes_nombre not in meses_config:
            print(f"ERROR: Mes {mes_nombre} no configurado")
            return pd.DataFrame()

        config = meses_config[mes_nombre]
        semanas_str = ','.join(map(str, config['semanas']))

        query = f"""
        WITH
        -- 1. Obtener el registro más reciente por SKU+Channel (última modificación)
        distribucion_manual_reciente AS (
            SELECT
                sku,
                Channel,
                argMax(cupo_manual, fecha_modificacion) as cupo_manual
            FROM Silver.Distribucion_Mensual_Canal_Manual
            WHERE mes = '{mes_nombre}'
              AND activo = 1
            GROUP BY sku, Channel
        ),

        -- 2. Distribución mensual por canal con totales y pesos
        distribucion_mensual_canal AS (
            SELECT
                sku,
                Channel,
                SUM(cupo_manual) OVER (PARTITION BY sku) as cupo_mensual_total,
                cupo_manual / SUM(cupo_manual) OVER (PARTITION BY sku) as peso_normalizado,
                cupo_manual as cupo_mensual_canal_final
            FROM distribucion_manual_reciente
            WHERE cupo_manual > 0
        ),

        -- 3. Inventario inicial para TODAS las semanas del mes (sábados correspondientes)
        inventario_semanal AS (
            SELECT
                ih.sku,
                CASE
                    {' '.join([f"WHEN ih.fecha = toDate('{config['fechas_inventario'][i]}') THEN {config['semanas'][i]}" for i in range(len(config['semanas']))])}
                END as semana_num,
                SUM(ih.cantidad_libre_de_usar + ih.cantidad_traslado) as inventario_inicial
            FROM Silver.Inventario_Historico ih
            WHERE ih.fecha IN ({', '.join([f"toDate('{fecha}')" for fecha in config['fechas_inventario']])})
            GROUP BY ih.sku, semana_num
            HAVING semana_num IS NOT NULL
        ),

        -- 4. Arribos y forecast por semana
        datos_semanales AS (
            SELECT
                t.sku,
                t.Semana_Num as semana_num,
                COALESCE(t.Arribos_Esperados, 0) as arribos_esperados,
                COALESCE(fb.Forecast, 0) as forecast_b2b
            FROM Silver.TableroCompras2 t
            LEFT JOIN (
                SELECT sku, Semana_Num, SUM(Forecast) as Forecast
                FROM Silver.Forecast_2026_V
                WHERE Tipo = 'B2B'
                GROUP BY sku, Semana_Num
            ) fb ON t.sku = fb.sku AND t.Semana_Num = fb.Semana_Num
            WHERE t.Semana_Num IN ({semanas_str})
              AND t.anio = {config['anio']}
        ),

        -- 5. Ventas reales por semana Y POR CANAL
        ventas_semanales AS (
            SELECT
                Producto.1 AS sku,
                Channel,
                CASE
                    {' '.join([f"WHEN toDate(Fecha) >= '{config.get('fechas_ventas_inicio', config['fechas_inicio'])[i]}' AND toDate(Fecha) <= '{config.get('fechas_ventas_fin', config['fechas_fin'])[i]}' THEN {config['semanas'][i]}" for i in range(len(config['semanas']))])}
                END as semana_num,
                SUM(toFloat64(cantidad) * toFloat64(Producto.2)) as ventas_reales
            FROM Silver.RPT_Ventas_Con_Costo_Prueba
            ARRAY JOIN producto_comercial AS Producto
            WHERE estado = 'Orden de Venta'
              AND cantidad > 0
              AND Producto.1 <> ''
              AND Channel IS NOT NULL
              AND Channel <> ''
              AND toDate(Fecha) >= '{config.get('fechas_ventas_inicio', config['fechas_inicio'])[0]}'
              AND toDate(Fecha) <= '{config.get('fechas_ventas_fin', config['fechas_fin'])[-1]}'
            GROUP BY Producto.1, Channel, semana_num
            HAVING semana_num IS NOT NULL
        )

        -- 6. Query final: combinar distribución mensual por canal con datos semanales
        SELECT
        dmc.sku as sku,
        any(d.descripcion) as descripcion,
        ds.semana_num as semana_num,
        COALESCE(inv_s.inventario_inicial, 0) as inventario_inicial,
        ds.arribos_esperados as arribos_esperados,
        ds.forecast_b2b as forecast_b2b,
        COALESCE(inv_s.inventario_inicial, 0) + ds.arribos_esperados - ds.forecast_b2b as inventario_fisico,
        COALESCE(vs.ventas_reales, 0) as ventas_reales,
        dmc.cupo_mensual_total as cupo_mensual_total,
        dmc.cupo_mensual_canal_final as cupo_mensual_canal,
        dmc.Channel as Channel,
        dmc.peso_normalizado as peso_canal
        FROM distribucion_mensual_canal dmc
        INNER JOIN datos_semanales ds ON dmc.sku = ds.sku
        LEFT JOIN inventario_semanal inv_s ON dmc.sku = inv_s.sku AND ds.semana_num = inv_s.semana_num
        LEFT JOIN ventas_semanales vs ON dmc.sku = vs.sku AND ds.semana_num = vs.semana_num AND dmc.Channel = vs.Channel
        LEFT JOIN (SELECT DISTINCT sku, any(descripcion) as descripcion FROM Silver.RPT_Ventas_Con_Costo_Prueba GROUP BY sku) d ON dmc.sku = d.sku
        WHERE dmc.Channel IS NOT NULL
          AND dmc.Channel != ''
          AND dmc.cupo_mensual_canal_final > 0
        GROUP BY dmc.sku, ds.semana_num, inv_s.inventario_inicial, ds.arribos_esperados, ds.forecast_b2b,
                 vs.ventas_reales, dmc.cupo_mensual_total, dmc.cupo_mensual_canal_final, dmc.Channel, dmc.peso_normalizado
        ORDER BY dmc.sku, ds.semana_num, dmc.peso_normalizado DESC
        """

        result = client.query(query)
        df = pd.DataFrame(result.result_rows, columns=result.column_names)

        print(f"OK: [DISTRIBUCIÓN SEMANAL] Datos cargados: {len(df)} registros")

        if not df.empty:
            # Calcular asignación semanal con algoritmo secuencial
            df = calcular_asignacion_semanal_secuencial(df, config)

            skus_unicos = df['sku'].nunique()
            print(f"   - SKUs: {skus_unicos}")
            print(f"   - Semanas: {len(config['semanas'])}")

        return df

    except Exception as e:
        print(f"ERROR: [DISTRIBUCIÓN SEMANAL] Error ejecutando query: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def calcular_asignacion_semanal_secuencial(df, config):
    """
    Distribuye el cupo mensual por canal (ya calculado con redistribución)
    a través de las semanas según estacionalidad, respetando inventario físico compartido

    Args:
        df: DataFrame con datos semanales (incluye cupo_mensual_canal por SKU-Canal)
        config: Configuración del mes (fechas, semanas, factores de estacionalidad)

    Returns:
        DataFrame con asignación semanal por SKU-Canal
    """
    from datetime import datetime

    resultados = []

    # Factores de estacionalidad
    factores = config.get('factores_estacionalidad', {})
    suma_factores = sum(factores.values())

    # Determinar semana actual para redistribución
    semana_actual = datetime.now().isocalendar()[1]

    # Agrupar por SKU (inventario físico es compartido entre canales)
    for sku, grupo_sku in df.groupby('sku'):
        # Variables para tracking de inventario físico del SKU (compartido)
        inventario_disponible_semana_anterior = 0

        # Variable para acumular asignaciones de semanas procesadas
        asignaciones_acumuladas = 0

        # Preparar cupos semanales por canal
        canales_data = {}
        cupo_mensual_total_sku = 0

        for canal in grupo_sku['Channel'].unique():
            datos_canal = grupo_sku[grupo_sku['Channel'] == canal]
            cupo_mensual_canal = datos_canal['cupo_mensual_canal'].iloc[0]
            peso_canal = datos_canal['peso_canal'].iloc[0]

            # Distribuir el cupo mensual del canal según estacionalidad
            cupos_semanales = {}
            for semana_num in config['semanas']:
                factor = factores.get(semana_num, 1.0)
                proporcion = factor / suma_factores if suma_factores > 0 else 0.25
                cupos_semanales[semana_num] = cupo_mensual_canal * proporcion

            canales_data[canal] = {
                'cupo_mensual': cupo_mensual_canal,
                'cupos_semanales': cupos_semanales,
                'peso_canal': peso_canal,
                'bolsa_disponible': cupo_mensual_canal,  # NUEVA LÓGICA: Bolsa que se reduce con ventas reales
            }

            cupo_mensual_total_sku += cupo_mensual_canal

        # Procesar semana por semana
        for semana_num in sorted(config['semanas']):
            datos_semana = grupo_sku[grupo_sku['semana_num'] == semana_num]

            if datos_semana.empty:
                continue

            # Datos generales del SKU para esta semana
            primera_fila = datos_semana.iloc[0]
            inv_inicial_db = float(primera_fila['inventario_inicial'])
            arribos = float(primera_fila['arribos_esperados'])
            forecast_b2b = float(primera_fila['forecast_b2b'])

            # Decidir qué inventario inicial usar:
            # - Si inv_inicial_db > 0: usar el inventario REAL de la BD (semana ya tiene datos históricos)
            # - Si inv_inicial_db == 0: usar el inventario proyectado (sobrante de semana anterior)
            if inv_inicial_db > 0:
                # Semana con inventario real en BD
                inventario_inicial_real = inv_inicial_db
            else:
                # Semana futura sin datos históricos - usar proyección
                inventario_inicial_real = inventario_disponible_semana_anterior

            inventario_fisico = max(0, inventario_inicial_real + arribos - forecast_b2b)

            # NUEVA LÓGICA: Calcular asignaciones basadas en bolsas disponibles dinámicas
            asignaciones_semana = []

            # Paso 1: RECALCULAR inventario disponible del mes PARA ESTA SEMANA
            # Suma de ventas reales de semanas que YA TERMINARON (< semana_actual)
            ventas_reales_pasadas = 0
            for s_pasada in config['semanas']:
                if s_pasada < semana_num and s_pasada < semana_actual:
                    # Solo sumar ventas de semanas que realmente ya terminaron
                    datos_s_pasada = grupo_sku[grupo_sku['semana_num'] == s_pasada]
                    if not datos_s_pasada.empty:
                        ventas_reales_pasadas += datos_s_pasada['ventas_reales'].sum()

            # Inventario disponible para distribuir desde esta semana en adelante
            # CORREGIDO: Restar TANTO ventas reales pasadas COMO asignaciones acumuladas de semanas procesadas
            inventario_disponible_mes = cupo_mensual_total_sku - ventas_reales_pasadas - asignaciones_acumuladas

            # Paso 2: Calcular semanas restantes (desde esta semana en adelante)
            semanas_restantes = [s for s in config['semanas'] if s >= semana_num]

            # Paso 3: Calcular proporción de estacionalidad para semanas restantes
            suma_factores_restantes = sum(factores.get(s, 1.0) for s in semanas_restantes)
            factor_semana_actual = factores.get(semana_num, 1.0)
            proporcion_semana = factor_semana_actual / suma_factores_restantes if suma_factores_restantes > 0 else 0

            # Paso 4: Calcular inventario teórico a distribuir esta semana
            # Es la proporción del inventario disponible del mes según estacionalidad
            inventario_teorico_semana = inventario_disponible_mes * proporcion_semana

            # Paso 5: Calcular la suma de bolsas disponibles de todos los canales
            suma_bolsas = sum(canal_info['bolsa_disponible'] for canal_info in canales_data.values() if canal_info['bolsa_disponible'] > 0)

            # Paso 6: Determinar inventario semanal a distribuir
            # Para semanas futuras: usar el inventario teórico COMPLETO (no limitar)
            # Esto garantiza que se distribuyan EXACTAMENTE los 6,460 entre las 3 semanas
            if semana_num >= semana_actual:
                # Semanas futuras: distribuir el inventario teórico completo
                inventario_semanal_total = inventario_teorico_semana
            else:
                # Semanas pasadas: limitar por inventario físico real
                inventario_semanal_total = min(inventario_fisico, inventario_teorico_semana, suma_bolsas)

            # Paso 3: Procesar cada canal
            for canal in canales_data.keys():
                datos_canal_semana = datos_semana[datos_semana['Channel'] == canal]

                if datos_canal_semana.empty:
                    continue

                canal_info = canales_data[canal]

                # Obtener ventas reales del canal para esta semana
                # IMPORTANTE: Solo considerar ventas reales si la semana YA TERMINÓ
                ventas_reales_bd = float(datos_canal_semana.iloc[0]['ventas_reales'])

                # Separar ventas para CÁLCULOS vs ventas para MOSTRAR
                if semana_num < semana_actual:
                    # Semana pasada: usar ventas reales en cálculos y en tabla
                    ventas_reales_canal = ventas_reales_bd  # Para cálculos
                    ventas_reales_informativas = ventas_reales_bd  # Para mostrar
                else:
                    # Semana futura o en curso:
                    # - Para CÁLCULOS: usar 0 (no afecta redistribución)
                    # - Para MOSTRAR: usar dato real de la BD (solo informativo)
                    ventas_reales_canal = 0  # ⚠️ Para cálculos (NO afecta redistribución)
                    ventas_reales_informativas = ventas_reales_bd  # ✅ Para mostrar en tabla

                # Paso 3.1: Calcular factor dinámico del canal basado en su bolsa disponible
                if suma_bolsas > 0:
                    factor_canal = canal_info['bolsa_disponible'] / suma_bolsas
                else:
                    factor_canal = 0

                # Paso 3.2: Calcular asignación del canal según su factor
                asignacion_teorica = inventario_semanal_total * factor_canal

                # Paso 3.3: Limitar por la bolsa disponible del canal
                asignacion_canal = min(asignacion_teorica, canal_info['bolsa_disponible'])

                # Paso 3.4: Redondear para evitar decimales
                asignacion_canal = round(asignacion_canal)

                # Paso 3.5: Actualizar la bolsa con las ventas reales (SIEMPRE, no solo semanas pasadas)
                # La bolsa se reduce con lo que realmente se vendió
                canal_info['bolsa_disponible'] = max(0, canal_info['bolsa_disponible'] - ventas_reales_canal)

                # Guardar para resultados
                row_canal = datos_canal_semana.iloc[0]
                asignaciones_semana.append({
                    'sku': sku,
                    'descripcion': row_canal['descripcion'],
                    'semana': int(semana_num),
                    'canal': canal,
                    'inventario_inicial': inventario_inicial_real,
                    'arribos_esperados': arribos,
                    'forecast_b2b': forecast_b2b,
                    'inventario_fisico': inventario_fisico,
                    'cupo_mensual_total': float(row_canal['cupo_mensual_total']),
                    'cupo_mensual_canal': canal_info['cupo_mensual'],
                    'cupo_canal_restante': canal_info['bolsa_disponible'],
                    'cupo_semanal_teorico_canal': asignacion_canal,
                    'asignacion_original': asignacion_canal,
                    'redistribucion_recibida': 0,
                    'fuente_redistribucion': None,
                    'asignacion_canal': asignacion_canal,
                    'ventas_reales': ventas_reales_canal,  # ⚠️ Para cálculos (0 si es futura)
                    'ventas_reales_informativas': ventas_reales_informativas,  # ✅ Para mostrar en tabla
                    'peso_canal': canal_info['peso_canal']
                })

            # Calcular asignación total de la semana (suma de asignaciones de todos los canales)
            asignacion_total_semana = sum(a['asignacion_canal'] for a in asignaciones_semana)

            # Calcular ventas totales de la semana
            # - ventas_reales: Para cálculos (0 si es semana futura/en curso)
            # - ventas_reales_informativas: Para mostrar en tabla (dato real de BD)
            ventas_totales_semana = sum(a['ventas_reales'] for a in asignaciones_semana)
            ventas_totales_informativas = sum(a['ventas_reales_informativas'] for a in asignaciones_semana)

            # Agregar asignacion_semana y ventas totales a cada registro
            for asig in asignaciones_semana:
                asig['asignacion_semana'] = asignacion_total_semana
                asig['ventas_totales_semana'] = ventas_totales_semana  # Para cálculos
                asig['ventas_totales_informativas'] = ventas_totales_informativas  # Para mostrar
                resultados.append(asig)

            # Inventario que queda para la siguiente semana
            # Solo importa lo que realmente se vendió, NO la asignación
            inventario_consumido = ventas_totales_semana
            inventario_disponible_semana_anterior = max(0, inventario_fisico - inventario_consumido)

            # IMPORTANTE: Acumular la asignación de esta semana para las siguientes
            # Solo acumular si es semana futura (>= semana_actual) porque las pasadas ya restaron ventas reales
            if semana_num >= semana_actual:
                asignaciones_acumuladas += asignacion_total_semana

    # Validación final: verificar que la suma de ASIGNACIONES coincida con el cupo mensual
    # Agrupar por SKU para validar
    df_resultado = pd.DataFrame(resultados)
    if not df_resultado.empty:
        validacion = df_resultado.groupby('sku').agg({
            'asignacion_canal': 'sum',
            'ventas_reales': 'sum',
            'cupo_mensual_total': 'first'
        }).reset_index()

        for _, row in validacion.iterrows():
            total_asignado = row['asignacion_canal']
            total_vendido = row['ventas_reales']
            cupo_mensual = row['cupo_mensual_total']
            diferencia_asignacion = abs(total_asignado - cupo_mensual)
            diferencia_ventas = cupo_mensual - total_vendido  # Lo que falta por vender

            # Tolerancia de 1% o 10 unidades (lo que sea mayor) por redondeos
            tolerancia = max(cupo_mensual * 0.01, 10)

            if diferencia_asignacion > tolerancia:
                print(f"WARNING [{row['sku']}]: Asignaciones ({total_asignado:.0f}) NO coinciden con cupo mensual ({cupo_mensual:.0f}) - Diferencia: {diferencia_asignacion:.0f}")
            else:
                print(f"OK [{row['sku']}]: Asignaciones={total_asignado:.0f}, Vendido={total_vendido:.0f}, Cupo={cupo_mensual:.0f}, Restante={diferencia_ventas:.0f}")

    return df_resultado




def guardar_distribucion_manual(sku, mes, distribuciones_canales, disponible_total_manual=0, disponible_total_automatico=0, usuario='sistema', comentario=''):
    """
    Guarda la distribución manual de un SKU para un mes específico

    Args:
        sku: Código del producto
        mes: Mes de la distribución (ej: 'Diciembre 2025')
        distribuciones_canales: Lista de diccionarios con formato:
            [
                {'canal': 'Mercado Libre', 'cupo_manual': 400, 'cupo_automatico': 350},
                {'canal': 'Amazon', 'cupo_manual': 300, 'cupo_automatico': 300},
                ...
            ]
        disponible_total_manual: Total disponible modificado manualmente
        disponible_total_automatico: Total disponible automático original
        usuario: Usuario que realiza la modificación
        comentario: Comentario opcional

    Returns:
        dict: {'success': bool, 'message': str, 'registros_insertados': int}
    """
    from database import get_db_connection

    client = get_db_connection()
    if not client:
        return {'success': False, 'message': 'No se pudo conectar a la base de datos', 'registros_insertados': 0}

    try:
        # Primero, desactivar cualquier distribución manual previa para este SKU y mes
        deactivate_query = f"""
        ALTER TABLE Silver.Distribucion_Mensual_Canal_Manual
        UPDATE activo = 0
        WHERE sku = '{sku}' AND mes = '{mes}'
        """

        client.command(deactivate_query)
        print(f"INFO: Distribuciones previas desactivadas para SKU {sku}, mes {mes}")

        # Calcular diferencia de totales disponibles
        diferencia_disponible_total = disponible_total_manual - disponible_total_automatico

        # Insertar las nuevas distribuciones
        registros_insertados = 0
        for dist in distribuciones_canales:
            canal = dist['canal']
            cupo_manual = float(dist['cupo_manual'])
            cupo_automatico = float(dist['cupo_automatico'])
            diferencia = cupo_manual - cupo_automatico

            insert_query = f"""
            INSERT INTO Silver.Distribucion_Mensual_Canal_Manual
            (sku, mes, Channel, cupo_manual, cupo_automatico, diferencia,
             disponible_total_manual, disponible_total_automatico, diferencia_disponible_total,
             usuario, comentario, activo)
            VALUES
            ('{sku}', '{mes}', '{canal}', {cupo_manual}, {cupo_automatico}, {diferencia},
             {disponible_total_manual}, {disponible_total_automatico}, {diferencia_disponible_total},
             '{usuario}', '{comentario}', 1)
            """

            client.command(insert_query)
            registros_insertados += 1

        print(f"OK: {registros_insertados} distribuciones manuales guardadas para SKU {sku}")

        return {
            'success': True,
            'message': f'Distribución manual guardada correctamente ({registros_insertados} canales)',
            'registros_insertados': registros_insertados
        }

    except Exception as e:
        print(f"ERROR: Error guardando distribución manual: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'message': f'Error al guardar: {str(e)}',
            'registros_insertados': 0
        }


def obtener_distribucion_por_sku_para_edicion(sku, mes):
    """
    Obtiene la distribución actual (manual o automática) de un SKU para edición

    Args:
        sku: Código del producto
        mes: Mes de la distribución

    Returns:
        dict: {
            'success': bool,
            'sku': str,
            'descripcion': str,
            'disponible_total': float,
            'canales': [
                {
                    'canal': str,
                    'cupo_automatico': float,
                    'cupo_manual': float or None,
                    'cupo_actual': float,
                    'peso_pct': float,
                    'es_manual': bool
                },
                ...
            ],
            'tiene_manual': bool
        }
    """
    from database import get_db_connection
    import pandas as pd

    client = get_db_connection()
    if not client:
        return {'success': False, 'message': 'No se pudo conectar a la base de datos'}

    try:
        query = f"""
        WITH
        dist_automatica AS (
            SELECT
                sku,
                any(descripcion) as descripcion,
                any(Disponible_Para_Vender) as disponible_total,
                Channel,
                Disponible_Para_Vender_Canal_FINAL as cupo_automatico,
                peso_combinado_normalizado
            FROM Silver.Distribucion_Mensual_Canal_Materializada
            WHERE sku = '{sku}'
            GROUP BY sku, Channel, Disponible_Para_Vender_Canal_FINAL, peso_combinado_normalizado
        ),

        dist_manual AS (
            SELECT
                sku,
                Channel,
                cupo_manual,
                cupo_automatico,
                disponible_total_manual
            FROM Silver.Distribucion_Mensual_Canal_Manual
            WHERE sku = '{sku}'
              AND mes = '{mes}'
              AND activo = 1
        ),

        canales_solo_manual AS (
            SELECT
                dm.sku,
                dm.Channel
            FROM dist_manual dm
            LEFT JOIN dist_automatica da ON dm.sku = da.sku AND dm.Channel = da.Channel
            WHERE da.sku IS NULL
        )

        SELECT
            da.sku AS sku,
            da.descripcion AS descripcion,
            da.disponible_total AS disponible_total,
            da.Channel AS Channel,
            da.cupo_automatico AS cupo_automatico,
            COALESCE(dm.cupo_manual, 0) AS cupo_manual,
            if(dm.sku != '', dm.cupo_manual, da.cupo_automatico) AS cupo_actual,
            da.peso_combinado_normalizado AS peso_combinado_normalizado,
            if(dm.sku != '', 1, 0) AS es_manual,
            COALESCE(dm.disponible_total_manual, 0) AS disponible_total_manual
        FROM dist_automatica da
        LEFT JOIN dist_manual dm ON da.sku = dm.sku AND da.Channel = dm.Channel

        UNION ALL

        SELECT
            dm.sku AS sku,
            COALESCE(da_desc.descripcion, 'Sin descripción') AS descripcion,
            COALESCE(da_desc.disponible_total, 0) AS disponible_total,
            dm.Channel AS Channel,
            COALESCE(dm.cupo_automatico, 0) AS cupo_automatico,
            dm.cupo_manual AS cupo_manual,
            dm.cupo_manual AS cupo_actual,
            0 AS peso_combinado_normalizado,
            1 AS es_manual,
            COALESCE(dm.disponible_total_manual, 0) AS disponible_total_manual
        FROM canales_solo_manual csm
        INNER JOIN dist_manual dm ON csm.sku = dm.sku AND csm.Channel = dm.Channel
        LEFT JOIN (SELECT sku, any(descripcion) as descripcion, any(disponible_total) as disponible_total FROM dist_automatica GROUP BY sku) da_desc ON dm.sku = da_desc.sku

        ORDER BY peso_combinado_normalizado DESC
        """

        result = client.query(query)
        df = pd.DataFrame(result.result_rows, columns=result.column_names)

        if df.empty:
            return {'success': False, 'message': f'No se encontró el SKU {sku}'}

        # Procesar resultados
        primera_fila = df.iloc[0]
        canales = []

        for _, row in df.iterrows():
            canales.append({
                'canal': row['Channel'],
                'cupo_automatico': float(row['cupo_automatico']),
                'cupo_manual': float(row['cupo_manual']) if row['cupo_manual'] > 0 else None,
                'cupo_actual': float(row['cupo_actual']),
                'peso_pct': float(row['peso_combinado_normalizado']) * 100,
                'es_manual': bool(row['es_manual'])
            })

        tiene_manual = any(c['es_manual'] for c in canales)

        # Obtener totales automático y manual
        disponible_total_automatico = float(primera_fila['disponible_total'])
        disponible_total_manual_db = float(primera_fila['disponible_total_manual'])
        disponible_total = disponible_total_manual_db if disponible_total_manual_db > 0 else disponible_total_automatico

        return {
            'success': True,
            'sku': sku,
            'descripcion': primera_fila['descripcion'],
            'disponible_total': disponible_total,
            'disponible_total_automatico': disponible_total_automatico,
            'canales': canales,
            'tiene_manual': tiene_manual
        }

    except Exception as e:
        print(f"ERROR: Error obteniendo distribución para edición: {e}")
        import traceback
        traceback.print_exc()
        return {'success': False, 'message': f'Error al obtener datos: {str(e)}'}


def revertir_a_distribucion_automatica(sku, mes):
    """
    Revierte la distribución manual a automática para un SKU y mes específico

    Args:
        sku: Código del producto
        mes: Mes de la distribución

    Returns:
        dict: {'success': bool, 'message': str}
    """
    from database import get_db_connection

    client = get_db_connection()
    if not client:
        return {'success': False, 'message': 'No se pudo conectar a la base de datos'}

    try:
        deactivate_query = f"""
        ALTER TABLE Silver.Distribucion_Mensual_Canal_Manual
        UPDATE activo = 0
        WHERE sku = '{sku}' AND mes = '{mes}'
        """

        client.command(deactivate_query)
        print(f"OK: Distribución manual revertida para SKU {sku}, mes {mes}")

        return {
            'success': True,
            'message': 'Distribución revertida a automática correctamente'
        }

    except Exception as e:
        print(f"ERROR: Error revirtiendo a automático: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'message': f'Error al revertir: {str(e)}'
        }


def crear_snapshot_mensual(mes_nombre):
    """
    Crea un snapshot completo de la distribución automática del mes en la tabla manual

    Esto sirve como histórico permanente del mes. Copia TODOS los registros de la tabla
    materializada (automática) a la tabla manual para preservar el histórico cuando
    la materializada se actualice al siguiente mes.

    Args:
        mes_nombre: Nombre del mes (ej: 'Diciembre 2025')

    Returns:
        dict: {
            'success': bool,
            'message': str,
            'estadisticas': {
                'total_skus': int,
                'total_registros': int,
                'total_disponible': float,
                'canales': list
            }
        }
    """
    from database import get_db_connection
    import pandas as pd

    client = get_db_connection()
    if not client:
        return {'success': False, 'message': 'No se pudo conectar a la base de datos'}

    try:
        print(f"INFO: [SNAPSHOT] Iniciando snapshot mensual para {mes_nombre}...")

        # Paso 1: Obtener registros existentes (modificaciones manuales previas)
        check_query = f"""
        SELECT sku, Channel
        FROM Silver.Distribucion_Mensual_Canal_Manual
        WHERE mes = '{mes_nombre}' AND activo = 1
        """

        result = client.query(check_query)
        registros_existentes_df = pd.DataFrame(result.result_rows, columns=['sku', 'Channel'])
        registros_existentes = len(registros_existentes_df)

        if registros_existentes > 0:
            print(f"INFO: [SNAPSHOT] Encontrados {registros_existentes} registros manuales existentes. Se completará el snapshot con registros faltantes.")
        else:
            print(f"INFO: [SNAPSHOT] No hay registros existentes. Se creará snapshot completo.")

        # Paso 2: Obtener todos los datos de la tabla materializada
        query_snapshot = """
        SELECT
            sku,
            descripcion,
            Channel,
            Disponible_Para_Vender,
            Disponible_Para_Vender_Canal_FINAL,
            peso_combinado_normalizado
        FROM Silver.Distribucion_Mensual_Canal_Materializada
        WHERE sku IS NOT NULL
          AND Disponible_Para_Vender_Canal_FINAL > 0
        ORDER BY sku, peso_combinado_normalizado DESC
        """

        result = client.query(query_snapshot)
        df = pd.DataFrame(result.result_rows, columns=result.column_names)

        if df.empty:
            return {
                'success': False,
                'message': 'No hay datos en la tabla materializada para crear el snapshot'
            }

        print(f"INFO: [SNAPSHOT] Datos obtenidos de materializada: {len(df)} registros de {df['sku'].nunique()} SKUs")

        # Paso 3: Filtrar registros que NO existen en la tabla manual
        if registros_existentes > 0:
            # Crear un identificador único para comparar
            df['sku_canal'] = df['sku'] + '|' + df['Channel']
            registros_existentes_df['sku_canal'] = registros_existentes_df['sku'] + '|' + registros_existentes_df['Channel']

            # Filtrar solo los que NO existen
            df_a_insertar = df[~df['sku_canal'].isin(registros_existentes_df['sku_canal'])].copy()
            df_a_insertar = df_a_insertar.drop(columns=['sku_canal'])

            print(f"INFO: [SNAPSHOT] Registros a insertar (faltantes): {len(df_a_insertar)}")
        else:
            df_a_insertar = df

        if df_a_insertar.empty:
            return {
                'success': True,
                'message': f'Snapshot ya completo para {mes_nombre}. No hay registros nuevos que agregar.',
                'estadisticas': {
                    'total_skus': df['sku'].nunique(),
                    'total_registros': registros_existentes,
                    'total_disponible': df.groupby('sku')['Disponible_Para_Vender'].first().sum(),
                    'canales': sorted(df['Channel'].unique().tolist()),
                    'registros_insertados': 0,
                    'registros_existentes': registros_existentes
                }
            }

        # Paso 4: Insertar solo los registros faltantes
        registros_insertados = 0

        for _, row in df_a_insertar.iterrows():
            sku = row['sku']
            channel = row['Channel']
            disponible_total = float(row['Disponible_Para_Vender'])
            cupo_canal = float(row['Disponible_Para_Vender_Canal_FINAL'])

            insert_query = f"""
            INSERT INTO Silver.Distribucion_Mensual_Canal_Manual
            (sku, mes, Channel, cupo_manual, cupo_automatico, diferencia,
             disponible_total_manual, disponible_total_automatico, diferencia_disponible_total,
             fecha_modificacion, usuario, comentario, activo)
            VALUES
            ('{sku}', '{mes_nombre}', '{channel}',
             {cupo_canal}, {cupo_canal}, 0,
             {disponible_total}, {disponible_total}, 0,
             now(), 'sistema', 'Snapshot automático del mes', 1)
            """

            client.command(insert_query)
            registros_insertados += 1

        # Paso 5: Calcular estadísticas finales
        total_skus = df['sku'].nunique()
        total_disponible = df.groupby('sku')['Disponible_Para_Vender'].first().sum()
        canales = sorted(df['Channel'].unique().tolist())
        total_registros_final = registros_existentes + registros_insertados

        mensaje = f'Snapshot completado para {mes_nombre}.'
        if registros_existentes > 0:
            mensaje += f' Se mantuvieron {registros_existentes} modificaciones manuales y se agregaron {registros_insertados} registros automáticos faltantes.'
        else:
            mensaje += f' Se crearon {registros_insertados} registros automáticos.'

        print(f"OK: [SNAPSHOT] {mensaje}")

        return {
            'success': True,
            'message': mensaje,
            'estadisticas': {
                'total_skus': int(total_skus),
                'total_registros': total_registros_final,
                'total_disponible': float(total_disponible),
                'canales': canales,
                'registros_insertados': registros_insertados,
                'registros_existentes': registros_existentes
            }
        }

    except Exception as e:
        print(f"ERROR: [SNAPSHOT] Error creando snapshot mensual: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'message': f'Error al crear snapshot: {str(e)}'
        }


def obtener_info_snapshot(mes_nombre):
    """
    Obtiene información del estado actual del snapshot para un mes

    Args:
        mes_nombre: Nombre del mes (ej: 'Diciembre 2025')

    Returns:
        dict: {
            'success': bool,
            'registros_manuales_existentes': int,
            'registros_totales_materializada': int,
            'registros_faltantes': int,
            'total_skus': int,
            'total_disponible': float,
            'tiene_modificaciones': bool
        }
    """
    from database import get_db_connection
    import pandas as pd

    client = get_db_connection()
    if not client:
        return {'success': False, 'message': 'No se pudo conectar a la base de datos'}

    try:
        print(f"INFO: [INFO SNAPSHOT] Obteniendo información para {mes_nombre}...")

        # Paso 1: Contar registros manuales existentes
        check_query = f"""
        SELECT COUNT(*) as total
        FROM Silver.Distribucion_Mensual_Canal_Manual
        WHERE mes = '{mes_nombre}' AND activo = 1
        """

        result = client.query(check_query)
        registros_manuales = result.result_rows[0][0]

        # Paso 2: Obtener info de la tabla materializada
        query_materializada = """
        SELECT
            COUNT(*) as total_registros,
            COUNT(DISTINCT sku) as total_skus,
            SUM(Disponible_Para_Vender_Canal_FINAL) as total_disponible
        FROM Silver.Distribucion_Mensual_Canal_Materializada
        WHERE sku IS NOT NULL
          AND Disponible_Para_Vender_Canal_FINAL > 0
        """

        result = client.query(query_materializada)
        row = result.result_rows[0]
        registros_materializada = row[0]
        total_skus = row[1]
        total_disponible = row[2]

        # Paso 3: Calcular registros faltantes
        registros_faltantes = max(0, registros_materializada - registros_manuales)

        print(f"INFO: [INFO SNAPSHOT] Registros manuales: {registros_manuales}, Materializada: {registros_materializada}, Faltantes: {registros_faltantes}")

        return {
            'success': True,
            'registros_manuales_existentes': registros_manuales,
            'registros_totales_materializada': registros_materializada,
            'registros_faltantes': registros_faltantes,
            'total_skus': total_skus,
            'total_disponible': float(total_disponible) if total_disponible else 0,
            'tiene_modificaciones': registros_manuales > 0
        }

    except Exception as e:
        print(f"ERROR: [INFO SNAPSHOT] Error obteniendo info: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'message': f'Error al obtener info: {str(e)}'
        }
