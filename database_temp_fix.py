def get_radar_comercial_datos_semanales(mes_nombre=None, semana_num=None):
    """
    Obtiene datos semanales de inventario y ventas para el Radar Comercial

    IMPORTANTE: Solo retorna datos para SKUs relevantes del Radar Comercial
    (mismo filtro que get_radar_comercial_data)

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

        # Paso 1: Obtener lista de SKUs relevantes (mismo filtro que Radar Comercial)
        from database import get_db_connection
        client = get_db_connection()
        if not client:
            print("ERROR: [RADAR SEMANAL] No se pudo conectar a la base de datos")
            return pd.DataFrame()

        query_skus_relevantes = """
        SELECT sku
        FROM Silver.catalogo_productos_BF
        WHERE (producto_relevante = 1 AND descripcion NOT LIKE '%Midea%')
           OR sku IN ('1000065', '1000066', '1000067', '1000068', '1000069','2000097','2000096')
        """

        result = client.query(query_skus_relevantes)
        skus_relevantes = [row[0] for row in result.result_rows]

        print(f"INFO: [RADAR SEMANAL] SKUs relevantes encontrados: {len(skus_relevantes)}")

        if not skus_relevantes:
            print("WARN: [RADAR SEMANAL] No hay SKUs relevantes")
            return pd.DataFrame()

        # Paso 2: Obtener datos semanales completos
        from database import get_distribucion_semanal_inventario
        df_semanal = get_distribucion_semanal_inventario(mes_nombre)

        if df_semanal.empty:
            print(f"WARN: [RADAR SEMANAL] No hay datos semanales para {mes_nombre}")
            return pd.DataFrame()

        # Paso 3: Filtrar SOLO por SKUs relevantes del Radar Comercial
        df_semanal = df_semanal[df_semanal['sku'].isin(skus_relevantes)].copy()

        print(f"DEBUG: [RADAR SEMANAL] Datos después de filtrar por SKUs relevantes: {len(df_semanal)} registros")

        if df_semanal.empty:
            print(f"WARN: [RADAR SEMANAL] No hay datos para SKUs relevantes en {mes_nombre}")
            return pd.DataFrame()

        # Paso 4: Determinar semana actual si no se especifica
        if semana_num is None:
            semana_num = 1

        print(f"INFO: [RADAR SEMANAL] Obteniendo datos para {mes_nombre}, Semana {semana_num}")
        print(f"DEBUG: [RADAR SEMANAL] SKUs únicos en datos: {df_semanal['sku'].nunique()}")
        print(f"DEBUG: [RADAR SEMANAL] Semanas únicas en datos: {sorted(df_semanal['semana'].unique())}")

        # Paso 5: Filtrar por la semana específica
        df_semana = df_semanal[df_semanal['semana'] == semana_num].copy()

        if df_semana.empty:
            print(f"WARN: [RADAR SEMANAL] No hay datos para semana {semana_num}")
            return pd.DataFrame()

        # Paso 6: Preparar datos en formato para el Radar Comercial
        df_resultado = df_semana[['sku', 'canal', 'asignacion_canal', 'ventas_reales_informativas']].copy()
        df_resultado.columns = ['sku', 'canal', 'inv_asignado_semana', 'ventas_semana']

        print(f"OK: [RADAR SEMANAL] Datos cargados: {len(df_resultado)} registros para {df_resultado['sku'].nunique()} SKUs")

        return df_resultado

    except Exception as e:
        print(f"ERROR: [RADAR SEMANAL] Error obteniendo datos semanales: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
