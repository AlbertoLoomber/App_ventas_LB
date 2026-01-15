# -*- coding: utf-8 -*-
"""
Servicios del módulo de Ventas por Hora Mercado Libre
Lógica de negocio para consultar y analizar datos de ventas por hora
"""

import pandas as pd
from datetime import datetime, timedelta
from database import get_db_connection


def obtener_ventas_por_hora(fecha_inicio=None, fecha_fin=None, sku=None, channel=None):
    """
    Obtiene datos de ventas por hora desde Silver.VentaXhora_Meli

    Args:
        fecha_inicio: Fecha de inicio del período (datetime), None para últimos 7 días
        fecha_fin: Fecha fin del período (datetime), None para hoy
        sku: SKU específico o None para todos
        channel: Canal específico o None para todos (aunque la tabla solo tiene Meli)

    Returns:
        DataFrame: Datos de ventas por hora con columnas:
                   dia, Hora, sku, Channel, Cantidad_Total, Venta_Neta_Total,
                   Ticket_Mediana, Killer, Precio_cliente, Var_vs_Dia_Anterior_Porc
    """
    client = get_db_connection()

    if not client:
        print("ERROR: No se pudo conectar a ClickHouse")
        return pd.DataFrame()

    try:
        # Si no se especifican fechas, usar últimos 7 días
        if fecha_fin is None:
            fecha_fin = datetime.now().date()

        if fecha_inicio is None:
            fecha_inicio = fecha_fin - timedelta(days=7)

        # Convertir a string para la query
        fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d') if isinstance(fecha_inicio, (datetime, date)) else str(fecha_inicio)
        fecha_fin_str = fecha_fin.strftime('%Y-%m-%d') if isinstance(fecha_fin, (datetime, date)) else str(fecha_fin)

        # Construir query con filtros opcionales
        where_conditions = [
            f"dia >= '{fecha_inicio_str}'",
            f"dia <= '{fecha_fin_str}'"
        ]

        if sku:
            where_conditions.append(f"sku = '{sku}'")

        if channel:
            where_conditions.append(f"Channel = '{channel}'")

        where_clause = " AND ".join(where_conditions)

        query = f"""
        SELECT
            dia,
            Hora,
            sku,
            Channel,
            Cantidad_Total,
            Venta_Neta_Total,
            Ticket_Mediana,
            Killer,
            Precio_cliente,
            Var_vs_Dia_Anterior_Porc
        FROM Silver.VentaXhora_Meli
        WHERE {where_clause}
        ORDER BY dia DESC, Hora DESC, Venta_Neta_Total DESC
        """

        print(f"INFO: Consultando ventas por hora desde {fecha_inicio_str} hasta {fecha_fin_str}")

        result = client.query(query)

        # Convertir a DataFrame
        if result.result_rows:
            df = pd.DataFrame(
                result.result_rows,
                columns=['dia', 'Hora', 'sku', 'Channel', 'Cantidad_Total',
                        'Venta_Neta_Total', 'Ticket_Mediana', 'Killer',
                        'Precio_cliente', 'Var_vs_Dia_Anterior_Porc']
            )

            print(f"OK: {len(df)} registros cargados desde Silver.VentaXhora_Meli")
            return df
        else:
            print("WARNING: No se encontraron datos para el período especificado")
            return pd.DataFrame()

    except Exception as e:
        print(f"ERROR al consultar ventas por hora: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def obtener_skus_disponibles():
    """
    Obtiene la lista de SKUs únicos disponibles en la tabla VentaXhora_Meli
    con sus descripciones desde Gold.RPT_Inventarios

    Returns:
        list: Lista de diccionarios con formato: [{'sku': 'ABC123', 'descripcion': 'Descripción del producto'}, ...]
    """
    client = get_db_connection()

    if not client:
        return []

    try:
        # Query con JOIN para obtener descripción desde Gold.RPT_Inventarios
        query = """
        SELECT DISTINCT
            v.sku,
            any(i.descripcion) as descripcion
        FROM Silver.VentaXhora_Meli v
        LEFT JOIN Gold.RPT_Inventarios i ON toString(v.sku) = toString(i.sku)
        GROUP BY v.sku
        ORDER BY v.sku
        """

        result = client.query(query)

        if result.result_rows:
            # Retornar lista de diccionarios con sku y descripción
            skus_con_descripcion = []
            for row in result.result_rows:
                sku = row[0]
                descripcion = row[1] if row[1] else 'Sin descripción'
                skus_con_descripcion.append({
                    'sku': sku,
                    'descripcion': descripcion
                })

            print(f"INFO: {len(skus_con_descripcion)} SKUs únicos encontrados con descripción")
            return skus_con_descripcion
        else:
            return []

    except Exception as e:
        print(f"ERROR al obtener SKUs: {e}")
        import traceback
        traceback.print_exc()
        return []


def obtener_resumen_por_hora(fecha_inicio=None, fecha_fin=None):
    """
    Obtiene un resumen agregado de ventas por hora del día

    Args:
        fecha_inicio: Fecha de inicio del período
        fecha_fin: Fecha fin del período

    Returns:
        DataFrame: Resumen con columnas: Hora, Total_Cantidad, Total_Ventas, SKUs_Distintos
    """
    client = get_db_connection()

    if not client:
        return pd.DataFrame()

    try:
        # Si no se especifican fechas, usar últimos 7 días
        if fecha_fin is None:
            fecha_fin = datetime.now().date()

        if fecha_inicio is None:
            fecha_inicio = fecha_fin - timedelta(days=7)

        fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d') if isinstance(fecha_inicio, (datetime, date)) else str(fecha_inicio)
        fecha_fin_str = fecha_fin.strftime('%Y-%m-%d') if isinstance(fecha_fin, (datetime, date)) else str(fecha_fin)

        query = f"""
        SELECT
            Hora,
            SUM(Cantidad_Total) as Total_Cantidad,
            SUM(Venta_Neta_Total) as Total_Ventas,
            COUNT(DISTINCT sku) as SKUs_Distintos
        FROM Silver.VentaXhora_Meli
        WHERE dia >= '{fecha_inicio_str}'
          AND dia <= '{fecha_fin_str}'
        GROUP BY Hora
        ORDER BY Hora
        """

        result = client.query(query)

        if result.result_rows:
            df = pd.DataFrame(
                result.result_rows,
                columns=['Hora', 'Total_Cantidad', 'Total_Ventas', 'SKUs_Distintos']
            )
            print(f"OK: Resumen por hora calculado para {len(df)} horas")
            return df
        else:
            return pd.DataFrame()

    except Exception as e:
        print(f"ERROR al obtener resumen por hora: {e}")
        return pd.DataFrame()


def obtener_top_productos_por_hora(hora, fecha_inicio=None, fecha_fin=None, limit=10):
    """
    Obtiene los productos más vendidos en una hora específica

    Args:
        hora: Hora del día (0-23)
        fecha_inicio: Fecha de inicio del período
        fecha_fin: Fecha fin del período
        limit: Número máximo de productos a retornar

    Returns:
        DataFrame: Top productos con columnas: sku, Total_Ventas, Total_Cantidad
    """
    client = get_db_connection()

    if not client:
        return pd.DataFrame()

    try:
        # Si no se especifican fechas, usar últimos 7 días
        if fecha_fin is None:
            fecha_fin = datetime.now().date()

        if fecha_inicio is None:
            fecha_inicio = fecha_fin - timedelta(days=7)

        fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d') if isinstance(fecha_inicio, (datetime, date)) else str(fecha_inicio)
        fecha_fin_str = fecha_fin.strftime('%Y-%m-%d') if isinstance(fecha_fin, (datetime, date)) else str(fecha_fin)

        query = f"""
        SELECT
            sku,
            SUM(Venta_Neta_Total) as Total_Ventas,
            SUM(Cantidad_Total) as Total_Cantidad
        FROM Silver.VentaXhora_Meli
        WHERE dia >= '{fecha_inicio_str}'
          AND dia <= '{fecha_fin_str}'
          AND Hora = {hora}
        GROUP BY sku
        ORDER BY Total_Ventas DESC
        LIMIT {limit}
        """

        result = client.query(query)

        if result.result_rows:
            df = pd.DataFrame(
                result.result_rows,
                columns=['sku', 'Total_Ventas', 'Total_Cantidad']
            )
            return df
        else:
            return pd.DataFrame()

    except Exception as e:
        print(f"ERROR al obtener top productos por hora: {e}")
        return pd.DataFrame()


def obtener_datos_completos_sku(sku):
    """
    Obtiene TODOS los datos históricos de un SKU específico para análisis de precio-cantidad

    Args:
        sku: SKU del producto a analizar

    Returns:
        DataFrame: Todos los registros del SKU ordenados por fecha y hora con columnas:
                   dia, Hora, Cantidad_Total, Venta_Neta_Total, Precio_cliente,
                   Var_vs_Dia_Anterior_Porc, Killer, Ticket_Mediana
    """
    client = get_db_connection()

    if not client:
        print("ERROR: No se pudo conectar a ClickHouse")
        return pd.DataFrame()

    try:
        query = f"""
        SELECT
            dia,
            Hora,
            Cantidad_Total,
            Venta_Neta_Total,
            Precio_cliente,
            Var_vs_Dia_Anterior_Porc,
            Killer,
            Ticket_Mediana
        FROM Silver.VentaXhora_Meli
        WHERE sku = '{sku}'
        ORDER BY dia ASC, Hora ASC
        """

        print(f"INFO: Consultando datos completos para SKU {sku}")

        result = client.query(query)

        if result.result_rows:
            df = pd.DataFrame(
                result.result_rows,
                columns=['dia', 'Hora', 'Cantidad_Total', 'Venta_Neta_Total',
                        'Precio_cliente', 'Var_vs_Dia_Anterior_Porc', 'Killer', 'Ticket_Mediana']
            )

            # Crear columna de timestamp combinado para el eje X
            df['timestamp'] = pd.to_datetime(df['dia'].astype(str)) + pd.to_timedelta(df['Hora'], unit='h')

            print(f"OK: {len(df)} registros cargados para SKU {sku}")
            return df
        else:
            print(f"WARNING: No se encontraron datos para SKU {sku}")
            return pd.DataFrame()

    except Exception as e:
        print(f"ERROR al consultar datos completos para SKU {sku}: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
