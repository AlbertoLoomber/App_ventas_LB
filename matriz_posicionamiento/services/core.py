"""
Core Functions - Matriz de Posicionamiento
Funciones básicas de clasificación y cálculo de métricas
"""


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
