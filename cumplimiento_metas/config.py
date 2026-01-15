"""
Configuración de tipos de meta para el módulo de cumplimiento de metas.
Define las características de cada tipo de meta de manera centralizada.
"""

# Configuración de tipos de meta
TIPOS_META = {
    'ventas': {
        'nombre': 'Metas de Ventas',
        'icono': 'bi-currency-dollar',
        'campo_meta_mensual': 'Meta_Mensual',
        'campo_meta_acumulada': 'Meta_Acumulada',
        'campo_meta_diaria': 'Meta_Diaria',
        'campo_real': 'Ventas_Reales',
        'tipo_grafico': 'barometro',  # barómetro con aguja
        'unidad': 'currency',
        'es_porcentaje': False,
        'descripcion': 'Metas basadas en ventas totales en pesos'
    },
    'costo': {
        'nombre': 'Metas de Costo',
        'icono': 'bi-graph-down',
        'campo_real': 'Costo_Venta_Porcentaje',
        'rango_objetivo': (48, 54),
        'tipo_grafico': 'gauge',  # gauge de Plotly
        'unidad': 'percentage',
        'es_porcentaje': True,
        'descripcion': 'Metas basadas en porcentaje de costo de venta (rango objetivo 48-54%)'
    },
    'ingreso_real': {
        'nombre': 'Metas Ingreso Real',
        'icono': 'bi-cash-coin',
        'campo_real': 'Ingreso_Real_Porcentaje',
        'rango_objetivo': (10, 15),
        'tipo_grafico': 'gauge',
        'unidad': 'percentage',
        'es_porcentaje': True,
        'descripcion': 'Metas basadas en porcentaje de ingreso real (rango objetivo 10-15%)'
    },
    'ingreso_real_nominal': {
        'nombre': 'Metas Ingreso Real Nominal',
        'icono': 'bi-coin',
        'campo_meta_mensual': 'Meta_Ingreso_Real_Mensual',
        'campo_meta_acumulada': 'Meta_Ingreso_Real_Acumulada',
        'campo_meta_diaria': 'Meta_Ingreso_Real_Diaria',
        'campo_real': 'Ingreso_Real',
        'tipo_grafico': 'barometro',  # barómetro con aguja
        'unidad': 'currency',
        'es_porcentaje': False,
        'descripcion': 'Metas basadas en ingreso real en valores absolutos (pesos)'
    }
}

# Validación de tipos de meta
def validar_tipo_meta(tipo_meta):
    """
    Valida que el tipo de meta sea válido.

    Args:
        tipo_meta (str): Tipo de meta a validar

    Returns:
        bool: True si es válido, False en caso contrario
    """
    return tipo_meta in TIPOS_META

def obtener_config_meta(tipo_meta):
    """
    Obtiene la configuración de un tipo de meta.

    Args:
        tipo_meta (str): Tipo de meta

    Returns:
        dict: Configuración del tipo de meta

    Raises:
        ValueError: Si el tipo de meta no es válido
    """
    if not validar_tipo_meta(tipo_meta):
        raise ValueError(f"Tipo de meta inválido: '{tipo_meta}'. Tipos válidos: {list(TIPOS_META.keys())}")

    return TIPOS_META[tipo_meta]

def listar_tipos_meta():
    """
    Lista todos los tipos de meta disponibles.

    Returns:
        list: Lista de nombres de tipos de meta
    """
    return list(TIPOS_META.keys())
