"""
Matriz de Posicionamiento - Services Module
Central import point for all service functions

ARCHITECTURE NOTE:
Por ahora, este módulo actúa como wrapper del código legacy.
En el futuro, se puede refactorizar en submódulos especializados.
"""

# ============================================================================
# IMPORTS FROM LEGACY MODULE (Temporalmente)
# ============================================================================

# Importar TODAS las funciones del archivo legacy
from matriz_posicionamiento.services_legacy import (
    # Core functions
    clasificar_zona,
    calcular_metricas_canal,
    calcular_metricas_categoria,
    escalar_radio_burbuja,
    escalar_tamano_marcador,

    # Data filtering
    filtrar_por_mes,
    filtrar_por_rango_dias,

    # Matrix generators
    generar_datos_matriz,
    generar_datos_matriz_categorias,
    generar_datos_matriz_clasificacion,
    generar_datos_matriz_clasificacion_con_rango_dias,

    # SKU functions
    obtener_lista_skus
)

print("✅ [SERVICES] All functions imported from legacy module")

# ============================================================================
# EXPORT ALL
# ============================================================================

__all__ = [
    # Core functions
    'clasificar_zona',
    'calcular_metricas_canal',
    'calcular_metricas_categoria',
    'escalar_radio_burbuja',
    'escalar_tamano_marcador',

    # Data filtering
    'filtrar_por_mes',
    'filtrar_por_rango_dias',

    # Matrix generators
    'generar_datos_matriz',
    'generar_datos_matriz_categorias',
    'generar_datos_matriz_clasificacion',
    'generar_datos_matriz_clasificacion_con_rango_dias',

    # SKU functions
    'obtener_lista_skus'
]
