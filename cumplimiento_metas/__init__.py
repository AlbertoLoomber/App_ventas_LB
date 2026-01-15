"""
Módulo de cumplimiento de metas.
Proporciona funcionalidad modular para calcular y analizar metas por canal.
"""

from cumplimiento_metas.blueprint import bp

from .config import (
    TIPOS_META,
    validar_tipo_meta,
    obtener_config_meta,
    listar_tipos_meta
)

from .strategies import (
    MetaStrategy,
    MetaAbsolutaStrategy,
    MetaRangoStrategy,
    get_strategy
)

from .calculators import (
    procesar_metas_por_tipo,
    calcular_metricas_canal,
    calcular_meta_total_mes
)

__all__ = [
    # Blueprint
    'bp',
    # Config
    'TIPOS_META',
    'validar_tipo_meta',
    'obtener_config_meta',
    'listar_tipos_meta',
    # Strategies
    'MetaStrategy',
    'MetaAbsolutaStrategy',
    'MetaRangoStrategy',
    'get_strategy',
    # Calculators
    'procesar_metas_por_tipo',
    'calcular_metricas_canal',
    'calcular_meta_total_mes',
]

__version__ = '1.0.0'
