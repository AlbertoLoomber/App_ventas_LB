"""
Blueprint Definition for Matriz de Posicionamiento Module
Configuración del blueprint para integración con el Hub
"""

from flask import Blueprint

# ============================================================================
# BLUEPRINT CONFIGURATION
# ============================================================================

bp = Blueprint(
    'matriz_posicionamiento',                    # ✅ Nombre único del módulo
    __name__,
    template_folder='../templates/matriz_posicionamiento',  # ✅ Relativo a este archivo
    static_folder='../static',                   # ✅ Carpeta de archivos estáticos
    static_url_path='/static/matriz-posicionamiento',  # ✅ URL única para evitar conflictos
    url_prefix='/matriz-posicionamiento'         # ✅ Prefijo de todas las rutas
)

# ============================================================================
# METADATA
# ============================================================================

MODULE_INFO = {
    'name': 'Matriz de Posicionamiento',
    'version': '1.0.0',
    'description': 'Análisis de % Ingreso Real vs % ROI por Canal, Categoría y SKU',
    'author': 'Tu Nombre',
    'requires_database': True,
    'database_type': 'clickhouse'
}

# ============================================================================
# EXPORTS
# ============================================================================

__all__ = ['bp', 'MODULE_INFO']
