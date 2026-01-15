# -*- coding: utf-8 -*-
"""
Blueprint de Distribución de Inventario
Define el blueprint para el módulo de distribución de inventario entre canales
"""

from flask import Blueprint

bp = Blueprint(
    'distribucion_inventario',
    __name__,
    template_folder='../templates',
    static_folder='../static'
)

# Importar rutas después de crear el blueprint para evitar imports circulares
from distribucion_inventario import routes
