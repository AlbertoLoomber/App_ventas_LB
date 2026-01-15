# -*- coding: utf-8 -*-
"""
Blueprint de Cumplimiento BF
Define el blueprint para el módulo de cumplimiento de metas de SKUs BF
"""

from flask import Blueprint

bp = Blueprint(
    'cumplimiento_bf',
    __name__,
    template_folder='../templates',
    static_folder='../static'
)

# Importar rutas después de crear el blueprint para evitar imports circulares
from cumplimiento_bf import routes
