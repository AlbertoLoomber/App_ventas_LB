# -*- coding: utf-8 -*-
"""
Blueprint de Cumplimiento de Metas
Define el blueprint para el módulo de cumplimiento de metas
"""

from flask import Blueprint

bp = Blueprint(
    'cumplimiento_metas',
    __name__,
    template_folder='../templates',
    static_folder='../static'
)

# Importar rutas después de crear el blueprint para evitar imports circulares
from cumplimiento_metas import routes
