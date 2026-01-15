# -*- coding: utf-8 -*-
"""
Blueprint de Ventas por Hora Mercado Libre
Define el blueprint para el módulo de análisis de ventas por hora
"""

from flask import Blueprint

bp = Blueprint(
    'ventas_hora_meli',
    __name__,
    template_folder='../templates',
    static_folder='../static'
)

# Importar rutas después de crear el blueprint para evitar imports circulares
from ventas_hora_meli import routes
