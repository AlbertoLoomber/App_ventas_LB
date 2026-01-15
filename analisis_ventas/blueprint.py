# -*- coding: utf-8 -*-
"""
Blueprint para el módulo de Análisis de Ventas
"""

from flask import Blueprint

# Crear blueprint sin prefijo para mantener la ruta /
bp = Blueprint(
    'analisis_ventas',
    __name__,
    template_folder='../templates',
    static_folder='../static'
)

# Importar rutas para registrarlas en el blueprint
from analisis_ventas import routes
