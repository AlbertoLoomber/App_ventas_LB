# -*- coding: utf-8 -*-
"""
Blueprint para el módulo de Análisis de Rentabilidad
"""

from flask import Blueprint

# Crear blueprint sin prefijo para mantener la ruta /analisis-rentabilidad
bp = Blueprint(
    'analisis_rentabilidad',
    __name__,
    template_folder='../templates',
    static_folder='../static'
)

# Importar rutas para registrarlas en el blueprint
from analisis_rentabilidad import routes
