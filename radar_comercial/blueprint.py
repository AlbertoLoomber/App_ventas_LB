# -*- coding: utf-8 -*-
"""
Blueprint para el módulo de Radar Comercial
"""

from flask import Blueprint

# Crear blueprint sin prefijo para mantener la ruta /radar-comercial
bp = Blueprint(
    'radar_comercial',
    __name__,
    template_folder='../templates',
    static_folder='../static'
)

# Importar rutas para registrarlas en el blueprint
from radar_comercial import routes
