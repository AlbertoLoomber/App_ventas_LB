# -*- coding: utf-8 -*-
"""
Blueprint para el módulo de Clasificación
"""

from flask import Blueprint

# Crear blueprint sin prefijo para mantener la ruta /clasificacion
bp = Blueprint(
    'clasificacion',
    __name__,
    template_folder='../templates',
    static_folder='../static'
)

# Importar rutas para registrarlas en el blueprint
from clasificacion import routes
