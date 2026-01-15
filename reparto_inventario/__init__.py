# -*- coding: utf-8 -*-
"""
Módulo de Reparto de Inventario
Vista consultiva para encargados de canal
"""

from flask import Blueprint

# Crear Blueprint
bp = Blueprint('reparto_inventario', __name__)

# Importar rutas
from reparto_inventario import routes
