# -*- coding: utf-8 -*-
"""
Configuración global de la aplicación
Constantes, configuraciones de base de datos y timezone
"""

import pytz

# ====== CONFIGURACIÓN DE CLICKHOUSE ======
CLICKHOUSE_CONFIG = {
    'host': 'tz0ze7bz6b.us-east1.gcp.clickhouse.cloud',
    'user': 'Carlos',
    'password': 'SuperSecreto123!',
    'secure': True,
    'database': 'Silver'
}

# ====== CONSTANTES DE NEGOCIO ======

# Canales específicos para clasificación
CANALES_CLASIFICACION = [
    'CrediTienda', 'Yuhu', 'Walmart', 'Mercado Libre',
    'Shein', 'Liverpool', 'Aliexpress', 'Coppel', 'TikTok Shop', 'Temu'
]

# Nombres de meses en español
MESES_ESPANOL = {
    1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
    5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
    9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
}

# Nombres de meses en español (lowercase para formato_periodo_texto)
MESES_ESPANOL_LOWER = {
    1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
    5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
    9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"
}

# ====== CONFIGURACIÓN DE TIMEZONE ======
MAZATLAN_TZ = pytz.timezone("America/Mazatlan")

# ====== CLASIFICACIÓN DE SKUs ======

# Umbrales de clasificación por ventas mensuales
UMBRALES_CLASIFICACION = {
    'estrella': 500,      # ≥ 500 ventas/mes
    'prometedores': 100,  # 100-499 ventas/mes
    'potenciales': 30,    # 30-99 ventas/mes
    'revision': 10        # 10-29 ventas/mes
    # < 10 ventas/mes = Remover
}

# Colores por clasificación
COLORES_CLASIFICACION = {
    'Estrella': '#D4AF37',      # Dorado
    'Prometedores': '#28a745',  # Verde
    'Potenciales': '#17a2b8',   # Azul
    'Revisión': '#fd7e14',      # Naranja
    'Remover': '#e63946'        # Rojo intenso
}

# Orden de prioridad de clasificación
ORDEN_CLASIFICACION = {
    'Estrella': 1,
    'Prometedores': 2,
    'Potenciales': 3,
    'Revisión': 4,
    'Remover': 5
}

# Mapeo de nomenclatura ClickHouse -> Python
MAPEO_CLASIFICACIONES = {
    'Estrellas': 'Estrella',
    'Prometedores': 'Prometedores',
    'Potenciales': 'Potenciales',
    'Revision': 'Revision',
    'Remover': 'Remover'
}
