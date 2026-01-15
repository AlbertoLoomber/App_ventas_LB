# -*- coding: utf-8 -*-
"""
Aplicación Flask - Dashboard de Ventas Loomber
Aplicación modularizada con Blueprints

Estructura:
- config.py: Configuración global
- database.py: Conexiones a ClickHouse
- utils.py: Utilidades compartidas
- radar_comercial/: Módulo de Radar Comercial (análisis de competencia)
- clasificacion/: Módulo de Clasificación de SKUs
- analisis_ventas/: Módulo de Análisis de Ventas (Dashboard principal)
- analisis_rentabilidad/: Módulo de Control de Ingreso Real
- cumplimiento_metas/: Módulo de Metas y Cumplimiento
- matriz_posicionamiento/: Módulo de Matriz de Posicionamiento
"""

import os
import sys

# ====== CONFIGURACIÓN DE ENCODING UTF-8 ======
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.detach())
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.detach())

from flask import Flask
from config import MAZATLAN_TZ

# ====== CREAR APLICACIÓN FLASK ======
app = Flask(__name__)

print("=" * 70)
print("INICIANDO APLICACIÓN FLASK - DASHBOARD DE VENTAS LOOMBER")
print("=" * 70)

# ====== REGISTRAR BLUEPRINTS ======
print("\n📦 Registrando módulos (Blueprints)...")

# 1. Módulo de Radar Comercial
try:
    from radar_comercial import bp as radar_comercial_bp
    app.register_blueprint(radar_comercial_bp)
    print("✅ Radar Comercial registrado en /radar-comercial")
except Exception as e:
    print(f"❌ Error registrando radar_comercial: {e}")

# 2. Módulo de Clasificación de SKUs
try:
    from clasificacion import bp as clasificacion_bp
    app.register_blueprint(clasificacion_bp)
    print("✅ Clasificación de SKUs registrado en /clasificacion, /clasificacion-skus, /clasificacion-ajax")
except Exception as e:
    print(f"❌ Error registrando clasificacion: {e}")

# 3. Módulo de Análisis de Ventas (Dashboard principal)
try:
    from analisis_ventas import bp as analisis_ventas_bp
    app.register_blueprint(analisis_ventas_bp)
    print("✅ Análisis de Ventas registrado en /")
except Exception as e:
    print(f"❌ Error registrando analisis_ventas: {e}")

# 4. Módulo de Análisis de Rentabilidad (Control de Ingreso Real)
try:
    from analisis_rentabilidad import bp as analisis_rentabilidad_bp
    app.register_blueprint(analisis_rentabilidad_bp)
    print("✅ Análisis de Rentabilidad registrado en /analisis-rentabilidad")
except Exception as e:
    print(f"❌ Error registrando analisis_rentabilidad: {e}")

# 5. Módulo de Matriz de Posicionamiento (ya existe)
try:
    from matriz_posicionamiento import bp as matriz_bp
    app.register_blueprint(matriz_bp, url_prefix='/matriz-posicionamiento')
    print("✅ Matriz de Posicionamiento registrado en /matriz-posicionamiento")
except Exception as e:
    print(f"❌ Error registrando matriz_posicionamiento: {e}")

# 6. Módulo de Cumplimiento de Metas
try:
    from cumplimiento_metas import bp as cumplimiento_metas_bp
    app.register_blueprint(cumplimiento_metas_bp)
    print("✅ Cumplimiento de Metas registrado en /cumplimiento-metas, /cumplimiento-metas-ajax, /cumplimiento-metas-actualizar")
except Exception as e:
    print(f"❌ Error registrando cumplimiento_metas: {e}")

# 7. Módulo de Cumplimiento BF (Black Friday - SKUs)
# MÓDULO REMOVIBLE: Puede ser eliminado sin afectar el código base
try:
    from cumplimiento_bf import bp as cumplimiento_bf_bp
    app.register_blueprint(cumplimiento_bf_bp)
    print("✅ Cumplimiento BF registrado en /cumplimiento-bf, /cumplimiento-bf-ajax")
except Exception as e:
    print(f"❌ Error registrando cumplimiento_bf: {e}")

# 8. Módulo de Ventas por Hora Mercado Libre
try:
    from ventas_hora_meli import bp as ventas_hora_meli_bp
    app.register_blueprint(ventas_hora_meli_bp)
    print("✅ Ventas por Hora Meli registrado en /ventas-hora-meli, /ventas-hora-meli-ajax")
except Exception as e:
    print(f"❌ Error registrando ventas_hora_meli: {e}")

# 9. Módulo de Distribución de Inventario
# MÓDULO REMOVIBLE: Puede ser eliminado sin afectar el código base
try:
    from distribucion_inventario import bp as distribucion_inventario_bp
    app.register_blueprint(distribucion_inventario_bp)
    print("✅ Distribución de Inventario registrado en /distribucion-inventario, /distribucion-inventario-datos, /distribucion-inventario-semanal")
except Exception as e:
    print(f"❌ Error registrando distribucion_inventario: {e}")

# 10. Módulo de Reparto de Inventario
# Vista consultiva para encargados de canal
try:
    from reparto_inventario import bp as reparto_inventario_bp
    app.register_blueprint(reparto_inventario_bp)
    print("✅ Reparto de Inventario registrado en /reparto-inventario, /reparto-inventario-datos, /reparto-inventario-consolidado, /reparto-inventario-metricas-canal")
except Exception as e:
    print(f"❌ Error registrando reparto_inventario: {e}")

# ====== RUTAS LEGACY QUE AÚN NO HAN SIDO MODULARIZADAS ======
# Estas rutas se mantendrán aquí temporalmente hasta completar la modularización

from database import analizar_estructura_tabla_fuente


# Ruta de debug de estructura (útil para desarrollo)
@app.route("/debug-estructura")
def debug_estructura_tabla():
    """Ruta temporal para analizar la estructura de la tabla fuente"""
    try:
        resultado = analizar_estructura_tabla_fuente()
        if resultado:
            columns_info, column_names = resultado
            return f"""
            <h1>Estructura de Silver.RPT_Ventas_Con_Costo_Prueba</h1>
            <h2>Campos Disponibles ({len(columns_info)}):</h2>
            <pre>{"<br>".join([f"{i:2d}. {col[0]:<25} | {col[1]:<20}" for i, col in enumerate(columns_info, 1)])}</pre>

            <h2>Nombres de Columnas ({len(column_names)}):</h2>
            <pre>{", ".join(column_names)}</pre>

            <p><a href="/">Volver al inicio</a></p>
            """
        else:
            return "<h1>Error al obtener estructura</h1><p><a href='/'>Volver</a></p>"
    except Exception as e:
        return f"<h1>Error: {e}</h1><p><a href='/'>Volver</a></p>"


# ====== CONFIGURACIÓN DE DATOS FRESCOS ======
print("\n" + "=" * 70)
print("CONFIGURACIÓN COMPLETADA")
print("=" * 70)
print("✅ Aplicación configurada para cargar datos frescos en cada request")
print("✅ Sin variables globales - datos siempre actualizados desde ClickHouse")
print("=" * 70)

# ====== PUNTO DE ENTRADA ======
if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
