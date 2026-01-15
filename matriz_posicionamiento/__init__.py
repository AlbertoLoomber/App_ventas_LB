"""
Módulo de Matriz de Posicionamiento
Análisis de % Ingreso Real vs % ROI por Canal

HUB INTEGRATION MODULE
"""

# ============================================================================
# MODULE INITIALIZATION - HUB COMPATIBLE
# ============================================================================

print("\n" + "="*60)
print("STARTING: Loading matriz_posicionamiento module")
print("="*60)

# 1. Importar el blueprint desde blueprint.py
from matriz_posicionamiento.blueprint import bp, MODULE_INFO
print(f"[OK] Blueprint imported: {bp.name}")
print(f"   - url_prefix: {bp.url_prefix}")
print(f"   - template_folder: {bp.template_folder}")
print(f"   - static_folder: {bp.static_folder}")
print(f"   - version: {MODULE_INFO['version']}")

# 2. Importar routes (esto registra las rutas en el blueprint)
print("\nImporting routes module...")
try:
    import matriz_posicionamiento.routes
    print("[OK] Routes module imported successfully")
except ImportError as e:
    print(f"[ERROR] Error importing routes: {e}")
    raise

# 3. Verificar rutas registradas
print(f"\nRegistered routes on blueprint '{bp.name}':")
route_count = 0
for func in bp.deferred_functions:
    route_count += 1
    print(f"   - Deferred function #{route_count}: {func}")

print("\n" + "="*60)
print(f"[COMPLETED] matriz_posicionamiento module loaded")
print(f"   Total deferred functions: {route_count}")
print("="*60 + "\n")

# ============================================================================
# EXPORTS - Para compatibilidad con código legacy
# ============================================================================

# Mantener alias 'matriz_bp' para código existente
matriz_bp = bp

# Exportar para el Hub
__all__ = ['bp', 'matriz_bp', 'MODULE_INFO']
