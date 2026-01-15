"""
Script rápido para verificar si hay datos de Temu en la base de datos
"""
from database import get_db_connection

client = get_db_connection()

if not client:
    print("❌ No se pudo conectar a la base de datos")
    exit(1)

# Query para buscar el canal Temu (con variaciones)
query = """
SELECT
    Channel,
    COUNT(*) as registros,
    SUM(Ventas) as ventas_totales,
    MIN(Fecha) as fecha_min,
    MAX(Fecha) as fecha_max
FROM Silver.RPT_Ventas_Acumulado_Mensual_SKU_Canal_MT
WHERE Channel LIKE '%emu%'
GROUP BY Channel
ORDER BY registros DESC
"""

print("\n=== BÚSQUEDA DE TEMU EN LA BASE DE DATOS ===\n")
result = client.query(query)

if result.result_rows:
    print(f"✅ Encontrados {len(result.result_rows)} canales que contienen 'emu':\n")
    for row in result.result_rows:
        print(f"  Canal: '{row[0]}'")
        print(f"  Registros: {row[1]:,}")
        print(f"  Ventas totales: ${row[2]:,.2f}")
        print(f"  Período: {row[3]} a {row[4]}")
        print()
else:
    print("❌ No se encontró ningún canal con 'emu' en el nombre")
    print("\n📋 Listando TODOS los canales disponibles:\n")

    query_all = """
    SELECT DISTINCT Channel
    FROM Silver.RPT_Ventas_Acumulado_Mensual_SKU_Canal_MT
    ORDER BY Channel
    """

    result_all = client.query(query_all)
    for i, row in enumerate(result_all.result_rows, 1):
        print(f"  {i}. '{row[0]}'")

print("\n=== FIN DEL ANÁLISIS ===\n")
