"""
Diagnóstico rápido: verificar si los SKUs del Radar tienen datos semanales
"""
from database import get_distribucion_semanal_inventario
from datetime import datetime

# SKUs que aparecen en el Radar Comercial (según screenshot)
skus_radar = ['2000005', '2000013', '2000016', '2000020', '2000033']

# Obtener mes actual
meses_es = {
    1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
    5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
    9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
}
ahora = datetime.now()
mes_nombre = f"{meses_es[ahora.month]} {ahora.year}"

print(f"\n=== DIAGNÓSTICO RÁPIDO ===")
print(f"Mes: {mes_nombre}")
print(f"SKUs a buscar: {skus_radar}")

# Obtener datos semanales
print(f"\n1. Llamando a get_distribucion_semanal_inventario('{mes_nombre}')...")
df_semanal = get_distribucion_semanal_inventario(mes_nombre)

print(f"\n2. Resultado:")
print(f"   - Total registros: {len(df_semanal)}")
if not df_semanal.empty:
    print(f"   - SKUs únicos: {df_semanal['sku'].nunique()}")
    print(f"   - Columnas: {df_semanal.columns.tolist()}")
    print(f"   - Canales únicos: {sorted(df_semanal['canal'].unique())}")
    print(f"   - Semanas únicas: {sorted(df_semanal['semana'].unique())}")

    # Ver si están los SKUs del Radar
    skus_encontrados = df_semanal[df_semanal['sku'].isin(skus_radar)]
    print(f"\n3. SKUs del Radar encontrados: {len(skus_encontrados)} registros")
    print(f"   - SKUs únicos encontrados: {sorted(skus_encontrados['sku'].unique())}")

    if not skus_encontrados.empty:
        print(f"\n4. Datos para Semana 1:")
        semana1 = skus_encontrados[skus_encontrados['semana'] == 1]
        print(f"   - Registros en Semana 1: {len(semana1)}")

        if not semana1.empty:
            print(f"\n   Ejemplo de datos (primeras 10 filas):")
            print(semana1[['sku', 'canal', 'asignacion_canal', 'ventas_reales_informativas']].head(10).to_string())
        else:
            print("   ❌ No hay datos para Semana 1")
            print(f"   Semanas disponibles para estos SKUs: {sorted(skus_encontrados['semana'].unique())}")
    else:
        print(f"\n   ❌ NINGÚN SKU del Radar encontrado en los datos semanales")
        print(f"\n   SKUs que SÍ están en datos semanales (primeros 10):")
        print(f"   {sorted(df_semanal['sku'].unique())[:10]}")
else:
    print("   ❌ No hay datos semanales para este mes")

print("\n=== FIN DEL DIAGNÓSTICO ===\n")
