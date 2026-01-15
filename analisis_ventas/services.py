# -*- coding: utf-8 -*-
"""
Servicios para el módulo de Análisis de Ventas
Lógica de negocio para rankings, métricas y resúmenes
"""

import pandas as pd


# ====== CONFIGURACIÓN DE PRODUCTOS UNIFICADOS ======
# Diccionario que mapea SKU principal -> lista de SKUs gemelos
# SOLO afecta la tabla del ranking en el index, NO modifica datos brutos
PRODUCTOS_UNIFICADOS = {
    '2000013': ['2000054', '2000057', '2000033'],  # Producto 1 + sus gemelos
    '2000005': ['2000032'],                        # Producto 2 + su gemelo
    '9900157': ['2000059'],                        # Producto 3 + su gemelo
    '2000020': ['2000078']                         # Producto 4 + su gemelo
}


# ====== FUNCIONES DE CÁLCULO DE TOP SKUs ======

def calcular_top_skus(df_filtrado, limite=None, channels=None, warehouses=None, skus=None):
    """
    Calcula el ranking completo de SKUs con ventas ordenados por unidades (descendente) - CON FILTROS APLICADOS

    Args:
        df_filtrado: DataFrame con datos de ventas
        limite: Número máximo de SKUs a retornar (opcional)
        channels: Lista de canales para filtrar (opcional)
        warehouses: Lista de bodegas para filtrar (opcional)
        skus: Lista de SKUs específicos para filtrar (opcional)

    Returns:
        list: Lista de diccionarios con ranking de SKUs
    """
    if df_filtrado.empty:
        print("DEBUG: DataFrame está vacío")
        return []

    print(f"DEBUG: DataFrame original tiene {len(df_filtrado)} filas")
    print(f"DEBUG: Estados únicos: {df_filtrado['estado'].unique()}")
    print(f"DEBUG: Primeras 3 filas de cantidad: {df_filtrado['cantidad'].head(3).tolist()}")

    # Filtrar solo órdenes de venta (excluir cancelados)
    df_activo = df_filtrado[df_filtrado["estado"] == "Orden de Venta"]

    if df_activo.empty:
        print("DEBUG: No hay órdenes de venta después del filtro")
        return []

    print(f"DEBUG: Después del filtro de estado: {len(df_activo)} filas")

    # APLICAR FILTROS ADICIONALES
    if channels:
        df_activo = df_activo[df_activo["Channel"].isin(channels)]
        print(f"DEBUG: Después del filtro de canales: {len(df_activo)} filas")

    if warehouses:
        df_activo = df_activo[df_activo["Warehouse"].isin(warehouses)]
        print(f"DEBUG: Después del filtro de bodegas: {len(df_activo)} filas")

    if skus:
        df_activo = df_activo[df_activo["sku"].isin(skus)]
        print(f"DEBUG: Después del filtro de SKUs: {len(df_activo)} filas")

    if df_activo.empty:
        print("DEBUG: No quedan datos después de aplicar filtros")
        return []

    # Verificar tipos de datos
    print(f"DEBUG: Tipo de dato en columna cantidad: {df_activo['cantidad'].dtype}")
    print(f"DEBUG: Ejemplo de valores cantidad: {df_activo['cantidad'].head(10).tolist()}")

    # Convertir cantidad a numérico si es necesario
    df_activo = df_activo.copy()
    df_activo['cantidad'] = pd.to_numeric(df_activo['cantidad'], errors='coerce')
    df_activo = df_activo.dropna(subset=['cantidad'])

    print(f"DEBUG: Después de limpiar cantidad: {len(df_activo)} filas")

    # Agrupar por SKU y descripción, sumar cantidades y totales
    top_skus = df_activo.groupby(['sku', 'descripcion']).agg({
        'cantidad': 'sum',      # Total unidades vendidas (suma de todas las cantidades)
        'Total': 'sum'          # Total monto vendido (suma de todos los montos)
    }).reset_index()

    print(f"DEBUG: Después de agrupar: {len(top_skus)} SKUs únicos")
    print(f"DEBUG: Top 3 cantidades: {top_skus.nlargest(3, 'cantidad')[['sku', 'cantidad']].to_dict('records')}")

    # Ordenar por cantidad (número de unidades) descendente
    top_skus = top_skus.sort_values('cantidad', ascending=False)
    if limite:
        top_skus = top_skus.head(limite)

    # Convertir a lista de diccionarios para el template
    resultado = []
    for _, row in top_skus.iterrows():
        unidades = int(row['cantidad'])
        monto = float(row['Total'])
        precio_promedio = monto / unidades if unidades > 0 else 0

        resultado.append({
            'sku': row['sku'],
            'descripcion': row['descripcion'],
            'unidades': unidades,           # Total de unidades vendidas
            'monto': monto,                 # Total monto vendido
            'precio_promedio': precio_promedio  # Precio promedio por unidad
        })

    print(f"DEBUG: Resultado final: {len(resultado)} SKUs")
    if resultado:
        print(f"DEBUG: Primer SKU: {resultado[0]}")

    return resultado


def unificar_productos_para_ranking(lista_skus):
    """
    Unifica productos gemelos SOLO para el ranking del index.
    NO modifica datos brutos ni afecta otras funcionalidades.

    Args:
        lista_skus: Lista de diccionarios con SKUs individuales

    Returns:
        Lista de diccionarios con productos unificados
    """
    if not lista_skus:
        return []

    print(f"DEBUG: Unificando productos. SKUs originales: {len(lista_skus)}")

    # Crear diccionario inverso para mapeo rápido: sku_gemelo -> sku_principal
    mapeo_inverso = {}
    for sku_principal, gemelos in PRODUCTOS_UNIFICADOS.items():
        mapeo_inverso[sku_principal] = sku_principal  # El principal se mapea a sí mismo
        for gemelo in gemelos:
            mapeo_inverso[gemelo] = sku_principal

    # Agrupar SKUs por producto unificado
    productos_agrupados = {}
    skus_no_unificados = []

    for sku_data in lista_skus:
        sku_actual = sku_data['sku']

        if sku_actual in mapeo_inverso:
            # Este SKU pertenece a un producto unificado
            sku_principal = mapeo_inverso[sku_actual]

            if sku_principal not in productos_agrupados:
                # Primera vez que vemos este producto, usar datos del SKU principal o el primero que encontremos
                productos_agrupados[sku_principal] = {
                    'sku': sku_principal,
                    'descripcion': sku_data['descripcion'],
                    'unidades': sku_data['unidades'],
                    'monto': sku_data['monto'],
                    'skus_componentes': [sku_actual],
                    'precio_promedio': sku_data['precio_promedio']
                }
            else:
                # Ya existe, sumar las métricas
                productos_agrupados[sku_principal]['unidades'] += sku_data['unidades']
                productos_agrupados[sku_principal]['monto'] += sku_data['monto']
                productos_agrupados[sku_principal]['skus_componentes'].append(sku_actual)

                # Recalcular precio promedio
                total_unidades = productos_agrupados[sku_principal]['unidades']
                total_monto = productos_agrupados[sku_principal]['monto']
                productos_agrupados[sku_principal]['precio_promedio'] = total_monto / total_unidades if total_unidades > 0 else 0
        else:
            # SKU no está en la lista de unificación, mantener como está
            skus_no_unificados.append(sku_data)

    # Combinar productos unificados con los no unificados
    resultado_final = list(productos_agrupados.values()) + skus_no_unificados

    # Reordenar por unidades vendidas (descendente)
    resultado_final.sort(key=lambda x: x['unidades'], reverse=True)

    print(f"DEBUG: Productos después de unificar: {len(resultado_final)}")
    print(f"DEBUG: Productos unificados: {len(productos_agrupados)}")

    return resultado_final


# ====== FUNCIONES DE RESUMEN Y MÉTRICAS ======

def resumen_periodo(df_periodo, df_comparado, granularidad=None):
    """
    Genera resumen de métricas comparando dos períodos

    Args:
        df_periodo: DataFrame del período principal
        df_comparado: DataFrame del período de comparación
        granularidad: Granularidad temporal (opcional, no se usa actualmente)

    Returns:
        list: Lista de diccionarios con métricas comparativas
    """
    resumen = []

    # Ventas brutas
    total_main = df_periodo["Total"].sum()
    total_compare = df_comparado["Total"].sum()
    delta = total_main - total_compare
    pct = (delta / total_compare * 100) if total_compare else 0
    resumen.append({
        "label": "Ventas brutas",
        "valor": f"${total_main:,.0f}",
        "diferencia": f"{'▲' if pct >= 0 else '▼'} {abs(pct):.1f} %",
        "delta": pct
    })

    # Cancelaciones
    canc_main = df_periodo[df_periodo["estado"] == "Cancelado"]["Total"].sum()
    canc_compare = df_comparado[df_comparado["estado"] == "Cancelado"]["Total"].sum()
    delta = canc_main - canc_compare
    pct = (delta / canc_compare * 100) if canc_compare else 0
    resumen.append({
        "label": "Cancelaciones",
        "valor": f"${canc_main:,.0f}",
        "diferencia": f"{'▲' if pct >= 0 else '▼'} {abs(pct):.1f} %",
        "delta": -pct  # Invertir: menos cancelaciones = mejor
    })

    # Ingreso Neto
    net_main = df_periodo[df_periodo["estado"] != "Cancelado"]["Total"].sum()
    net_compare = df_comparado[df_comparado["estado"] != "Cancelado"]["Total"].sum()
    delta = net_main - net_compare
    pct = (delta / net_compare * 100) if net_compare else 0
    resumen.append({
        "label": "Ingreso Neto",
        "valor": f"${net_main:,.0f}",
        "diferencia": f"{'▲' if pct >= 0 else '▼'} {abs(pct):.1f} %",
        "delta": pct
    })

    return resumen
