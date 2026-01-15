# -*- coding: utf-8 -*-
"""
Servicios del módulo de Cumplimiento de Metas
Lógica de negocio para el cálculo de cumplimiento de metas
"""

import sys
import pandas as pd
from datetime import date

from config import CANALES_CLASIFICACION
from cumplimiento_metas.calculators import (
    procesar_metas_por_tipo,
    calcular_metricas_canal,
    calcular_meta_total_mes
)
from cumplimiento_metas.config import validar_tipo_meta
from utils import crear_gauge_costo_config, crear_gauge_ingreso_config


def get_default_resumen_general():
    """Retorna un diccionario con valores por defecto para resumen_general"""
    return {
        'ventas_totales': 0,
        'costo_venta_porcentaje': 0,
        'evolucion_costo': [],
        'evolucion_ventas': [],
        'evolucion_ingreso': [],
        'evolucion_roi': [],
        'gastos_directos': 0,
        'gastos_directos_porcentaje': 0,
        # DESGLOSE INDIVIDUAL DE GASTOS DIRECTOS
        'comision_periodo': 0,
        'destino_periodo': 0,
        'milla_periodo': 0,
        # PORCENTAJES INDIVIDUALES DE CADA COMPONENTE
        'comision_porcentaje': 0,
        'destino_porcentaje': 0,
        'milla_porcentaje': 0,
        'ingreso_real': 0,
        # NUEVOS CAMPOS PARA TARJETAS DINÁMICAS
        'ventas_periodo_total': 0,  # Total de ventas del período (todos los canales)
        'ingreso_real_periodo_total': 0,  # Total de ingreso real del período (todos los canales)
        'num_transacciones': 0,
        'precio_promedio_ventas': 0,
        'precio_promedio_costo': 0,
        'precio_promedio_gastos': 0,
        'precio_promedio_ingreso': 0,
        'cantidad_total_unidades': 0,
        'meta_total': 0,
        'meta_total_mes': 0,
        'cumplimiento_vs_meta_mes': 0,
        'cumplimiento_global': 0,
        # NUEVOS CAMPOS DE COMPARACIÓN CON PERÍODO ANTERIOR
        'ingreso_real_porcentaje': 0,
        'ingreso_real_porcentaje_anterior': 0,
        'variacion_ingreso_pct': 0,
        'periodo_comparacion': 'Comparación con período anterior',
        'dias_comparados': 0,
        # ✅ NUEVO: Campo ROI
        'roi_promedio': 0.0,
        'roi_promedio_anterior': 0.0,
        'variacion_roi_pct': 0.0
    }


def calcular_cumplimiento_metas(df_ventas, df_metas, f1, f2, tipo_meta="ventas", df_completo=None, skip_preprocessing=False):
    """Calcula cumplimiento vs metas por canal para un período específico

    Args:
        tipo_meta: 'ventas' o 'costo' - determina el tipo de análisis
        skip_preprocessing: Si True, asume que df_ventas ya está pre-procesado
    """
    import sys  # ✅ FIX: Agregar import sys faltante

    if df_ventas.empty:
        print("DEBUG: DataFrame de ventas vacío")
        return [], {}

    # Si no hay metas, calcular solo el resumen general (sin cumplimiento vs metas)
    if df_metas.empty:
        print("DEBUG: No hay metas disponibles, calculando solo análisis de rentabilidad")
        # Continuar con el cálculo pero sin procesar metas

    print(f"=== CALCULANDO CUMPLIMIENTO ({tipo_meta.upper()}) ===")
    print(f"Período: {f1} a {f2}")

    # ✅ OPTIMIZACIÓN: Si skip_preprocessing=True, usar datos ya procesados
    if skip_preprocessing:
        print(f"✅ OPTIMIZADO: Usando datos pre-procesados")
        ventas_periodo = df_ventas
        f1_naive = pd.to_datetime(f1).tz_localize(None) if hasattr(f1, 'tzinfo') and f1.tzinfo else pd.to_datetime(f1)
        f2_naive = pd.to_datetime(f2).tz_localize(None) if hasattr(f2, 'tzinfo') and f2.tzinfo else pd.to_datetime(f2)
    else:
        # Proceso original completo
        print(f"Tipos de fecha - f1: {type(f1)}, f2: {type(f2)}")

        # SOLUCIÓN CORRECTA: Remover zona horaria de las VENTAS, no de f1/f2
        # El problema es que df_ventas["Fecha"] tiene zona horaria y df_metas["Fecha"] no
        print(f"Tipo de fecha en ventas: {type(df_ventas['Fecha'].iloc[0]) if not df_ventas.empty else 'N/A'}")
        print(f"Tipo de fecha en metas: {type(df_metas['Fecha'].iloc[0]) if not df_metas.empty else 'N/A'}")
        print(f"Tipos originales - f1: {type(f1)}, f2: {type(f2)}")

        # Crear copias de ventas sin zona horaria para comparación
        df_ventas_naive = df_ventas.copy()
        df_ventas_naive["Fecha"] = df_ventas_naive["Fecha"].dt.tz_localize(None)

        print(f"Tipo de fecha en ventas después de normalizar: {type(df_ventas_naive['Fecha'].iloc[0])}")

        # Convertir f1, f2 a pandas Timestamp sin zona horaria
        f1_naive = pd.to_datetime(f1).tz_localize(None) if hasattr(f1, 'tzinfo') and f1.tzinfo else pd.to_datetime(f1)
        f2_naive = pd.to_datetime(f2).tz_localize(None) if hasattr(f2, 'tzinfo') and f2.tzinfo else pd.to_datetime(f2)

        print(f"Fechas normalizadas - f1_naive: {f1_naive} (tipo: {type(f1_naive)})")
        print(f"Fechas normalizadas - f2_naive: {f2_naive} (tipo: {type(f2_naive)})")

        # Los datos YA están filtrados por mes en la base de datos
        # Solo excluir cancelados, NO filtrar por fechas otra vez
        try:
            ventas_periodo = df_ventas_naive[
                df_ventas_naive["estado"] != "Cancelado"  # Solo excluir cancelados
            ].copy()
            print(f"OK: Filtro de ventas exitoso (excluye cancelados, datos ya filtrados por mes)")

            # Debug: Mostrar total y desglose
            total_ventas = ventas_periodo["Total"].sum()
            print(f"Total ventas (sin cancelados): ${total_ventas:,.0f}")

            # Debug: Mostrar rango de fechas real de los datos
            if not ventas_periodo.empty:
                fecha_min = ventas_periodo["Fecha"].min()
                fecha_max = ventas_periodo["Fecha"].max()
                print(f"Rango real de fechas en datos: {fecha_min} a {fecha_max}")

            # Verificar si hay otros estados además de "Orden de Venta"
            estados_presentes = ventas_periodo["estado"].unique()
            print(f"Estados presentes (sin cancelados): {estados_presentes}")

        except Exception as e:
            print(f"ERROR en filtro de ventas: {e}")
            raise e

        print(f"Ventas en período: {len(ventas_periodo)} registros")
        print(f"Canales con ventas: {ventas_periodo['Channel'].nunique()}")

        # FILTRAR POR LOS 8 CANALES OFICIALES ANTES DEL AGRUPAMIENTO
        print(f"INFO: Filtrando por los 8 canales oficiales: {CANALES_CLASIFICACION}")
        canales_antes = ventas_periodo['Channel'].unique().tolist()
        ventas_periodo = ventas_periodo[ventas_periodo['Channel'].isin(CANALES_CLASIFICACION)].copy()
        canales_despues = ventas_periodo['Channel'].unique().tolist()
        print(f"Canales antes del filtro: {canales_antes}")
        print(f"Canales después del filtro: {canales_despues}")
        print(f"Registros después del filtro: {len(ventas_periodo)}")

        # DEBUG: Verificar que la columna cantidad existe y tiene datos
        if 'cantidad' in ventas_periodo.columns:
            print(f"DEBUG: Columna 'cantidad' encontrada en ventas_periodo")
            print(f"DEBUG: Tipo de dato original: {ventas_periodo['cantidad'].dtype}")
            print(f"DEBUG: Valores de ejemplo (antes de conversión): {ventas_periodo['cantidad'].head(10).tolist()}")

            # Convertir cantidad a numérico ANTES de hacer operaciones
            ventas_periodo['cantidad'] = pd.to_numeric(ventas_periodo['cantidad'], errors='coerce').fillna(0)

            print(f"DEBUG: Tipo de dato después de conversión: {ventas_periodo['cantidad'].dtype}")
            print(f"DEBUG: Valores de ejemplo (después de conversión): {ventas_periodo['cantidad'].head(10).tolist()}")
            print(f"DEBUG: Suma total de cantidad en ventas_periodo: {ventas_periodo['cantidad'].sum()}")
            print(f"DEBUG: Cantidad de registros con cantidad > 0: {(ventas_periodo['cantidad'] > 0).sum()}")
        else:
            print(f"ERROR: Columna 'cantidad' NO EXISTE en ventas_periodo")
            print(f"DEBUG: Columnas disponibles: {ventas_periodo.columns.tolist()}")

    # ✅ OPTIMIZADO: Agrupar ventas por canal usando campos pre-calculados de ClickHouse
    ventas_por_canal = ventas_periodo.groupby('Channel').agg({
        'Total': 'sum',                    # Ventas totales
        'Costo de venta': 'sum',          # Costo de venta (ya con IVA incluido)
        'Gastos_directos': 'sum',         # ✅ PRE-CALCULADO EN CLICKHOUSE
        'Ingreso real': 'sum',            # ✅ PRE-CALCULADO EN CLICKHOUSE
        'cantidad': 'sum',                # Cantidad total de unidades
        'estado': 'count'                 # Contar transacciones por canal
    }).reset_index()

    # Renombrar columnas para consistencia con optimización
    ventas_por_canal.columns = ['Canal', 'Ventas_Reales', 'Costo_Venta', 'Gastos_Directos', 'Ingreso_Real', 'Cantidad_Total', 'Num_Transacciones']
    
    # ✅ OPTIMIZADO: Agrupar ventas por canal Y marca usando campos pre-calculados
    ventas_por_canal_marca = ventas_periodo.groupby(['Channel', 'Marca']).agg({
        'Total': 'sum',                    # Ventas totales
        'Costo de venta': 'sum',          # Costo de venta (ya con IVA)
        'Gastos_directos': 'sum',         # ✅ PRE-CALCULADO EN CLICKHOUSE
        'Ingreso real': 'sum',            # ✅ PRE-CALCULADO EN CLICKHOUSE
        'cantidad': 'sum',                # Cantidad total de unidades
        'estado': 'count'
    }).reset_index()

    # Renombrar columnas para consistencia con optimización
    ventas_por_canal_marca.columns = ['Canal', 'Marca', 'Ventas_Reales', 'Costo_Venta', 'Gastos_Directos', 'Ingreso_Real', 'Cantidad_Total', 'Num_Transacciones']

    # ✅ OPTIMIZADO: Agrupar ventas por canal, marca Y categoría usando campos pre-calculados
    ventas_por_canal_marca_categoria = ventas_periodo.groupby(['Channel', 'Marca', 'Categoria_Catalogo']).agg({
        'Total': 'sum',                    # Ventas totales
        'Costo de venta': 'sum',          # Costo de venta (ya con IVA)
        'Gastos_directos': 'sum',         # ✅ PRE-CALCULADO EN CLICKHOUSE
        'Ingreso real': 'sum',            # ✅ PRE-CALCULADO EN CLICKHOUSE
        'cantidad': 'sum',                # Cantidad total de unidades
        'estado': 'count'
    }).reset_index()

    # Renombrar columnas para consistencia con optimización
    ventas_por_canal_marca_categoria.columns = ['Canal', 'Marca', 'Categoria', 'Ventas_Reales', 'Costo_Venta', 'Gastos_Directos', 'Ingreso_Real', 'Cantidad_Total', 'Num_Transacciones']
    
    # IVA ya incluido en los datos base - no es necesario aplicarlo
    # # ventas_por_canal['Costo_Venta'] = ventas_por_canal['Costo_Venta'] * 1.16  # IVA ya incluido en los datos base
    # # ventas_por_canal['Gastos_Destino'] = ventas_por_canal['Gastos_Destino'] * 1.16  # IVA ya incluido en los datos base
    
    # ✅ OPTIMIZADO: Los campos Gastos_Directos e Ingreso_Real ya vienen pre-calculados de ClickHouse
    print(f"✅ OPTIMIZADO: Usando Gastos_Directos e Ingreso_Real pre-calculados de ClickHouse en calcular_cumplimiento_metas")
    sys.stdout.flush()
    
    # DEBUG: Verificar datos de cantidad en la agrupación
    print(f"DEBUG: Valores de Cantidad_Total en agrupación: {ventas_por_canal['Cantidad_Total'].head(10).tolist()}")
    print(f"DEBUG: Tipo de dato en agrupación: {ventas_por_canal['Cantidad_Total'].dtype}")
    print(f"DEBUG: Suma total de cantidades agrupadas: {ventas_por_canal['Cantidad_Total'].sum()}")
    print(f"DEBUG: Canales con cantidad > 0: {(ventas_por_canal['Cantidad_Total'] > 0).sum()} de {len(ventas_por_canal)} canales")

    # Asegurar que Cantidad_Total sea numérica (ya debería serlo por la agregación)
    ventas_por_canal['Cantidad_Total'] = pd.to_numeric(ventas_por_canal['Cantidad_Total'], errors='coerce').fillna(0)

    # Calcular PRECIO PROMEDIO POR UNIDAD (usando cantidad total de unidades vendidas)
    ventas_por_canal['Ventas_Reales_Promedio'] = ventas_por_canal.apply(lambda row: row['Ventas_Reales'] / row['Cantidad_Total'] if row['Cantidad_Total'] > 0 else 0, axis=1)
    ventas_por_canal['Costo_Venta_Promedio'] = ventas_por_canal.apply(lambda row: row['Costo_Venta'] / row['Cantidad_Total'] if row['Cantidad_Total'] > 0 else 0, axis=1)
    ventas_por_canal['Gastos_Directos_Promedio'] = ventas_por_canal.apply(lambda row: row['Gastos_Directos'] / row['Cantidad_Total'] if row['Cantidad_Total'] > 0 else 0, axis=1)
    ventas_por_canal['Ingreso_Real_Promedio'] = ventas_por_canal.apply(lambda row: row['Ingreso_Real'] / row['Cantidad_Total'] if row['Cantidad_Total'] > 0 else 0, axis=1)
    
    # Calcular PORCENTAJES POR CANAL (para las columnas adicionales)
    ventas_por_canal['Costo_Venta_Porcentaje'] = (ventas_por_canal['Costo_Venta'] / ventas_por_canal['Ventas_Reales'] * 100).fillna(0)
    ventas_por_canal['Gastos_Directos_Porcentaje'] = (ventas_por_canal['Gastos_Directos'] / ventas_por_canal['Ventas_Reales'] * 100).fillna(0)
    ventas_por_canal['Ingreso_Real_Porcentaje'] = (ventas_por_canal['Ingreso_Real'] / ventas_por_canal['Ventas_Reales'] * 100).fillna(0)
    
    # NUEVO: Aplicar mismos cálculos al desglose por canal y marca
    # IVA ya incluido en los datos base - no es necesario aplicarlo
    # # ventas_por_canal_marca['Costo_Venta'] = ventas_por_canal_marca['Costo_Venta'] * 1.16  # IVA ya incluido en los datos base
    # # ventas_por_canal_marca['Gastos_Destino'] = ventas_por_canal_marca['Gastos_Destino'] * 1.16  # IVA ya incluido en los datos base
    
    # ✅ OPTIMIZADO: Los campos Gastos_Directos e Ingreso_Real ya vienen pre-calculados de ClickHouse
    print(f"✅ OPTIMIZADO: Canal+Marca usando campos pre-calculados de ClickHouse")
    sys.stdout.flush()
    
    # Asegurar que Cantidad_Total sea numérica
    ventas_por_canal_marca['Cantidad_Total'] = pd.to_numeric(ventas_por_canal_marca['Cantidad_Total'], errors='coerce').fillna(0)

    # Calcular PRECIO PROMEDIO POR UNIDAD por canal y marca
    ventas_por_canal_marca['Ventas_Reales_Promedio'] = ventas_por_canal_marca.apply(lambda row: row['Ventas_Reales'] / row['Cantidad_Total'] if row['Cantidad_Total'] > 0 else 0, axis=1)
    ventas_por_canal_marca['Costo_Venta_Promedio'] = ventas_por_canal_marca.apply(lambda row: row['Costo_Venta'] / row['Cantidad_Total'] if row['Cantidad_Total'] > 0 else 0, axis=1)
    ventas_por_canal_marca['Gastos_Directos_Promedio'] = ventas_por_canal_marca.apply(lambda row: row['Gastos_Directos'] / row['Cantidad_Total'] if row['Cantidad_Total'] > 0 else 0, axis=1)
    ventas_por_canal_marca['Ingreso_Real_Promedio'] = ventas_por_canal_marca.apply(lambda row: row['Ingreso_Real'] / row['Cantidad_Total'] if row['Cantidad_Total'] > 0 else 0, axis=1)
    
    # Calcular PORCENTAJES POR CANAL Y MARCA (para las columnas adicionales)
    ventas_por_canal_marca['Costo_Venta_Porcentaje'] = (ventas_por_canal_marca['Costo_Venta'] / ventas_por_canal_marca['Ventas_Reales'] * 100).fillna(0)
    ventas_por_canal_marca['Gastos_Directos_Porcentaje'] = (ventas_por_canal_marca['Gastos_Directos'] / ventas_por_canal_marca['Ventas_Reales'] * 100).fillna(0)
    ventas_por_canal_marca['Ingreso_Real_Porcentaje'] = (ventas_por_canal_marca['Ingreso_Real'] / ventas_por_canal_marca['Ventas_Reales'] * 100).fillna(0)

    # NUEVO: Aplicar mismos cálculos al desglose por canal, marca Y categoría
    # IVA ya incluido en los datos base - no es necesario aplicarlo
    # # ventas_por_canal_marca_categoria['Costo_Venta'] = ventas_por_canal_marca_categoria['Costo_Venta'] * 1.16  # IVA ya incluido en los datos base
    # # ventas_por_canal_marca_categoria['Gastos_Destino'] = ventas_por_canal_marca_categoria['Gastos_Destino'] * 1.16  # IVA ya incluido en los datos base

    # ✅ OPTIMIZADO: Los campos Gastos_Directos e Ingreso_Real ya vienen pre-calculados de ClickHouse
    print(f"✅ OPTIMIZADO: Canal+Marca+Categoría usando campos pre-calculados de ClickHouse")
    sys.stdout.flush()

    # Asegurar que Cantidad_Total sea numérica
    ventas_por_canal_marca_categoria['Cantidad_Total'] = pd.to_numeric(ventas_por_canal_marca_categoria['Cantidad_Total'], errors='coerce').fillna(0)

    # Calcular PRECIO PROMEDIO POR UNIDAD por canal, marca y categoría
    ventas_por_canal_marca_categoria['Ventas_Reales_Promedio'] = ventas_por_canal_marca_categoria.apply(lambda row: row['Ventas_Reales'] / row['Cantidad_Total'] if row['Cantidad_Total'] > 0 else 0, axis=1)
    ventas_por_canal_marca_categoria['Costo_Venta_Promedio'] = ventas_por_canal_marca_categoria.apply(lambda row: row['Costo_Venta'] / row['Cantidad_Total'] if row['Cantidad_Total'] > 0 else 0, axis=1)
    ventas_por_canal_marca_categoria['Gastos_Directos_Promedio'] = ventas_por_canal_marca_categoria.apply(lambda row: row['Gastos_Directos'] / row['Cantidad_Total'] if row['Cantidad_Total'] > 0 else 0, axis=1)
    ventas_por_canal_marca_categoria['Ingreso_Real_Promedio'] = ventas_por_canal_marca_categoria.apply(lambda row: row['Ingreso_Real'] / row['Cantidad_Total'] if row['Cantidad_Total'] > 0 else 0, axis=1)

    # Calcular PORCENTAJES POR CANAL, MARCA Y CATEGORÍA (para las columnas adicionales)
    ventas_por_canal_marca_categoria['Costo_Venta_Porcentaje'] = (ventas_por_canal_marca_categoria['Costo_Venta'] / ventas_por_canal_marca_categoria['Ventas_Reales'] * 100).fillna(0)
    ventas_por_canal_marca_categoria['Gastos_Directos_Porcentaje'] = (ventas_por_canal_marca_categoria['Gastos_Directos'] / ventas_por_canal_marca_categoria['Ventas_Reales'] * 100).fillna(0)
    ventas_por_canal_marca_categoria['Ingreso_Real_Porcentaje'] = (ventas_por_canal_marca_categoria['Ingreso_Real'] / ventas_por_canal_marca_categoria['Ventas_Reales'] * 100).fillna(0)
    
    print(f"Canales con ventas: {len(ventas_por_canal)}")

    # ====== OPTIMIZACIÓN: COMPARACIÓN CON MES ANTERIOR DESHABILITADA PARA METAS ======
    # ❌ ELIMINADO: ~325 líneas de cálculo de comparación con mes anterior
    # No se usa en la pestaña "Cumplimiento de Metas", solo en "Control de Ingreso Real"
    # ====== INICIO BLOQUE COMENTADO ======
    # print(f"=== CALCULANDO VARIACIÓN CON MES ANTERIOR ===")
     #     # Determinar el período de comparación basado en días transcurridos
    # from datetime import date
    # hoy = date.today()
    # mes_actual = f1_naive.month
    # año_actual = f1_naive.year
     #     # Calcular días transcurridos del período actual
    # if mes_actual == hoy.month and año_actual == hoy.year:
        # Estamos en el mes actual - usar días transcurridos
        # dia_actual = min(hoy.day, f2_naive.day)  # No superar el día final del período
        # dias_transcurridos = dia_actual
         #         # Período anterior: mismos días del mes anterior
        # if mes_actual > 1:
            # mes_anterior = mes_actual - 1
            # año_anterior = año_actual
        # else:
            # mes_anterior = 12
            # año_anterior = año_actual - 1
         #         # f1_anterior = pd.Timestamp(año_anterior, mes_anterior, 1)
        # f2_anterior = pd.Timestamp(año_anterior, mes_anterior, dia_actual)
         #         # periodo_comparacion = f"vs mismos {dias_transcurridos} días de {f1_anterior.strftime('%B %Y')}"
        # print(f"COMPARACIÓN: {dias_transcurridos} días transcurridos")
         #     # else:
        # Mes completo - usar todo el mes anterior
        # dias_transcurridos = (f2_naive - f1_naive).days + 1
         #         # if mes_actual > 1:
            # mes_anterior = mes_actual - 1
            # año_anterior = año_actual
        # else:
            # mes_anterior = 12
            # año_anterior = año_actual - 1
         #         # f1_anterior = pd.Timestamp(año_anterior, mes_anterior, 1)
        # Último día del mes anterior
        # if mes_anterior == 12:
            # f2_anterior = pd.Timestamp(año_anterior, 12, 31)
        # else:
            # siguiente_mes = pd.Timestamp(año_anterior, mes_anterior + 1, 1)
            # f2_anterior = siguiente_mes - pd.Timedelta(days=1)
         #         # periodo_comparacion = f"vs {f1_anterior.strftime('%B %Y')} completo"
        # print(f"COMPARACIÓN: Meses completos")
     #     # print(f"Período actual: {f1_naive.date()} al {f2_naive.date()}")
    # print(f"Período anterior: {f1_anterior.date()} al {f2_anterior.date()}")
    # print(f"Descripción: {periodo_comparacion}")
     #     # Inicializar DataFrames para datos del mes anterior
    # ventas_por_canal_anterior = pd.DataFrame()
    # ventas_por_canal_marca_anterior = pd.DataFrame()
    # ventas_por_canal_marca_categoria_anterior = pd.DataFrame()
     #     # try:
        # OPTIMIZACIÓN: Usar nueva función con carga inteligente de comparación
        # print(f"Cargando datos optimizados para comparación: {mes_actual}/{año_actual} vs {mes_anterior}/{año_anterior}")
        # df_ventas_comparacion, _, _ = load_data_improved(
            # mes_filtro=mes_actual, 
            # incluir_comparacion=True,
            # año_especifico=año_actual
        # )
         #         # if not df_ventas_comparacion.empty:
            # Normalizar fechas
            # df_ventas_comparacion_naive = df_ventas_comparacion.copy()
            # df_ventas_comparacion_naive["Fecha"] = df_ventas_comparacion_naive["Fecha"].dt.tz_localize(None)
             #             # Filtrar solo datos del mes anterior del dataset completo
            # ventas_periodo_anterior = df_ventas_comparacion_naive[
                # (df_ventas_comparacion_naive["Fecha"] >= f1_anterior) &
                # (df_ventas_comparacion_naive["Fecha"] <= f2_anterior) &
                # (df_ventas_comparacion_naive["estado"] != "Cancelado") &
                # (df_ventas_comparacion_naive['Channel'].isin(CANALES_CLASIFICACION))
            # ].copy()
             #             # print(f"✅ OPTIMIZADO: {len(ventas_periodo_anterior)} registros del período anterior obtenidos en una sola query")
             #             # if not ventas_periodo_anterior.empty:
                # ✅ OPTIMIZADO: Calcular métricas por canal del mes anterior usando campos pre-calculados
                # ventas_por_canal_anterior = ventas_periodo_anterior.groupby('Channel').agg({
                    # 'Total': 'sum',                    # Ventas totales
                    # 'Costo de venta': 'sum',          # Costo de venta (ya con IVA)
                    # 'Gastos_directos': 'sum',         # ✅ PRE-CALCULADO EN CLICKHOUSE
                    # 'Ingreso real': 'sum',            # ✅ PRE-CALCULADO EN CLICKHOUSE
                    # 'cantidad': 'sum',                # Cantidad total de unidades
                    # 'estado': 'count'
                # }).reset_index()
 #                 # ventas_por_canal_anterior.columns = ['Canal', 'Ventas_Reales', 'Costo_Venta', 'Gastos_Directos', 'Ingreso_Real', 'Cantidad_Total', 'Num_Transacciones']
                 #                 # Aplicar IVA y calcular métricas derivadas (mismo proceso)
                # ventas_por_canal_anterior['Costo_Venta'] = ventas_por_canal_anterior['Costo_Venta'] * 1.16  # IVA ya incluido en los datos base
                # ventas_por_canal_anterior['Gastos_Destino'] = ventas_por_canal_anterior['Gastos_Destino'] * 1.16  # IVA ya incluido en los datos base
                # ✅ OPTIMIZADO: Solo calcular porcentajes (campos ya pre-calculados)
                # ventas_por_canal_anterior['Cantidad_Total'] = pd.to_numeric(ventas_por_canal_anterior['Cantidad_Total'], errors='coerce').fillna(0)
 #                 # Calcular SOLO porcentajes ponderados
                # ventas_por_canal_anterior['Costo_Venta_Porcentaje'] = (
                    # ventas_por_canal_anterior['Costo_Venta'] / ventas_por_canal_anterior['Ventas_Reales'] * 100
                # ).fillna(0)
                # ventas_por_canal_anterior['Gastos_Directos_Porcentaje'] = (
                    # ventas_por_canal_anterior['Gastos_Directos'] / ventas_por_canal_anterior['Ventas_Reales'] * 100
                # ).fillna(0)
                # ventas_por_canal_anterior['Ingreso_Real_Porcentaje'] = (
                    # ventas_por_canal_anterior['Ingreso_Real'] / ventas_por_canal_anterior['Ventas_Reales'] * 100
                # ).fillna(0)
 #                 # ✅ NUEVO: Calcular ROI porcentaje para datos del mes anterior
                # ventas_por_canal_anterior['ROI_Porcentaje'] = (
                    # ventas_por_canal_anterior['Ingreso_Real'] / ventas_por_canal_anterior['Costo_Venta'] * 100
                # ).fillna(0)
 #                 # ✅ OPTIMIZADO: Calcular métricas por canal y marca del mes anterior usando campos pre-calculados
                # ventas_por_canal_marca_anterior = ventas_periodo_anterior.groupby(['Channel', 'Marca']).agg({
                    # 'Total': 'sum',                    # Ventas totales
                    # 'Costo de venta': 'sum',          # Costo de venta (ya con IVA)
                    # 'Gastos_directos': 'sum',         # ✅ PRE-CALCULADO EN CLICKHOUSE
                    # 'Ingreso real': 'sum',            # ✅ PRE-CALCULADO EN CLICKHOUSE
                    # 'cantidad': 'sum',                # Cantidad total de unidades
                    # 'estado': 'count'
                # }).reset_index()
 #                 # ventas_por_canal_marca_anterior.columns = ['Canal', 'Marca', 'Ventas_Reales', 'Costo_Venta', 'Gastos_Directos', 'Ingreso_Real', 'Cantidad_Total', 'Num_Transacciones']
                 #                 # Aplicar mismos cálculos
                # ventas_por_canal_marca_anterior['Costo_Venta'] = ventas_por_canal_marca_anterior['Costo_Venta'] * 1.16  # IVA ya incluido en los datos base
                # ventas_por_canal_marca_anterior['Gastos_Destino'] = ventas_por_canal_marca_anterior['Gastos_Destino'] * 1.16  # IVA ya incluido en los datos base
                # ✅ OPTIMIZADO: Solo calcular porcentajes (campos ya pre-calculados)
                # ventas_por_canal_marca_anterior['Cantidad_Total'] = pd.to_numeric(ventas_por_canal_marca_anterior['Cantidad_Total'], errors='coerce').fillna(0)
 #                 # Calcular SOLO porcentajes ponderados
                # ventas_por_canal_marca_anterior['Costo_Venta_Porcentaje'] = (
                    # ventas_por_canal_marca_anterior['Costo_Venta'] / ventas_por_canal_marca_anterior['Ventas_Reales'] * 100
                # ).fillna(0)
                # ventas_por_canal_marca_anterior['Gastos_Directos_Porcentaje'] = (
                    # ventas_por_canal_marca_anterior['Gastos_Directos'] / ventas_por_canal_marca_anterior['Ventas_Reales'] * 100
                # ).fillna(0)
                # ventas_por_canal_marca_anterior['Ingreso_Real_Porcentaje'] = (
                    # ventas_por_canal_marca_anterior['Ingreso_Real'] / ventas_por_canal_marca_anterior['Ventas_Reales'] * 100
                # ).fillna(0)
 #                 # ✅ NUEVO: Calcular ROI porcentaje para mes anterior
                # ventas_por_canal_marca_anterior['ROI_Porcentaje'] = (
                    # ventas_por_canal_marca_anterior['Ingreso_Real'] / ventas_por_canal_marca_anterior['Costo_Venta'] * 100
                # ).fillna(0)
                 #                 # ✅ OPTIMIZADO: Calcular métricas por canal, marca Y categoría del mes anterior usando campos pre-calculados
                # ventas_por_canal_marca_categoria_anterior = ventas_periodo_anterior.groupby(['Channel', 'Marca', 'Categoria_Catalogo']).agg({
                    # 'Total': 'sum',                    # Ventas totales
                    # 'Costo de venta': 'sum',          # Costo de venta (ya con IVA)
                    # 'Gastos_directos': 'sum',         # ✅ PRE-CALCULADO EN CLICKHOUSE
                    # 'Ingreso real': 'sum',            # ✅ PRE-CALCULADO EN CLICKHOUSE
                    # 'cantidad': 'sum',                # Cantidad total de unidades
                    # 'estado': 'count'
                # }).reset_index()
 #                 # ventas_por_canal_marca_categoria_anterior.columns = ['Canal', 'Marca', 'Categoria', 'Ventas_Reales', 'Costo_Venta', 'Gastos_Directos', 'Ingreso_Real', 'Cantidad_Total', 'Num_Transacciones']
 #                 # Aplicar mismo procesamiento que a los datos actuales
                # ventas_por_canal_marca_categoria_anterior['Costo_Venta'] = ventas_por_canal_marca_categoria_anterior['Costo_Venta'] * 1.16  # IVA ya incluido en los datos base
                # ventas_por_canal_marca_categoria_anterior['Gastos_Destino'] = ventas_por_canal_marca_categoria_anterior['Gastos_Destino'] * 1.16  # IVA ya incluido en los datos base
                # ✅ OPTIMIZADO: Solo calcular porcentajes (campos ya pre-calculados)
                # ventas_por_canal_marca_categoria_anterior['Cantidad_Total'] = pd.to_numeric(ventas_por_canal_marca_categoria_anterior['Cantidad_Total'], errors='coerce').fillna(0)
 #                 # Calcular SOLO porcentajes ponderados
                # ventas_por_canal_marca_categoria_anterior['Costo_Venta_Porcentaje'] = (
                    # ventas_por_canal_marca_categoria_anterior['Costo_Venta'] / ventas_por_canal_marca_categoria_anterior['Ventas_Reales'] * 100
                # ).fillna(0)
                # ventas_por_canal_marca_categoria_anterior['Gastos_Directos_Porcentaje'] = (
                    # ventas_por_canal_marca_categoria_anterior['Gastos_Directos'] / ventas_por_canal_marca_categoria_anterior['Ventas_Reales'] * 100
                # ).fillna(0)
                # ventas_por_canal_marca_categoria_anterior['Ingreso_Real_Porcentaje'] = (
                    # ventas_por_canal_marca_categoria_anterior['Ingreso_Real'] / ventas_por_canal_marca_categoria_anterior['Ventas_Reales'] * 100
                # ).fillna(0)
 #                 # ✅ NUEVO: Calcular ROI porcentaje para categorías mes anterior
                # ventas_por_canal_marca_categoria_anterior['ROI_Porcentaje'] = (
                    # ventas_por_canal_marca_categoria_anterior['Ingreso_Real'] / ventas_por_canal_marca_categoria_anterior['Costo_Venta'] * 100
                # ).fillna(0)
 #                 # print(f"Canales en mes anterior: {len(ventas_por_canal_anterior)}")
                # print(f"Categorías en mes anterior: {len(ventas_por_canal_marca_categoria_anterior)}")
            # else:
                # print("No hay datos válidos para el período anterior")
        # else:
            # print("⚠️  No se pudieron cargar datos de comparación - usando query optimizada individual")
            # Fallback: intentar carga individual del mes anterior
            # try:
                # df_fallback, _, _ = load_data_improved(mes_filtro=mes_anterior, año_especifico=año_anterior)
                # if not df_fallback.empty:
                    # df_fallback_naive = df_fallback.copy()
                    # df_fallback_naive["Fecha"] = df_fallback_naive["Fecha"].dt.tz_localize(None)
                     #                     # ventas_periodo_anterior = df_fallback_naive[
                        # (df_fallback_naive["Fecha"] >= f1_anterior) &
                        # (df_fallback_naive["Fecha"] <= f2_anterior) &
                        # (df_fallback_naive["estado"] != "Cancelado") &
                        # (df_fallback_naive['Channel'].isin(CANALES_CLASIFICACION))
                    # ].copy()
                    # print(f"✅ FALLBACK: {len(ventas_periodo_anterior)} registros obtenidos con query individual")
            # except Exception as ef:
                # print(f"❌ Fallback también falló: {ef}")
                 #     # except Exception as e:
        # print(f"❌ Error en carga optimizada de comparación: {e}")
        # print("Intentando fallback a carga individual...")
        # try:
            # df_fallback, _, _ = load_data_improved(mes_filtro=mes_anterior, año_especifico=año_anterior)
            # if not df_fallback.empty:
                # df_fallback_naive = df_fallback.copy()
                # df_fallback_naive["Fecha"] = df_fallback_naive["Fecha"].dt.tz_localize(None)
                 #                 # ventas_periodo_anterior = df_fallback_naive[
                    # (df_fallback_naive["Fecha"] >= f1_anterior) &
                    # (df_fallback_naive["Fecha"] <= f2_anterior) &
                    # (df_fallback_naive["estado"] != "Cancelado") &
                    # (df_fallback_naive['Channel'].isin(CANALES_CLASIFICACION))
                # ].copy()
                # print(f"✅ RECOVERY: {len(ventas_periodo_anterior)} registros obtenidos con fallback")
        # except Exception as ef:
            # print(f"❌ Recovery total falló: {ef}")
        # Continuar sin comparación si hay error
     #     # Agregar datos de comparación a los DataFrames principales
    # if not ventas_por_canal_anterior.empty:
        # Merge para obtener porcentajes del mes anterior
        # ventas_por_canal = pd.merge(
            # ventas_por_canal,
            # ventas_por_canal_anterior[['Canal', 'Ingreso_Real_Porcentaje', 'Ventas_Reales', 'Costo_Venta', 'Costo_Venta_Porcentaje', 'Gastos_Directos', 'Gastos_Directos_Porcentaje', 'ROI_Porcentaje']].rename(columns={
                # 'Ingreso_Real_Porcentaje': 'Ingreso_Real_Porcentaje_Anterior',
                # 'Ventas_Reales': 'Ventas_Reales_Anterior',
                # 'Costo_Venta': 'Costo_Venta_Anterior',
                # 'Costo_Venta_Porcentaje': 'Costo_Venta_Porcentaje_Anterior',
                # 'Gastos_Directos': 'Gastos_Directos_Anterior',
                # 'Gastos_Directos_Porcentaje': 'Gastos_Directos_Porcentaje_Anterior',
                # 'ROI_Porcentaje': 'ROI_Porcentaje_Anterior'
            # }),
            # on='Canal',
            # how='left'
        # )
         #         # Calcular variación en puntos porcentuales para ingreso real
        # ventas_por_canal['Variacion_Ingreso_Pct'] = (
            # ventas_por_canal['Ingreso_Real_Porcentaje'] - 
            # ventas_por_canal['Ingreso_Real_Porcentaje_Anterior'].fillna(0)
        # )
         #         # Calcular variación porcentual de ventas vs mes anterior
        # ventas_por_canal['Variacion_Ventas_Pct'] = (
            # (ventas_por_canal['Ventas_Reales'] - ventas_por_canal['Ventas_Reales_Anterior'].fillna(0)) /
            # ventas_por_canal['Ventas_Reales_Anterior'].fillna(1) * 100
        # ).fillna(0)
 #         # Calcular variación de porcentaje de costo de venta vs mes anterior (puntos porcentuales)
        # ventas_por_canal['Variacion_Costo_Venta_Pct'] = (
            # ventas_por_canal['Costo_Venta_Porcentaje'] - ventas_por_canal['Costo_Venta_Porcentaje_Anterior'].fillna(0)
        # ).fillna(0)
 #         # Calcular variación de porcentaje de gastos directos vs mes anterior (puntos porcentuales)
        # ventas_por_canal['Variacion_Gastos_Directos_Pct'] = (
            # ventas_por_canal['Gastos_Directos_Porcentaje'] - ventas_por_canal['Gastos_Directos_Porcentaje_Anterior'].fillna(0)
        # ).fillna(0)
 #         # VALIDACIÓN: Marcar variaciones confiables para canales principales
        # MIN_TRANSACCIONES_CANAL = 5  # Mínimo para canales principales
        # ventas_por_canal['Num_Transacciones_Anterior'] = ventas_por_canal_anterior.set_index('Canal')['Num_Transacciones'].reindex(ventas_por_canal['Canal']).fillna(0).values
         #         # ventas_por_canal['Variacion_Confiable'] = (
            # (ventas_por_canal['Num_Transacciones'] >= MIN_TRANSACCIONES_CANAL) &
            # (ventas_por_canal['Num_Transacciones_Anterior'] >= MIN_TRANSACCIONES_CANAL)
        # )
         #         # print("✓ Variaciones por canal calculadas exitosamente")
         #         # DEBUG: Mostrar variaciones de canales principales
        # print("=== VARIACIONES POR CANAL PRINCIPAL ===")
        # for _, canal in ventas_por_canal.iterrows():
            # if canal['Variacion_Ingreso_Pct'] != 0:
                # signo = "+" if canal['Variacion_Ingreso_Pct'] > 0 else ""
                # confianza = "✓" if canal['Variacion_Confiable'] else "⚠"
                # print(f"{confianza} {canal['Canal']}: {canal['Ingreso_Real_Porcentaje']:.1f}% "
                      # f"(anterior: {canal['Ingreso_Real_Porcentaje_Anterior']:.1f}%) = "
                      # f"{signo}{canal['Variacion_Ingreso_Pct']:.1f}pp "
                      # f"({canal['Num_Transacciones']} trans)")
            # else:
                # print(f"⚠ {canal['Canal']}: {canal['Ingreso_Real_Porcentaje']:.1f}% (sin datos anteriores)")
         #         # NOTA: Las variaciones para marcas (Loomber vs Otros) se calculan
        # individualmente en la sección de display (Método 2) - líneas ~2259-2363
             #     # else:
        # Sin datos anteriores - agregar columnas vacías para canales
        # ventas_por_canal['Ingreso_Real_Porcentaje_Anterior'] = 0
        # ventas_por_canal['Ventas_Reales_Anterior'] = 0
        # ventas_por_canal['Num_Transacciones_Anterior'] = 0
        # ventas_por_canal['Variacion_Ingreso_Pct'] = 0
        # ventas_por_canal['Variacion_Ventas_Pct'] = 0
        # ventas_por_canal['Variacion_Costo_Venta_Pct'] = 0
        # ventas_por_canal['Variacion_Gastos_Directos_Pct'] = 0
        # ventas_por_canal['Variacion_Confiable'] = False
        # print("⚠ Sin datos del mes anterior para canales - variaciones en 0")
         #         # NOTA: Las columnas de variación para marcas se agregan dinámicamente
        # en el Método 2 durante el rendering de la tabla
     #     # También agregar información del período de comparación a cada canal
    # ventas_por_canal['Periodo_Comparacion'] = periodo_comparacion
    # ventas_por_canal['Dias_Comparados'] = dias_transcurridos
     #     # RESUMEN FINAL DE VARIACIONES
    # canales_con_variacion = len(ventas_por_canal[ventas_por_canal['Variacion_Ingreso_Pct'] != 0])
     #     # print(f"=== RESUMEN DE VARIACIONES CALCULADAS ===")
    # print(f"✓ Canales con datos de comparación: {canales_con_variacion}/{len(ventas_por_canal)}")
    # print(f"✓ Variaciones para marcas (Loomber/Otros) calculadas en display individual")
    # print(f"✓ Período de comparación: {periodo_comparacion}")
    # print(f"✓ Días comparados: {dias_transcurridos}")
    # ====== FIN BLOQUE COMENTADO ======

    # ✅ OPTIMIZADO: Inicializar columnas requeridas sin cálculos pesados de comparación
    ventas_por_canal['Ingreso_Real_Porcentaje_Anterior'] = 0
    ventas_por_canal['Ventas_Reales_Anterior'] = 0
    ventas_por_canal['Variacion_Ingreso_Pct'] = 0
    ventas_por_canal['Variacion_Ventas_Pct'] = 0
    ventas_por_canal['ROI_Porcentaje_Anterior'] = 0
    ventas_por_canal['Variacion_ROI_Pct'] = 0
    ventas_por_canal['Periodo_Comparacion'] = 'Sin comparación'
    ventas_por_canal['Dias_Comparados'] = 0

    # ✅ OPTIMIZADO: Crear resultado (se actualizará con metas si existen)

    # ✅ OPTIMIZADO: Inicializar DataFrames de comparación vacíos (sin cálculos)
    ventas_por_canal_anterior = pd.DataFrame()
    ventas_por_canal_marca_anterior = pd.DataFrame()
    ventas_por_canal_marca_categoria_anterior = pd.DataFrame()

    # ✅ PROCESAMIENTO SIMPLIFICADO DE METAS
    metas_periodo = pd.DataFrame()
    metas_por_canal = pd.DataFrame()
    meta_total_periodo = 0
    meta_total_mes = 0

    # ✅ PROCESAMIENTO MODULAR DE METAS
    # Validar tipo de meta
    if not validar_tipo_meta(tipo_meta):
        print(f"ERROR: Tipo de meta inválido: '{tipo_meta}'")
        return [], {}

    # Solo procesar metas si están disponibles
    if not df_metas.empty:
        try:
            # Filtrar metas para el período
            if not ventas_periodo.empty:
                fecha_min_ventas = ventas_periodo["Fecha"].min()
                fecha_max_ventas = ventas_periodo["Fecha"].max()

                # CORRECCIÓN: Normalizar fechas a medianoche para comparar con metas (que están a 00:00:00)
                fecha_min_ventas_normalizada = fecha_min_ventas.normalize()
                fecha_max_ventas_normalizada = fecha_max_ventas.normalize()

                metas_periodo = df_metas[
                    (df_metas["Fecha"] >= fecha_min_ventas_normalizada.replace(day=1)) &
                    (df_metas["Fecha"] <= fecha_max_ventas_normalizada)
                ].copy()
                print(f"OK: Filtro de metas exitoso usando rango real de ventas")
                print(f"DEBUG: Fecha mín ventas: {fecha_min_ventas} → normalizada: {fecha_min_ventas_normalizada}")
                print(f"DEBUG: Fecha máx ventas: {fecha_max_ventas} → normalizada: {fecha_max_ventas_normalizada}")
                print(f"DEBUG: Buscando metas desde {fecha_min_ventas_normalizada.replace(day=1)} hasta {fecha_max_ventas_normalizada}")
                print(f"DEBUG: Metas encontradas en período: {len(metas_periodo)}")
                if not metas_periodo.empty:
                    print(f"DEBUG: Fechas de metas disponibles: {sorted(metas_periodo['Fecha'].unique())[:10]}")
                    print(f"DEBUG: Canales en metas: {metas_periodo['Canal'].unique().tolist()}")
                else:
                    print(f"WARNING: No se encontraron metas para el período")
                    print(f"DEBUG: Fechas disponibles en df_metas completo: {sorted(df_metas['Fecha'].unique())[:10]}")
            else:
                metas_periodo = df_metas.copy()
                print(f"OK: Usando todas las metas disponibles")

            # Si hay metas, procesarlas
            if not metas_periodo.empty:
                from datetime import date
                hoy = pd.Timestamp(date.today()).normalize()
                fecha_fin_periodo = f2_naive.normalize()

                # CORRECCIÓN: Si estamos consultando un mes futuro (ej: noviembre cuando estamos en octubre),
                # usar la fecha máxima de ventas reales en lugar de "hoy"
                if not ventas_periodo.empty:
                    fecha_max_ventas_real = ventas_periodo["Fecha"].max()
                    fecha_actual_periodo = min(fecha_fin_periodo, fecha_max_ventas_real)
                else:
                    fecha_actual_periodo = min(fecha_fin_periodo, hoy)

                print(f"DEBUG: Hoy: {hoy}, Fecha fin período: {fecha_fin_periodo}, Fecha actual período: {fecha_actual_periodo}")

                # Filtrar día actual
                metas_dia_actual = metas_periodo[metas_periodo["Fecha"] == fecha_actual_periodo].copy()

                if metas_dia_actual.empty:
                    print(f"WARNING: No hay metas exactas para {fecha_actual_periodo}")
                    # Usar fecha más cercana
                    fechas_disponibles = metas_periodo['Fecha'].unique()
                    fechas_validas = [f for f in fechas_disponibles if f <= fecha_actual_periodo]
                    print(f"DEBUG: Fechas válidas disponibles: {sorted(fechas_validas) if fechas_validas else 'Ninguna'}")
                    if fechas_validas:
                        fecha_mas_cercana = max(fechas_validas)
                        print(f"INFO: Usando fecha más cercana: {fecha_mas_cercana}")
                        metas_dia_actual = metas_periodo[metas_periodo["Fecha"] == fecha_mas_cercana].copy()
                        fecha_actual_periodo = fecha_mas_cercana
                    else:
                        print(f"ERROR: No hay fechas válidas de metas <= {fecha_actual_periodo}")
                        fecha_actual_periodo = None  # No hay fechas válidas
                else:
                    print(f"INFO: Usando metas del día {fecha_actual_periodo}")

                print(f"DEBUG: Registros en metas_dia_actual: {len(metas_dia_actual)}")

                # ✅ USAR FUNCIÓN MODULAR para procesar metas
                metas_por_canal = procesar_metas_por_tipo(metas_dia_actual, tipo_meta, fecha_actual_periodo)

                # ✅ USAR FUNCIÓN MODULAR para calcular meta total del mes
                if not metas_por_canal.empty:
                    meta_total_mes = calcular_meta_total_mes(metas_por_canal, tipo_meta)
                    print(f"Canales con metas: {len(metas_por_canal)}")

        except Exception as e:
            print(f"ERROR procesando metas: {e}")
            import traceback
            traceback.print_exc()
            metas_por_canal = pd.DataFrame()
    else:
        print("INFO: Sin metas disponibles")

    # ✅ USAR FUNCIÓN MODULAR para calcular métricas por canal
    print(f"=== CALCULANDO MÉTRICAS CON MÓDULO MODULAR (tipo: {tipo_meta}) ===")
    resultado = calcular_metricas_canal(ventas_por_canal, metas_por_canal, tipo_meta)

    if resultado.empty:
        print("ERROR: No se pudieron calcular métricas")
        return [], {}

     # (Resto del código de metas comentado sigue abajo)
        # try:
            # Si tenemos datos de ventas, usar el mismo rango de fechas
            # if not ventas_periodo.empty:
                # fecha_min_ventas = ventas_periodo["Fecha"].min()
                # fecha_max_ventas = ventas_periodo["Fecha"].max()
                 #                 # Buscar metas para el mismo rango de fechas que las ventas reales
                # metas_periodo = df_metas[
                    # (df_metas["Fecha"] >= fecha_min_ventas.replace(day=1)) &  # Primer día del mes
                    # (df_metas["Fecha"] <= fecha_max_ventas)  # Hasta el último día de datos reales
                # ].copy()
                # print(f"OK: Filtro de metas exitoso usando rango real de ventas")
            # else:
                # Si no hay ventas, usar todas las metas disponibles
                # metas_periodo = df_metas.copy()
                # print(f"OK: Usando todas las metas disponibles (sin datos de ventas)")
        # except Exception as e:
            # print(f"ERROR en filtro de metas: {e}")
            # raise e
    # else:
        # print("INFO: Sin metas disponibles, saltando procesamiento de metas")
     #     # print(f"Registros de metas en período: {len(metas_periodo)}")
     #     # Solo procesar metas por canal si hay metas disponibles
    # if not metas_periodo.empty:
        # CORRECCIÓN: Usar Meta_Acumulada del día correcto (NO de fechas futuras)
        # Si f2_naive es futuro, usar HOY. Si es pasado, usar f2_naive.
        # from datetime import date
        # hoy = pd.Timestamp(date.today()).normalize()
        # fecha_fin_periodo = f2_naive.normalize()
        
        # Usar la fecha más temprana entre f2_naive y hoy
        # fecha_actual_periodo = min(fecha_fin_periodo, hoy)
        
        # print(f"DEBUG: Período de análisis hasta: {fecha_fin_periodo}")
        # print(f"DEBUG: Fecha actual (hoy): {hoy}")
        # print(f"DEBUG: Fecha a usar para Meta_Acumulada: {fecha_actual_periodo}")
        # print(f"DEBUG: Fechas disponibles en metas_periodo: {metas_periodo['Fecha'].min()} a {metas_periodo['Fecha'].max()}")
        
        # if fecha_actual_periodo < fecha_fin_periodo:
            # print(f"DEBUG: ✓ Usando fecha actual (hoy) porque el período incluye fechas futuras")
        # else:
            # print(f"DEBUG: ✓ Usando fecha final del período porque es pasado")
        
        # Filtrar solo los registros del día actual para obtener Meta_Acumulada correcta
        # metas_dia_actual = metas_periodo[metas_periodo["Fecha"] == fecha_actual_periodo].copy()
        
        # Validar que existan datos para esa fecha
        # if metas_dia_actual.empty:
            # print(f"WARNING: No hay datos de metas para {fecha_actual_periodo}")
            # print(f"Fechas más cercanas disponibles: {sorted(metas_periodo['Fecha'].unique())}")
            # Usar la fecha más cercana menor o igual
            # fechas_disponibles = metas_periodo['Fecha'].unique()
            # fecha_mas_cercana = max([f for f in fechas_disponibles if f <= fecha_actual_periodo])
            # print(f"Usando fecha más cercana: {fecha_mas_cercana}")
            # metas_dia_actual = metas_periodo[metas_periodo["Fecha"] == fecha_mas_cercana].copy()
            # fecha_actual_periodo = fecha_mas_cercana
        
        # Agrupar metas por canal usando Meta_Acumulada del día actual
        # metas_por_canal = metas_dia_actual.groupby('Canal').agg({
            # 'Meta_Acumulada': 'first',  # Tomar Meta_Acumulada del día actual
            # 'Modelo_Usado': 'first'
        # }).reset_index()
        # metas_por_canal.columns = ['Canal', 'Meta_Periodo', 'Modelo_Usado']
        
        # print(f"DEBUG: Meta_Acumulada por canal en día {fecha_actual_periodo}:")
        # for _, row in metas_por_canal.iterrows():
            # print(f"  {row['Canal']}: ${row['Meta_Periodo']:,.0f}")
        
        # Debug adicional: mostrar suma total de metas del período
        # suma_metas_periodo = metas_por_canal['Meta_Periodo'].sum()
        # print(f"DEBUG: Suma total de Meta_Acumulada del período: ${suma_metas_periodo:,.0f}")
        
        # print(f"Canales con metas: {len(metas_por_canal)}")
    # else:
        # print("INFO: Sin metas por canal disponibles")
    
    # NUEVA FUNCIONALIDAD: Calcular meta total del mes correspondiente al período
    # Inicializar variables independientemente de si hay metas
    # meta_total_mes = 0
    
    # Determinar qué mes corresponde al período analizado (siempre)
    # mes_inicio = f1_naive.replace(day=1)  # Primer día del mes de inicio
    # if f1_naive.month == f2_naive.month and f1_naive.year == f2_naive.year:
        # Mismo mes - usar ese mes
        # primer_dia_mes = mes_inicio
        # ultimo_dia_mes = (mes_inicio + pd.DateOffset(months=1))
    # else:
        # Período cruzado - usar mes de inicio 
        # primer_dia_mes = mes_inicio
        # ultimo_dia_mes = (mes_inicio + pd.DateOffset(months=1))
    
    # Solo calcular metas si hay datos disponibles
    # if not df_metas.empty:
        # Filtrar metas del mes completo
        # try:
            # print(f"DEBUG: Filtrando metas entre {primer_dia_mes} y {ultimo_dia_mes}")
            # print(f"DEBUG: Total registros disponibles: {len(df_metas)}")
            # print(f"DEBUG: Rango de fechas en df_metas: {df_metas['Fecha'].min()} a {df_metas['Fecha'].max()}")
            
            # metas_mes_completo = df_metas[
                # (df_metas["Fecha"] >= primer_dia_mes) & 
                # (df_metas["Fecha"] < ultimo_dia_mes)
            # ].copy()
            # print(f"DEBUG: Registros filtrados para el mes: {len(metas_mes_completo)}")
            
            # if not metas_mes_completo.empty:
                # print(f"DEBUG: Canales en mes filtrado: {metas_mes_completo['Canal'].unique().tolist()}")
                # print(f"DEBUG: Fechas únicas en mes filtrado: {sorted(metas_mes_completo['Fecha'].unique())}")
            
            # print(f"OK: Filtro de metas mes completo exitoso: {primer_dia_mes.strftime('%Y-%m')}")
        # except Exception as e:
            # print(f"ERROR en filtro de metas mes completo: {e}")
            # metas_mes_completo = pd.DataFrame()
        
        # Calcular meta total del mes
        # if not metas_mes_completo.empty:
            # meta_total_mes = float(metas_mes_completo['Meta_Diaria'].sum())
            # print(f"DEBUG: Calculando suma de Meta_Diaria: {len(metas_mes_completo)} registros = ${meta_total_mes:,.0f}")
        # else:
            # print("WARNING: No hay datos de Meta_Diaria para el mes específico")
            # print("INFO: Intentando calcular usando Meta_Mensual como alternativa...")
            
            # CORRECCIÓN: Filtrar Meta_Mensual solo para el mes del período analizado
            # try:
                # Filtrar df_metas solo para el mes del período (no todos los meses)
                # metas_del_mes = df_metas[
                    # (df_metas["Fecha"] >= primer_dia_mes) & 
                    # (df_metas["Fecha"] < ultimo_dia_mes)
                # ].copy()
                
                # if not metas_del_mes.empty:
                    # Obtener Meta_Mensual única por canal solo del mes correcto
                    # metas_mensuales_unicas = metas_del_mes.groupby('Canal')['Meta_Mensual'].first().reset_index()
                    # meta_total_mes = float(metas_mensuales_unicas['Meta_Mensual'].sum())
                    # print(f"DEBUG: Usando Meta_Mensual del mes {primer_dia_mes.strftime('%Y-%m')}: {len(metas_mensuales_unicas)} canales = ${meta_total_mes:,.0f}")
                    # print(f"DEBUG: Detalle por canal: {dict(zip(metas_mensuales_unicas['Canal'], metas_mensuales_unicas['Meta_Mensual']))}")
                # else:
                    # print(f"ERROR: No hay datos de metas para el mes {primer_dia_mes.strftime('%Y-%m')}")
                    # meta_total_mes = 0
            # except Exception as e:
                # print(f"ERROR calculando Meta_Mensual alternativa del mes: {e}")
                # meta_total_mes = 0
    # else:
        # print(f"INFO: No hay datos de metas disponibles para el mes {primer_dia_mes.strftime('%Y-%m')}")
        # meta_total_mes = 0
    
    # print(f"Meta total del mes {primer_dia_mes.strftime('%Y-%m')}: ${meta_total_mes:,.0f}")
    
    # NORMALIZAR NOMBRES DE CANALES PARA HACER MATCH CORRECTO
    # Crear mapeo de normalización para casos conocidos
    # canal_mapping = {
        # 'AliExpress': 'Aliexpress',  # Normalizar AliExpress de metas -> Aliexpress de ventas
        # 'Mercado Libre': 'Mercado Libre',  # Ya coinciden
        # Agregar más mapeos según sea necesario
    # }
    
    # Aplicar normalización a metas_por_canal (solo si hay metas)
    # metas_por_canal_normalizado = metas_por_canal.copy()
    # if not metas_por_canal.empty and 'Canal' in metas_por_canal.columns:
        # metas_por_canal_normalizado['Canal'] = metas_por_canal_normalizado['Canal'].replace(canal_mapping)
    
    # print(f"NORMALIZACION DE CANALES:")
    # if not metas_por_canal.empty and 'Canal' in metas_por_canal.columns:
        # print(f"Canales en metas (original): {metas_por_canal['Canal'].tolist()}")
        # print(f"Canales en metas (normalizado): {metas_por_canal_normalizado['Canal'].tolist()}")
    # else:
        # print("Canales en metas: Sin metas disponibles")
    # print(f"Canales en ventas: {ventas_por_canal['Canal'].tolist()}")
    
    # Join ventas con metas (usando nombres normalizados)
    # CORRECCIÓN: Usar ventas como base para asegurar que todos los canales con ventas aparezcan
    # if not metas_por_canal.empty:
        # resultado = pd.merge(
            # ventas_por_canal,           # CAMBIO: Ventas como base
            # metas_por_canal_normalizado,
            # on='Canal',
            # how='left'                  # Left join mantiene todos los canales con ventas
        # )
        
        # Debug: verificar el resultado del merge
        # print(f"DEBUG: Resultado después del merge:")
        # print(f"  Canales en resultado: {resultado['Canal'].tolist()}")
        # print(f"  Suma Meta_Periodo antes de fillna: {resultado['Meta_Periodo'].sum():.0f}")
        # if 'Meta_Periodo' in resultado.columns:
            # for _, row in resultado.iterrows():
                # meta_val = row['Meta_Periodo'] if pd.notna(row['Meta_Periodo']) else 'NaN'
                # print(f"    {row['Canal']}: ${meta_val}")
        # else:
            # print("  PROBLEMA: Columna Meta_Periodo no encontrada en resultado")
    # else:
        # Sin metas: usar solo datos de ventas para el resumen general
        # resultado = ventas_por_canal.copy()
        # Agregar columnas faltantes para mantener compatibilidad
        # resultado['Meta_Periodo'] = 0
        # resultado['Modelo_Usado'] = 'N/A'
    
    
    # Llenar valores faltantes con 0 para canales que no tienen ventas
    resultado['Ventas_Reales'] = resultado['Ventas_Reales'].fillna(0)
    resultado['Costo_Venta'] = resultado['Costo_Venta'].fillna(0)
    resultado['Gastos_Directos'] = resultado['Gastos_Directos'].fillna(0)
    resultado['Ingreso_Real'] = resultado['Ingreso_Real'].fillna(0)
    resultado['Num_Transacciones'] = resultado['Num_Transacciones'].fillna(0)
    resultado['Cantidad_Total'] = pd.to_numeric(resultado['Cantidad_Total'], errors='coerce').fillna(0)
    resultado['Ventas_Reales_Promedio'] = resultado['Ventas_Reales_Promedio'].fillna(0)
    resultado['Costo_Venta_Promedio'] = resultado['Costo_Venta_Promedio'].fillna(0)
    resultado['Gastos_Directos_Promedio'] = resultado['Gastos_Directos_Promedio'].fillna(0)
    resultado['Ingreso_Real_Promedio'] = resultado['Ingreso_Real_Promedio'].fillna(0)
    resultado['Costo_Venta_Porcentaje'] = resultado['Costo_Venta_Porcentaje'].fillna(0)
    resultado['Gastos_Directos_Porcentaje'] = resultado['Gastos_Directos_Porcentaje'].fillna(0)
    resultado['Ingreso_Real_Porcentaje'] = resultado['Ingreso_Real_Porcentaje'].fillna(0)

    # ✅ Las métricas ya están calculadas por el módulo modular (Cumplimiento, Diferencia, Meta_Display, etc.)
    print(f"✓ Métricas calculadas por módulo modular para tipo '{tipo_meta}'")

    # Convertir a lista de diccionarios para el template
    # GENERAR SIEMPRE los datos por canal para el Análisis de Rentabilidad (incluso sin metas)
    cumplimiento_list = []
    if not resultado.empty:  # Si hay datos de ventas, generar la lista
        # ORDENAR RESULTADO POR VENTAS REALES ANTES DE ITERAR
        resultado_ordenado = resultado.sort_values('Ventas_Reales', ascending=False)
        
        for _, row in resultado_ordenado.iterrows():
            # Manejar meta_periodo según el tipo de meta
            if tipo_meta == "costo":
                meta_periodo_value = row['Meta_Periodo']  # Mantener como string "48% - 54%"
            elif tipo_meta == "ingreso_real":
                meta_periodo_value = row['Meta_Periodo']  # Mantener como string "10% - 15%"
            else:
                meta_periodo_value = float(row['Meta_Periodo_Numerico'])  # Usar la versión numérica para ventas
                
            # Generar configuración de gauge para metas de costo e ingreso real
            gauge_config = None
            if tipo_meta == "costo":
                gauge_config = crear_gauge_costo_config(float(row['Costo_Venta_Porcentaje']), row['Canal'])
            elif tipo_meta == "ingreso_real":
                gauge_config = crear_gauge_ingreso_config(float(row['Ingreso_Real_Porcentaje']), row['Canal'])
            
            canal_data = {
                'canal': row['Canal'],
                'meta_periodo': meta_periodo_value,
                'ventas_reales': float(row['Ventas_Reales']),
                'cumplimiento': float(row['Cumplimiento']),
                'diferencia': float(row['Diferencia']),
                'modelo_usado': row['Modelo_Usado'],
                # NUEVOS CAMPOS PARA LA TABLA DETALLADA
                'costo_venta': float(row['Costo_Venta']),
                'costo_venta_promedio': float(row['Costo_Venta_Promedio']),
                'gastos_directos': float(row['Gastos_Directos']),
                'gastos_directos_promedio': float(row['Gastos_Directos_Promedio']),
                'ingreso_real': float(row['Ingreso_Real']),
                'ingreso_real_promedio': float(row['Ingreso_Real_Promedio']),
                'ventas_reales_promedio': float(row['Ventas_Reales_Promedio']),
                'num_transacciones': int(row['Num_Transacciones']),
                # COLUMNAS ADICIONALES DE PORCENTAJES
                'costo_venta_porcentaje': float(row['Costo_Venta_Porcentaje']),
                'gastos_directos_porcentaje': float(row['Gastos_Directos_Porcentaje']),
                'ingreso_real_porcentaje': float(row['Ingreso_Real_Porcentaje']),
                # NUEVOS CAMPOS DE COMPARACIÓN CON MES ANTERIOR
                'ingreso_real_porcentaje_anterior': float(row['Ingreso_Real_Porcentaje_Anterior']),
                'variacion_ingreso_pct': float(row['Variacion_Ingreso_Pct']),
                'ventas_reales_anterior': float(row['Ventas_Reales_Anterior']),
                'variacion_ventas_pct': float(row['Variacion_Ventas_Pct']),
                'periodo_comparacion': row['Periodo_Comparacion'],
                'dias_comparados': int(row['Dias_Comparados']),
                # TIPO DE META PARA EL TEMPLATE
                'tipo_meta': tipo_meta,
                # CAMPOS PARA MOSTRAR SEGÚN TIPO DE META
                'meta_display': row['Meta_Display'],
                'ventas_reales_display': row['Ventas_Reales_Display'],
                # NUEVO: Indicador de que es fila principal
                'es_fila_principal': True,
                'es_subfila': False,
                'marca_tipo': None
            }
            
            # Solo incluir gauge_config si no es None
            if gauge_config is not None:
                canal_data['gauge_config'] = gauge_config
                
            cumplimiento_list.append(canal_data)
            
            # NUEVO: Agregar sub-filas por marca para este canal
            canal_actual = row['Canal']
            datos_canal_marca = ventas_por_canal_marca[ventas_por_canal_marca['Canal'] == canal_actual]
            
            # Clasificar marcas en Loomber y Otros (ya vienen clasificadas desde la tabla base)
            loomber_data = datos_canal_marca[datos_canal_marca['Marca'] == 'Loomber']
            otros_data = datos_canal_marca[datos_canal_marca['Marca'] == 'Otros']
            
            # Calcular datos del mes anterior para el mismo canal si están disponibles
            loomber_ingreso_pct_anterior = 0
            loomber_variacion_ingreso = 0
            loomber_ventas_anterior = 0
            loomber_variacion_ventas = 0
            loomber_gastos_pct_anterior = 0
            loomber_variacion_gastos = 0
            loomber_costo_pct_anterior = 0
            loomber_variacion_costo = 0
            otros_ingreso_pct_anterior = 0
            otros_variacion_ingreso = 0
            otros_ventas_anterior = 0
            otros_variacion_ventas = 0
            otros_gastos_pct_anterior = 0
            otros_variacion_gastos = 0
            otros_costo_pct_anterior = 0
            otros_variacion_costo = 0
            
            if not ventas_por_canal_marca_anterior.empty:
                # Datos de Loomber del mes anterior para este canal
                loomber_anterior = ventas_por_canal_marca_anterior[
                    (ventas_por_canal_marca_anterior['Canal'] == canal_actual) & 
                    (ventas_por_canal_marca_anterior['Marca'] == 'Loomber')
                ]
                
                if not loomber_anterior.empty:
                    loomber_anterior_totals = loomber_anterior.sum(numeric_only=True)
                    if loomber_anterior_totals['Ventas_Reales'] > 0:
                        loomber_ingreso_pct_anterior = (loomber_anterior_totals['Ingreso_Real'] / loomber_anterior_totals['Ventas_Reales'] * 100)
                        loomber_ventas_anterior = loomber_anterior_totals['Ventas_Reales']
                        loomber_gastos_pct_anterior = (loomber_anterior_totals['Gastos_Directos'] / loomber_anterior_totals['Ventas_Reales'] * 100)
                        loomber_costo_pct_anterior = (loomber_anterior_totals['Costo_Venta'] / loomber_anterior_totals['Ventas_Reales'] * 100)
                
                # Datos de Otros del mes anterior para este canal
                otros_anterior = ventas_por_canal_marca_anterior[
                    (ventas_por_canal_marca_anterior['Canal'] == canal_actual) &
                    (ventas_por_canal_marca_anterior['Marca'] == 'Otros')
                ]
                
                if not otros_anterior.empty:
                    otros_anterior_totals = otros_anterior.sum(numeric_only=True)
                    if otros_anterior_totals['Ventas_Reales'] > 0:
                        otros_ingreso_pct_anterior = (otros_anterior_totals['Ingreso_Real'] / otros_anterior_totals['Ventas_Reales'] * 100)
                        otros_ventas_anterior = otros_anterior_totals['Ventas_Reales']
                        otros_gastos_pct_anterior = (otros_anterior_totals['Gastos_Directos'] / otros_anterior_totals['Ventas_Reales'] * 100)
                        otros_costo_pct_anterior = (otros_anterior_totals['Costo_Venta'] / otros_anterior_totals['Ventas_Reales'] * 100)
            
            # Agregar sub-fila Loomber si existe
            if not loomber_data.empty:
                loomber_totals = loomber_data.sum(numeric_only=True)
                loomber_count = loomber_data['Num_Transacciones'].sum()
                
                # Calcular % de representación de Loomber sobre el total del canal
                loomber_representacion_ventas = (loomber_totals['Ventas_Reales'] / float(row['Ventas_Reales']) * 100) if float(row['Ventas_Reales']) > 0 else 0
                loomber_representacion_ingreso = (loomber_totals['Ingreso_Real'] / float(row['Ingreso_Real']) * 100) if float(row['Ingreso_Real']) > 0 else 0
                
                # Calcular promedios para Loomber
                loomber_ventas_prom = (loomber_totals['Ventas_Reales'] / loomber_count) if loomber_count > 0 else 0
                loomber_costo_prom = (loomber_totals['Costo_Venta'] / loomber_count) if loomber_count > 0 else 0
                loomber_gastos_prom = (loomber_totals['Gastos_Directos'] / loomber_count) if loomber_count > 0 else 0
                loomber_ingreso_prom = (loomber_totals['Ingreso_Real'] / loomber_count) if loomber_count > 0 else 0
                
                # Calcular porcentajes para Loomber
                loomber_costo_pct = (loomber_totals['Costo_Venta'] / loomber_totals['Ventas_Reales'] * 100) if loomber_totals['Ventas_Reales'] > 0 else 0
                loomber_gastos_pct = (loomber_totals['Gastos_Directos'] / loomber_totals['Ventas_Reales'] * 100) if loomber_totals['Ventas_Reales'] > 0 else 0
                loomber_ingreso_pct = (loomber_totals['Ingreso_Real'] / loomber_totals['Ventas_Reales'] * 100) if loomber_totals['Ventas_Reales'] > 0 else 0
                
                # Calcular variación de ingreso real para Loomber
                loomber_variacion_ingreso = loomber_ingreso_pct - loomber_ingreso_pct_anterior
                
                # Calcular variación de ventas para Loomber
                loomber_variacion_ventas = (
                    (loomber_totals['Ventas_Reales'] - loomber_ventas_anterior) / loomber_ventas_anterior * 100
                    if loomber_ventas_anterior > 0 else 0
                )

                # Calcular variación de gastos directos para Loomber (puntos porcentuales)
                loomber_variacion_gastos = loomber_gastos_pct - loomber_gastos_pct_anterior

                # Calcular variación de costo de venta para Loomber (puntos porcentuales)
                loomber_variacion_costo = loomber_costo_pct - loomber_costo_pct_anterior

                loomber_subfila = {
                    'canal': f"├── Marca Loomber",
                    'meta_periodo': meta_periodo_value,
                    'ventas_reales': float(loomber_totals['Ventas_Reales']),
                    'cumplimiento': 0,  # Sub-filas no tienen cumplimiento
                    'diferencia': 0,
                    'modelo_usado': 'N/A',
                    'costo_venta': float(loomber_totals['Costo_Venta']),
                    'costo_venta_promedio': float(loomber_costo_prom),
                    'gastos_directos': float(loomber_totals['Gastos_Directos']),
                    'gastos_directos_promedio': float(loomber_gastos_prom),
                    'ingreso_real': float(loomber_totals['Ingreso_Real']),
                    'ingreso_real_promedio': float(loomber_ingreso_prom),
                    'ventas_reales_promedio': float(loomber_ventas_prom),
                    'num_transacciones': int(loomber_count),
                    'costo_venta_porcentaje': float(loomber_costo_pct),
                    'gastos_directos_porcentaje': float(loomber_gastos_pct),
                    'ingreso_real_porcentaje': float(loomber_ingreso_pct),
                    # NUEVOS CAMPOS DE COMPARACIÓN CON MES ANTERIOR (SUB-FILAS)
                    'ingreso_real_porcentaje_anterior': float(loomber_ingreso_pct_anterior),
                    'variacion_ingreso_pct': float(loomber_variacion_ingreso),
                    'ventas_reales_anterior': float(loomber_ventas_anterior),
                    'variacion_ventas_pct': float(loomber_variacion_ventas),
                    'gastos_directos_porcentaje_anterior': float(loomber_gastos_pct_anterior),
                    'variacion_gastos_directos_pct': float(loomber_variacion_gastos),
                    'costo_venta_porcentaje_anterior': float(loomber_costo_pct_anterior),
                    'variacion_costo_venta_pct': float(loomber_variacion_costo),
                    'periodo_comparacion': row['Periodo_Comparacion'],
                    'dias_comparados': int(row['Dias_Comparados']),
                    'tipo_meta': tipo_meta,
                    'meta_display': row['Meta_Display'],
                    'ventas_reales_display': str(int(loomber_totals['Ventas_Reales'])),
                    'es_fila_principal': False,
                    'es_subfila': True,
                    'marca_tipo': 'Loomber',
                    # NUEVOS: Porcentajes de representación
                    'representacion_ventas': float(loomber_representacion_ventas),
                    'representacion_ingreso': float(loomber_representacion_ingreso),
                    # EXPLÍCITAMENTE SIN GAUGE_CONFIG
                    'gauge_config': None
                }
                cumplimiento_list.append(loomber_subfila)
            
            # Agregar sub-fila Otros si existe
            if not otros_data.empty:
                otros_totals = otros_data.sum(numeric_only=True)
                otros_count = otros_data['Num_Transacciones'].sum()
                
                # Calcular % de representación de Otros sobre el total del canal
                otros_representacion_ventas = (otros_totals['Ventas_Reales'] / float(row['Ventas_Reales']) * 100) if float(row['Ventas_Reales']) > 0 else 0
                otros_representacion_ingreso = (otros_totals['Ingreso_Real'] / float(row['Ingreso_Real']) * 100) if float(row['Ingreso_Real']) > 0 else 0
                
                # Calcular promedios para Otros
                otros_ventas_prom = (otros_totals['Ventas_Reales'] / otros_count) if otros_count > 0 else 0
                otros_costo_prom = (otros_totals['Costo_Venta'] / otros_count) if otros_count > 0 else 0
                otros_gastos_prom = (otros_totals['Gastos_Directos'] / otros_count) if otros_count > 0 else 0
                otros_ingreso_prom = (otros_totals['Ingreso_Real'] / otros_count) if otros_count > 0 else 0
                
                # Calcular porcentajes para Otros
                otros_costo_pct = (otros_totals['Costo_Venta'] / otros_totals['Ventas_Reales'] * 100) if otros_totals['Ventas_Reales'] > 0 else 0
                otros_gastos_pct = (otros_totals['Gastos_Directos'] / otros_totals['Ventas_Reales'] * 100) if otros_totals['Ventas_Reales'] > 0 else 0
                otros_ingreso_pct = (otros_totals['Ingreso_Real'] / otros_totals['Ventas_Reales'] * 100) if otros_totals['Ventas_Reales'] > 0 else 0
                
                # Calcular variación de ingreso real para Otros
                otros_variacion_ingreso = otros_ingreso_pct - otros_ingreso_pct_anterior
                
                # Calcular variación de ventas para Otros
                otros_variacion_ventas = (
                    (otros_totals['Ventas_Reales'] - otros_ventas_anterior) / otros_ventas_anterior * 100
                    if otros_ventas_anterior > 0 else 0
                )

                # Calcular variación de gastos directos para Otros (puntos porcentuales)
                otros_variacion_gastos = otros_gastos_pct - otros_gastos_pct_anterior

                # Calcular variación de costo de venta para Otros (puntos porcentuales)
                otros_variacion_costo = otros_costo_pct - otros_costo_pct_anterior
                
                otros_subfila = {
                    'canal': f"└── Otros",
                    'meta_periodo': meta_periodo_value,
                    'ventas_reales': float(otros_totals['Ventas_Reales']),
                    'cumplimiento': 0,  # Sub-filas no tienen cumplimiento
                    'diferencia': 0,
                    'modelo_usado': 'N/A',
                    'costo_venta': float(otros_totals['Costo_Venta']),
                    'costo_venta_promedio': float(otros_costo_prom),
                    'gastos_directos': float(otros_totals['Gastos_Directos']),
                    'gastos_directos_promedio': float(otros_gastos_prom),
                    'ingreso_real': float(otros_totals['Ingreso_Real']),
                    'ingreso_real_promedio': float(otros_ingreso_prom),
                    'ventas_reales_promedio': float(otros_ventas_prom),
                    'num_transacciones': int(otros_count),
                    'costo_venta_porcentaje': float(otros_costo_pct),
                    'gastos_directos_porcentaje': float(otros_gastos_pct),
                    'ingreso_real_porcentaje': float(otros_ingreso_pct),
                    # NUEVOS CAMPOS DE COMPARACIÓN CON MES ANTERIOR (SUB-FILAS)
                    'ingreso_real_porcentaje_anterior': float(otros_ingreso_pct_anterior),
                    'variacion_ingreso_pct': float(otros_variacion_ingreso),
                    'ventas_reales_anterior': float(otros_ventas_anterior),
                    'variacion_ventas_pct': float(otros_variacion_ventas),
                    'gastos_directos_porcentaje_anterior': float(otros_gastos_pct_anterior),
                    'variacion_gastos_directos_pct': float(otros_variacion_gastos),
                    'costo_venta_porcentaje_anterior': float(otros_costo_pct_anterior),
                    'variacion_costo_venta_pct': float(otros_variacion_costo),
                    'periodo_comparacion': row['Periodo_Comparacion'],
                    'dias_comparados': int(row['Dias_Comparados']),
                    'tipo_meta': tipo_meta,
                    'meta_display': row['Meta_Display'],
                    'ventas_reales_display': str(int(otros_totals['Ventas_Reales'])),
                    'es_fila_principal': False,
                    'es_subfila': True,
                    'marca_tipo': 'Otros',
                    # NUEVOS: Porcentajes de representación
                    'representacion_ventas': float(otros_representacion_ventas),
                    'representacion_ingreso': float(otros_representacion_ingreso),
                    # EXPLÍCITAMENTE SIN GAUGE_CONFIG
                    'gauge_config': None
                }
                cumplimiento_list.append(otros_subfila)
        
        # ORDENAR POR VENTAS REALES (RANKING DESCENDENTE) - Los canales más importantes primero
        # Mantener el orden: Canal principal -> Sub-filas -> Siguiente canal principal
        # No reordenar la lista completa para preservar la estructura de sub-filas
    
    # Calcular nuevos KPIs del período
    # 1. Ventas Totales (ya existía)
    ventas_totales_periodo = float(resultado['Ventas_Reales'].sum())
    
    # 2. Costo de Venta - USAR SOLO LOS 8 CANALES PRINCIPALES (YA CON IVA APLICADO POR CANAL)
    costo_venta_periodo = float(resultado['Costo_Venta'].sum())  # Sumatoria de canales que ya tienen IVA 16%
    costo_venta_porcentaje = float((costo_venta_periodo / ventas_totales_periodo * 100) if ventas_totales_periodo > 0 else 0)
    
    # 3. Gastos Directos - ✅ OPTIMIZADO: Usar campo pre-calculado de ClickHouse
    gastos_directos_periodo = float(resultado['Gastos_Directos'].sum())
    gastos_directos_porcentaje = float((gastos_directos_periodo / ventas_totales_periodo * 100) if ventas_totales_periodo > 0 else 0)

    # 4. Ingreso Real - ✅ OPTIMIZADO: Usar campo pre-calculado de ClickHouse
    ingreso_real_periodo = float(resultado['Ingreso_Real'].sum())
    ingreso_real_porcentaje = float((ingreso_real_periodo / ventas_totales_periodo * 100) if ventas_totales_periodo > 0 else 0)
    
    # 5. Calcular cantidad total de unidades y precio promedio - USAR SOLO LOS 8 CANALES PRINCIPALES
    num_transacciones = int(resultado['Num_Transacciones'].sum())
    cantidad_total_unidades = int(resultado['Cantidad_Total'].sum())
    precio_promedio_ventas = float(ventas_totales_periodo / cantidad_total_unidades) if cantidad_total_unidades > 0 else 0
    precio_promedio_costo = float(costo_venta_periodo / cantidad_total_unidades) if cantidad_total_unidades > 0 else 0
    precio_promedio_gastos = float(gastos_directos_periodo / cantidad_total_unidades) if cantidad_total_unidades > 0 else 0
    precio_promedio_ingreso = float(ingreso_real_periodo / cantidad_total_unidades) if cantidad_total_unidades > 0 else 0

    # 6. Calcular evolución simplificada (datos vacíos por optimización)
    # NOTA: Los mini-gráficos no se usan en el template, pero los campos son requeridos
    evolucion_costo = []
    evolucion_ventas = []
    evolucion_ingreso = []

    # Calcular cumplimiento según el tipo de meta
    if tipo_meta == "costo":
        # Para costo: usar el porcentaje de costo de venta
        cumplimiento_global = costo_venta_porcentaje
        meta_total_periodo = 51  # Centro del rango 48-54%
        cumplimiento_vs_mes = costo_venta_porcentaje
    elif tipo_meta == "ingreso_real":
        # Para ingreso real: usar el porcentaje de ingreso real
        cumplimiento_global = ingreso_real_porcentaje
        meta_total_periodo = 12.5  # Centro del rango 10-15%
        cumplimiento_vs_mes = ingreso_real_porcentaje
    elif tipo_meta == "ingreso_real_nominal":
        # Para ingreso real nominal: usar Ingreso_Real vs Meta_Ingreso_Real_Acumulada
        cumplimiento_global = float((resultado['Ingreso_Real'].sum() / resultado['Meta_Periodo'].sum() * 100) if resultado['Meta_Periodo'].sum() > 0 else 0)
        meta_total_periodo = float(resultado['Meta_Periodo'].sum())  # Suma de Meta_Ingreso_Real_Acumulada del día actual
        cumplimiento_vs_mes = float((ingreso_real_periodo / meta_total_mes * 100) if meta_total_mes > 0 else 0)
    else:
        # Para ventas: usar Meta_Acumulada corregida del día actual
        # Ahora resultado['Meta_Periodo'] contiene las Meta_Acumulada correctas del día actual
        cumplimiento_global = float((resultado['Ventas_Reales'].sum() / resultado['Meta_Periodo'].sum() * 100) if resultado['Meta_Periodo'].sum() > 0 else 0)
        meta_total_periodo = float(resultado['Meta_Periodo'].sum())  # Suma de Meta_Acumulada del día actual
        cumplimiento_vs_mes = float((ventas_totales_periodo / meta_total_mes * 100) if meta_total_mes > 0 else 0)
    
    # CALCULAR VARIACIÓN TOTAL PONDERADA POR VENTAS
    variacion_total_ponderada = 0
    ingreso_real_porcentaje_anterior_total = 0
    variacion_ventas_total_ponderada = 0
    ventas_anterior_total = 0
    
    if not resultado.empty and not ventas_por_canal_anterior.empty:
        # Calcular variación total ponderada por el peso de ventas de cada canal
        for _, row in resultado.iterrows():
            peso_canal = row['Ventas_Reales'] / ventas_totales_periodo if ventas_totales_periodo > 0 else 0
            variacion_total_ponderada += row['Variacion_Ingreso_Pct'] * peso_canal
            variacion_ventas_total_ponderada += row['Variacion_Ventas_Pct'] * peso_canal
        
        # También calcular el % anterior total ponderado para referencia
        for _, row in resultado.iterrows():
            peso_canal = row['Ventas_Reales'] / ventas_totales_periodo if ventas_totales_periodo > 0 else 0
            ingreso_real_porcentaje_anterior_total += row['Ingreso_Real_Porcentaje_Anterior'] * peso_canal
            ventas_anterior_total += row['Ventas_Reales_Anterior'] * peso_canal
    
    resumen_general = {
        'ventas_totales': ventas_totales_periodo,
        'costo_venta': costo_venta_periodo,
        'costo_venta_porcentaje': costo_venta_porcentaje,
        'evolucion_costo': evolucion_costo,
        'evolucion_ventas': evolucion_ventas,
        'evolucion_ingreso': evolucion_ingreso,
        'gastos_directos': gastos_directos_periodo,
        'gastos_directos_porcentaje': gastos_directos_porcentaje,
        'ingreso_real_porcentaje': ingreso_real_porcentaje,
        'tipo_meta': tipo_meta,
        # DESGLOSE INDIVIDUAL DE GASTOS DIRECTOS - ✅ OPTIMIZADO: Usa campo pre-calculado total
        # Nota: Los valores individuales ya no están disponibles con la optimización ClickHouse
        'comision_periodo': 0.0,  # ✅ Campo individual no disponible en optimización
        'destino_periodo': 0.0,    # ✅ Campo individual no disponible en optimización
        'milla_periodo': 0.0,      # ✅ Campo individual no disponible en optimización
        # PORCENTAJES INDIVIDUALES DE CADA COMPONENTE - Total consolidado
        'comision_porcentaje': 0.0,    # ✅ Incluido en gastos_directos_porcentaje
        'destino_porcentaje': 0.0,     # ✅ Incluido en gastos_directos_porcentaje
        'milla_porcentaje': 0.0,       # ✅ Incluido en gastos_directos_porcentaje
        'ingreso_real': ingreso_real_periodo,
        # NUEVOS CAMPOS PARA TARJETAS DINÁMICAS
        'ventas_periodo_total': ventas_totales_periodo,  # Total de ventas del período (todos los canales)
        'ingreso_real_periodo_total': ingreso_real_periodo,  # Total de ingreso real del período (todos los canales)
        'num_transacciones': num_transacciones,
        'precio_promedio_ventas': precio_promedio_ventas,
        'precio_promedio_costo': precio_promedio_costo,
        'precio_promedio_gastos': precio_promedio_gastos,
        'precio_promedio_ingreso': precio_promedio_ingreso,
        'cantidad_total_unidades': cantidad_total_unidades,
        'meta_total': meta_total_periodo,
        'meta_total_mes': meta_total_mes,
        'cumplimiento_global': cumplimiento_global,
        'cumplimiento_vs_meta_mes': cumplimiento_vs_mes,
        # NUEVOS CAMPOS DE COMPARACIÓN CON PERÍODO ANTERIOR
        'ingreso_real_porcentaje_anterior': float(ingreso_real_porcentaje_anterior_total),
        'variacion_ingreso_pct': float(variacion_total_ponderada),
        'ventas_totales_anterior': float(ventas_anterior_total),
        'variacion_ventas_pct': float(variacion_ventas_total_ponderada),
        'periodo_comparacion': periodo_comparacion if 'periodo_comparacion' in locals() else 'Comparación con período anterior',
        'dias_comparados': dias_transcurridos if 'dias_transcurridos' in locals() else 0,
        'tipo_meta': tipo_meta
    }
    
    print(f"=== RESULTADO CUMPLIMIENTO ({tipo_meta.upper()}) ===")
    print(f"Canales analizados: {len(cumplimiento_list)}")
    if tipo_meta == "ingreso_real_nominal":
        print(f"Ingreso Real total: ${resultado['Ingreso_Real'].sum():,.2f}")
        print(f"Meta total período: ${meta_total_periodo:,.2f}")
        print(f"Meta total mes: ${meta_total_mes:,.2f}")
    elif tipo_meta == "ventas":
        print(f"Ventas totales: ${resultado['Ventas_Reales'].sum():,.2f}")
        print(f"Meta total período: ${meta_total_periodo:,.2f}")
    print(f"Cumplimiento global: {resumen_general['cumplimiento_global']:.1f}%")
    
    return cumplimiento_list, resumen_general

def get_default_resumen_general():
    """Retorna un diccionario con valores por defecto para resumen_general"""
    return {
        'ventas_totales': 0,
        'costo_venta_porcentaje': 0,
        'evolucion_costo': [],
        'evolucion_ventas': [],
        'evolucion_ingreso': [],
        'evolucion_roi': [],
        'gastos_directos': 0,
        'gastos_directos_porcentaje': 0,
        # DESGLOSE INDIVIDUAL DE GASTOS DIRECTOS
        'comision_periodo': 0,
        'destino_periodo': 0,
        'milla_periodo': 0,
        # PORCENTAJES INDIVIDUALES DE CADA COMPONENTE
        'comision_porcentaje': 0,
        'destino_porcentaje': 0,
        'milla_porcentaje': 0,
        'ingreso_real': 0,
        # NUEVOS CAMPOS PARA TARJETAS DINÁMICAS
        'ventas_periodo_total': 0,  # Total de ventas del período (todos los canales)
        'ingreso_real_periodo_total': 0,  # Total de ingreso real del período (todos los canales)
        'num_transacciones': 0,
        'precio_promedio_ventas': 0,
        'precio_promedio_costo': 0,
        'precio_promedio_gastos': 0,
        'precio_promedio_ingreso': 0,
        'cantidad_total_unidades': 0,
        'meta_total': 0,
        'meta_total_mes': 0,
        'cumplimiento_vs_meta_mes': 0,
        'cumplimiento_global': 0,
        # NUEVOS CAMPOS DE COMPARACIÓN CON PERÍODO ANTERIOR
        'ingreso_real_porcentaje': 0,
        'ingreso_real_porcentaje_anterior': 0,
        'variacion_ingreso_pct': 0,
        'periodo_comparacion': 'Comparación con período anterior',
        'dias_comparados': 0,
        # ✅ NUEVO: Campo ROI
        'roi_promedio': 0.0,
        'roi_promedio_anterior': 0.0,
        'variacion_roi_pct': 0.0
    }

