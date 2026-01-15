"""
Funciones de cálculo para métricas de cumplimiento de metas.
Separa la lógica de cálculo de la lógica de negocio principal.
"""

import pandas as pd
from .strategies import get_strategy
from .config import obtener_config_meta


def procesar_metas_por_tipo(df_metas, tipo_meta, fecha_actual):
    """
    Procesa las metas según el tipo y las agrupa por canal.

    Args:
        df_metas (pd.DataFrame): DataFrame con metas diarias de ClickHouse
        tipo_meta (str): Tipo de meta ('ventas', 'costo', 'ingreso_real', 'ingreso_real_nominal')
        fecha_actual (pd.Timestamp): Fecha actual del período

    Returns:
        pd.DataFrame: DataFrame con metas agrupadas por canal
                     Retorna DataFrame vacío si no hay metas o tipo es de rango
    """
    if df_metas.empty:
        print(f"INFO: No hay metas disponibles para tipo '{tipo_meta}'")
        return pd.DataFrame()

    config = obtener_config_meta(tipo_meta)

    # Los tipos de porcentaje (rango) no necesitan metas del DataFrame
    if config['es_porcentaje']:
        print(f"INFO: Tipo '{tipo_meta}' es de rango, no requiere procesamiento de metas")
        return pd.DataFrame()

    # Filtrar metas para la fecha actual
    metas_fecha = df_metas[df_metas["Fecha"] == fecha_actual].copy()

    if metas_fecha.empty:
        print(f"WARNING: No hay metas para la fecha {fecha_actual}")
        return pd.DataFrame()

    # Campos a agrupar según configuración
    campos_agg = {
        'Modelo_Usado': 'first'
    }

    # Agregar campos de meta según configuración
    if config['campo_meta_acumulada'] in metas_fecha.columns:
        campos_agg[config['campo_meta_acumulada']] = 'first'
    else:
        print(f"WARNING: Campo '{config['campo_meta_acumulada']}' no encontrado en df_metas")

    if config['campo_meta_mensual'] in metas_fecha.columns:
        campos_agg[config['campo_meta_mensual']] = 'first'
    else:
        print(f"WARNING: Campo '{config['campo_meta_mensual']}' no encontrado en df_metas")

    # Agrupar por canal
    try:
        metas_agrupadas = metas_fecha.groupby('Canal').agg(campos_agg).reset_index()
        print(f"INFO: Procesadas metas para {len(metas_agrupadas)} canales (tipo: {tipo_meta})")
        return metas_agrupadas
    except Exception as e:
        print(f"ERROR al agrupar metas: {e}")
        return pd.DataFrame()


def calcular_metricas_canal(ventas_por_canal, metas_por_canal, tipo_meta):
    """
    Calcula métricas de cumplimiento para cada canal según el tipo de meta.
    Utiliza el patrón Strategy para delegar los cálculos específicos.

    Args:
        ventas_por_canal (pd.DataFrame): DataFrame con ventas agrupadas por canal
        metas_por_canal (pd.DataFrame): DataFrame con metas por canal (puede estar vacío para tipos de rango)
        tipo_meta (str): Tipo de meta

    Returns:
        pd.DataFrame: DataFrame con todas las métricas calculadas
    """
    config = obtener_config_meta(tipo_meta)
    strategy = get_strategy(tipo_meta)

    print(f"=== CALCULANDO MÉTRICAS PARA TIPO: {tipo_meta.upper()} ===")

    # Inicializar resultado con datos de ventas
    resultado = ventas_por_canal.copy()

    if config['es_porcentaje']:
        # === METAS DE RANGO (costo, ingreso_real %) ===
        print(f"Procesando meta de rango: {config['nombre']}")

        campo_real = config['campo_real']

        # Verificar que el campo real exista
        if campo_real not in resultado.columns:
            print(f"ERROR: Campo '{campo_real}' no encontrado en ventas_por_canal")
            return pd.DataFrame()

        # Calcular cumplimiento y diferencia usando estrategia
        resultado['Cumplimiento'] = resultado[campo_real].apply(
            lambda x: strategy.calcular_cumplimiento(x)
        )
        resultado['Diferencia'] = resultado[campo_real].apply(
            lambda x: strategy.calcular_diferencia(x)
        )

        # Preparar displays
        resultado['Meta_Display'] = strategy.preparar_meta_display()
        resultado['Ventas_Reales_Display'] = resultado[campo_real].apply(
            lambda x: strategy.preparar_valor_display(x)
        )

        # Meta_Periodo es el rango (string) para estos tipos
        resultado['Meta_Periodo'] = strategy.preparar_meta_display()
        resultado['Meta_Periodo_Numerico'] = 0  # No aplica para rangos
        resultado['Modelo_Usado'] = 'RANGO'

    else:
        # === METAS ABSOLUTAS (ventas, ingreso_real_nominal) ===
        print(f"Procesando meta absoluta: {config['nombre']}")

        campo_real = config['campo_real']
        campo_meta_acumulada = config['campo_meta_acumulada']
        campo_meta_mensual = config['campo_meta_mensual']

        # Verificar que el campo real exista
        if campo_real not in resultado.columns:
            print(f"ERROR: Campo '{campo_real}' no encontrado en ventas_por_canal")
            return pd.DataFrame()

        # Merge con metas (si existen)
        if not metas_por_canal.empty:
            # Normalizar nombres de canales antes del merge
            canal_mapping = {
                'AliExpress': 'Aliexpress',
                'Mercado Libre': 'Mercado Libre',
            }
            metas_normalizadas = metas_por_canal.copy()
            metas_normalizadas['Canal'] = metas_normalizadas['Canal'].replace(canal_mapping)

            resultado = pd.merge(
                resultado,
                metas_normalizadas,
                on='Canal',
                how='left'
            )
            print(f"Merge completado: {len(resultado)} canales")
        else:
            print("WARNING: No hay metas disponibles, agregando columnas con valores 0")
            resultado[campo_meta_acumulada] = 0
            resultado[campo_meta_mensual] = 0
            resultado['Modelo_Usado'] = 'N/A'

        # Convertir meta acumulada a numérico
        resultado['Meta_Periodo_Numerico'] = pd.to_numeric(
            resultado[campo_meta_acumulada], errors='coerce'
        ).fillna(0)

        # Calcular cumplimiento y diferencia usando estrategia
        resultado['Cumplimiento'] = resultado.apply(
            lambda row: strategy.calcular_cumplimiento(
                row[campo_real], row['Meta_Periodo_Numerico']
            ), axis=1
        )
        resultado['Diferencia'] = resultado.apply(
            lambda row: strategy.calcular_diferencia(
                row[campo_real], row['Meta_Periodo_Numerico']
            ), axis=1
        )

        # Preparar displays
        resultado['Meta_Display'] = resultado['Meta_Periodo_Numerico'].apply(
            lambda x: strategy.preparar_meta_display(x)
        )
        resultado['Ventas_Reales_Display'] = resultado[campo_real].apply(
            lambda x: strategy.preparar_valor_display(x)
        )

        # Meta_Periodo mantiene el valor numérico para estos tipos
        resultado['Meta_Periodo'] = resultado['Meta_Periodo_Numerico']

    # Llenar valores faltantes
    resultado['Meta_Periodo_Numerico'] = resultado['Meta_Periodo_Numerico'].fillna(0)
    resultado['Cumplimiento'] = resultado['Cumplimiento'].fillna(0)
    resultado['Diferencia'] = resultado['Diferencia'].fillna(0)

    if 'Modelo_Usado' in resultado.columns:
        resultado['Modelo_Usado'] = resultado['Modelo_Usado'].fillna('N/A')
    else:
        resultado['Modelo_Usado'] = 'N/A'

    print(f"✓ Métricas calculadas para {len(resultado)} canales")
    print(f"  Cumplimiento promedio: {resultado['Cumplimiento'].mean():.2f}%")

    return resultado


def calcular_meta_total_mes(metas_por_canal, tipo_meta):
    """
    Calcula la meta total del mes sumando las metas mensuales de todos los canales.

    Args:
        metas_por_canal (pd.DataFrame): DataFrame con metas por canal
        tipo_meta (str): Tipo de meta

    Returns:
        float: Meta total del mes (0 si no aplica o no hay datos)
    """
    config = obtener_config_meta(tipo_meta)

    # Solo aplica para metas absolutas
    if config['es_porcentaje']:
        print(f"INFO: Tipo '{tipo_meta}' no tiene meta mensual total (es de rango)")
        return 0.0

    if metas_por_canal.empty:
        print(f"WARNING: No hay metas por canal para calcular meta total del mes")
        return 0.0

    campo_meta_mensual = config['campo_meta_mensual']

    if campo_meta_mensual not in metas_por_canal.columns:
        print(f"WARNING: Campo '{campo_meta_mensual}' no encontrado en metas_por_canal")
        return 0.0

    meta_total = float(metas_por_canal[campo_meta_mensual].sum())
    print(f"INFO: Meta total del mes calculada: ${meta_total:,.0f}")

    return meta_total
