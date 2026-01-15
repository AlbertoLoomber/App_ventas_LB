"""
Data Filtering Functions - Matriz de Posicionamiento
Funciones para filtrar datos por mes y rango de días
"""


def filtrar_por_mes(df, mes_filtro):
    """
    Filtra DataFrame por mes, manejando formato YYYYMM (202410) o entero (10)

    Args:
        df: DataFrame con columna 'Fecha'
        mes_filtro: Mes en formato YYYYMM (ej: 202410) o entero (1-12)

    Returns:
        DataFrame filtrado
    """
    mes_filtro_str = str(mes_filtro)

    if len(mes_filtro_str) == 6:  # Formato YYYYMM (202410)
        año = int(mes_filtro_str[:4])
        mes = int(mes_filtro_str[4:6])
        return df[(df['Fecha'].dt.year == año) & (df['Fecha'].dt.month == mes)].copy()
    else:  # Formato antiguo (1-12)
        mes = int(mes_filtro_str)
        return df[df['Fecha'].dt.month == mes].copy()


def filtrar_por_rango_dias(df, mes_filtro, dia_maximo=None):
    """
    Filtra DataFrame por mes y mismo rango de días
    Si dia_maximo es None, toma todos los días del mes
    Si dia_maximo es 5, filtra del día 1 al 5 del mes

    Args:
        df: DataFrame con columna 'Fecha'
        mes_filtro: Mes en formato YYYYMM (ej: 202410)
        dia_maximo: Día máximo a incluir (1-31), None para todo el mes

    Returns:
        DataFrame filtrado
    """
    mes_filtro_str = str(mes_filtro)

    if len(mes_filtro_str) != 6:
        # Si no es formato YYYYMM, usar filtro normal
        return filtrar_por_mes(df, mes_filtro)

    año = int(mes_filtro_str[:4])
    mes = int(mes_filtro_str[4:6])

    # Filtrar por año y mes
    df_mes = df[(df['Fecha'].dt.year == año) & (df['Fecha'].dt.month == mes)].copy()

    # Si no se especifica día máximo, devolver todo el mes
    if dia_maximo is None:
        return df_mes

    # Filtrar por rango de días (1 hasta dia_maximo)
    df_filtrado = df_mes[df_mes['Fecha'].dt.day <= dia_maximo].copy()

    print(f"📅 [FILTRO] Mes {mes_filtro}: Días 1-{dia_maximo} → {len(df_filtrado)} registros")

    return df_filtrado
