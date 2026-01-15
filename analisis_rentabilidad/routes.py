# -*- coding: utf-8 -*-
"""
Rutas del módulo de Análisis de Rentabilidad
Control de Ingreso Real y métricas de rentabilidad
"""

from flask import render_template, request
from datetime import datetime
import time
import sys

from analisis_rentabilidad.blueprint import bp
from database import (
    cargar_ultimos_3_meses_rentabilidad,
    filtrar_por_mes_actual_rentabilidad,
    obtener_mes_actual,
    cargar_inventario_disponible,
    cargar_inventario_en_transito
)
from analisis_rentabilidad.services import (
    calcular_resumen_rentabilidad,
    generar_datos_canales_principales,
    generar_todos_los_skus_optimizado,
    unificar_productos_por_categoria_dominante,
    get_default_resumen_general
)
from utils import formato_periodo_texto, clean_data_for_json


@bp.route("/analisis-rentabilidad", methods=["GET", "POST"])
def analisis_rentabilidad():
    """Pestaña de Control de Ingreso Real"""
    tiempo_inicio_total = time.time()
    print(f"\n[PERFORMANCE] INICIO - Control de Ingreso Real")
    sys.stdout.flush()

    # Obtener filtro de mes global (GET) - Por defecto el mes actual del sistema
    mes_actual = obtener_mes_actual()  # Detecta el mes actual automáticamente
    mes_filtro = request.args.get('mes_filtro', str(mes_actual))
    mes_seleccionado = int(mes_filtro) if mes_filtro and mes_filtro.isdigit() else mes_actual

    #  OPCIÓN 3: Cargar datos de últimos 3 meses para desglose temporal
    print(f"INFO: Cargando datos de últimos 3 meses para análisis de rentabilidad...")
    sys.stdout.flush()
    tiempo_bd_inicio = time.time()

    tiempo_ventas_inicio = time.time()
    df_completo, channels_disponibles, warehouses_disponibles = cargar_ultimos_3_meses_rentabilidad()
    tiempo_ventas_fin = time.time()
    print(f"  [PERFORMANCE] BD - Carga ventas 3 meses: {tiempo_ventas_fin - tiempo_ventas_inicio:.3f} segundos")
    sys.stdout.flush()

    # ✅ OPTIMIZACIÓN 1: Eliminar carga innecesaria de metas (no se usan en función optimizada)
    print(f"✅ [OPTIMIZADO] Carga de metas eliminada - no requerida para Control de Ingreso Real")
    sys.stdout.flush()

    # ✅ NUEVO: Cargar datos de inventario (disponible + en tránsito)
    tiempo_inventario_inicio = time.time()
    inventario_disponible = cargar_inventario_disponible()
    inventario_transito = cargar_inventario_en_transito()
    tiempo_inventario_fin = time.time()
    print(f"  [PERFORMANCE] BD - Carga inventario: {tiempo_inventario_fin - tiempo_inventario_inicio:.3f} segundos")
    sys.stdout.flush()

    tiempo_bd_fin = time.time()
    print(f"\n" + "="*50)
    print(f"  [PERFORMANCE] *** CARGA DESDE BD TOTAL: {tiempo_bd_fin - tiempo_bd_inicio:.3f} segundos ***")
    print(f"="*50 + "\n")
    sys.stdout.flush()

    # Filtrar solo el mes actual para métricas principales (resumen general)
    tiempo_filtrado_inicio = time.time()
    df_mes_actual = filtrar_por_mes_actual_rentabilidad(df_completo, mes_seleccionado)
    tiempo_filtrado_fin = time.time()
    print(f"\n" + ""*50)
    print(f"  [PERFORMANCE] *** FILTRADO DE DATOS: {tiempo_filtrado_fin - tiempo_filtrado_inicio:.3f} segundos ***")
    print(f""*50 + "\n")
    sys.stdout.flush()

    # DEBUG: Información de datos
    print(f"DEBUG: Total registros (3 meses): {len(df_completo)}")
    print(f"DEBUG: Registros mes actual ({mes_seleccionado}): {len(df_mes_actual)}")
    sys.stdout.flush()

    # ✅ NUEVO: Obtener meses disponibles con año para selector dinámico
    meses_disponibles_data = []
    if not df_completo.empty:
        # Diccionario para traducir meses al español
        meses_español = {
            1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
            5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
            9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
        }

        # Agrupar por año y mes, ordenar del más reciente al más antiguo
        df_completo['año_mes'] = df_completo['Fecha'].dt.to_period('M')
        periodos_unicos = sorted(df_completo['año_mes'].unique(), reverse=True)

        for periodo in periodos_unicos:
            año = periodo.year
            mes = periodo.month
            nombre_mes = meses_español.get(mes, f'Mes {mes}')
            # Si el año es diferente al actual, agregarlo al nombre
            if año != datetime.now().year:
                nombre_completo = f'{nombre_mes} {año}'
            else:
                nombre_completo = nombre_mes

            meses_disponibles_data.append({
                'valor': mes,
                'año': año,
                'nombre': nombre_completo
            })

        print(f"DEBUG: Meses disponibles encontrados: {[m['nombre'] for m in meses_disponibles_data]}")
        sys.stdout.flush()

    # Si el mes seleccionado no tiene datos, redirigir al mes más reciente
    if df_mes_actual.empty and meses_disponibles_data:
        mes_mas_reciente = meses_disponibles_data[0]['valor']
        print(f"INFO: Mes seleccionado ({mes_seleccionado}) sin datos. Redirigiendo al mes más reciente: {mes_mas_reciente}")
        sys.stdout.flush()
        mes_seleccionado = mes_mas_reciente
        df_mes_actual = filtrar_por_mes_actual_rentabilidad(df_completo, mes_seleccionado)

    if df_completo.empty:
        # Error si no hay datos de ningún mes
        tiempo_total_fin = time.time()
        print(f"[PERFORMANCE] ERROR - Sin datos completos. TIEMPO TOTAL: {tiempo_total_fin - tiempo_inicio_total:.3f} segundos")
        sys.stdout.flush()
        mensaje_error = f"No se encontraron datos de ventas para los últimos 3 meses."
        return render_template("analisis_rentabilidad.html",
                             resumen_general=get_default_resumen_general(),
                             periodo_texto="",
                             active_tab="analisis-rentabilidad",
                             selected_preset_main="mes_actual",
                             selected_main_range="",
                             mes_seleccionado=mes_seleccionado,
                             meses_disponibles=meses_disponibles_data,
                             error=mensaje_error)

    if df_mes_actual.empty:
        # Warning si no hay datos del mes específico, pero sí de otros meses
        tiempo_total_fin = time.time()
        print(f"[PERFORMANCE] ERROR - Sin datos mes actual. TIEMPO TOTAL: {tiempo_total_fin - tiempo_inicio_total:.3f} segundos")
        sys.stdout.flush()
        meses_disponibles = sorted(df_completo['Fecha'].dt.month.unique())
        mensaje_error = f"No hay datos para el mes {mes_seleccionado}. Meses disponibles: {meses_disponibles}"
        return render_template("analisis_rentabilidad.html",
                             resumen_general=get_default_resumen_general(),
                             periodo_texto="",
                             active_tab="analisis-rentabilidad",
                             selected_preset_main="mes_actual",
                             selected_main_range="",
                             mes_seleccionado=mes_seleccionado,
                             meses_disponibles=meses_disponibles_data,
                             error=mensaje_error)

    # CALCULAR ANÁLISIS DE RENTABILIDAD SIEMPRE QUE HAY DATOS DE VENTAS
    print("INFO: Hay datos de ventas, calculando Análisis de Rentabilidad...")

    # Obtener fechas del período para texto descriptivo (del mes actual)
    fecha_inicio = df_mes_actual['Fecha'].min()
    fecha_fin = df_mes_actual['Fecha'].max()

    # Configuración de preset por defecto para mes actual
    selected_preset_main = "mes_actual"
    selected_main_range = ""

    # Generar texto del período
    periodo_texto = formato_periodo_texto(selected_preset_main, fecha_inicio, fecha_fin)

    # ✅ OPTIMIZADO: Usar función específica para Control de Ingreso Real
    # Elimina lógica innecesaria de metas y cumplimiento que no se usa

    # Calcular resumen de rentabilidad OPTIMIZADO - USANDO DATOS DEL MES ACTUAL
    tiempo_resumen_inicio = time.time()
    print(f" [OPTIMIZADO] Iniciando cálculo resumen rentabilidad...")
    sys.stdout.flush()
    resumen_general = calcular_resumen_rentabilidad(df_mes_actual, fecha_inicio, fecha_fin, df_completo, mes_seleccionado)
    tiempo_resumen_fin = time.time()
    print(f"\n" + ""*50)
    print(f"  [PERFORMANCE OPTIMIZADO] *** CÁLCULO RESUMEN RENTABILIDAD: {tiempo_resumen_fin - tiempo_resumen_inicio:.3f} segundos ***")
    print(f""*50 + "\n")
    sys.stdout.flush()

    # NUEVO: Generar datos de canales principales para la tabla de rentabilidad - USANDO DATOS DEL MES ACTUAL
    tiempo_canales_inicio = time.time()
    print(f" [PYTHON] Iniciando generación datos canales...")
    sys.stdout.flush()
    canales_principales = generar_datos_canales_principales(df_mes_actual, fecha_inicio, fecha_fin)
    tiempo_canales_fin = time.time()
    print(f"\n" + ""*50)
    print(f"  [PERFORMANCE] *** GENERACIÓN DATOS CANALES: {tiempo_canales_fin - tiempo_canales_inicio:.3f} segundos ***")
    print(f""*50 + "\n")
    sys.stdout.flush()

    #  GENERAR SKUs CON DATOS DEL MES ACTUAL (métricas principales) + DATOS COMPLETOS (desglose temporal)
    tiempo_skus_inicio = time.time()
    print(f" [SÚPER OPTIMIZADO] Generando TODOS los SKUs en UNA SOLA operación...")
    sys.stdout.flush()

    # SÚPER OPTIMIZACIÓN: Una sola operación para todos los SKUs
    tiempo_todos_skus_inicio = time.time()
    año_actual = datetime.now().year
    todos_skus_result = generar_todos_los_skus_optimizado(
        df_mes_actual, df_completo, fecha_inicio, fecha_fin, año_actual, mes_seleccionado
    )

    # Extraer resultados
    skus_estrella = todos_skus_result['skus_estrella']
    skus_prometedores = todos_skus_result['skus_prometedores']
    skus_potenciales = todos_skus_result['skus_potenciales']
    skus_revision = todos_skus_result['skus_revision']
    skus_remover = todos_skus_result['skus_remover']

    # ✅ NUEVO: Aplicar unificación de productos considerando todas las categorías
    tiempo_unificacion_inicio = time.time()
    skus_estrella, skus_prometedores, skus_potenciales, skus_revision, skus_remover = unificar_productos_por_categoria_dominante(
        skus_estrella, skus_prometedores, skus_potenciales, skus_revision, skus_remover
    )
    tiempo_unificacion_fin = time.time()
    print(f" Unificación de productos completada: {tiempo_unificacion_fin - tiempo_unificacion_inicio:.3f} segundos")

    tiempo_todos_skus_fin = time.time()
    print(f" [SÚPER OPTIMIZADO] *** TODOS LOS SKUs COMPLETADO: {tiempo_todos_skus_fin - tiempo_todos_skus_inicio:.3f} segundos ***")
    sys.stdout.flush()

    tiempo_skus_fin = time.time()

    # MOSTRAR RESUMEN OPTIMIZADO DE TIEMPOS SKUs
    tiempo_total_skus = tiempo_skus_fin - tiempo_skus_inicio
    tiempo_optimizado = tiempo_todos_skus_fin - tiempo_todos_skus_inicio
    ganancia_estimada = 31.0 - tiempo_optimizado  # Estimación basada en el problema original

    print(f"\n" + "="*80)
    print(f" [RESUMEN SÚPER OPTIMIZADO] TIEMPOS SKUs:")
    print(f"    TODOS los SKUs (1 operación): {tiempo_optimizado:.3f} segundos")
    print(f"    Estrella: {len(skus_estrella)} SKUs")
    print(f"    Prometedores: {len(skus_prometedores)} SKUs")
    print(f"    Potenciales: {len(skus_potenciales)} SKUs")
    print(f"    Revisión: {len(skus_revision)} SKUs")
    print(f"    Remover: {len(skus_remover)} SKUs")
    print(f"   TOTAL SKUs:        {tiempo_total_skus:.3f} segundos")
    print(f"    GANANCIA ESTIMADA: ~{ganancia_estimada:.1f} segundos (-{((31.0 - tiempo_optimizado) / 31.0 * 100):.1f}%)")
    print(f"="*80 + "\n")

    print(f" [PERFORMANCE OPTIMIZADO] Generación TODOS los SKUs: {tiempo_skus_fin - tiempo_skus_inicio:.3f} segundos")
    sys.stdout.flush()


    # Limpiar datos para JSON
    tiempo_limpieza_inicio = time.time()
    clean_resumen_general = clean_data_for_json(resumen_general, "resumen_general")
    clean_canales_principales = clean_data_for_json(canales_principales, "canales_principales")
    clean_skus_estrella = clean_data_for_json(skus_estrella, "skus_estrella")
    clean_skus_prometedores = clean_data_for_json(skus_prometedores, "skus_prometedores")
    clean_skus_potenciales = clean_data_for_json(skus_potenciales, "skus_potenciales")
    clean_skus_revision = clean_data_for_json(skus_revision, "skus_revision")
    clean_skus_remover = clean_data_for_json(skus_remover, "skus_remover")
    tiempo_limpieza_fin = time.time()
    print(f"  [PERFORMANCE] Limpieza datos para JSON: {tiempo_limpieza_fin - tiempo_limpieza_inicio:.3f} segundos")
    sys.stdout.flush()

    # Renderizar template
    tiempo_render_inicio = time.time()
    resultado = render_template("analisis_rentabilidad.html",
                         resumen_general=clean_resumen_general,
                         canales_principales=clean_canales_principales,
                         skus_estrella=clean_skus_estrella,
                         skus_prometedores=clean_skus_prometedores,
                         skus_potenciales=clean_skus_potenciales,
                         skus_revision=clean_skus_revision,
                         skus_remover=clean_skus_remover,
                         periodo_texto=periodo_texto,
                         active_tab="analisis-rentabilidad",
                         selected_preset_main=selected_preset_main,
                         selected_main_range=selected_main_range,
                         mes_seleccionado=mes_seleccionado,
                         meses_disponibles=meses_disponibles_data,
                         inventario_disponible=inventario_disponible,
                         inventario_transito=inventario_transito)
    tiempo_render_fin = time.time()
    print(f"  [PERFORMANCE] Renderizado de template: {tiempo_render_fin - tiempo_render_inicio:.3f} segundos")
    sys.stdout.flush()

    # Tiempo total
    tiempo_total_fin = time.time()
    tiempo_total = tiempo_total_fin - tiempo_inicio_total

    # Calcular tiempos parciales para verificar suma
    tiempo_bd = tiempo_bd_fin - tiempo_bd_inicio
    tiempo_filtrado = tiempo_filtrado_fin - tiempo_filtrado_inicio
    tiempo_resumen = tiempo_resumen_fin - tiempo_resumen_inicio
    tiempo_canales = tiempo_canales_fin - tiempo_canales_inicio
    tiempo_skus = tiempo_skus_fin - tiempo_skus_inicio
    tiempo_limpieza = tiempo_limpieza_fin - tiempo_limpieza_inicio
    tiempo_render = tiempo_render_fin - tiempo_render_inicio

    suma_parciales = tiempo_bd + tiempo_filtrado + tiempo_resumen + tiempo_canales + tiempo_skus + tiempo_limpieza + tiempo_render
    tiempo_no_medido = tiempo_total - suma_parciales

    print(f"[PERFORMANCE] TIEMPO TOTAL: {tiempo_total:.3f} segundos")
    print(f" [PERFORMANCE] SUMA PARCIALES: {suma_parciales:.3f} segundos")
    print(f" [PERFORMANCE] TIEMPO NO MEDIDO: {tiempo_no_medido:.3f} segundos")
    print(f"INFO: Análisis de rentabilidad calculado exitosamente")
    print(f"INFO: Ventas totales: ${resumen_general.get('ventas_totales', 0):,.0f}")
    print(f"INFO: Ingreso real: ${resumen_general.get('ingreso_real', 0):,.0f}")
    print(f"INFO: Rentabilidad: {resumen_general.get('ingreso_real_porcentaje', 0):.1f}%")
    sys.stdout.flush()

    return resultado
