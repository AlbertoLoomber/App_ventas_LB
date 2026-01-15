# -*- coding: utf-8 -*-
"""
Rutas del módulo de Radar Comercial
"""

from flask import render_template, request, jsonify
from radar_comercial.blueprint import bp
from radar_comercial.services import (
    procesar_datos_radar,
    filtrar_productos,
    clasificar_ir,
    procesar_analisis_competencia,
    procesar_datos_semanales
)
from database import get_radar_comercial_datos_semanales


@bp.route("/radar-comercial", methods=["GET"])
def radar_comercial():
    """
    Ruta principal para el Radar Comercial

    Muestra comparación de precios e IR por canal para productos relevantes:
    - Mercado Libre
    - CrediTienda
    - Walmart
    - Shein
    """
    try:
        print("INFO: [RADAR COMERCIAL] Cargando datos...")

        # Procesar datos del radar
        productos, estadisticas = procesar_datos_radar()

        # Procesar análisis de competencia
        competencia = procesar_analisis_competencia()

        # Aplicar filtro de búsqueda si existe
        filtro_busqueda = request.args.get('buscar', '').strip()
        if filtro_busqueda:
            productos = filtrar_productos(productos, filtro_busqueda)

        print(f"INFO: [RADAR COMERCIAL] Mostrando {len(productos)} productos")
        print(f"INFO: [RADAR COMERCIAL] Análisis de competencia para {len(competencia)} SKUs")

        return render_template(
            "radar_comercial.html",
            productos=productos,
            estadisticas=estadisticas,
            competencia=competencia,
            filtro_busqueda=filtro_busqueda,
            clasificar_ir=clasificar_ir,
            active_tab="radar_comercial"
        )

    except Exception as e:
        print(f"ERROR: [RADAR COMERCIAL] {e}")
        import traceback
        traceback.print_exc()

        return render_template(
            "radar_comercial.html",
            productos=[],
            estadisticas={},
            competencia={},
            filtro_busqueda='',
            clasificar_ir=clasificar_ir,
            active_tab="radar_comercial",
            error=f"Error cargando datos: {str(e)}"
        )


@bp.route("/radar-comercial-ajax", methods=["POST"])
def radar_comercial_ajax():
    """
    Endpoint AJAX para búsqueda filtrada en tiempo real
    """
    try:
        filtro_busqueda = request.json.get('buscar', '').strip()

        # Procesar datos
        productos, estadisticas = procesar_datos_radar()

        # Aplicar filtro
        if filtro_busqueda:
            productos = filtrar_productos(productos, filtro_busqueda)

        return jsonify({
            'success': True,
            'productos': productos,
            'estadisticas': estadisticas,
            'total_resultados': len(productos)
        })

    except Exception as e:
        print(f"ERROR: [RADAR COMERCIAL AJAX] {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@bp.route("/radar-comercial-datos-semanales", methods=["GET"])
def radar_comercial_datos_semanales():
    """
    Endpoint AJAX para obtener datos semanales de inventario y ventas
    """
    try:
        # Obtener parámetros
        semana_num = request.args.get('semana', type=int)

        print(f"INFO: [RADAR SEMANAL AJAX] Solicitando datos para semana {semana_num if semana_num else 'actual'}")

        # Obtener datos semanales
        df_semanal = get_radar_comercial_datos_semanales(semana_num=semana_num)

        print(f"DEBUG: [RADAR AJAX] DataFrame recibido: {len(df_semanal)} filas")
        if not df_semanal.empty:
            print(f"DEBUG: [RADAR AJAX] Columnas: {df_semanal.columns.tolist()}")
            print(f"DEBUG: [RADAR AJAX] Primeras 3 filas:\n{df_semanal.head(3)}")

        if df_semanal.empty:
            print(f"WARN: [RADAR AJAX] DataFrame vacío, retornando datos vacíos")
            return jsonify({
                'success': True,
                'datos_semanales': {},
                'semana_actual': semana_num if semana_num else 1
            })

        # Procesar datos para formato JSON
        datos_procesados = procesar_datos_semanales(df_semanal)

        print(f"DEBUG: [RADAR AJAX] Datos procesados: {len(datos_procesados)} SKUs")
        if datos_procesados:
            primer_sku = list(datos_procesados.keys())[0]
            print(f"DEBUG: [RADAR AJAX] Ejemplo SKU {primer_sku}: {datos_procesados[primer_sku]}")

        return jsonify({
            'success': True,
            'datos_semanales': datos_procesados,
            'semana_actual': semana_num if semana_num else 1
        })

    except Exception as e:
        print(f"ERROR: [RADAR SEMANAL AJAX] {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
