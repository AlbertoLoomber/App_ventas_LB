# -*- coding: utf-8 -*-
"""
Rutas del módulo de Reparto de Inventario
Vista consultiva para encargados de canal
"""

from flask import render_template, jsonify, request
from reparto_inventario import bp
from database import (
    get_db_connection,
    get_distribucion_semanal_inventario,
    calcular_asignacion_semanal_secuencial
)
import pandas as pd
from datetime import datetime


def obtener_cupos_manuales_originales(mes):
    """
    Consulta directa a la tabla Silver.Distribucion_Mensual_Canal_Manual
    para obtener los valores de cupo_manual originales (solo para mostrar en UI)

    Returns:
        dict: Diccionario con clave (sku, canal) y valor cupo_manual
    """
    try:
        client = get_db_connection()

        # Convertir nombre del mes a formato de la tabla (ej: "Diciembre 2025")
        query = f"""
        SELECT
            sku,
            Channel,
            cupo_manual
        FROM Silver.Distribucion_Mensual_Canal_Manual
        WHERE mes = '{mes}'
          AND activo = 1
        """

        result = client.query(query)

        # Crear diccionario con clave (sku, canal) -> cupo_manual
        cupos_dict = {}
        for row in result.result_rows:
            sku_val = row[0]
            canal_val = row[1]
            cupo_manual = float(row[2])
            cupos_dict[(sku_val, canal_val)] = cupo_manual

        print(f"OK: [CUPOS MANUALES] Cargados {len(cupos_dict)} registros de cupos manuales para {mes}")
        return cupos_dict

    except Exception as e:
        print(f"ERROR: [CUPOS MANUALES] Error obteniendo cupos manuales: {e}")
        import traceback
        traceback.print_exc()
        return {}


@bp.route("/reparto-inventario")
def reparto_inventario():
    """Página principal de reparto de inventario - Vista consultiva"""
    return render_template('reparto_inventario.html', active_tab='reparto-inventario')


@bp.route("/reparto-inventario-datos", methods=["GET"])
def reparto_inventario_datos():
    """Endpoint AJAX para obtener datos de reparto con todas las reglas de negocio aplicadas"""

    try:
        mes = request.args.get('mes', '')
        canal = request.args.get('canal', '')
        sku = request.args.get('sku', '')

        if not mes:
            return jsonify({'success': False, 'message': 'Mes requerido'}), 400

        # Obtener datos con la función que ya tiene todas las reglas implementadas
        # Esta función ya llama internamente a calcular_asignacion_semanal_secuencial
        df_resultado = get_distribucion_semanal_inventario(mes)

        if df_resultado is None or df_resultado.empty:
            return jsonify({
                'success': True,
                'data': [],
                'canales': [],
                'skus': []
            })

        # Obtener cupos manuales originales de la tabla (solo para mostrar en UI)
        cupos_manuales_dict = obtener_cupos_manuales_originales(mes)

        # Obtener listas de canales y SKUs ANTES de filtrar
        canales_disponibles = sorted(df_resultado['canal'].unique().tolist())
        skus_disponibles = sorted(df_resultado['sku'].unique().tolist())

        # Aplicar filtros
        if canal and canal != 'Todos':
            df_resultado = df_resultado[df_resultado['canal'] == canal]

        if sku and sku.strip():
            df_resultado = df_resultado[
                df_resultado['sku'].str.contains(sku, case=False, na=False) |
                df_resultado['descripcion'].str.contains(sku, case=False, na=False)
            ]

        if df_resultado.empty:
            return jsonify({
                'success': True,
                'data': [],
                'canales': canales_disponibles,
                'skus': skus_disponibles
            })

        # Preparar datos para el frontend
        # Agrupar por SKU y Canal para tener todas las semanas juntas
        registros = []
        for (sku_val, canal_val), grupo in df_resultado.groupby(['sku', 'canal']):
            # Obtener datos de la primera fila para información general
            primera_fila = grupo.iloc[0]

            # Obtener el cupo manual original de la tabla (solo para mostrar en UI)
            # La clave es (sku, canal)
            cupo_manual_original = cupos_manuales_dict.get((sku_val, canal_val), 0)

            registro = {
                'sku': sku_val,
                'descripcion': primera_fila.get('descripcion', ''),
                'canal': canal_val,
                'disponible_total': cupo_manual_original,  # ✅ Cupo manual de la tabla (solo visual)
                'semanas': []
            }

            # Ordenar grupo por semana y procesar cada semana
            grupo_ordenado = grupo.sort_values('semana')

            for idx, row in grupo_ordenado.iterrows():
                semana_nombre = row['semana']
                asignacion = float(row.get('asignacion_canal', 0))
                # Usar ventas informativas para mostrar (incluye semana actual)
                ventas = float(row.get('ventas_reales_informativas', 0))

                # Calcular cumplimiento
                cumplimiento = (ventas / asignacion * 100) if asignacion > 0 else 0

                # Determinar estado
                if ventas > asignacion * 1.05:  # Tolerancia 5%
                    estado = 'sobre-venta'
                elif cumplimiento >= 95:
                    estado = 'cumplido'
                elif cumplimiento >= 80:
                    estado = 'parcial'
                else:
                    estado = 'bajo'

                registro['semanas'].append({
                    'semana': semana_nombre,
                    'asignacion': asignacion,
                    'ventas': ventas,
                    'cumplimiento': cumplimiento,
                    'estado': estado
                })

            # Agregar el inventario asignado (cupo manual original de la tabla)
            # IMPORTANTE: Usar cupo_manual_original (de la tabla) NO inventario_asignado_total (suma de semanas)
            registro['inventario_asignado'] = cupo_manual_original

            registros.append(registro)

        # Devolver las listas completas (sin filtrar) para los selectores
        return jsonify({
            'success': True,
            'data': registros,
            'canales': canales_disponibles,
            'skus': skus_disponibles
        })

    except Exception as e:
        print(f"ERROR: [AJAX] Error obteniendo datos de reparto: {e}")
        import traceback
        traceback.print_exc()

        return jsonify({
            'success': False,
            'message': f'Error al obtener datos: {str(e)}'
        }), 500


@bp.route("/reparto-inventario-metricas-canal", methods=["GET"])
def reparto_inventario_metricas_canal():
    """Endpoint AJAX para obtener métricas generales de un canal"""

    try:
        mes = request.args.get('mes', '')
        canal = request.args.get('canal', '')

        if not mes or not canal:
            return jsonify({'success': False, 'message': 'Mes y canal requeridos'}), 400

        # Obtener datos (ya procesados con todas las reglas de negocio)
        df_resultado = get_distribucion_semanal_inventario(mes)

        if df_resultado is None or df_resultado.empty:
            return jsonify({
                'success': True,
                'metricas': {
                    'total_asignado': 0,
                    'total_vendido': 0,
                    'cumplimiento_general': 0,
                    'inventario_restante': 0,
                    'skus_totales': 0
                }
            })

        # Filtrar por canal
        df_canal = df_resultado[df_resultado['canal'] == canal]

        if df_canal.empty:
            return jsonify({
                'success': True,
                'metricas': {
                    'total_asignado': 0,
                    'total_vendido': 0,
                    'cumplimiento_general': 0,
                    'inventario_restante': 0,
                    'skus_totales': 0
                }
            })

        # Calcular métricas
        # Sumar asignaciones de todas las semanas por SKU-Canal
        total_asignado = df_canal.groupby(['sku', 'canal'])['asignacion_canal'].sum().sum()

        # Sumar ventas informativas de todas las semanas (incluye semana actual)
        total_vendido = df_canal['ventas_reales_informativas'].sum()

        cumplimiento_general = (total_vendido / total_asignado * 100) if total_asignado > 0 else 0

        # Inventario restante: tomar el inventario inicial menos las ventas
        inventario_inicial_total = df_canal.groupby(['sku'])['inventario_inicial'].first().sum()
        inventario_restante = inventario_inicial_total - total_vendido

        skus_totales = len(df_canal['sku'].unique())

        return jsonify({
            'success': True,
            'metricas': {
                'total_asignado': float(total_asignado),
                'total_vendido': float(total_vendido),
                'cumplimiento_general': float(cumplimiento_general),
                'inventario_restante': float(inventario_restante),
                'skus_totales': int(skus_totales)
            }
        })

    except Exception as e:
        print(f"ERROR: [AJAX] Error obteniendo métricas de canal: {e}")
        import traceback
        traceback.print_exc()

        return jsonify({
            'success': False,
            'message': f'Error al obtener métricas: {str(e)}'
        }), 500


@bp.route("/reparto-inventario-consolidado", methods=["GET"])
def reparto_inventario_consolidado():
    """Endpoint AJAX para obtener resumen consolidado (todos los canales agrupados por SKU)"""

    try:
        mes = request.args.get('mes', '')
        sku = request.args.get('sku', '')

        if not mes:
            return jsonify({'success': False, 'message': 'Mes requerido'}), 400

        # Obtener datos con todas las reglas de negocio
        df_resultado = get_distribucion_semanal_inventario(mes)

        if df_resultado is None or df_resultado.empty:
            return jsonify({
                'success': True,
                'data': []
            })

        # Aplicar filtro de SKU si existe
        if sku and sku.strip():
            df_resultado = df_resultado[
                df_resultado['sku'].str.contains(sku, case=False, na=False) |
                df_resultado['descripcion'].str.contains(sku, case=False, na=False)
            ]

        if df_resultado.empty:
            return jsonify({
                'success': True,
                'data': []
            })

        # Obtener cupos manuales originales
        cupos_manuales_dict = obtener_cupos_manuales_originales(mes)

        # Agrupar por SKU y Semana, consolidando todos los canales
        registros = []
        for sku_val, grupo_sku in df_resultado.groupby('sku'):
            # Obtener descripción (tomar la primera)
            primera_fila = grupo_sku.iloc[0]
            descripcion = primera_fila.get('descripcion', '')

            # Calcular inventario total asignado sumando cupos manuales de todos los canales
            inventario_total = 0
            for canal_val in grupo_sku['canal'].unique():
                inventario_total += cupos_manuales_dict.get((sku_val, canal_val), 0)

            registro = {
                'sku': sku_val,
                'descripcion': descripcion,
                'inventario_total': inventario_total,
                'semanas': []
            }

            # Agrupar por semana y consolidar
            for semana_nombre, grupo_semana in grupo_sku.groupby('semana'):
                # Sumar asignaciones y ventas de todos los canales para esta semana
                asignacion_total = grupo_semana['asignacion_canal'].sum()
                ventas_totales = grupo_semana['ventas_reales_informativas'].sum()

                # Calcular cumplimiento
                cumplimiento = (ventas_totales / asignacion_total * 100) if asignacion_total > 0 else 0

                # Determinar estado
                if ventas_totales > asignacion_total * 1.05:
                    estado = 'sobre-venta'
                elif cumplimiento >= 95:
                    estado = 'cumplido'
                elif cumplimiento >= 80:
                    estado = 'parcial'
                else:
                    estado = 'bajo'

                registro['semanas'].append({
                    'semana': semana_nombre,
                    'asignacion': float(asignacion_total),
                    'ventas': float(ventas_totales),
                    'cumplimiento': float(cumplimiento),
                    'estado': estado
                })

            # Ordenar semanas
            registro['semanas'] = sorted(registro['semanas'], key=lambda x: x['semana'])

            registros.append(registro)

        return jsonify({
            'success': True,
            'data': registros
        })

    except Exception as e:
        print(f"ERROR: [AJAX] Error obteniendo datos consolidados: {e}")
        import traceback
        traceback.print_exc()

        return jsonify({
            'success': False,
            'message': f'Error al obtener datos consolidados: {str(e)}'
        }), 500
