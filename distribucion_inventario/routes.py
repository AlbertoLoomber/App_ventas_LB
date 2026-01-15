# -*- coding: utf-8 -*-
"""
Rutas del módulo de Distribución de Inventario
Maneja las rutas y endpoints relacionados con la distribución de inventario entre canales
"""

from flask import request, render_template, jsonify
from distribucion_inventario.blueprint import bp
from distribucion_inventario.services import (
    obtener_meses_disponibles,
    procesar_distribucion_inventario,
    obtener_resumen_por_canal,
    obtener_distribucion_por_sku,
    procesar_distribucion_semanal
)


@bp.route("/distribucion-inventario", methods=["GET"])
def distribucion_inventario():
    """Página principal de distribución de inventario"""

    # Obtener mes del parámetro GET, default a Diciembre 2025
    mes_seleccionado = request.args.get('mes', 'Diciembre 2025')

    # Obtener lista de meses disponibles
    meses_disponibles = obtener_meses_disponibles()

    # Procesar datos de distribución
    try:
        resultado = procesar_distribucion_inventario(mes_seleccionado)

        datos_tabla = resultado['datos_tabla']
        resumen_general = resultado['resumen_general']
        canales = resultado['canales']

        # Obtener resumen por canal
        resumen_canales = obtener_resumen_por_canal(mes_seleccionado)

        error = None

    except Exception as e:
        print(f"ERROR: Error procesando distribución de inventario: {e}")
        import traceback
        traceback.print_exc()

        error = f"Error al cargar datos: {str(e)}"
        datos_tabla = []
        resumen_general = {
            'total_skus': 0,
            'total_disponible': 0,
            'total_asignado': 0,
            'total_canales': 0
        }
        canales = []
        resumen_canales = []

    return render_template("distribucion_inventario.html",
                         error=error,
                         datos_tabla=datos_tabla,
                         resumen_general=resumen_general,
                         canales=canales,
                         resumen_canales=resumen_canales,
                         meses_disponibles=meses_disponibles,
                         mes_seleccionado=mes_seleccionado)


@bp.route("/distribucion-inventario-datos", methods=["POST"])
def distribucion_inventario_datos():
    """Endpoint AJAX para obtener datos de distribución filtrados por mes"""

    try:
        # Obtener parámetros
        mes_seleccionado = request.form.get('mes', 'Diciembre 2025')

        print(f"INFO: [AJAX] Obteniendo distribución para mes: {mes_seleccionado}")

        # Procesar datos
        resultado = procesar_distribucion_inventario(mes_seleccionado)

        # Obtener resumen por canal
        resumen_canales = obtener_resumen_por_canal(mes_seleccionado)

        return jsonify({
            'success': True,
            'datos_tabla': resultado['datos_tabla'],
            'resumen_general': resultado['resumen_general'],
            'canales': resultado['canales'],
            'resumen_canales': resumen_canales
        })

    except Exception as e:
        print(f"ERROR: [AJAX] Error en distribución de inventario: {e}")
        import traceback
        traceback.print_exc()

        return jsonify({
            'success': False,
            'error': f'Error al obtener datos: {str(e)}'
        })


@bp.route("/distribucion-inventario-sku/<sku>", methods=["GET"])
def distribucion_inventario_sku(sku):
    """Endpoint para obtener distribución de un SKU específico"""

    try:
        mes_seleccionado = request.args.get('mes', 'Diciembre 2025')

        print(f"INFO: Obteniendo distribución para SKU: {sku}, Mes: {mes_seleccionado}")

        # Obtener distribución del SKU
        info_sku = obtener_distribucion_por_sku(sku, mes_seleccionado)

        if info_sku is None:
            return jsonify({
                'success': False,
                'error': f'No se encontraron datos para el SKU {sku}'
            })

        return jsonify({
            'success': True,
            'data': info_sku
        })

    except Exception as e:
        print(f"ERROR: Error obteniendo distribución del SKU {sku}: {e}")
        import traceback
        traceback.print_exc()

        return jsonify({
            'success': False,
            'error': f'Error al obtener datos: {str(e)}'
        })


@bp.route("/distribucion-inventario-semanal", methods=["GET"])
def distribucion_inventario_semanal():
    """Página de distribución semanal de inventario"""

    # Obtener mes del parámetro GET, default a Diciembre 2025
    mes_seleccionado = request.args.get('mes', 'Diciembre 2025')

    # Obtener lista de meses disponibles
    meses_disponibles = obtener_meses_disponibles()

    # Procesar datos de distribución semanal
    try:
        resultado = procesar_distribucion_semanal(mes_seleccionado)

        datos_tabla = resultado['datos_tabla']
        resumen_semanal = resultado['resumen_semanal']
        semanas = resultado['semanas']
        canales = resultado['canales']

        error = None

    except Exception as e:
        print(f"ERROR: Error procesando distribución semanal: {e}")
        import traceback
        traceback.print_exc()

        error = f"Error al cargar datos: {str(e)}"
        datos_tabla = []
        resumen_semanal = []
        semanas = []
        canales = []

    return render_template("distribucion_inventario_semanal.html",
                         error=error,
                         datos_tabla=datos_tabla,
                         resumen_semanal=resumen_semanal,
                         semanas=semanas,
                         canales=canales,
                         meses_disponibles=meses_disponibles,
                         mes_seleccionado=mes_seleccionado)


@bp.route("/distribucion-inventario-semanal-datos", methods=["POST"])
def distribucion_inventario_semanal_datos():
    """Endpoint AJAX para obtener datos de distribución semanal filtrados por mes"""

    try:
        # Obtener parámetros
        mes_seleccionado = request.form.get('mes', 'Diciembre 2025')

        print(f"INFO: [AJAX] Obteniendo distribución semanal para mes: {mes_seleccionado}")

        # Procesar datos
        resultado = procesar_distribucion_semanal(mes_seleccionado)

        return jsonify({
            'success': True,
            'datos_tabla': resultado['datos_tabla'],
            'resumen_semanal': resultado['resumen_semanal'],
            'semanas': resultado['semanas'],
            'canales': resultado['canales']
        })

    except Exception as e:
        print(f"ERROR: [AJAX] Error en distribución semanal: {e}")
        import traceback
        traceback.print_exc()

        return jsonify({
            'success': False,
            'error': f'Error al obtener datos: {str(e)}'
        })


@bp.route("/distribucion-inventario-editar-sku", methods=["POST"])
def distribucion_inventario_editar_sku():
    """Endpoint AJAX para obtener datos de un SKU para edici贸n"""

    try:
        sku = request.form.get('sku')
        mes = request.form.get('mes', 'Diciembre 2025')

        print(f"INFO: [AJAX] Obteniendo distribuci贸n para editar SKU: {sku}, mes: {mes}")

        from database import obtener_distribucion_por_sku_para_edicion
        resultado = obtener_distribucion_por_sku_para_edicion(sku, mes)

        if resultado['success']:
            return jsonify(resultado)
        else:
            return jsonify(resultado), 404

    except Exception as e:
        print(f"ERROR: [AJAX] Error obteniendo SKU para edici贸n: {e}")
        import traceback
        traceback.print_exc()

        return jsonify({
            'success': False,
            'message': f'Error al obtener datos: {str(e)}'
        }), 500


@bp.route("/distribucion-inventario-guardar-manual", methods=["POST"])
def distribucion_inventario_guardar_manual():
    """Endpoint AJAX para guardar distribuci贸n manual"""

    try:
        import json

        sku = request.form.get('sku')
        mes = request.form.get('mes', 'Diciembre 2025')
        distribuciones_json = request.form.get('distribuciones')
        usuario = request.form.get('usuario', 'sistema')
        comentario = request.form.get('comentario', '')
        disponible_total_manual = float(request.form.get('disponible_total_manual', 0))
        disponible_total_automatico = float(request.form.get('disponible_total_automatico', 0))

        print(f"INFO: [AJAX] Guardando distribuci贸n manual para SKU: {sku}, mes: {mes}, total manual: {disponible_total_manual}, total auto: {disponible_total_automatico}")

        # Parsear distribuciones
        distribuciones_canales = json.loads(distribuciones_json)

        from database import guardar_distribucion_manual
        resultado = guardar_distribucion_manual(sku, mes, distribuciones_canales, disponible_total_manual, disponible_total_automatico, usuario, comentario)

        return jsonify(resultado)

    except Exception as e:
        print(f"ERROR: [AJAX] Error guardando distribuci贸n manual: {e}")
        import traceback
        traceback.print_exc()

        return jsonify({
            'success': False,
            'message': f'Error al guardar: {str(e)}'
        }), 500


@bp.route("/distribucion-inventario-revertir-automatica", methods=["POST"])
def distribucion_inventario_revertir_automatica():
    """Endpoint AJAX para revertir distribuci贸n a autom谩tica"""

    try:
        sku = request.form.get('sku')
        mes = request.form.get('mes', 'Diciembre 2025')

        print(f"INFO: [AJAX] Revirtiendo a autom谩tica SKU: {sku}, mes: {mes}")

        from database import revertir_a_distribucion_automatica
        resultado = revertir_a_distribucion_automatica(sku, mes)

        return jsonify(resultado)

    except Exception as e:
        print(f"ERROR: [AJAX] Error revirtiendo a autom谩tica: {e}")
        import traceback
        traceback.print_exc()

        return jsonify({
            'success': False,
            'message': f'Error al revertir: {str(e)}'
        }), 500


@bp.route("/distribucion-inventario-info-snapshot", methods=["POST"])
def distribucion_inventario_info_snapshot():
    """Endpoint AJAX para obtener información del estado actual del snapshot"""

    try:
        mes = request.form.get('mes', 'Diciembre 2025')

        print(f"INFO: [AJAX] Obteniendo info de snapshot para: {mes}")

        from database import obtener_info_snapshot
        resultado = obtener_info_snapshot(mes)

        return jsonify(resultado)

    except Exception as e:
        print(f"ERROR: [AJAX] Error obteniendo info de snapshot: {e}")
        import traceback
        traceback.print_exc()

        return jsonify({
            'success': False,
            'message': f'Error al obtener info: {str(e)}'
        }), 500


@bp.route("/distribucion-inventario-canales-disponibles", methods=["GET"])
def distribucion_inventario_canales_disponibles():
    """Endpoint AJAX para obtener lista de canales disponibles"""

    try:
        from database import get_db_connection

        client = get_db_connection()
        if not client:
            return jsonify({'success': False, 'message': 'Error de conexión'}), 500

        # Obtener todos los canales únicos de la tabla materializada
        query = """
        SELECT DISTINCT Channel
        FROM Silver.Distribucion_Mensual_Canal_Materializada
        WHERE Channel != ''
        ORDER BY Channel
        """

        result = client.query(query)
        canales = [row[0] for row in result.result_rows]

        return jsonify({
            'success': True,
            'canales': canales
        })

    except Exception as e:
        print(f"ERROR: [AJAX] Error obteniendo canales: {e}")
        import traceback
        traceback.print_exc()

        return jsonify({
            'success': False,
            'message': f'Error al obtener canales: {str(e)}'
        }), 500


@bp.route("/distribucion-inventario-crear-snapshot", methods=["POST"])
def distribucion_inventario_crear_snapshot():
    """Endpoint AJAX para crear snapshot mensual completo"""

    try:
        mes = request.form.get('mes', 'Diciembre 2025')

        print(f"INFO: [AJAX] Creando snapshot mensual para: {mes}")

        from database import crear_snapshot_mensual
        resultado = crear_snapshot_mensual(mes)

        return jsonify(resultado)

    except Exception as e:
        print(f"ERROR: [AJAX] Error creando snapshot mensual: {e}")
        import traceback
        traceback.print_exc()

        return jsonify({
            'success': False,
            'message': f'Error al crear snapshot: {str(e)}'
        }), 500


@bp.route("/distribucion-inventario-skus-disponibles", methods=["GET"])
def distribucion_inventario_skus_disponibles():
    """Endpoint AJAX para obtener lista de SKUs disponibles desde tabla de ventas"""

    try:
        from database import get_db_connection

        client = get_db_connection()
        if not client:
            return jsonify({'success': False, 'message': 'Error de conexión'}), 500

        query = """
        SELECT DISTINCT
            Producto.1 AS sku,
            any(descripcion) AS descripcion
        FROM Silver.RPT_Ventas_Con_Costo_Prueba
        ARRAY JOIN producto_comercial AS Producto
        WHERE Producto.1 != ''
          AND Producto.1 IS NOT NULL
        GROUP BY Producto.1
        ORDER BY Producto.1
        """

        result = client.query(query)

        skus = []
        for row in result.result_rows:
            skus.append({
                'sku': row[0],
                'descripcion': row[1] if row[1] else 'Sin descripción'
            })

        return jsonify({
            'success': True,
            'skus': skus
        })

    except Exception as e:
        print(f"ERROR: [AJAX] Error obteniendo SKUs disponibles: {e}")
        import traceback
        traceback.print_exc()

        return jsonify({
            'success': False,
            'message': f'Error al obtener SKUs: {str(e)}'
        }), 500


@bp.route("/distribucion-inventario-agregar-sku", methods=["POST"])
def distribucion_inventario_agregar_sku():
    """Endpoint AJAX para agregar un SKU nuevo que no existe en la distribución automática"""

    try:
        sku = request.form.get('sku')
        mes = request.form.get('mes')
        descripcion = request.form.get('descripcion', '')
        disponible_total = float(request.form.get('disponible_total', 0))
        distribuciones_json = request.form.get('distribuciones')

        print(f"INFO: [AJAX] Agregando nuevo SKU: {sku}, mes: {mes}, disponible total: {disponible_total}")

        if not sku or not mes:
            return jsonify({
                'success': False,
                'message': 'SKU y mes son requeridos'
            }), 400

        # Parsear distribuciones de canales
        distribuciones_canales = json.loads(distribuciones_json)

        if not distribuciones_canales:
            return jsonify({
                'success': False,
                'message': 'Debes agregar al menos un canal'
            }), 400

        # Guardar el SKU nuevo en la tabla manual
        from database import guardar_distribucion_manual

        # Preparar distribuciones con cupo_automatico = 0 (no existe en automática)
        distribuciones_preparadas = []
        for dist in distribuciones_canales:
            distribuciones_preparadas.append({
                'canal': dist['canal'],
                'cupo_manual': dist['cupo'],
                'cupo_automatico': 0  # No existe en automática
            })

        resultado = guardar_distribucion_manual(
            sku=sku,
            mes=mes,
            distribuciones_canales=distribuciones_preparadas,
            disponible_total_manual=disponible_total,
            disponible_total_automatico=0,  # No existe en automática
            usuario='sistema',
            comentario=f'SKU nuevo agregado manualmente - {descripcion}'
        )

        return jsonify(resultado)

    except Exception as e:
        print(f"ERROR: [AJAX] Error agregando nuevo SKU: {e}")
        import traceback
        traceback.print_exc()

        return jsonify({
            'success': False,
            'message': f'Error al agregar SKU: {str(e)}'
        }), 500
