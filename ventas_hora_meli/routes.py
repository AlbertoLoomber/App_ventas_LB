# -*- coding: utf-8 -*-
"""
Rutas del módulo de Ventas por Hora Mercado Libre
Maneja las rutas y endpoints relacionados con el análisis de ventas por hora
"""

from flask import request, render_template, jsonify
from datetime import datetime

from ventas_hora_meli.blueprint import bp
from ventas_hora_meli.services import (
    obtener_skus_disponibles,
    obtener_datos_completos_sku
)


@bp.route("/ventas-hora-meli", methods=["GET"])
def ventas_hora_meli():
    """Página principal de análisis de impacto de precios en ventas por hora"""

    # Inicializar variables
    skus_disponibles = []
    sku_seleccionado = None
    error = None

    try:
        # Obtener SKUs disponibles para el filtro (ahora incluye descripción)
        skus_disponibles = obtener_skus_disponibles()

        # Si hay SKUs, seleccionar el primero por defecto
        if skus_disponibles:
            # skus_disponibles ahora es una lista de diccionarios: [{'sku': 'ABC', 'descripcion': 'Desc'}, ...]
            sku_seleccionado = skus_disponibles[0]['sku']  # Solo pasar el SKU
        else:
            error = "No hay SKUs disponibles en la base de datos"
            print(f"WARNING: {error}")

    except Exception as e:
        error = f"Error cargando datos: {str(e)}"
        print(f"ERROR en ventas_hora_meli: {e}")
        import traceback
        traceback.print_exc()

    # Renderizar template
    return render_template("ventas_hora_meli.html",
                         error=error,
                         skus_disponibles=skus_disponibles,
                         sku_seleccionado=sku_seleccionado)


@bp.route("/ventas-hora-meli-datos", methods=["POST"])
def ventas_hora_meli_datos():
    """Endpoint para obtener datos del gráfico por SKU"""
    try:
        sku = request.form.get("sku")

        if not sku:
            return jsonify({
                'success': False,
                'error': 'Por favor selecciona un SKU'
            })

        # Obtener todos los datos del SKU
        df = obtener_datos_completos_sku(sku)

        if df.empty:
            return jsonify({
                'success': False,
                'error': f'No se encontraron datos para el SKU {sku}'
            })

        # Preparar datos para Chart.js
        # Convertir timestamp a string para JSON
        labels = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M').tolist()

        datos_grafico = {
            'labels': labels,  # Eje X: timestamps
            'cantidades': df['Cantidad_Total'].tolist(),  # Barras: cantidad vendida
            'precios': df['Precio_cliente'].tolist(),  # Línea: precio
            'ventas_netas': df['Venta_Neta_Total'].tolist(),  # Para tooltip
            'variaciones': df['Var_vs_Dia_Anterior_Porc'].tolist(),  # Para tooltip
            'killers': df['Killer'].tolist(),  # Para tooltip
            'dias': df['dia'].astype(str).tolist(),  # Para tooltip
            'horas': df['Hora'].tolist()  # Para tooltip
        }

        return jsonify({
            'success': True,
            'sku': sku,
            'datos': datos_grafico,
            'total_registros': len(df)
        })

    except Exception as e:
        print(f"Error en ventas_hora_meli_datos: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': f'Error procesando datos: {str(e)}'
        })
