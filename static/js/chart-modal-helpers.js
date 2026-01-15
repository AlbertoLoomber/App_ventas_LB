/**
 * ============================================
 * CHART MODAL HELPERS
 * ============================================
 * Funciones helper para dibujar gráficos Canvas ampliados
 */

/**
 * Función genérica para dibujar un gráfico de línea con área
 * Compatible con los gráficos de evolución de rentabilidad
 */
function drawExpandedLineChart(canvas, ctx, datos, options = {}) {
    const {
        color = '#28a745',
        colorRgba = '40, 167, 69',
        label = '',
        isPercentage = true,
        isCurrency = false
    } = options;

    console.log(`🎨 Dibujando gráfico ampliado: ${label}`, datos);

    // PASO 1: Obtener tamaño del contenedor
    const container = canvas.parentElement;
    const rect = container.getBoundingClientRect();

    // PASO 2: Calcular dimensiones óptimas para el modal
    // Usar más espacio vertical y horizontal
    const width = Math.floor(rect.width - 80); // Más padding horizontal
    const height = 500; // Aumentado de 400 a 500 para mejor visualización

    console.log(`📐 Tamaño del contenedor: ${rect.width}x${rect.height}`);
    console.log(`📐 Tamaño del canvas calculado: ${width}x${height}`);

    // PASO 3: RESETEAR completamente el canvas
    // Primero resetear a 0 para limpiar cualquier estado anterior
    canvas.width = 0;
    canvas.height = 0;

    // Luego configurar las nuevas dimensiones
    canvas.width = width;
    canvas.height = height;
    canvas.style.width = width + 'px';
    canvas.style.height = height + 'px';

    // PASO 4: Obtener contexto COMPLETAMENTE NUEVO
    const context = canvas.getContext('2d', {
        alpha: false,
        desynchronized: false,
        willReadFrequently: false
    });

    // PASO 5: Configurar renderizado de alta calidad
    context.imageSmoothingEnabled = true;
    context.imageSmoothingQuality = 'high';
    context.textBaseline = 'middle';
    context.textAlign = 'left';

    // PASO 6: Fondo blanco sólido
    context.fillStyle = '#ffffff';
    context.fillRect(0, 0, width, height);

    console.log(`✅ Canvas completamente limpio y configurado: ${width}x${height}`);

    if (!datos || datos.length === 0) {
        context.fillStyle = '#6c757d';
        context.font = '18px Arial';
        context.textAlign = 'center';
        context.textBaseline = 'middle';
        context.fillText('Sin datos disponibles', width / 2, height / 2);
        return;
    }

    // PASO 7: Calcular márgenes DINÁMICAMENTE basados en el tamaño del canvas
    // Usar porcentajes en lugar de valores fijos, con mínimos más generosos
    const margin = {
        top: Math.max(70, height * 0.14),           // 14% del alto, mínimo 70px (para título)
        right: Math.max(60, width * 0.10),          // 10% del ancho, mínimo 60px
        bottom: Math.max(90, height * 0.18),        // 18% del alto para etiquetas (más espacio)
        left: isCurrency ? Math.max(140, width * 0.14) : Math.max(100, width * 0.12)  // 14% o 12% (más espacio para valores largos)
    };

    const chartWidth = width - margin.left - margin.right;
    const chartHeight = height - margin.top - margin.bottom;

    console.log(`📊 Márgenes: top=${margin.top.toFixed(0)}, right=${margin.right.toFixed(0)}, bottom=${margin.bottom.toFixed(0)}, left=${margin.left.toFixed(0)}`);
    console.log(`📊 Área de gráfico: ${chartWidth.toFixed(0)}x${chartHeight.toFixed(0)}`);

    // Obtener rango de valores - detectar automáticamente el campo correcto
    let valores;
    if (datos[0].porcentaje !== undefined) {
        valores = datos.map(d => parseFloat(d.porcentaje));
    } else if (datos[0].ventas !== undefined) {
        valores = datos.map(d => parseFloat(d.ventas));
    } else if (datos[0].ingreso_real !== undefined) {
        valores = datos.map(d => parseFloat(d.ingreso_real));
    } else if (datos[0].roi !== undefined) {
        valores = datos.map(d => parseFloat(d.roi));
    } else if (datos[0].valor !== undefined) {
        valores = datos.map(d => parseFloat(d.valor));
    } else {
        console.error('❌ Estructura de datos no reconocida:', datos[0]);
        return;
    }

    const minVal = isCurrency ? Math.min(...valores) * 0.95 : Math.min(...valores) - 2;
    const maxVal = isCurrency ? Math.max(...valores) * 1.05 : Math.max(...valores) + 2;
    const range = maxVal - minVal;

    console.log(`📊 Rango: ${minVal.toFixed(1)} - ${maxVal.toFixed(1)}`);

    // Función helper para obtener el valor correcto
    const getValor = (punto) => {
        if (punto.porcentaje !== undefined) return parseFloat(punto.porcentaje);
        if (punto.ventas !== undefined) return parseFloat(punto.ventas);
        if (punto.ingreso_real !== undefined) return parseFloat(punto.ingreso_real);
        if (punto.roi !== undefined) return parseFloat(punto.roi);
        if (punto.valor !== undefined) return parseFloat(punto.valor);
        return 0;
    };

    // PASO 8: Dibujar área degradada (fondo del gráfico)
    const gradient = context.createLinearGradient(0, margin.top, 0, height - margin.bottom);
    gradient.addColorStop(0, `rgba(${colorRgba}, 0.4)`);
    gradient.addColorStop(0.6, `rgba(${colorRgba}, 0.15)`);
    gradient.addColorStop(1, `rgba(${colorRgba}, 0.02)`);

    context.fillStyle = gradient;
    context.beginPath();
    context.moveTo(margin.left, height - margin.bottom);

    // Dibujar la línea superior del área
    datos.forEach((punto, index) => {
        const x = margin.left + (index / Math.max(datos.length - 1, 1)) * chartWidth;
        const valor = getValor(punto);
        const normalizedY = (valor - minVal) / range;
        const y = margin.top + chartHeight - (normalizedY * chartHeight);

        context.lineTo(x, y);
    });

    // Cerrar el área hacia abajo
    context.lineTo(margin.left + chartWidth, height - margin.bottom);
    context.closePath();
    context.fill();

    // PASO 9: Dibujar línea principal
    context.strokeStyle = color;
    context.lineWidth = 4; // Aumentado de 3 a 4 para mejor visibilidad
    context.lineCap = 'round';
    context.lineJoin = 'round';

    context.beginPath();
    datos.forEach((punto, index) => {
        const x = margin.left + (index / Math.max(datos.length - 1, 1)) * chartWidth;
        const valor = getValor(punto);
        const normalizedY = (valor - minVal) / range;
        const y = margin.top + chartHeight - (normalizedY * chartHeight);

        if (index === 0) {
            context.moveTo(x, y);
        } else {
            context.lineTo(x, y);
        }
    });
    context.stroke();

    // Función helper para formatear valores de forma inteligente
    const formatearValor = (valor, esPorcentaje, esMoneda) => {
        if (esPorcentaje) {
            return valor.toFixed(1) + '%';
        }

        if (esMoneda) {
            const valorAbsoluto = Math.abs(valor);

            // Si el valor es mayor a 1 millón, mostrar en formato "M"
            if (valorAbsoluto >= 1000000) {
                return '$' + (valor / 1000000).toFixed(1) + 'M';
            }
            // Si es mayor a 100K, mostrar con 1 decimal en formato "M" para mejor legibilidad
            else if (valorAbsoluto >= 100000) {
                return '$' + (valor / 1000000).toFixed(2) + 'M';
            }
            // Si es mayor a 10K, mostrar en formato "K" sin decimales
            else if (valorAbsoluto >= 10000) {
                return '$' + (valor / 1000).toFixed(0) + 'K';
            }
            // Si es mayor a 1K, mostrar con formato de miles separados por comas
            else if (valorAbsoluto >= 1000) {
                return '$' + valor.toLocaleString('es-MX', {minimumFractionDigits: 0, maximumFractionDigits: 0});
            }
            // Si es menor, mostrar el valor completo
            else {
                return '$' + valor.toLocaleString('es-MX', {minimumFractionDigits: 0, maximumFractionDigits: 0});
            }
        }

        return valor.toFixed(1);
    };

    // PASO 10: Dibujar puntos y etiquetas
    const fontSize = Math.max(13, Math.min(16, width / 70)); // Tamaño de fuente dinámico
    const pointRadius = Math.max(6, Math.min(9, width / 150)); // Radio de punto dinámico

    datos.forEach((punto, index) => {
        const x = margin.left + (index / Math.max(datos.length - 1, 1)) * chartWidth;
        const valor = getValor(punto);
        const normalizedY = (valor - minVal) / range;
        const y = margin.top + chartHeight - (normalizedY * chartHeight);

        // Punto principal
        context.fillStyle = color;
        context.beginPath();
        context.arc(x, y, pointRadius, 0, 2 * Math.PI);
        context.fill();

        // Punto interior
        context.fillStyle = '#ffffff';
        context.beginPath();
        context.arc(x, y, pointRadius / 2, 0, 2 * Math.PI);
        context.fill();

        // Etiqueta del mes (abajo)
        context.fillStyle = '#495057';
        context.font = `bold ${fontSize}px Arial`;
        context.textAlign = 'center';
        context.textBaseline = 'top';
        context.fillText(punto.fecha || punto.label || '', x, height - margin.bottom + 20);

        // Valor (arriba del punto) - usar función de formateo mejorada
        context.fillStyle = color;
        context.font = `bold ${fontSize}px Arial`;
        context.textBaseline = 'bottom';
        const valorTexto = formatearValor(valor, isPercentage, isCurrency);
        context.fillText(valorTexto, x, y - 18);
    });

    // PASO 11: Dibujar ejes y líneas de cuadrícula
    // Eje Y
    context.strokeStyle = '#dee2e6';
    context.lineWidth = 2;
    context.beginPath();
    context.moveTo(margin.left, margin.top);
    context.lineTo(margin.left, height - margin.bottom);
    context.stroke();

    // Eje X
    context.beginPath();
    context.moveTo(margin.left, height - margin.bottom);
    context.lineTo(width - margin.right, height - margin.bottom);
    context.stroke();

    // PASO 12: Etiquetas del eje Y (5 valores)
    const labelFontSize = Math.max(12, Math.min(14, width / 80));
    context.fillStyle = '#6c757d';
    context.font = `${labelFontSize}px Arial`;
    context.textAlign = 'right';
    context.textBaseline = 'middle';

    for (let i = 0; i <= 4; i++) {
        const valor = minVal + (range * (i / 4));
        const y = height - margin.bottom - (chartHeight * (i / 4));

        // Usar la misma función de formateo para consistencia
        const texto = formatearValor(valor, isPercentage, isCurrency);
        context.fillText(texto, margin.left - 20, y);

        // Líneas de cuadrícula
        context.strokeStyle = '#f8f9fa';
        context.lineWidth = 1;
        context.beginPath();
        context.moveTo(margin.left, y);
        context.lineTo(width - margin.right, y);
        context.stroke();
    }

    // PASO 13: Título del gráfico
    if (label) {
        const titleFontSize = Math.max(16, Math.min(20, width / 50));
        context.fillStyle = '#212529';
        context.font = `bold ${titleFontSize}px Arial`;
        context.textAlign = 'center';
        context.textBaseline = 'top';
        context.fillText(label, width / 2, 20);
    }

    console.log('✅ Gráfico ampliado dibujado exitosamente');
}

/**
 * Helper para registrar gráfico de Costo
 */
function registerCostoChart(datos) {
    if (window.chartModalInstance && datos && datos.length > 0) {
        window.chartModalInstance.registerCanvasChart(
            'costoEvolucionChart',
            (canvas, ctx, data) => {
                drawExpandedLineChart(canvas, ctx, data, {
                    color: '#dc3545',
                    colorRgba: '220, 53, 69',
                    label: 'Evolución de Costo de Venta',
                    isPercentage: true
                });
            },
            datos,
            'Evolución de Costo de Venta',
            {
                'Total Puntos': datos.length,
                'Valor Actual': datos[datos.length - 1].porcentaje.toFixed(1) + '%',
                'Promedio': (datos.reduce((sum, d) => sum + parseFloat(d.porcentaje), 0) / datos.length).toFixed(1) + '%'
            }
        );
        console.log('✅ Gráfico de Costo registrado en modal');
    }
}

/**
 * Helper para registrar gráfico de Ventas
 */
function registerVentasChart(datos) {
    if (window.chartModalInstance && datos && datos.length > 0) {
        const valorActual = datos[datos.length - 1].ventas || datos[datos.length - 1].valor || 0;
        const promedio = datos.reduce((sum, d) => sum + parseFloat(d.ventas || d.valor || 0), 0) / datos.length;

        window.chartModalInstance.registerCanvasChart(
            'ventasEvolucionChart',
            (canvas, ctx, data) => {
                drawExpandedLineChart(canvas, ctx, data, {
                    color: '#28a745',
                    colorRgba: '40, 167, 69',
                    label: 'Evolución de Ventas Totales',
                    isPercentage: false,
                    isCurrency: true
                });
            },
            datos,
            'Evolución de Ventas Totales',
            {
                'Total Puntos': datos.length,
                'Ventas Actuales': '$' + valorActual.toLocaleString('es-MX', {maximumFractionDigits: 0}),
                'Promedio': '$' + promedio.toLocaleString('es-MX', {maximumFractionDigits: 0})
            }
        );
        console.log('✅ Gráfico de Ventas registrado en modal');
    }
}

/**
 * Helper para registrar gráfico de Ingreso Real
 */
function registerIngresoChart(datos) {
    if (window.chartModalInstance && datos && datos.length > 0) {
        const valorActual = datos[datos.length - 1].ingreso_real || datos[datos.length - 1].valor || 0;
        const promedio = datos.reduce((sum, d) => sum + parseFloat(d.ingreso_real || d.valor || 0), 0) / datos.length;

        window.chartModalInstance.registerCanvasChart(
            'ingresoEvolucionChart',
            (canvas, ctx, data) => {
                drawExpandedLineChart(canvas, ctx, data, {
                    color: '#17a2b8',
                    colorRgba: '23, 162, 184',
                    label: 'Evolución de Ingreso Real (Nominal)',
                    isPercentage: false,
                    isCurrency: true
                });
            },
            datos,
            'Evolución de Ingreso Real (Nominal)',
            {
                'Total Puntos': datos.length,
                'Ingreso Actual': '$' + valorActual.toLocaleString('es-MX', {maximumFractionDigits: 0}),
                'Promedio': '$' + promedio.toLocaleString('es-MX', {maximumFractionDigits: 0})
            }
        );
        console.log('✅ Gráfico de Ingreso Real registrado en modal');
    }
}

/**
 * Helper para registrar gráfico de Ingreso Real (Porcentaje)
 */
function registerIngresoPorcentajeChart(datos) {
    if (window.chartModalInstance && datos && datos.length > 0) {
        window.chartModalInstance.registerCanvasChart(
            'ingresoEvolucionChartPorcentaje',
            (canvas, ctx, data) => {
                drawExpandedLineChart(canvas, ctx, data, {
                    color: '#28a745',
                    colorRgba: '40, 167, 69',
                    label: 'Evolución de % Ingreso Real',
                    isPercentage: true
                });
            },
            datos,
            'Evolución de % Ingreso Real',
            {
                'Total Puntos': datos.length,
                'Porcentaje Actual': datos[datos.length - 1].porcentaje.toFixed(1) + '%',
                'Promedio': (datos.reduce((sum, d) => sum + parseFloat(d.porcentaje), 0) / datos.length).toFixed(1) + '%'
            }
        );
        console.log('✅ Gráfico de % Ingreso Real registrado en modal');
    }
}

/**
 * Helper para registrar gráfico de ROI
 */
function registerRoiChart(datos) {
    if (window.chartModalInstance && datos && datos.length > 0) {
        const valorActual = datos[datos.length - 1].roi || datos[datos.length - 1].porcentaje || 0;
        const promedio = datos.reduce((sum, d) => sum + parseFloat(d.roi || d.porcentaje || 0), 0) / datos.length;

        window.chartModalInstance.registerCanvasChart(
            'roiEvolucionChart',
            (canvas, ctx, data) => {
                drawExpandedLineChart(canvas, ctx, data, {
                    color: '#20c997',
                    colorRgba: '32, 201, 151',
                    label: 'Evolución de ROI',
                    isPercentage: true
                });
            },
            datos,
            'Evolución de ROI (Retorno de Inversión)',
            {
                'Total Puntos': datos.length,
                'ROI Actual': valorActual.toFixed(1) + '%',
                'Promedio': promedio.toFixed(1) + '%'
            }
        );
        console.log('✅ Gráfico de ROI registrado en modal');
    }
}

console.log('✅ Chart Modal Helpers cargado');
