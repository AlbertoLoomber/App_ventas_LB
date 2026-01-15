/**
 * ============================================
 * CHART MODAL HELPERS - VERSIÓN SIMPLIFICADA
 * ============================================
 * Funciones helper para dibujar gráficos Canvas ampliados
 * Sin problemas de pixelación
 */

/**
 * Función para copiar y ampliar el canvas original
 * SIMPLEMENTE COPIA EL GRÁFICO QUE YA FUNCIONA BIEN
 */
function drawExpandedLineChart(canvasDestino, ctx, datos, options = {}) {
    const {
        canvasSourceId = '',
        label = ''
    } = options;

    console.log(`🎨 Copiando gráfico desde: ${canvasSourceId}`);

    // Obtener el canvas original que YA se ve bien
    const canvasOriginal = document.getElementById(canvasSourceId);

    if (!canvasOriginal) {
        console.error(`❌ No se encontró el canvas original: ${canvasSourceId}`);
        return;
    }

    // Configurar canvas destino
    const rect = canvasDestino.parentElement.getBoundingClientRect();
    const width = Math.floor(rect.width - 40);
    const height = 400;

    canvasDestino.width = width;
    canvasDestino.height = height;
    canvasDestino.style.width = width + 'px';
    canvasDestino.style.height = height + 'px';

    // Obtener contexto
    const c = canvasDestino.getContext('2d');

    // Fondo blanco
    c.fillStyle = '#ffffff';
    c.fillRect(0, 0, width, height);

    // COPIAR el canvas original al destino (escalado suave)
    c.imageSmoothingEnabled = true;
    c.imageSmoothingQuality = 'high';

    // Calcular escalado proporcional
    const originalWidth = canvasOriginal.width;
    const originalHeight = canvasOriginal.height;

    console.log(`📐 Canvas original: ${originalWidth}x${originalHeight}`);

    // Usar TODO el espacio disponible (dejar margen solo arriba para título)
    const topMargin = 50;  // Espacio para el título
    const sideMargin = 20;
    const availableWidth = width - (sideMargin * 2);
    const availableHeight = height - topMargin - 20;

    // Escalar para llenar el espacio manteniendo proporción
    const scaleX = availableWidth / originalWidth;
    const scaleY = availableHeight / originalHeight;
    const scale = Math.min(scaleX, scaleY) * 0.9; // 90% del espacio para tener margen

    const scaledWidth = originalWidth * scale;
    const scaledHeight = originalHeight * scale;

    // Centrar horizontalmente y verticalmente
    const x = (width - scaledWidth) / 2;
    const y = topMargin + (availableHeight - scaledHeight) / 2;

    // DIBUJAR el canvas original escalado
    c.drawImage(canvasOriginal, x, y, scaledWidth, scaledHeight);

    // Título
    if (label) {
        c.fillStyle = '#212529';
        c.font = 'bold 18px Arial';
        c.textAlign = 'center';
        c.fillText(label, width / 2, 28);
    }

    console.log(`✅ Gráfico copiado: ${originalWidth}x${originalHeight} → ${scaledWidth.toFixed(0)}x${scaledHeight.toFixed(0)} en posición (${x.toFixed(0)}, ${y.toFixed(0)})`);
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
                    canvasSourceId: 'costoEvolucionChart',
                    label: 'Evolución de Costo de Venta'
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
        console.log('✅ Gráfico de Costo registrado');
    }
}

/**
 * Helper para registrar gráfico de Ventas
 */
function registerVentasChart(datos) {
    if (window.chartModalInstance && datos && datos.length > 0) {
        const valorActual = datos[datos.length - 1].ventas || 0;
        const promedio = datos.reduce((sum, d) => sum + parseFloat(d.ventas || 0), 0) / datos.length;

        window.chartModalInstance.registerCanvasChart(
            'ventasEvolucionChart',
            (canvas, ctx, data) => {
                drawExpandedLineChart(canvas, ctx, data, {
                    canvasSourceId: 'ventasEvolucionChart',
                    label: 'Evolución de Ventas Totales'
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
        console.log('✅ Gráfico de Ventas registrado');
    }
}

/**
 * Helper para registrar gráfico de Ingreso Real
 */
function registerIngresoChart(datos) {
    if (window.chartModalInstance && datos && datos.length > 0) {
        const valorActual = datos[datos.length - 1].ingreso_real || 0;
        const promedio = datos.reduce((sum, d) => sum + parseFloat(d.ingreso_real || 0), 0) / datos.length;

        window.chartModalInstance.registerCanvasChart(
            'ingresoEvolucionChart',
            (canvas, ctx, data) => {
                drawExpandedLineChart(canvas, ctx, data, {
                    canvasSourceId: 'ingresoEvolucionChart',
                    label: 'Evolución de Ingreso Real (Nominal)'
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
        console.log('✅ Gráfico de Ingreso Real registrado');
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
                    canvasSourceId: 'ingresoEvolucionChartPorcentaje',
                    label: 'Evolución de % Ingreso Real'
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
        console.log('✅ Gráfico de % Ingreso Real registrado');
    }
}

/**
 * Helper para registrar gráfico de ROI
 */
function registerRoiChart(datos) {
    if (window.chartModalInstance && datos && datos.length > 0) {
        const valorActual = datos[datos.length - 1].roi || 0;
        const promedio = datos.reduce((sum, d) => sum + parseFloat(d.roi || 0), 0) / datos.length;

        window.chartModalInstance.registerCanvasChart(
            'roiEvolucionChart',
            (canvas, ctx, data) => {
                drawExpandedLineChart(canvas, ctx, data, {
                    canvasSourceId: 'roiEvolucionChart',
                    label: 'Evolución de ROI'
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
        console.log('✅ Gráfico de ROI registrado');
    }
}

console.log('✅ Chart Modal Helpers cargado (versión simplificada)');
