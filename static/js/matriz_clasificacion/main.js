/**
 * Matriz de Clasificación - Main Script
 * Maneja la actualización AJAX del gráfico de clasificación por SKU-Canal
 */

console.log('📦 [CLASIFICACION] Script cargado');

/**
 * Función para actualizar la matriz de clasificación con los filtros seleccionados
 */
async function actualizarMatrizClasificacion() {
    const mesSeleccionado = document.getElementById('filtroMes').value;
    const canalesSeleccionados = getSelectedCanales();
    const skusSeleccionados = getSelectedSKUs();

    console.log(`📊 [CLASIFICACION] Filtros - Mes: ${mesSeleccionado}, Canales: ${canalesSeleccionados.join(', ')}, SKUs: ${skusSeleccionados.length}`);

    // Si no hay SKUs seleccionados, mostrar mensaje
    if (skusSeleccionados.length === 0) {
        console.log('⚠️ [CLASIFICACION] No hay SKUs seleccionados');
        actualizarUIVacia();
        return;
    }

    // Mostrar indicador de carga
    const loadingIndicator = document.getElementById('loadingIndicatorClasif');
    const btnActualizar = document.getElementById('btnActualizarClasif');

    if (loadingIndicator) loadingIndicator.style.display = 'block';
    if (btnActualizar) btnActualizar.disabled = true;

    try {
        const params = {
            mes: parseInt(mesSeleccionado),
            canales: canalesSeleccionados,
            skus: skusSeleccionados
        };

        console.log('📤 [CLASIFICACION] Enviando peticiones AJAX en paralelo...');

        // Cargar AMBAS vistas en paralelo: Vista Actual + Comparación 3 Meses
        const [resultClasif, resultComparar] = await Promise.all([
            // Petición 1: Vista Actual
            fetch('/matriz-posicionamiento/actualizar-clasificacion', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(params)
            }).then(res => res.json()),

            // Petición 2: Comparación 3 Meses
            fetch('/matriz-posicionamiento/comparar-3-meses', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ mes: mesSeleccionado, canales: canalesSeleccionados, skus: skusSeleccionados })
            }).then(res => res.json())
        ]);

        // Actualizar Vista Actual
        if (resultClasif.success) {
            console.log('✅ [CLASIFICACION] Datos de Vista Actual recibidos:', resultClasif.data);
            // Guardar datos para restaurar tabla al cambiar de tab
            window.datosClasificacionActual = resultClasif.data;
            actualizarGraficoClasificacion(resultClasif.data);
            actualizarEstadisticasClasificacion(resultClasif.data.estadisticas);
            actualizarTablaSkus(resultClasif.data.skus);
        } else {
            console.error('❌ [CLASIFICACION] Error en Vista Actual:', resultClasif.error);
            alert('Error al actualizar la matriz de clasificación: ' + resultClasif.error);
        }

        // Actualizar Comparación 3 Meses (en background)
        if (resultComparar.success) {
            console.log('✅ [COMPARAR] Datos de 3 Meses precargados:', resultComparar.data);
            // Guardar datos en variable global para uso posterior
            window.datosComparativo3Meses = resultComparar.data;
            // Actualizar cards, gráfico y tabla comparativa
            actualizarCardsComparativos(resultComparar.data.resumen);
            actualizarGraficoComparativo(resultComparar.data);
            // NO actualizar la tabla aquí, solo cuando el tab esté activo
        } else {
            console.warn('⚠️ [COMPARAR] Error en Comparación 3 Meses:', resultComparar.error);
            // No mostrar error al usuario, es una carga en background
        }

        console.log('✅ [CLASIFICACION] Ambas vistas cargadas exitosamente');

    } catch (error) {
        console.error('❌ [CLASIFICACION] Error en la petición AJAX:', error);
        alert('Error de conexión al actualizar la matriz de clasificación');
    } finally {
        // Ocultar indicador de carga
        if (loadingIndicator) loadingIndicator.style.display = 'none';
        if (btnActualizar) btnActualizar.disabled = false;
    }
}

/**
 * Actualizar el gráfico con los nuevos datos
 */
function actualizarGraficoClasificacion(data) {
    if (!window.matrizClasificacionChart) {
        console.error('❌ [CLASIFICACION] Gráfico no encontrado');
        return;
    }

    // Actualizar datasets
    window.matrizClasificacionChart.data.datasets = data.datasets;

    // Actualizar el eje Y dinámicamente
    if (data.estadisticas && data.estadisticas.eje_y_max) {
        window.matrizClasificacionChart.options.scales.y.max = data.estadisticas.eje_y_max;
        console.log(`📊 Eje Y de clasificación ajustado a: 0% - ${data.estadisticas.eje_y_max}%`);
    }

    // Re-renderizar
    window.matrizClasificacionChart.update();

    console.log(`✅ [CLASIFICACION] Gráfico actualizado con ${data.datasets.length} datasets`);
}

/**
 * Actualizar las tarjetas de estadísticas
 */
function actualizarEstadisticasClasificacion(stats) {
    // Ingreso Real Total (Primera tarjeta)
    const statIngresoReal = document.getElementById('statIngresoRealClasif');
    if (statIngresoReal) {
        const ingresoReal = stats.ingreso_real_total || 0;
        statIngresoReal.textContent = `$${ingresoReal.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ",")}`;
    }

    // Ventas Totales (Segunda tarjeta)
    const statVentasTotales = document.getElementById('statVentasTotalesClasif');
    if (statVentasTotales) {
        const ventasTotales = stats.ventas_totales || 0;
        statVentasTotales.textContent = `$${ventasTotales.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ",")}`;
    }

    // Ingreso Real Promedio (Tercera tarjeta)
    const statIngresoProm = document.getElementById('statIngresoPromClasif');
    if (statIngresoProm) {
        statIngresoProm.textContent = `${(stats.ingreso_promedio || 0).toFixed(1)}%`;
    }

    // ROI Promedio (Cuarta tarjeta)
    const statRoiProm = document.getElementById('statRoiPromClasif');
    if (statRoiProm) {
        statRoiProm.textContent = `${(stats.roi_promedio || 0).toFixed(1)}%`;
    }

    console.log('✅ [CLASIFICACION] Estadísticas actualizadas');
}

/**
 * Actualizar la tabla de SKUs
 */
function actualizarTablaSkus(skus) {
    const tbody = document.getElementById('tablaSkusClasif');

    if (!tbody) {
        console.error('❌ [CLASIFICACION] Tabla de SKUs no encontrada');
        return;
    }

    if (!skus || skus.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="6" class="text-center text-muted py-4">
                    <i class="bi bi-inbox" style="font-size: 2rem; display: block; margin-bottom: 10px;"></i>
                    No hay datos para mostrar
                </td>
            </tr>
        `;
        return;
    }

    let html = '';

    skus.forEach(sku => {
        // Color de clasificación
        const clasificacionClass = obtenerClaseClasificacionBadge(sku.clasificacion);

        html += `
            <tr>
                <td>
                    <span style="display: inline-block; width: 12px; height: 12px; border-radius: 50%; background: ${sku.color_canal}; margin-right: 8px;"></span>
                    <strong>${sku.sku} - ${sku.canal}</strong>
                    <div style="font-size: 0.8rem; color: #6c757d;">${sku.descripcion}</div>
                </td>
                <td class="text-center">
                    <span class="badge ${clasificacionClass}" style="font-size: 0.8rem;">
                        ${sku.clasificacion}
                    </span>
                </td>
                <td class="text-center">
                    <span class="badge" style="background: rgba(111, 66, 193, 0.1); color: #6f42c1; font-size: 0.9rem;">
                        ${sku.ingreso_real_pct.toFixed(1)}%
                    </span>
                </td>
                <td class="text-end">
                    <strong style="color: #17a2b8;">$${sku.ingreso_real.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ",")}</strong>
                </td>
                <td class="text-center">
                    <span class="badge" style="background: rgba(255, 193, 7, 0.1); color: #ffc107; font-size: 0.9rem;">
                        ${sku.roi_pct.toFixed(1)}%
                    </span>
                </td>
                <td class="text-center">
                    <span class="badge" style="background: ${sku.color_zona}; color: ${sku.color_texto}; font-size: 0.85rem;">
                        ${sku.icono} ${sku.zona}
                    </span>
                </td>
            </tr>
        `;
    });

    tbody.innerHTML = html;

    console.log(`✅ [CLASIFICACION] Tabla actualizada con ${skus.length} SKUs`);
}

/**
 * Actualizar la tabla con datos comparativos de 3 meses
 * Muestra el mes actual + 2 sub-filas con meses anteriores
 */
function actualizarTablaComparativa3Meses(data) {
    const tbody = document.getElementById('tablaSkusClasif');

    if (!tbody) {
        console.error('❌ [COMPARAR] Tabla de SKUs no encontrada');
        return;
    }

    if (!data || !data.datasets || data.datasets.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="6" class="text-center text-muted py-4">
                    <i class="bi bi-inbox" style="font-size: 2rem; display: block; margin-bottom: 10px;"></i>
                    No hay datos comparativos para mostrar
                </td>
            </tr>
        `;
        return;
    }

    const mesesLabels = data.meses_labels || ['Mes -2', 'Mes -1', 'Mes Actual'];
    let html = '';

    // Iterar sobre cada dataset (cada uno representa un SKU-Canal)
    data.datasets.forEach(dataset => {
        const label = dataset.label || 'SKU Desconocido';
        const puntos = dataset.data || []; // Array de 3 puntos: [mes-2, mes-1, mes-actual]

        // Validar que tenga 3 puntos
        if (puntos.length !== 3) {
            console.warn(`⚠️ [COMPARAR] Dataset ${label} no tiene 3 puntos:`, puntos);
            return;
        }

        // Iterar en orden inverso: [2, 1, 0] = [Mes Actual, Mes -1, Mes -2]
        for (let i = 2; i >= 0; i--) {
            const punto = puntos[i];
            const mesLabel = mesesLabels[i] || `Mes ${i}`;
            const esPrincipal = (i === 2); // El mes actual es la fila principal

            // Calcular clasificación basada en zona
            const clasificacion = punto.clasificacion || 'Sin Clasificar';
            const clasificacionClass = obtenerClaseClasificacionBadge(clasificacion);

            // Zona y colores
            const zona = punto.zona || 'Sin Clasificar';
            const colorZona = punto.color_zona || '#e9ecef';
            const colorTexto = punto.color_texto || '#000';
            const icono = punto.icono || '⚪';

            // Estilos para sub-filas
            const estiloFila = esPrincipal ? '' : 'style="background: #f8f9fa;"';
            const estiloCelda = esPrincipal ? '' : 'style="padding-left: 30px; color: #6c757d; font-size: 0.85rem;"';

            // Nombre en primera columna
            let nombreColumna = '';
            if (esPrincipal) {
                // Fila principal: SKU - Canal con punto de color
                nombreColumna = `
                    <span style="display: inline-block; width: 12px; height: 12px; border-radius: 50%; background: ${dataset.borderColor || '#ccc'}; margin-right: 8px;"></span>
                    <strong>${label}</strong>
                `;
            } else {
                // Sub-fila: Solo el mes con flecha
                nombreColumna = `<small>↳ ${mesLabel}</small>`;
            }

            html += `
                <tr ${estiloFila}>
                    <td ${estiloCelda}>
                        ${nombreColumna}
                    </td>
                    <td class="text-center">
                        <span class="badge ${clasificacionClass}" style="font-size: 0.8rem;">
                            ${clasificacion}
                        </span>
                    </td>
                    <td class="text-center">
                        <span class="badge" style="background: rgba(111, 66, 193, 0.1); color: #6f42c1; font-size: 0.9rem;">
                            ${punto.x.toFixed(1)}%
                        </span>
                    </td>
                    <td class="text-end">
                        <strong style="color: #17a2b8;">$${(punto.ingreso_real || 0).toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ",")}</strong>
                    </td>
                    <td class="text-center">
                        <span class="badge" style="background: rgba(255, 193, 7, 0.1); color: #ffc107; font-size: 0.9rem;">
                            ${punto.y.toFixed(1)}%
                        </span>
                    </td>
                    <td class="text-center">
                        <span class="badge" style="background: ${colorZona}; color: ${colorTexto}; font-size: 0.85rem;">
                            ${icono} ${zona}
                        </span>
                    </td>
                </tr>
            `;
        }
    });

    tbody.innerHTML = html;

    console.log(`✅ [COMPARAR] Tabla comparativa actualizada con ${data.datasets.length} SKUs (${data.datasets.length * 3} filas)`);
}

/**
 * Obtener clase CSS para badge de clasificación
 */
function obtenerClaseClasificacionBadge(clasificacion) {
    const clases = {
        'Estrellas': 'bg-warning text-dark',
        'Prometedores': 'bg-success',
        'Potenciales': 'bg-info',
        'Revision': 'bg-warning text-dark',
        'Remover': 'bg-danger',
        'Sin Clasificar': 'bg-secondary'
    };
    return clases[clasificacion] || 'bg-secondary';
}

/**
 * Actualizar UI cuando no hay SKUs seleccionados
 */
function actualizarUIVacia() {
    // Limpiar gráfico
    if (window.matrizClasificacionChart) {
        window.matrizClasificacionChart.data.datasets = [];
        window.matrizClasificacionChart.options.scales.y.max = 100; // Reset al valor por defecto
        window.matrizClasificacionChart.update();
    }

    // Limpiar estadísticas
    actualizarEstadisticasClasificacion({
        total_skus: 0,
        ventas_totales: 0,
        ingreso_promedio: 0,
        roi_promedio: 0
    });

    // Limpiar tabla
    const tbody = document.getElementById('tablaSkusClasif');
    if (tbody) {
        tbody.innerHTML = `
            <tr>
                <td colspan="6" class="text-center text-muted py-4">
                    <i class="bi bi-inbox" style="font-size: 2rem; display: block; margin-bottom: 10px;"></i>
                    Selecciona SKUs para visualizar
                </td>
            </tr>
        `;
    }
}

/**
 * Limpiar filtros de clasificación
 */
function limpiarFiltrosClasif() {
    console.log('🧹 [CLASIFICACION] Limpiando filtros...');

    // Llamar la función definida en filtros_clasificacion.html
    if (typeof limpiarFiltrosClasificacion === 'function') {
        limpiarFiltrosClasificacion();
    }

    // Actualizar con datos vacíos
    actualizarUIVacia();
}

// ============================================
// Event Listeners
// ============================================

document.addEventListener('DOMContentLoaded', function() {
    console.log('🎬 [CLASIFICACION] Inicializando event listeners...');

    // Botón Filtrar
    const btnActualizar = document.getElementById('btnActualizarClasif');
    if (btnActualizar) {
        btnActualizar.addEventListener('click', function() {
            console.log('🖱️ [CLASIFICACION] Click en botón Filtrar');
            actualizarMatrizClasificacion();
        });
    }

    // Botón Limpiar Filtros
    const btnLimpiar = document.getElementById('btnLimpiarFiltrosClasif');
    if (btnLimpiar) {
        btnLimpiar.addEventListener('click', function() {
            console.log('🧹 [CLASIFICACION] Click en botón Limpiar Filtros');
            limpiarFiltrosClasif();
        });
    }

    console.log('✅ [CLASIFICACION] Event listeners configurados');
});

// ============================================
// Sincronización con Filtro de Mes Global
// ============================================

// Detectar cambios en el filtro de mes global y recargar SKUs
const filtroMesGlobal = document.getElementById('filtroMes');
if (filtroMesGlobal) {
    filtroMesGlobal.addEventListener('change', function() {
        console.log('📅 [CLASIFICACION] Mes global cambió, recargando SKUs...');

        // Recargar lista de SKUs para el nuevo mes
        if (typeof cargarSKUs === 'function') {
            cargarSKUs();
        }

        // Si hay SKUs seleccionados, actualizar el gráfico
        const skusSeleccionados = getSelectedSKUs();
        if (skusSeleccionados.length > 0) {
            actualizarMatrizClasificacion();
        }
    });
}

// ============================================
// COMPARACIÓN DE 3 MESES
// ============================================

/**
 * Cargar datos comparativos de 3 meses
 */
async function cargarDatosComparativos() {
    const mesSeleccionado = document.getElementById('filtroMes').value;
    const canalesSeleccionados = getSelectedCanales();
    const skusSeleccionados = getSelectedSKUs();

    console.log(`📊 [COMPARAR] Cargando datos comparativos - Mes: ${mesSeleccionado}, Canales: ${canalesSeleccionados.join(', ')}, SKUs: ${skusSeleccionados.length}`);

    if (skusSeleccionados.length === 0) {
        console.warn('⚠️ [COMPARAR] No hay SKUs seleccionados');
        alert('Por favor selecciona al menos un SKU para comparar');
        // Volver al tab Vista Actual
        document.getElementById('tab-vista-actual').click();
        return;
    }

    try {
        const params = {
            mes: mesSeleccionado,
            canales: canalesSeleccionados,
            skus: skusSeleccionados
        };

        console.log('📤 [COMPARAR] Enviando petición:', params);

        const response = await fetch('/matriz-posicionamiento/comparar-3-meses', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(params)
        });

        const result = await response.json();

        if (result.success) {
            console.log('✅ [COMPARAR] Datos recibidos:', result.data);

            // Guardar datos en variable global
            window.datosComparativo3Meses = result.data;

            // Actualizar cards de resumen
            actualizarCardsComparativos(result.data.resumen);

            // Actualizar gráfico comparativo
            actualizarGraficoComparativo(result.data);

            // Actualizar tabla comparativa
            actualizarTablaComparativa3Meses(result.data);
        } else {
            console.error('❌ [COMPARAR] Error:', result.error);
            alert('Error al cargar datos comparativos: ' + result.error);
        }

    } catch (error) {
        console.error('❌ [COMPARAR] Error en la petición:', error);
        alert('Error de conexión al cargar datos comparativos');
    }
}

/**
 * Actualizar cards de resumen
 */
function actualizarCardsComparativos(resumen) {
    document.getElementById('countMejoraron').textContent = resumen.mejoraron || 0;
    document.getElementById('countEstable').textContent = resumen.estable || 0;
    document.getElementById('countEmpeoraron').textContent = resumen.empeoraron || 0;

    // Mostrar cards
    document.getElementById('cardsResumenComparativo').style.display = 'flex';

    console.log('✅ [COMPARAR] Cards actualizados:', resumen);
}

/**
 * Actualizar gráfico comparativo con líneas de tendencia
 */
function actualizarGraficoComparativo(data) {
    const ctx = document.getElementById('matrizComparativaChart');

    if (!ctx) {
        console.error('❌ [COMPARAR] Canvas matrizComparativaChart no encontrado');
        return;
    }

    // Destruir gráfico existente si hay uno
    if (window.matrizComparativaChart) {
        console.log('🔄 [COMPARAR] Destruyendo gráfico existente...');
        if (typeof window.matrizComparativaChart.destroy === 'function') {
            window.matrizComparativaChart.destroy();
        }
        window.matrizComparativaChart = null;
    }

    console.log('🔧 [COMPARAR] Creando nuevo gráfico comparativo...');

    try {
        // Función helper para convertir color a rgba con opacidad
        const colorConOpacidad = (color, opacidad) => {
            // Si ya es rgba, reemplazar la opacidad
            if (color.includes('rgba')) {
                return color.replace(/[\d.]+\)$/g, opacidad + ')');
            }
            // Si es rgb, convertir a rgba
            if (color.includes('rgb')) {
                return color.replace('rgb', 'rgba').replace(')', ', ' + opacidad + ')');
            }
            // Si es hex, convertir a rgba (simplificado para colores comunes)
            return color; // Mantener color original si no se puede convertir
        };

        // Personalizar estilos de puntos según el mes (índice)
        // Opción 3: Todas X con opacidad gradual + tamaño decreciente
        const datasetsPersonalizados = data.datasets.map(dataset => {
            return {
                ...dataset,
                pointStyle: 'cross',  // Todas X
                pointRadius: dataset.data.map((point, index) => {
                    if (index === 2) return 8;              // Mes Actual: grande
                    if (index === 1) return 6;              // Mes -1: medio
                    return 5;                               // Mes -2: pequeño
                }),
                pointBorderWidth: dataset.data.map((point, index) => {
                    if (index === 2) return 3;              // Mes Actual: borde grueso
                    if (index === 1) return 2;              // Mes -1: medio
                    return 2;                               // Mes -2: medio
                }),
                pointBackgroundColor: dataset.data.map((point, index) => {
                    const color = dataset.borderColor || '#6c757d';
                    if (index === 2) return colorConOpacidad(color, 1.0);   // Mes Actual: 100%
                    if (index === 1) return colorConOpacidad(color, 0.8);   // Mes -1: 80%
                    return colorConOpacidad(color, 0.5);                     // Mes -2: 50%
                }),
                pointBorderColor: dataset.data.map((point, index) => {
                    const color = dataset.borderColor || '#6c757d';
                    if (index === 2) return colorConOpacidad(color, 1.0);   // Mes Actual: 100%
                    if (index === 1) return colorConOpacidad(color, 0.8);   // Mes -1: 80%
                    return colorConOpacidad(color, 0.5);                     // Mes -2: 50%
                }),
                pointHoverRadius: dataset.data.map((point, index) => {
                    if (index === 2) return 10;             // Mes Actual
                    if (index === 1) return 8;              // Mes -1
                    return 7;                               // Mes -2
                })
            };
        });

        // Plugin para mostrar etiquetas de mes en cada punto
        const labelPlugin = {
            id: 'pointLabels',
            afterDatasetsDraw(chart) {
                const ctx = chart.ctx;
                const mesesLabels = data.meses_labels || ['Mes -2', 'Mes -1', 'Mes Actual'];

                chart.data.datasets.forEach((dataset, datasetIndex) => {
                    const meta = chart.getDatasetMeta(datasetIndex);

                    // No dibujar etiquetas si el dataset está oculto
                    if (meta.hidden) {
                        return;
                    }

                    meta.data.forEach((point, index) => {
                        const label = mesesLabels[index] || '';

                        ctx.save();
                        ctx.fillStyle = '#000000';
                        ctx.font = 'bold 10px Arial';
                        ctx.textAlign = 'center';
                        ctx.textBaseline = 'top';

                        // Dibujar etiqueta debajo del punto
                        ctx.fillText(label, point.x, point.y + 12);
                        ctx.restore();
                    });
                });
            }
        };

        // Crear gráfico con el mismo plugin de zonas + plugin de etiquetas
        window.matrizComparativaChart = new Chart(ctx, {
            type: 'scatter',
            data: {
                datasets: datasetsPersonalizados
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: true,
                        position: 'bottom',
                        onClick: (e, legendItem, legend) => {
                            const index = legendItem.datasetIndex;
                            const ci = legend.chart;
                            const meta = ci.getDatasetMeta(index);

                            // Toggle visibilidad
                            meta.hidden = meta.hidden === null ? !ci.data.datasets[index].hidden : null;
                            ci.update();
                        },
                        onHover: (event, legendItem, legend) => {
                            event.native.target.style.cursor = 'pointer';
                        },
                        onLeave: (event, legendItem, legend) => {
                            event.native.target.style.cursor = 'default';
                        },
                        labels: {
                            usePointStyle: true,
                            padding: 6,
                            font: { size: 9, weight: '600' },
                            boxWidth: 10,
                            boxHeight: 10,
                            generateLabels: (chart) => {
                                const datasets = chart.data.datasets;
                                return datasets.map((dataset, i) => {
                                    const meta = chart.getDatasetMeta(i);
                                    const hidden = meta.hidden;

                                    return {
                                        text: dataset.label,
                                        fillStyle: hidden ? '#d3d3d3' : dataset.borderColor,
                                        strokeStyle: hidden ? '#d3d3d3' : dataset.borderColor,
                                        lineWidth: hidden ? 1 : 2,
                                        hidden: hidden,
                                        datasetIndex: i,
                                        fontColor: hidden ? '#999999' : '#333333',
                                        textDecoration: hidden ? 'line-through' : 'none'
                                    };
                                });
                            }
                        }
                    },
                    tooltip: {
                        callbacks: {
                            title: function(tooltipItems) {
                                return tooltipItems[0].dataset.label || 'SKU';
                            },
                            label: function(context) {
                                const point = context.parsed;
                                const dataIndex = context.dataIndex;
                                const mesesLabels = data.meses_labels || ['Mes -2', 'Mes -1', 'Mes Actual'];
                                return [
                                    `Período: ${mesesLabels[dataIndex] || `Punto ${dataIndex + 1}`}`,
                                    `% IR: ${point.x.toFixed(2)}%`,
                                    `% ROI: ${point.y.toFixed(2)}%`
                                ];
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        title: {
                            display: true,
                            text: '% Ingreso Real',
                            font: { size: 14, weight: 'bold' }
                        },
                        min: 0,
                        max: 50,
                        ticks: {
                            callback: function(value) {
                                return value + '%';
                            }
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: 'ROI %',
                            font: { size: 14, weight: 'bold' }
                        },
                        min: 0,
                        max: data.estadisticas?.eje_y_max || 100,
                        ticks: {
                            callback: function(value) {
                                return value + '%';
                            }
                        }
                    }
                }
            },
            plugins: [
                labelPlugin,
                ...(typeof window.backgroundZonesPluginClasif !== 'undefined' ? [window.backgroundZonesPluginClasif] : [])
            ]
        });

        console.log(`✅ [COMPARAR] Gráfico creado exitosamente con ${data.datasets.length} líneas de tendencia`);
    } catch (error) {
        console.error('❌ [COMPARAR] Error al crear gráfico:', error);
        console.error('Error details:', error.message, error.stack);
        return;
    }
}

/**
 * Función para mostrar/ocultar todas las líneas del gráfico comparativo
 */
function toggleTodasLineas() {
    if (!window.matrizComparativaChart) {
        console.warn('⚠️ [COMPARAR] Gráfico no disponible');
        return;
    }

    const chart = window.matrizComparativaChart;
    const btn = document.getElementById('btnToggleAllLineas');

    // Verificar si hay al menos una línea visible
    const hayVisibles = chart.data.datasets.some((dataset, i) => {
        const meta = chart.getDatasetMeta(i);
        return !meta.hidden;
    });

    // Si hay visibles, ocultar todas. Si no, mostrar todas
    chart.data.datasets.forEach((dataset, i) => {
        const meta = chart.getDatasetMeta(i);
        meta.hidden = hayVisibles ? true : null;
    });

    // Actualizar texto del botón
    if (hayVisibles) {
        btn.innerHTML = '<i class="bi bi-eye me-1"></i>Mostrar todas';
    } else {
        btn.innerHTML = '<i class="bi bi-eye-slash me-1"></i>Ocultar todas';
    }

    chart.update();
    console.log(`✅ [COMPARAR] Líneas ${hayVisibles ? 'ocultadas' : 'mostradas'}`);
}

// ============================================
// EVENT LISTENERS PARA TABS
// ============================================

// Al hacer clic en el tab "Comparar 3 Meses"
document.addEventListener('DOMContentLoaded', function() {
    const tabComparar = document.getElementById('tab-comparar-3meses');
    const tabActual = document.getElementById('tab-vista-actual');

    if (tabComparar) {
        tabComparar.addEventListener('shown.bs.tab', function (event) {
            console.log('📊 [COMPARAR] Tab activado');

            // Si ya hay datos precargados, actualizar la tabla
            if (window.datosComparativo3Meses) {
                console.log('✅ [COMPARAR] Usando datos precargados');
                actualizarTablaComparativa3Meses(window.datosComparativo3Meses);
                return;
            }

            // Si no hay datos precargados, cargarlos ahora
            console.log('⚠️ [COMPARAR] Datos no precargados, cargando ahora...');
            cargarDatosComparativos();
        });

        console.log('✅ [COMPARAR] Event listener de tab configurado');
    }

    // Al volver al tab "Vista Actual", restaurar la tabla normal
    if (tabActual) {
        tabActual.addEventListener('shown.bs.tab', function (event) {
            console.log('📊 [CLASIFICACION] Tab Vista Actual activado');

            // Restaurar tabla normal si existe data guardada
            if (window.datosClasificacionActual && window.datosClasificacionActual.skus) {
                actualizarTablaSkus(window.datosClasificacionActual.skus);
            }
        });

        console.log('✅ [CLASIFICACION] Event listener de tab Vista Actual configurado');
    }
});

console.log('✅ [CLASIFICACION] Módulo cargado completamente');
