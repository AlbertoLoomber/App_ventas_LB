/**
 * JavaScript para Matriz de Posicionamiento
 * Maneja filtros y actualización dinámica
 */

// Variables globales (matrizChart se define en matriz.html)

/**
 * Actualiza la matriz con los filtros seleccionados
 */
async function actualizarMatriz() {
    const mesSeleccionado = document.getElementById('filtroMes').value;
    const marcaSeleccionada = document.getElementById('filtroMarca').value;
    const btnActualizar = document.getElementById('btnActualizar');
    const loadingIndicator = document.getElementById('loadingIndicator');

    console.log(`📊 [MATRIZ] Filtros - Mes: ${mesSeleccionado}, Marca: ${marcaSeleccionada}`);

    // Mostrar indicador de carga
    btnActualizar.disabled = true;
    loadingIndicator.style.display = 'block';

    try {
        // Construir parámetros
        const params = {
            mes: parseInt(mesSeleccionado),
            marca: marcaSeleccionada
        };

        // Llamada AJAX
        const response = await fetch('/matriz-posicionamiento/actualizar', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(params)
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const result = await response.json();
        console.log('📊 Datos recibidos:', result);

        if (result.success) {
            // Actualizar gráfico con nuevos datos
            actualizarGrafico(result.data);

            // Actualizar estadísticas
            actualizarEstadisticas(result.data.estadisticas);

            // Actualizar tabla
            actualizarTabla(result.data.canales);

            // Actualizar también la matriz de categorías con el mismo filtro
            if (typeof actualizarMatrizCategorias === 'function') {
                console.log('🔄 [MATRIZ] Actualizando matriz de categorías con mes:', mesSeleccionado);
                await actualizarMatrizCategorias(parseInt(mesSeleccionado));
                console.log('✅ [MATRIZ] Matriz de categorías actualizada');
            } else {
                console.warn('⚠️ [MATRIZ] Función actualizarMatrizCategorias no disponible');
            }

            // CRÍTICO: Asegurar que el botón de categorías esté habilitado después de la actualización automática
            setTimeout(() => {
                const btnCat = document.getElementById('btnActualizarCat');
                if (btnCat) {
                    btnCat.disabled = false;
                    console.log('✅ [FIX] Botón de categorías forzado a enabled después de actualización automática');
                }
            }, 200);

            console.log('✅ Matriz actualizada exitosamente');
        } else {
            console.error('❌ Error:', result.error);
            alert('Error al actualizar la matriz: ' + result.error);
        }
    } catch (error) {
        console.error('❌ Error en la petición:', error);
        console.error('Detalles:', error.message);
        alert('Error de conexión al actualizar la matriz: ' + error.message);
    } finally {
        // Ocultar indicador de carga
        btnActualizar.disabled = false;
        loadingIndicator.style.display = 'none';
    }
}

/**
 * Actualiza el gráfico Chart.js con nuevos datos
 */
function actualizarGrafico(data) {
    console.log('🔄 Actualizando gráfico...');
    console.log(`📊 Total datasets recibidos: ${data.datasets.length}`);
    console.log('Datasets:', data.datasets.map(d => `${d.label} (r=${d.data[0].r.toFixed(2)})`));

    // Verificar que window.matrizChart existe
    if (!window.matrizChart) {
        console.error('❌ window.matrizChart no existe');
        console.log('Intentando obtener chart por ID...');

        // Intentar obtener por ID como fallback
        const chartInstance = Chart.getChart('matrizPosicionamiento');
        if (chartInstance) {
            console.log('✅ Chart obtenido por ID');
            window.matrizChart = chartInstance;
        } else {
            console.error('❌ No se pudo obtener el chart de ninguna manera');
            return;
        }
    }

    // Actualizar el gráfico
    console.log('🔄 Limpiando datasets antiguos...');
    window.matrizChart.data.datasets = [];

    console.log('🔄 Agregando nuevos datasets...');
    window.matrizChart.data.datasets = data.datasets;

    // Actualizar el eje Y dinámicamente
    if (data.estadisticas && data.estadisticas.eje_y_max) {
        window.matrizChart.options.scales.y.max = data.estadisticas.eje_y_max;
        console.log(`📊 Eje Y ajustado a: 0% - ${data.estadisticas.eje_y_max}%`);
    }

    console.log('🔄 Llamando a chart.update()...');
    window.matrizChart.update('active');

    console.log(`✅ Gráfico actualizado exitosamente con ${window.matrizChart.data.datasets.length} datasets`);
    console.log('Datasets actualizados:', window.matrizChart.data.datasets.map(d => d.label));
}

/**
 * Actualiza las estadísticas en las tarjetas
 */
function actualizarEstadisticas(stats) {
    console.log('📊 Actualizando estadísticas:', stats);

    try {
        // Ingreso Real Total (card1 ahora)
        const statIngresoReal = document.getElementById('statIngresoReal');
        if (statIngresoReal) {
            statIngresoReal.textContent = '$' + stats.ingreso_real_total.toLocaleString('es-MX', { maximumFractionDigits: 0 });
        }

        // Subtítulo - Canales analizados
        const statCanalesSubtitle = statIngresoReal?.nextElementSibling;
        if (statCanalesSubtitle) {
            statCanalesSubtitle.textContent = stats.total_canales + ' canales';
        }

        // Ventas totales
        const card2 = document.querySelector('.col-6:nth-child(2) .stat-value');
        if (card2) card2.textContent = '$' + stats.ventas_totales.toLocaleString('es-MX', { maximumFractionDigits: 0 });

        // % Ingreso Real Promedio
        const card3 = document.querySelector('.col-6:nth-child(3) .stat-value');
        if (card3) card3.textContent = stats.ingreso_promedio.toFixed(1) + '%';

        // ROI Promedio
        const card4 = document.querySelector('.col-6:nth-child(4) .stat-value');
        if (card4) card4.textContent = stats.roi_promedio.toFixed(1) + '%';

        console.log('✅ Estadísticas actualizadas');
    } catch (error) {
        console.error('❌ Error actualizando estadísticas:', error);
    }
}

/**
 * Actualiza la tabla de canales
 */
function actualizarTabla(canales) {
    console.log('📋 Actualizando tabla con', canales.length, 'canales');

    const tbody = document.querySelector('.tabla-canales tbody');

    if (!tbody) {
        console.error('❌ No se encontró el tbody de la tabla');
        return;
    }

    tbody.innerHTML = canales.map(canal => `
        <tr>
            <td>
                <span style="display: inline-block; width: 12px; height: 12px; border-radius: 50%; background: ${canal.color_canal}; margin-right: 8px;"></span>
                <strong>${canal.canal}</strong>
            </td>
            <td class="text-center">
                <span class="badge" style="background: rgba(111, 66, 193, 0.1); color: #6f42c1; font-size: 0.9rem;">
                    ${canal.ingreso_real_pct.toFixed(1)}%
                </span>
            </td>
            <td class="text-end">
                <strong style="color: #17a2b8;">$${canal.ingreso_real.toLocaleString('es-MX', { maximumFractionDigits: 0 })}</strong>
            </td>
            <td class="text-center">
                <span class="badge" style="background: rgba(255, 193, 7, 0.1); color: #ffc107; font-size: 0.9rem;">
                    ${canal.roi_pct.toFixed(1)}%
                </span>
            </td>
            <td class="text-end">
                <strong>$${canal.ventas.toLocaleString('es-MX', { maximumFractionDigits: 0 })}</strong>
            </td>
            <td class="text-center">
                <span class="zona-badge" style="background: ${canal.color_zona}; color: ${canal.color_texto};">
                    ${canal.icono} ${canal.zona}
                </span>
            </td>
        </tr>
    `).join('');

    console.log('✅ Tabla actualizada');
}

/**
 * Inicialización al cargar la página
 */
document.addEventListener('DOMContentLoaded', function() {
    // Asignar manejador al botón de actualizar
    const btnActualizar = document.getElementById('btnActualizar');
    if (btnActualizar) {
        btnActualizar.addEventListener('click', actualizarMatriz);
    }

    // Verificar si el gráfico ya existe (se crea en matriz.html)
    setTimeout(() => {
        if (window.matrizChart) {
            console.log('✅ Referencia al gráfico Chart.js obtenida desde variable global');
            console.log(`   Total datasets: ${window.matrizChart.data.datasets.length}`);
        } else {
            console.error('❌ Variable global matrizChart no encontrada');
        }
    }, 500);

    console.log('✅ Módulo de Matriz de Posicionamiento inicializado');
});
