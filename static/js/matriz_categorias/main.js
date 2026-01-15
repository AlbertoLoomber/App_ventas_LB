/**
 * JavaScript para Matriz de Categorías
 * Maneja filtros y actualización dinámica de la matriz por Canal + Categoría
 */

// Variable global para el gráfico (se define en matriz.html)
// window.matrizCategoriasChart

/**
 * Actualiza la matriz de categorías con los filtros seleccionados
 * IMPORTANTE: Usa el filtro de mes GLOBAL (filtroMes) de la Matriz de Posicionamiento
 */
async function actualizarMatrizCategorias(mes = null) {
    const btnActualizarCat = document.getElementById('btnActualizarCat');
    const loadingIndicatorCat = document.getElementById('loadingIndicatorCat');

    // Obtener mes del selector GLOBAL (filtroMes) si no se proporciona
    if (mes === null) {
        const filtroMesGlobal = document.getElementById('filtroMes');
        mes = filtroMesGlobal ? parseInt(filtroMesGlobal.value) : null;
    }

    // Obtener canales seleccionados (usar la función del filtro)
    let canalesSeleccionados = typeof getSelectedCanalesCat === 'function'
        ? getSelectedCanalesCat()
        : Array.from(document.querySelectorAll('.canal-checkbox-cat:checked')).map(cb => cb.value);

    // DEBUG: Mostrar qué checkboxes existen
    const todosCheckboxesCanales = document.querySelectorAll('.canal-checkbox-cat');
    const checkboxesChecked = document.querySelectorAll('.canal-checkbox-cat:checked');
    console.log(`🔍 [DEBUG] Total checkboxes de canales DISPONIBLES: ${todosCheckboxesCanales.length}`);
    console.log(`🔍 [DEBUG] Total checkboxes de canales MARCADOS: ${checkboxesChecked.length}`);

    // Mostrar cada checkbox marcado con su valor
    checkboxesChecked.forEach((cb, idx) => {
        console.log(`🔍 [DEBUG] Checkbox ${idx}: id="${cb.id}", value="${cb.value}", checked=${cb.checked}`);
    });

    console.log(`🔍 [DEBUG] Canales seleccionados ANTES de filtrar:`, canalesSeleccionados);
    console.log(`🔍 [DEBUG] Tipo de canalesSeleccionados:`, typeof canalesSeleccionados, Array.isArray(canalesSeleccionados));
    console.log(`🔍 [DEBUG] getSelectedCanalesCat existe?`, typeof getSelectedCanalesCat === 'function');

    // Filtrar valores especiales que no son canales reales
    console.log(`🔍 [DEBUG] Canales ANTES de filtrar 'Todos':`, canalesSeleccionados);
    console.log(`🔍 [DEBUG] Detalle de cada canal ANTES del filtro:`);
    canalesSeleccionados.forEach((c, idx) => {
        console.log(`   [${idx}] valor="${c}", tipo=${typeof c}, longitud=${c ? c.length : 'null'}, trim="${c ? c.trim() : 'null'}"`);
    });

    const canalesAntesDeFilter = [...canalesSeleccionados];
    // Filtrar solo valores vacíos, null, undefined, 'Todos' y 'todos'
    canalesSeleccionados = canalesSeleccionados.filter(c => {
        const resultado = c && c !== 'Todos' && c !== 'todos' && c.trim() !== '';
        if (!resultado) {
            console.warn(`⚠️ [FILTRO] Canal "${c}" será eliminado (c=${c}, tipo=${typeof c})`);
        }
        return resultado;
    });
    console.log(`🔍 [DEBUG] Canales DESPUÉS de filtrar 'Todos':`, canalesSeleccionados);
    if (canalesAntesDeFilter.length !== canalesSeleccionados.length) {
        console.warn(`⚠️ [DEBUG] Se eliminaron ${canalesAntesDeFilter.length - canalesSeleccionados.length} canales en el filtro!`);
        console.warn(`⚠️ [DEBUG] Canales eliminados:`, canalesAntesDeFilter.filter(c => !canalesSeleccionados.includes(c)));
    }

    // Obtener categorías seleccionadas (usar la función del filtro)
    let categoriasSeleccionadas = typeof getSelectedCategoriasCat === 'function'
        ? getSelectedCategoriasCat()
        : Array.from(document.querySelectorAll('.categoria-checkbox:checked')).map(cb => cb.value);

    // Filtrar valores especiales que no son categorías reales
    categoriasSeleccionadas = categoriasSeleccionadas.filter(c => c !== 'Todas' && c !== 'todas');

    // Si todos los canales están seleccionados, enviar array vacío (sin filtro)
    const totalCanales = document.querySelectorAll('.canal-checkbox-cat').length;
    if (totalCanales > 0 && canalesSeleccionados.length === totalCanales) {
        console.log(`✅ [DEBUG] Todos los ${totalCanales} canales seleccionados → enviando array vacío`);
        canalesSeleccionados = [];
    }

    // Si todas las categorías están seleccionadas, enviar array vacío (sin filtro)
    const totalCategorias = document.querySelectorAll('.categoria-checkbox').length;
    if (totalCategorias > 0 && categoriasSeleccionadas.length === totalCategorias) {
        console.log(`✅ [DEBUG] Todas las ${totalCategorias} categorías seleccionadas → enviando array vacío`);
        categoriasSeleccionadas = [];
    }

    console.log(`🔍 [DEBUG] Canales seleccionados DESPUÉS de filtrar:`, canalesSeleccionados);
    console.log(`📊 [MATRIZ CAT] Filtros - Mes: ${mes}, Canales: ${canalesSeleccionados.length > 0 ? canalesSeleccionados : 'Todos'}, Categorías: ${categoriasSeleccionadas.length > 0 ? categoriasSeleccionadas : 'Todas'}`);

    // Mostrar indicador de carga
    if (btnActualizarCat) btnActualizarCat.disabled = true;
    if (loadingIndicatorCat) loadingIndicatorCat.style.display = 'block';

    try {
        // Construir parámetros
        const params = {
            mes: mes,
            canales: canalesSeleccionados,
            categorias: categoriasSeleccionadas
        };

        console.log(`📤 [AJAX DEBUG] Parámetros que se enviarán:`, JSON.stringify(params, null, 2));

        // Llamada AJAX
        const response = await fetch('/matriz-posicionamiento/actualizar-categorias', {
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
        console.log('📊 [MATRIZ CAT] Datos recibidos:', result);

        if (result.success) {
            // Actualizar gráfico con nuevos datos
            actualizarGraficoCategorias(result.data);

            // Actualizar estadísticas
            actualizarEstadisticasCategorias(result.data.estadisticas);

            // Actualizar tabla
            actualizarTablaCategorias(result.data.categorias);

            // Mostrar advertencia si no hay datos
            if (result.data.categorias.length === 0) {
                console.warn('⚠️ [MATRIZ CAT] No hay datos para los filtros seleccionados');
            }

            console.log('✅ [MATRIZ CAT] Matriz de categorías actualizada exitosamente');
        } else {
            console.error('❌ [MATRIZ CAT] Error:', result.error);
            alert('Error al actualizar la matriz de categorías: ' + result.error);
        }
    } catch (error) {
        console.error('❌ [MATRIZ CAT] Error en la petición:', error);
        console.error('Detalles:', error.message);
        alert('Error de conexión al actualizar la matriz de categorías: ' + error.message);
    } finally {
        // Ocultar indicador de carga
        if (btnActualizarCat) btnActualizarCat.disabled = false;
        if (loadingIndicatorCat) loadingIndicatorCat.style.display = 'none';
    }
}

/**
 * Actualiza el gráfico Chart.js de categorías con nuevos datos
 */
function actualizarGraficoCategorias(data) {
    console.log('🔄 [MATRIZ CAT] Actualizando gráfico...');
    console.log(`📊 [MATRIZ CAT] Total datasets recibidos: ${data.datasets.length}`);

    // Verificar que window.matrizCategoriasChart existe
    if (!window.matrizCategoriasChart) {
        console.error('❌ [MATRIZ CAT] window.matrizCategoriasChart no existe');

        // Intentar obtener por ID como fallback
        const chartInstance = Chart.getChart('matrizCategorias');
        if (chartInstance) {
            console.log('✅ [MATRIZ CAT] Chart obtenido por ID');
            window.matrizCategoriasChart = chartInstance;
        } else {
            console.error('❌ [MATRIZ CAT] No se pudo obtener el chart de ninguna manera');
            return;
        }
    }

    // Actualizar el gráfico
    console.log('🔄 [MATRIZ CAT] Limpiando datasets antiguos...');
    window.matrizCategoriasChart.data.datasets = [];

    console.log('🔄 [MATRIZ CAT] Agregando nuevos datasets...');
    window.matrizCategoriasChart.data.datasets = data.datasets;

    // Actualizar el eje Y dinámicamente
    if (data.estadisticas && data.estadisticas.eje_y_max) {
        window.matrizCategoriasChart.options.scales.y.max = data.estadisticas.eje_y_max;
        console.log(`📊 [MATRIZ CAT] Eje Y ajustado a: 0% - ${data.estadisticas.eje_y_max}%`);
    }

    console.log('🔄 [MATRIZ CAT] Llamando a chart.update()...');
    window.matrizCategoriasChart.update('active');

    console.log(`✅ [MATRIZ CAT] Gráfico actualizado exitosamente con ${window.matrizCategoriasChart.data.datasets.length} datasets`);
}

/**
 * Actualiza las estadísticas en las tarjetas de categorías
 */
function actualizarEstadisticasCategorias(stats) {
    console.log('📊 [MATRIZ CAT] Actualizando estadísticas:', stats);

    try {
        // Valores por defecto si stats no tiene datos
        const ingresoRealTotal = stats?.ingreso_real_total || 0;
        const ventasTotales = stats?.ventas_totales || 0;
        const ingresoPromedio = stats?.ingreso_promedio || 0;
        const roiPromedio = stats?.roi_promedio || 0;
        const totalCombinaciones = stats?.total_combinaciones || 0;

        // Ingreso Real Total
        const statIngresoRealCat = document.getElementById('statIngresoRealCat');
        if (statIngresoRealCat) {
            statIngresoRealCat.textContent = '$' + ingresoRealTotal.toLocaleString('es-MX', { maximumFractionDigits: 0 });
        }

        // Subtítulo - Total combinaciones
        const subtituloCombinaciones = statIngresoRealCat?.parentElement.querySelector('small');
        if (subtituloCombinaciones) {
            subtituloCombinaciones.textContent = totalCombinaciones + ' combinaciones';
        }

        // Ventas totales
        const statVentasCat = document.getElementById('statVentasCat');
        if (statVentasCat) {
            statVentasCat.textContent = '$' + ventasTotales.toLocaleString('es-MX', { maximumFractionDigits: 0 });
        }

        // % Ingreso Real Promedio
        const statIngresoPromCat = document.getElementById('statIngresoPromCat');
        if (statIngresoPromCat) {
            statIngresoPromCat.textContent = ingresoPromedio.toFixed(1) + '%';
        }

        // ROI Promedio
        const statRoiPromCat = document.getElementById('statRoiPromCat');
        if (statRoiPromCat) {
            statRoiPromCat.textContent = roiPromedio.toFixed(1) + '%';
        }

        console.log('✅ [MATRIZ CAT] Estadísticas actualizadas');
    } catch (error) {
        console.error('❌ [MATRIZ CAT] Error actualizando estadísticas:', error);
    }
}

/**
 * Actualiza la tabla de categorías
 */
function actualizarTablaCategorias(categorias) {
    console.log('📋 [MATRIZ CAT] Actualizando tabla con', categorias.length, 'categorías');

    const tbody = document.getElementById('tablaCategorias');

    if (!tbody) {
        console.error('❌ [MATRIZ CAT] No se encontró el tbody de la tabla (id: tablaCategorias)');
        return;
    }

    if (categorias.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="6" class="text-center text-muted py-4">
                    <i class="bi bi-inbox" style="font-size: 2rem; display: block; margin-bottom: 10px;"></i>
                    No hay datos para los filtros seleccionados
                </td>
            </tr>
        `;
        return;
    }

    tbody.innerHTML = categorias.map(cat => `
        <tr>
            <td>
                <span style="display: inline-block; width: 12px; height: 12px; border-radius: 50%; background: ${cat.color_canal}; margin-right: 8px;"></span>
                <strong>${cat.label}</strong>
            </td>
            <td class="text-center">
                <span class="badge" style="background: rgba(111, 66, 193, 0.1); color: #6f42c1; font-size: 0.9rem;">
                    ${cat.ingreso_real_pct.toFixed(1)}%
                </span>
            </td>
            <td class="text-end">
                <strong style="color: #17a2b8;">$${cat.ingreso_real.toLocaleString('es-MX', { maximumFractionDigits: 0 })}</strong>
            </td>
            <td class="text-center">
                <span class="badge" style="background: rgba(255, 193, 7, 0.1); color: #ffc107; font-size: 0.9rem;">
                    ${cat.roi_pct.toFixed(1)}%
                </span>
            </td>
            <td class="text-end">
                <strong>$${cat.ventas.toLocaleString('es-MX', { maximumFractionDigits: 0 })}</strong>
            </td>
            <td class="text-center">
                <span class="zona-badge" style="background: ${cat.color_zona}; color: ${cat.color_texto};">
                    ${cat.icono} ${cat.zona}
                </span>
            </td>
        </tr>
    `).join('');

    console.log('✅ [MATRIZ CAT] Tabla actualizada');
}


/**
 * Seleccionar/Deseleccionar todos los canales
 */
function toggleTodosCanales(checkbox) {
    const checkboxes = document.querySelectorAll('.canal-checkbox-cat');
    checkboxes.forEach(cb => {
        cb.checked = checkbox.checked;
    });
    console.log(`📋 [MATRIZ CAT] Todos los canales ${checkbox.checked ? 'seleccionados' : 'deseleccionados'}`);
}

/**
 * Seleccionar/Deseleccionar todas las categorías
 */
function toggleTodasCategorias(checkbox) {
    const checkboxes = document.querySelectorAll('input[name="categorias_cat"]');
    checkboxes.forEach(cb => {
        cb.checked = checkbox.checked;
    });
    console.log(`📋 [MATRIZ CAT] Todas las categorías ${checkbox.checked ? 'seleccionadas' : 'deseleccionadas'}`);
}

/**
 * Inicialización al cargar la página
 */
document.addEventListener('DOMContentLoaded', function() {
    // Asignar manejador al botón de actualizar
    const btnActualizarCat = document.getElementById('btnActualizarCat');
    if (btnActualizarCat) {
        btnActualizarCat.addEventListener('click', () => actualizarMatrizCategorias());
        console.log('✅ [MATRIZ CAT] Botón de actualizar conectado');
    }

    // Asignar manejador al botón de limpiar filtros
    const btnLimpiarFiltrosCat = document.getElementById('btnLimpiarFiltrosCat');
    if (btnLimpiarFiltrosCat) {
        btnLimpiarFiltrosCat.addEventListener('click', function() {
            // Limpiar filtros usando la función del partial
            if (typeof limpiarFiltrosCategorias === 'function') {
                limpiarFiltrosCategorias();
            }
            // Actualizar la matriz automáticamente
            actualizarMatrizCategorias();
            console.log('🔄 [MATRIZ CAT] Filtros limpiados y matriz actualizada');
        });
        console.log('✅ [MATRIZ CAT] Botón de limpiar filtros conectado');
    }

    // Verificar conflictos de nombres
    console.log('🔍 [MATRIZ CAT] Verificando funciones:');
    console.log('  - getSelectedCanalesCat:', typeof getSelectedCanalesCat);
    console.log('  - getSelectedCategoriasCat:', typeof getSelectedCategoriasCat);
    console.log('  - getSelectedCanales (OLD):', typeof getSelectedCanales);

    // Verificar si el gráfico ya existe (se crea en matriz.html)
    setTimeout(() => {
        if (window.matrizCategoriasChart) {
            console.log('✅ [MATRIZ CAT] Referencia al gráfico Chart.js obtenida desde variable global');
            console.log(`   Total datasets: ${window.matrizCategoriasChart.data.datasets.length}`);
        } else {
            console.warn('⚠️  [MATRIZ CAT] Variable global matrizCategoriasChart no encontrada aún');
        }
    }, 500);

    console.log('✅ [MATRIZ CAT] Módulo de Matriz de Categorías inicializado');
});
