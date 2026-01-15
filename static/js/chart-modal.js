/**
 * ============================================
 * CHART MODAL - Sistema de Ampliación de Gráficos
 * ============================================
 * Permite expandir los mini-gráficos de las tarjetas
 * en un modal de pantalla completa con mayor detalle
 * Compatible con gráficos Canvas personalizados
 */

class ChartModal {
    constructor() {
        this.modal = null;
        this.currentChart = null;
        this.chartConfigs = {}; // Almacena las configuraciones de los gráficos originales
        this.canvasDrawFunctions = {}; // Almacena funciones de dibujo personalizadas
        this.init();
    }

    init() {
        // Crear el modal si no existe
        this.createModal();

        // Event listeners
        this.setupEventListeners();

        console.log('✅ ChartModal inicializado');
    }

    createModal() {
        // Verificar si ya existe el modal
        if (document.getElementById('chartModal')) {
            this.modal = document.getElementById('chartModal');
            return;
        }

        // Crear estructura del modal
        const modalHTML = `
            <div id="chartModal" class="chart-modal">
                <div class="chart-modal-content">
                    <!-- Header -->
                    <div class="chart-modal-header">
                        <h2 class="chart-modal-title">
                            <i class="fas fa-chart-line"></i>
                            <span id="chartModalTitle">Gráfico Ampliado</span>
                        </h2>
                        <button class="chart-modal-close" id="chartModalClose">
                            <span>&times;</span>
                        </button>
                    </div>

                    <!-- Body -->
                    <div class="chart-modal-body">
                        <!-- Estadísticas principales -->
                        <div class="chart-stats-container" id="chartStatsContainer">
                            <!-- Se llenará dinámicamente -->
                        </div>

                        <!-- Gráfico ampliado -->
                        <div class="chart-expanded-container">
                            <canvas id="chartExpandedCanvas" class="chart-expanded-canvas"></canvas>
                        </div>
                    </div>

                    <!-- Footer -->
                    <div class="chart-modal-footer">
                        <span class="chart-info-text">
                            <i class="fas fa-info-circle"></i>
                            Click fuera del gráfico o presiona ESC para cerrar
                        </span>
                        <button class="chart-download-btn" id="chartDownloadBtn">
                            <i class="fas fa-download"></i>
                            Descargar Gráfico
                        </button>
                    </div>
                </div>
            </div>
        `;

        // Insertar en el DOM
        document.body.insertAdjacentHTML('beforeend', modalHTML);
        this.modal = document.getElementById('chartModal');
    }

    setupEventListeners() {
        // Cerrar modal con botón X
        const closeBtn = document.getElementById('chartModalClose');
        if (closeBtn) {
            closeBtn.addEventListener('click', () => this.closeModal());
        }

        // Cerrar modal al hacer click fuera del contenido
        this.modal.addEventListener('click', (e) => {
            if (e.target === this.modal) {
                this.closeModal();
            }
        });

        // Cerrar modal con tecla ESC
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.modal.classList.contains('show')) {
                this.closeModal();
            }
        });

        // Botón de descarga
        const downloadBtn = document.getElementById('chartDownloadBtn');
        if (downloadBtn) {
            downloadBtn.addEventListener('click', () => this.downloadChart());
        }
    }

    /**
     * Registra la configuración de un gráfico para poder recrearlo
     * Soporta tanto Chart.js como Canvas personalizado
     */
    registerChart(chartId, config, data, title, stats = {}) {
        this.chartConfigs[chartId] = {
            config: config,
            data: data,
            title: title,
            stats: stats
        };
    }

    /**
     * Registra una función de dibujo personalizada para gráficos Canvas
     */
    registerCanvasChart(chartId, drawFunction, data, title, stats = {}) {
        this.canvasDrawFunctions[chartId] = {
            drawFunction: drawFunction,
            data: data,
            title: title,
            stats: stats
        };
        console.log(`✅ Gráfico Canvas registrado: ${chartId}`);
    }

    /**
     * Abre el modal con el gráfico especificado
     */
    openModal(chartId) {
        // Intentar primero con gráficos Canvas personalizados
        const canvasConfig = this.canvasDrawFunctions[chartId];
        const chartConfig = this.chartConfigs[chartId];

        if (!canvasConfig && !chartConfig) {
            console.error(`Gráfico ${chartId} no encontrado en configuraciones`);
            return;
        }

        // Usar el que esté disponible
        const config = canvasConfig || chartConfig;

        // Actualizar título
        document.getElementById('chartModalTitle').textContent = config.title;

        // Actualizar estadísticas
        this.updateStats(config.stats);

        // Renderizar gráfico ampliado
        if (canvasConfig) {
            this.renderExpandedCanvasChart(canvasConfig);
        } else {
            this.renderExpandedChart(chartConfig);
        }

        // Mostrar modal
        this.modal.classList.add('show');
        document.body.style.overflow = 'hidden'; // Prevenir scroll del body
    }

    /**
     * Cierra el modal
     */
    closeModal() {
        this.modal.classList.remove('show');
        document.body.style.overflow = ''; // Restaurar scroll del body

        // Destruir gráfico actual si existe
        if (this.currentChart) {
            this.currentChart.destroy();
            this.currentChart = null;
        }
    }

    /**
     * Actualiza las tarjetas de estadísticas
     */
    updateStats(stats) {
        const container = document.getElementById('chartStatsContainer');

        if (!stats || Object.keys(stats).length === 0) {
            container.innerHTML = '';
            return;
        }

        let statsHTML = '';
        for (const [label, value] of Object.entries(stats)) {
            statsHTML += `
                <div class="chart-stat-card">
                    <div class="chart-stat-label">${label}</div>
                    <div class="chart-stat-value">${value}</div>
                </div>
            `;
        }

        container.innerHTML = statsHTML;
    }

    /**
     * Renderiza el gráfico ampliado (Chart.js)
     */
    renderExpandedChart(chartConfig) {
        const canvas = document.getElementById('chartExpandedCanvas');
        const ctx = canvas.getContext('2d');

        // Destruir gráfico anterior si existe
        if (this.currentChart) {
            this.currentChart.destroy();
        }

        // Configuración mejorada para el gráfico ampliado
        const expandedConfig = JSON.parse(JSON.stringify(chartConfig.config)); // Deep clone

        // Mejoras para el gráfico ampliado
        if (expandedConfig.options) {
            // Aumentar tamaño de fuentes
            if (!expandedConfig.options.plugins) expandedConfig.options.plugins = {};
            if (!expandedConfig.options.plugins.legend) expandedConfig.options.plugins.legend = {};
            expandedConfig.options.plugins.legend.labels = {
                ...expandedConfig.options.plugins.legend.labels,
                font: { size: 14 }
            };

            // Mejorar tooltips
            if (!expandedConfig.options.plugins.tooltip) expandedConfig.options.plugins.tooltip = {};
            expandedConfig.options.plugins.tooltip = {
                ...expandedConfig.options.plugins.tooltip,
                enabled: true,
                backgroundColor: 'rgba(0, 0, 0, 0.8)',
                titleFont: { size: 14, weight: 'bold' },
                bodyFont: { size: 13 },
                padding: 12,
                cornerRadius: 8
            };

            // Mejorar escalas
            if (expandedConfig.options.scales) {
                Object.keys(expandedConfig.options.scales).forEach(scaleKey => {
                    if (expandedConfig.options.scales[scaleKey].ticks) {
                        expandedConfig.options.scales[scaleKey].ticks.font = { size: 12 };
                    }
                });
            }

            // Hacer el gráfico responsive
            expandedConfig.options.responsive = true;
            expandedConfig.options.maintainAspectRatio = false;
        }

        // Usar los datos registrados
        expandedConfig.data = chartConfig.data;

        // Crear nuevo gráfico
        this.currentChart = new Chart(ctx, expandedConfig);
    }

    /**
     * Renderiza un gráfico Canvas personalizado ampliado
     */
    renderExpandedCanvasChart(canvasConfig) {
        const canvas = document.getElementById('chartExpandedCanvas');

        console.log('🎨 Renderizando gráfico canvas ampliado...', canvasConfig);

        // PASO 1: Resetear completamente el canvas antes de obtener contexto
        // Esto elimina cualquier contenido previo y estado del canvas
        canvas.width = 1;
        canvas.height = 1;

        // PASO 2: Usar requestAnimationFrame para asegurar que el DOM está listo
        // y que el reseteo se ha completado antes de redibujar
        requestAnimationFrame(() => {
            console.log('🔄 Preparando canvas en el siguiente frame...');

            // PASO 3: Obtener contexto DESPUÉS del reseteo
            const ctx = canvas.getContext('2d', {
                alpha: false,
                desynchronized: false,
                willReadFrequently: false
            });

            // PASO 4: Ejecutar la función de dibujo personalizada
            // La función se encargará de configurar el tamaño correcto y dibujar
            if (typeof canvasConfig.drawFunction === 'function') {
                try {
                    canvasConfig.drawFunction(canvas, ctx, canvasConfig.data);
                    console.log('✅ Gráfico canvas renderizado exitosamente');
                } catch (error) {
                    console.error('❌ Error al renderizar gráfico:', error);

                    // Mostrar mensaje de error en el canvas
                    canvas.width = 800;
                    canvas.height = 400;
                    ctx.fillStyle = '#ffffff';
                    ctx.fillRect(0, 0, canvas.width, canvas.height);
                    ctx.fillStyle = '#dc3545';
                    ctx.font = '16px Arial';
                    ctx.textAlign = 'center';
                    ctx.textBaseline = 'middle';
                    ctx.fillText('Error al renderizar el gráfico', canvas.width / 2, canvas.height / 2);
                    ctx.fillStyle = '#6c757d';
                    ctx.font = '12px Arial';
                    ctx.fillText(error.message, canvas.width / 2, canvas.height / 2 + 25);
                }
            } else {
                console.error('❌ La función de dibujo no es válida');

                canvas.width = 800;
                canvas.height = 400;
                ctx.fillStyle = '#ffffff';
                ctx.fillRect(0, 0, canvas.width, canvas.height);
                ctx.fillStyle = '#dc3545';
                ctx.font = '16px Arial';
                ctx.textAlign = 'center';
                ctx.textBaseline = 'middle';
                ctx.fillText('Error: Función de dibujo no válida', canvas.width / 2, canvas.height / 2);
            }
        });
    }

    /**
     * Descarga el gráfico como imagen PNG
     */
    downloadChart() {
        const canvas = document.getElementById('chartExpandedCanvas');

        if (!canvas) {
            console.error('No se encontró el canvas del gráfico');
            return;
        }

        // Verificar que el canvas tiene contenido
        if (canvas.width === 0 || canvas.height === 0) {
            console.error('El canvas no tiene contenido para descargar');
            return;
        }

        try {
            // Convertir canvas a imagen PNG
            const url = canvas.toDataURL('image/png', 1.0);

            // Crear link de descarga
            const link = document.createElement('a');
            const title = document.getElementById('chartModalTitle').textContent;
            const fileName = `${title.replace(/\s+/g, '_')}_${new Date().toISOString().slice(0,10)}.png`;

            link.download = fileName;
            link.href = url;

            // Disparar descarga
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);

            console.log('✅ Gráfico descargado:', fileName);
        } catch (error) {
            console.error('❌ Error al descargar gráfico:', error);
            alert('Error al descargar el gráfico. Por favor, intente nuevamente.');
        }
    }

    /**
     * Método helper para hacer un canvas clickeable
     */
    static makeChartClickable(canvasId, chartId) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) return;

        const container = canvas.closest('.chart-container-clickable') || canvas.parentElement;
        container.style.cursor = 'pointer';

        container.addEventListener('click', (e) => {
            // No hacer nada si se hizo click en el botón de expandir
            if (e.target.closest('.chart-expand-btn')) return;

            if (window.chartModalInstance) {
                window.chartModalInstance.openModal(chartId);
            }
        });
    }

    /**
     * Método helper para agregar botón de expandir
     */
    static addExpandButton(canvasId, chartId) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) return;

        const card = canvas.closest('.rentabilidad-card, .card');
        if (!card) return;

        // Asegurar que la tarjeta tenga position relative
        card.style.position = 'relative';

        // Crear botón si no existe
        if (!card.querySelector('.chart-expand-btn')) {
            const button = document.createElement('button');
            button.className = 'chart-expand-btn';
            button.innerHTML = '<i class="fas fa-expand-alt"></i>';
            button.title = 'Ver gráfico ampliado';

            button.addEventListener('click', (e) => {
                e.stopPropagation();
                if (window.chartModalInstance) {
                    window.chartModalInstance.openModal(chartId);
                }
            });

            card.appendChild(button);
        }
    }
}

// Inicializar instancia global cuando el DOM esté listo
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        window.chartModalInstance = new ChartModal();
    });
} else {
    window.chartModalInstance = new ChartModal();
}

// Exportar para uso en módulos (opcional)
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ChartModal;
}
