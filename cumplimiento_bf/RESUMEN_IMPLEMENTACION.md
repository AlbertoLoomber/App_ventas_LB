# Módulo Cumplimiento BF - Resumen de Implementación

## Descripción General

Módulo completamente modular y autocontenido para el seguimiento de metas de SKUs específicos durante eventos como Black Friday. Conectado directamente a las tablas de ClickHouse para obtener datos en tiempo real.

---

## Características Principales

### 1. Conexión a Base de Datos

El módulo se conecta a dos tablas de ClickHouse:

#### Tabla 1: `Silver.catalogo_productos_BF`
```sql
-- Catálogo de productos BF con clasificación
Campos:
- sku (String)
- descripcion (String)
- categoria (String)
- producto_relevante (UInt8) - 1 si es producto relevante, 0 si no
- producto_nuevo (UInt8) - 1 si es producto nuevo, 0 si no
- remate (UInt8) - 1 si está en remate, 0 si no
- fecha_carga (DateTime)
```

**Uso:** Esta tabla define qué SKUs son parte del seguimiento BF y su clasificación

#### Tabla 2: `Silver.RPT_Ventas_Con_Costo_Prueba`
```sql
-- Tabla de ventas (compartida con otros módulos)
Campos utilizados:
- sku (String)
- descripcion (String)
- cantidad (Int32)
- Fecha (DateTime64)
- Total (Float64)
- Costo de venta (Float64)
- Ingreso real (Float64)
- estado (String)
- Channel, Warehouse, etc.
```

**Uso:** Filtrada para obtener solo ventas de SKUs que están en el catálogo BF

### 2. Filtros Disponibles

#### Filtro por Tipo de Producto:
- **Todos los productos**: Muestra todos los SKUs del catálogo BF
- **Solo relevantes**: Filtra `producto_relevante = 1`
- **Solo nuevos**: Filtra `producto_nuevo = 1`
- **Solo remates**: Filtra `remate = 1`

#### Filtro por Canal:
Solo se muestran ventas de los siguientes canales BF:
- Mercado Libre
- CrediTienda
- Walmart
- Shein
- Yuhu
- Liverpool
- AliExpress

El filtro permite seleccionar:
- **Todos los canales**: Muestra ventas de todos los canales BF
- **Canal específico**: Filtra solo ventas del canal seleccionado

#### Filtro por Período:
- Hoy
- Últimos 7 días
- Mes actual hasta hoy

### 3. Métricas Calculadas

Para cada SKU:
- **Ventas Totales**: Suma del campo `Total`
- **Cantidad Vendida**: Suma del campo `cantidad`
- **Ingreso Real**: Suma del campo `Ingreso real`
- **Costo de Venta**: Suma del campo `Costo de venta`
- **Margen**: Ventas - Costo
- **Margen %**: (Margen / Ventas) * 100

Resumen General:
- Total de SKUs BF
- Ventas Totales
- Ingreso Real Total
- Margen Total y Porcentaje

### 4. Visualización

- **Tarjetas de Resumen**: 4 tarjetas con métricas clave
- **Gráfico de Tendencia**: Ventas acumuladas por día (Chart.js)
- **Tabla Detallada**:
  - SKU
  - Descripción
  - Categoría
  - Tipo (badges: Relevante, Nuevo, Remate)
  - Ventas
  - Cantidad
  - Ingreso Real
  - Margen (valor y porcentaje)

---

## Arquitectura del Módulo

### Archivos Creados

```
cumplimiento_bf/
├── __init__.py                    # Inicialización del módulo
├── blueprint.py                   # Blueprint Flask
├── routes.py                      # Rutas y endpoints
├── services.py                    # Lógica de negocio
├── README_ELIMINACION.md          # Guía de eliminación
└── RESUMEN_IMPLEMENTACION.md      # Este archivo

templates/
└── cumplimiento_bf.html           # Template HTML

database.py
└── get_catalogo_productos_bf()    # Función agregada (líneas 370-415)
```

### Archivos Modificados

1. **app.py** (líneas 88-95)
   - Registro del blueprint `cumplimiento_bf`
   - Comentado como "MÓDULO REMOVIBLE"

2. **templates/navbar.html** (líneas 155-157)
   - Agregada opción "Cumplimiento BF" en menú desplegable "Más"
   - Icono: Lightning charge (⚡)

3. **database.py** (líneas 370-415)
   - Agregada función `get_catalogo_productos_bf()`
   - Consulta a tabla `Silver.catalogo_productos_BF`

---

## Flujo de Datos

```
1. Usuario accede a "Más" → "Cumplimiento BF"
   ↓
2. Route /cumplimiento-bf se ejecuta
   ↓
3. get_fresh_data() carga ventas del mes
   ↓
4. get_catalogo_productos_bf() carga catálogo BF
   ↓
5. obtener_skus_bf(filtro_tipo) filtra SKUs según tipo
   ↓
6. calcular_cumplimiento_skus() procesa ventas
   - Filtra ventas por canales BF (Mercado Libre, CrediTienda, etc.)
   - Filtra por canal específico si se seleccionó
   - Filtra ventas por SKUs BF
   - Agrupa por SKU
   - Calcula métricas (Ventas, Ingreso Real, Margen)
   - Combina con catálogo para info adicional (descripción, categoría, badges)
   ↓
7. obtener_grafico_cumplimiento_diario() genera datos gráfico
   - Aplica mismos filtros de canal y tipo
   - Agrupa por día
   - Calcula acumulados
   ↓
8. Render template con datos procesados
```

---

## Endpoints

### GET/POST `/cumplimiento-bf`
Vista principal del módulo

**Parámetros (POST):**
- `preset`: Período de análisis (hoy, 7, mes_actual)
- `filtro_tipo`: Tipo de producto (todos, relevante, nuevo, remate)
- `filtro_canal`: Canal específico o "todos"

**Parámetros (GET):**
- `mes_filtro`: Mes a consultar (default: mes actual)

**Returns:** HTML renderizado con datos de cumplimiento

**Variables en template:**
- `skus_data`: Lista de SKUs con métricas
- `resumen_general`: Totales generales
- `grafico_data`: Datos para Chart.js
- `canales_bf`: Lista de canales disponibles
- `filtro_tipo`: Filtro actual de tipo
- `filtro_canal`: Filtro actual de canal

### POST `/cumplimiento-bf-ajax`
Endpoint AJAX para actualización dinámica

**Parámetros:**
- `preset`: Período de análisis
- `filtro_tipo`: Tipo de producto
- `filtro_canal`: Canal específico
- `mes_filtro`: Mes seleccionado

**Returns:** JSON con:
```json
{
  "success": true,
  "skus_data": [
    {
      "sku": "SKU123",
      "descripcion": "Producto ejemplo",
      "categoria": "Categoría A",
      "Ventas_Reales": 15000.50,
      "Cantidad_Vendida": 25,
      "Ingreso_Real": 12000.00,
      "Margen": 3000.50,
      "Margen_Porcentaje": 20.0,
      "Es_Relevante": true,
      "Es_Nuevo": false,
      "Es_Remate": false
    }
  ],
  "resumen_general": {
    "total_skus": 50,
    "total_real": 500000.00,
    "total_cantidad": 1200,
    "total_ingreso_real": 400000.00,
    "margen_total": 100000.00,
    "margen_porcentaje": 20.0
  },
  "grafico_data": {
    "fechas": ["2025-11-01", "2025-11-02"],
    "ventas": [10000, 25000],
    "metas": []
  }
}
```

---

## Queries SQL Ejecutadas

### Query 1: Catálogo BF
```sql
SELECT
    sku,
    descripcion,
    categoria,
    producto_relevante,
    producto_nuevo,
    remate,
    fecha_carga
FROM Silver.catalogo_productos_BF
ORDER BY categoria, sku
```

### Query 2: Ventas (heredada de get_fresh_data)
```sql
SELECT * FROM Silver.RPT_Ventas_Con_Costo_Prueba
WHERE toYear(Fecha) = [año]
AND toMonth(Fecha) = [mes]
ORDER BY Fecha DESC
```

Luego filtrada en Python por:
- `estado != "Cancelado"`
- `sku IN (skus_del_catalogo_bf)`
- Rango de fechas según preset

---

## Rendimiento

### Optimizaciones Implementadas

1. **Carga de catálogo una sola vez** por request
2. **Filtrado eficiente** usando pandas y listas de SKUs
3. **Agrupación en memoria** con pandas groupby
4. **Sin joins en SQL**, todo procesado en Python
5. **Template pre-renderizado** para gráficos

### Volumen de Datos Esperado

- Catálogo BF: ~100-1,000 SKUs típicamente
- Ventas filtradas: Depende del mes, típicamente 1,000-10,000 registros
- Tiempo de respuesta: ~2-5 segundos (dependiendo de la conexión a ClickHouse)

---

## Diseño Visual

### Colores Principales

- **Dorado Loomber**: #D4AF37 (Ventas, botones principales)
- **Verde**: #28a745 (Ingreso Real, margen alto)
- **Amarillo**: #ffc107 (Advertencias, margen medio)
- **Rojo**: #dc3545 (Errores, margen bajo)
- **Gris**: #6c757d (Textos secundarios)

### Badges de Tipo

- **Relevante**: Púrpura (#6f42c1)
- **Nuevo**: Cian (#0dcaf0)
- **Remate**: Naranja (#fd7e14)

---

## Casos de Uso

### Caso 1: Seguimiento General BF
```
1. Usuario accede sin filtros
2. Ve todos los SKUs BF de todos los canales
3. Identifica productos con mejor desempeño
4. Analiza margen por categoría
```

### Caso 2: Enfoque en Productos Nuevos
```
1. Usuario selecciona filtro "Solo nuevos"
2. Sistema filtra por producto_nuevo = 1
3. Usuario compara ventas de productos nuevos
4. Identifica cuáles tienen mejor aceptación
```

### Caso 3: Análisis de Remates
```
1. Usuario selecciona filtro "Solo remates"
2. Sistema muestra solo productos en remate
3. Usuario verifica margen de remates
4. Identifica oportunidades de clearance
```

### Caso 4: Análisis por Canal Específico
```
1. Usuario selecciona "Mercado Libre" en filtro de canal
2. Sistema filtra solo ventas de Mercado Libre
3. Usuario ve performance de SKUs BF en ese canal
4. Identifica productos más vendidos en Mercado Libre
```

### Caso 5: Productos Relevantes en Walmart
```
1. Usuario selecciona "Solo relevantes" + "Walmart"
2. Sistema filtra productos relevantes vendidos en Walmart
3. Usuario analiza desempeño de productos estratégicos
4. Compara margen vs otros canales
```

---

## Mantenimiento

### Actualizar Catálogo BF

Los datos se cargan directamente desde `Silver.catalogo_productos_BF`. Para actualizar:

1. Insertar/actualizar registros en la tabla
2. Recargar la página (los datos se cargan frescos en cada request)

### Agregar Nuevas Métricas

1. Modificar `calcular_cumplimiento_skus()` en `services.py`
2. Agregar campos al resumen_general
3. Actualizar template HTML para mostrar nueva métrica

### Agregar Nuevos Filtros

1. Agregar opción en template HTML (select filtro_tipo)
2. Modificar `obtener_skus_bf()` en `services.py` para manejar nuevo filtro
3. Actualizar lógica de filtrado en `calcular_cumplimiento_skus()`

---

## Troubleshooting

### Problema: No muestra datos
**Causa:** Catálogo BF vacío o sin ventas en el período
**Solución:** Verificar que `Silver.catalogo_productos_BF` tenga datos

### Problema: Ventas en $0
**Causa:** SKUs en catálogo no coinciden con SKUs en ventas
**Solución:** Verificar consistencia de SKUs entre tablas (mayúsculas, espacios)

### Problema: Error al cargar
**Causa:** Conexión a ClickHouse fallida
**Solución:** Verificar credenciales en `config.py`

---

## Versión

**1.0.0** - 2025-11-07

## Autor

Dashboard de Ventas Loomber - Módulo Cumplimiento BF

---

## Próximas Mejoras (Futuras)

- [ ] Agregar metas por SKU (tabla adicional en ClickHouse)
- [ ] Gráfico de cumplimiento vs meta
- [ ] Exportar a Excel con formato
- [ ] Comparación año anterior
- [ ] Alertas de bajo desempeño
- [ ] Dashboard de categorías
