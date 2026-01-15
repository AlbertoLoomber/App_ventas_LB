# 📊 Matriz de Posicionamiento - Módulo Hub-Compatible

## 📋 Descripción

Módulo Flask Blueprint para análisis y visualización del posicionamiento de canales de venta mediante gráficos de dispersión que comparan:
- **Eje X**: % Ingreso Real (0-50%)
- **Eje Y**: % ROI (0-100%)
- **Tamaño de burbuja**: Ventas Totales

**Compatible con el Hub** - Incluye fallbacks a `app.py` para integración inmediata.

## 📁 Estructura del Módulo (Hub-Compatible)

```
matriz_posicionamiento/
├── __init__.py                 # ✅ Punto de entrada del módulo
├── blueprint.py                # ✅ Definición del Blueprint
├── routes.py                   # ✅ Rutas/endpoints Flask
├── database.py                 # ✅ Manager de base de datos
├── services_legacy.py          # Lógica de negocio (legacy)
│
├── services/                   # ✅ Servicios modulares (wrapper)
│   ├── __init__.py             # Exportador central
│   ├── core.py                 # Funciones básicas
│   └── filters.py              # Funciones de filtrado
│
├── templates/                  # (En nivel superior)
│   └── matriz_posicionamiento/
│       ├── matriz.html
│       └── partials/
│
├── static/                     # (En nivel superior)
│   └── js/matriz_posicionamiento/
│       └── main.js
│
└── README.md                   # Esta documentación
```

## 🎯 Zonas de Clasificación

### 1. 🔴 Crítico
- **Rango**: IR 0-20%, ROI 0-40%
- **Significado**: Bajo ingreso + Bajo ROI
- **Acción**: Requiere atención urgente

### 2. 🔵 Eficiente
- **Rango**: IR 0-20%, ROI 40-100%
- **Significado**: Bajo ingreso pero buen ROI
- **Acción**: Potencial para aumentar volumen

### 3. 🟡 A Desarrollar
- **Rango**: IR 20-50%, ROI 0-40%
- **Significado**: Buen ingreso pero bajo ROI
- **Acción**: Optimizar costos y gastos

### 4. 🟢 Ideal
- **Rango**: IR 20-50%, ROI 40-100%
- **Significado**: Alto ingreso + Alto ROI
- **Acción**: Mantener y potenciar

## 🔌 Integración con el Hub

### Paso 1: Copiar el Módulo

```bash
cp -r matriz_posicionamiento/ hub/modules/
```

### Paso 2: Registrar en el Hub

En `hub/app.py`:

```python
# Importar el módulo
from modules.matriz_posicionamiento import bp as matriz_bp

# Registrar el blueprint
app.register_blueprint(matriz_bp)
```

### Paso 3: Acceder

```
http://tu-hub.com/matriz-posicionamiento/
```

## 🚀 Testing Local

```bash
cd ventas
python test_matriz_hub.py
```

Acceder a: `http://localhost:5000/matriz-posicionamiento/`

## 🔧 Uso

### Endpoint Principal
```
GET /matriz-posicionamiento/
```

Página principal con matriz de posicionamiento por canal.

### Endpoints AJAX

#### Actualizar Matriz por Canal
```
POST /matriz-posicionamiento/actualizar
Content-Type: application/json

{
    "mes": 202410,
    "marca": "Ambos"  // "Loomber", "Otros", "Ambos"
}
```

#### Actualizar Matriz por Categorías
```
POST /matriz-posicionamiento/actualizar-categorias
Content-Type: application/json

{
    "mes": 202410,
    "canales": ["Mercado Libre", "Walmart"],
    "categorias": ["Electrónicos", "Hogar"]
}
```

#### Actualizar Matriz de Clasificación
```
POST /matriz-posicionamiento/actualizar-clasificacion
Content-Type: application/json

{
    "mes": 202410,
    "canales": ["Mercado Libre"],
    "skus": ["2000005", "2000010"]
}
```

#### Obtener SKUs Disponibles
```
POST /matriz-posicionamiento/obtener-skus
Content-Type: application/json

{
    "mes": 202410
}
```

#### Comparar 3 Meses
```
POST /matriz-posicionamiento/comparar-3-meses
Content-Type: application/json

{
    "mes": "202410",
    "canales": ["Mercado Libre"],
    "skus": ["2000005"]
}
```

## 📊 Funciones Principales

### `clasificar_zona(ingreso_real_pct, roi_pct)`
Clasifica un punto en una de las 4 zonas.

**Args:**
- `ingreso_real_pct` (float): % Ingreso Real
- `roi_pct` (float): % ROI

**Returns:**
- `tuple`: (nombre_zona, color_fondo, color_texto, icono)

### `generar_datos_matriz(df, mes_filtro, canales_clasificacion)`
Genera datos formateados para Chart.js.

**Args:**
- `df` (DataFrame): Datos de ventas
- `mes_filtro` (int): Mes a filtrar (1-12)
- `canales_clasificacion` (list): Canales oficiales

**Returns:**
- `dict`: {datasets, canales, estadisticas}

## 🎨 Tecnologías Utilizadas

- **Backend**: Flask Blueprints, Pandas
- **Frontend**: Chart.js 3.9.1, Bootstrap 5
- **Visualización**: Gráfico de burbujas (scatter) con plugin personalizado

## 🗄️ Base de Datos

### Conexión

- **Tipo**: ClickHouse Cloud
- **Tabla**: `RPT_Ventas_Con_Costo`
- **Columnas requeridas**:
  - `Fecha`, `sku`, `Descripcion`, `Marca`, `Categoria`
  - `Channel`, `Warehouse`, `estado`
  - `Total`, `Costo de venta`, `Gastos_directos`, `Ingreso real`
  - `Clasificacion` (opcional)

### Manager de Base de Datos

El módulo incluye `database.py` que:
- ✅ Intenta usar el módulo `database` del Hub
- ✅ Hace fallback a `app.py` si no existe
- ✅ Maneja la carga de datos acumulados mensuales

## 🐛 Troubleshooting

### Error: "Cannot import bp"

Verifica que estés en el directorio correcto:
```bash
cd ventas
python test_matriz_hub.py
```

### Error: "cargar_acumulado_mensual_matriz not found"

El módulo busca esta función en `app.py`. Verifica que existe y está exportada.

## ✅ Checklist de Integración al Hub

- [x] Blueprint definido en `blueprint.py`
- [x] `__init__.py` con logging del Hub
- [x] `routes.py` con imports Hub-compatible
- [x] `database.py` manager con fallback
- [x] `services/` estructura modular
- [x] Imports con try/except para dependencias
- [x] Templates con `url_for` del blueprint
- [x] Test app creado (`test_matriz_hub.py`)
- [x] README.md con documentación completa
- [ ] Templates actualizados con `url_for` (pendiente validar)
- [ ] Testing completo local

## 📝 Notas

- **Hub-Compatible**: Funciona tanto en el Hub como standalone
- **Fallback Automático**: Si no encuentra módulos del Hub, usa `app.py`
- **Sin Emojis en Logs**: Para compatibilidad con diferentes consolas
- **Modular**: Fácil de refactorizar en el futuro
