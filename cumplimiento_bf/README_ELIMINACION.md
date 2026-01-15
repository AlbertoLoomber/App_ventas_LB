# Módulo Cumplimiento BF - Guía de Eliminación

## Descripción
Este módulo proporciona funcionalidad para el seguimiento de metas de SKUs específicos durante eventos como Black Friday.

**IMPORTANTE:** Este módulo está completamente autocontenido y puede ser eliminado sin afectar el resto del código base.

---

## Cómo Eliminar Este Módulo

Para eliminar completamente este módulo de la aplicación, sigue estos pasos en orden:

### Paso 1: Eliminar la carpeta del módulo
```bash
# Eliminar toda la carpeta cumplimiento_bf
rm -rf cumplimiento_bf
```

### Paso 2: Eliminar el registro del blueprint en app.py
Abre el archivo `app.py` y **elimina** las siguientes líneas (aproximadamente líneas 88-95):

```python
# 7. Módulo de Cumplimiento BF (Black Friday - SKUs)
# MÓDULO REMOVIBLE: Puede ser eliminado sin afectar el código base
try:
    from cumplimiento_bf import bp as cumplimiento_bf_bp
    app.register_blueprint(cumplimiento_bf_bp)
    print("✅ Cumplimiento BF registrado en /cumplimiento-bf, /cumplimiento-bf-ajax")
except Exception as e:
    print(f"❌ Error registrando cumplimiento_bf: {e}")
```

### Paso 3: Eliminar la opción del menú en navbar.html
Abre el archivo `templates/navbar.html` y **elimina** las siguientes líneas (aproximadamente líneas 155-157):

```html
<li><a class="dropdown-item" href="/cumplimiento-bf" style="color: #6c757d; transition: all 0.2s ease;">
  <i class="bi bi-lightning-charge-fill me-2"></i>Cumplimiento BF</a></li>
<li><hr class="dropdown-divider"></li>
```

### Paso 4: Eliminar el template HTML (OPCIONAL)
Si deseas limpiar completamente, elimina también el archivo de template:

```bash
# Eliminar el archivo HTML del template
rm templates/cumplimiento_bf.html
```

### Paso 5: Verificar que la aplicación funciona
Reinicia la aplicación Flask para verificar que todo funciona correctamente:

```bash
python app.py
```

---

## Archivos Modificados

Al instalar este módulo, se modificaron los siguientes archivos del código base:

1. **app.py** (líneas 88-95): Registro del blueprint
2. **templates/navbar.html** (líneas 155-157): Opción en menú desplegable

Al eliminar el módulo, solo necesitas revertir estos cambios.

---

## Estructura del Módulo

```
cumplimiento_bf/
├── __init__.py           # Inicialización del módulo
├── blueprint.py          # Definición del blueprint Flask
├── routes.py             # Rutas y endpoints
├── services.py           # Lógica de negocio
└── README_ELIMINACION.md # Este archivo

templates/
└── cumplimiento_bf.html  # Template HTML de la vista
```

---

## Dependencias

Este módulo NO introduce nuevas dependencias externas. Utiliza las mismas librerías que el resto de la aplicación:
- Flask
- Pandas
- datetime
- Conexión a ClickHouse (a través de `database.py`)

## Tablas de Base de Datos

Este módulo utiliza las siguientes tablas de ClickHouse:

1. **Silver.catalogo_productos_BF** - Catálogo de productos BF con clasificación
   - Campos: `sku`, `descripcion`, `categoria`, `producto_relevante`, `producto_nuevo`, `remate`
   - Función en `database.py`: `get_catalogo_productos_bf()`

2. **Silver.RPT_Ventas_Con_Costo_Prueba** - Datos de ventas (tabla compartida con otros módulos)
   - El módulo filtra las ventas usando solo los SKUs del catálogo BF

**IMPORTANTE:** Al eliminar este módulo, también deberías eliminar la función `get_catalogo_productos_bf()` del archivo `database.py` (aproximadamente líneas 370-415).

---

## Funcionalidad

El módulo proporciona:
- Vista para seguimiento de metas de SKUs específicos
- Cálculo de cumplimiento por SKU
- Gráfico de tendencia de ventas acumuladas
- Tabla detallada con métricas por SKU
- Endpoint AJAX para actualización dinámica

---

## Soporte

Si tienes problemas al eliminar este módulo, contacta al equipo de desarrollo.

---

**Versión:** 1.0.0
**Fecha de creación:** 2025-11-07
**Estado:** Módulo removible - No afecta código base
