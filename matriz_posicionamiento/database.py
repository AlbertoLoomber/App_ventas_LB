"""
Database Manager for Matriz de Posicionamiento
Abstracción para acceso a ClickHouse con fallback al módulo principal
"""

import pandas as pd
from datetime import datetime

# ============================================================================
# CONSTANTES
# ============================================================================

# Canales oficiales para clasificación
CANALES_CLASIFICACION = [
    'CrediTienda',
    'Yuhu',
    'Walmart',
    'Mercado Libre',
    'Shein',
    'Liverpool',
    'Aliexpress',
    'Coppel',
    'TikTok Shop',
    'Temu'
]


class MatrizDatabaseManager:
    """
    Database manager for Matriz de Posicionamiento
    Gestiona la conexión a ClickHouse y carga de datos
    """

    def __init__(self):
        self.connection_type = 'clickhouse'
        self.client = None
        self._test_hub_database()

    def _test_hub_database(self):
        """Intenta usar el módulo de database del hub"""
        try:
            # Intentar importar desde el Hub
            from database import get_clickhouse_client
            self.get_client = get_clickhouse_client
            self.use_hub_database = True
            print("✅ [DATABASE] Using Hub database module")
        except ImportError:
            # Fallback: usar función del app.py
            self.use_hub_database = False
            print("⚠️  [DATABASE] Hub database not available, using app.py fallback")

    def get_connection(self):
        """
        Obtiene conexión a ClickHouse
        Intenta usar el hub primero, luego fallback a app.py
        """
        if self.use_hub_database:
            try:
                from database import get_clickhouse_client
                client = get_clickhouse_client()
                if client:
                    return client
            except Exception as e:
                print(f"⚠️  [DATABASE] Hub connection failed: {e}")

        # Fallback: usar database module (app modular)
        try:
            from database import get_db_connection
            return get_db_connection()
        except ImportError:
            print("❌ [DATABASE] No database connection available")
            return None

    def execute_query(self, query):
        """
        Ejecuta query y retorna DataFrame

        Args:
            query (str): Query SQL a ejecutar

        Returns:
            pd.DataFrame: Resultados de la query
        """
        try:
            client = self.get_connection()
            if not client:
                print("❌ [DATABASE] No client available")
                return pd.DataFrame()

            # Ejecutar query
            result = client.query(query)
            df = pd.DataFrame(result.result_rows, columns=result.column_names)

            print(f"✅ [DATABASE] Query executed: {len(df)} rows returned")
            return df

        except Exception as e:
            print(f"❌ [DATABASE] Query failed: {e}")
            return pd.DataFrame()
        finally:
            # Cerrar conexión si existe el método
            if client and hasattr(client, 'close'):
                try:
                    client.close()
                except:
                    pass

    def cargar_acumulado_mensual(self):
        """
        Carga datos acumulados mensuales
        Intenta usar app.py primero, si falla usa query directa

        Returns:
            tuple: (df, channels, warehouses)
        """
        # OPCIÓN 1: Intentar usar la función del app.py (si existe)
        try:
            from app import cargar_acumulado_mensual_matriz
            df, channels, warehouses = cargar_acumulado_mensual_matriz()
            print(f"✅ [DATABASE] Loaded {len(df)} records from app.py")
            return df, channels, warehouses
        except ImportError:
            print("⚠️  [DATABASE] app.py not available, using direct query")
        except Exception as e:
            print(f"⚠️  [DATABASE] Error from app.py: {e}, using direct query")

        # OPCIÓN 2: Query directa a ClickHouse (FALLBACK AUTOSUFICIENTE)
        try:
            from datetime import datetime
            import time

            print(f"🔍 [DATABASE] Loading data via direct ClickHouse query...")

            client = self.get_connection()
            if not client:
                print("❌ [DATABASE] No client available")
                return pd.DataFrame(), [], []

            # Query a la vista acumulada
            query = """
            SELECT
                Fecha,
                sku,
                Descripcion,
                Marca,
                Categoria,
                Channel,
                Cantidad,
                Ventas AS Total,
                Costo AS "Costo de venta",
                Comision,
                Gastos_Destino,
                Ultima_milla,
                Gastos_Directos AS Gastos_directos,
                Ingreso_real AS "Ingreso real",
                Costo_Pct,
                Gastos_Directos_Pct,
                Ingreso_Real_Pct,
                ROI_Pct,
                Ordenes,
                Clasificacion,
                'Activo' AS estado
            FROM Silver.RPT_Ventas_Acumulado_Mensual_SKU_Canal_MT
            WHERE toYear(Fecha) = toYear(today())
            ORDER BY Fecha DESC, sku ASC
            """

            result = client.query(query)

            if not result.result_rows:
                print("⚠️  [DATABASE] No data found")
                return pd.DataFrame(), [], []

            # Convertir a DataFrame
            df = pd.DataFrame(result.result_rows, columns=result.column_names)
            df['Fecha'] = pd.to_datetime(df['Fecha'])

            # Obtener listas
            channels_disponibles = sorted(df['Channel'].unique().tolist())
            warehouses_disponibles = []  # No disponible en esta vista

            print(f"✅ [DATABASE] Loaded {len(df):,} records via direct query")
            print(f"   Channels: {len(channels_disponibles)}")

            return df, channels_disponibles, warehouses_disponibles

        except Exception as e:
            print(f"❌ [DATABASE] Error loading data: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame(), [], []

    def test_connection(self):
        """
        Test database connection

        Returns:
            bool: True si la conexión funciona
        """
        try:
            query = "SELECT 1 as test"
            result = self.execute_query(query)
            return not result.empty
        except:
            return False


# ============================================================================
# GLOBAL INSTANCE
# ============================================================================

db_manager = MatrizDatabaseManager()

print(f"✅ [DATABASE] MatrizDatabaseManager initialized")
print(f"   - Connection type: {db_manager.connection_type}")
print(f"   - Using Hub database: {db_manager.use_hub_database}")
