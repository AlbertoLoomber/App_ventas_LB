"""
Estrategias de cálculo para diferentes tipos de meta.
Implementa el patrón Strategy para manejar los diferentes tipos de cálculos.
"""

class MetaStrategy:
    """
    Clase base abstracta para estrategias de cálculo de metas.
    Define la interfaz que todas las estrategias deben implementar.
    """

    def calcular_cumplimiento(self, valor_real, meta_periodo=None):
        """
        Calcula el porcentaje de cumplimiento.

        Args:
            valor_real: Valor real obtenido
            meta_periodo: Meta del período (puede ser None para estrategias de rango)

        Returns:
            float: Porcentaje de cumplimiento
        """
        raise NotImplementedError("Las subclases deben implementar calcular_cumplimiento()")

    def calcular_diferencia(self, valor_real, meta_periodo=None):
        """
        Calcula la diferencia respecto a la meta.

        Args:
            valor_real: Valor real obtenido
            meta_periodo: Meta del período (puede ser None para estrategias de rango)

        Returns:
            float: Diferencia calculada
        """
        raise NotImplementedError("Las subclases deben implementar calcular_diferencia()")

    def preparar_meta_display(self, meta_periodo=None):
        """
        Prepara el texto de la meta para mostrar en la UI.

        Args:
            meta_periodo: Meta del período (puede ser None para estrategias de rango)

        Returns:
            str: Texto formateado de la meta
        """
        raise NotImplementedError("Las subclases deben implementar preparar_meta_display()")

    def preparar_valor_display(self, valor_real):
        """
        Prepara el texto del valor real para mostrar en la UI.

        Args:
            valor_real: Valor real obtenido

        Returns:
            str: Texto formateado del valor real
        """
        raise NotImplementedError("Las subclases deben implementar preparar_valor_display()")


class MetaAbsolutaStrategy(MetaStrategy):
    """
    Estrategia para metas de valores absolutos (ventas, ingreso_real_nominal).
    Calcula cumplimiento como porcentaje: (real / meta) * 100
    """

    def calcular_cumplimiento(self, valor_real, meta_periodo=None):
        """
        Calcula cumplimiento como porcentaje del valor real vs meta.

        Args:
            valor_real (float): Valor real obtenido
            meta_periodo (float): Meta del período

        Returns:
            float: Porcentaje de cumplimiento (0 si meta es 0)
        """
        if meta_periodo is None or meta_periodo == 0:
            return 0.0
        return round((valor_real / meta_periodo * 100), 2)

    def calcular_diferencia(self, valor_real, meta_periodo=None):
        """
        Calcula diferencia absoluta: real - meta.

        Args:
            valor_real (float): Valor real obtenido
            meta_periodo (float): Meta del período

        Returns:
            float: Diferencia en las mismas unidades (positivo = superó, negativo = no alcanzó)
        """
        if meta_periodo is None:
            return 0.0
        return round(valor_real - meta_periodo, 2)

    def preparar_meta_display(self, meta_periodo=None):
        """
        Formatea la meta como moneda.

        Args:
            meta_periodo (float): Meta del período

        Returns:
            str: Meta formateada como "$XXX,XXX"
        """
        if meta_periodo is None or meta_periodo == 0:
            return "$0"
        return f"${meta_periodo:,.0f}"

    def preparar_valor_display(self, valor_real):
        """
        Formatea el valor real como moneda.

        Args:
            valor_real (float): Valor real obtenido

        Returns:
            str: Valor formateado como "$XXX,XXX"
        """
        return f"${valor_real:,.0f}"


class MetaRangoStrategy(MetaStrategy):
    """
    Estrategia para metas de rangos (costo, ingreso_real %).
    El valor actual es el cumplimiento, y la diferencia es relativa al rango objetivo.
    """

    def __init__(self, rango_min, rango_max):
        """
        Inicializa la estrategia con el rango objetivo.

        Args:
            rango_min (float): Límite inferior del rango objetivo
            rango_max (float): Límite superior del rango objetivo
        """
        self.rango_min = rango_min
        self.rango_max = rango_max
        self.centro_rango = (rango_min + rango_max) / 2

    def calcular_cumplimiento(self, valor_actual, meta_periodo=None):
        """
        Para rangos, el cumplimiento es el valor actual (ya es un porcentaje).

        Args:
            valor_actual (float): Porcentaje actual
            meta_periodo: Ignorado para esta estrategia

        Returns:
            float: El valor actual redondeado
        """
        return round(valor_actual, 2)

    def calcular_diferencia(self, valor_actual, meta_periodo=None):
        """
        Calcula diferencia respecto al rango objetivo.
        - Si está dentro del rango: 0
        - Si está por debajo: negativo (distancia al límite inferior)
        - Si está por arriba: positivo (distancia al límite superior)

        Args:
            valor_actual (float): Porcentaje actual
            meta_periodo: Ignorado para esta estrategia

        Returns:
            float: Diferencia en puntos porcentuales
        """
        if valor_actual < self.rango_min:
            return round(valor_actual - self.rango_min, 2)  # Negativo
        elif valor_actual > self.rango_max:
            return round(valor_actual - self.rango_max, 2)  # Positivo
        else:
            return 0.0  # Dentro del rango objetivo

    def preparar_meta_display(self, meta_periodo=None):
        """
        Formatea la meta como rango de porcentajes.

        Args:
            meta_periodo: Ignorado para esta estrategia

        Returns:
            str: Rango formateado como "XX% - YY%"
        """
        return f"{self.rango_min}% - {self.rango_max}%"

    def preparar_valor_display(self, valor_actual):
        """
        Formatea el valor actual como porcentaje.

        Args:
            valor_actual (float): Porcentaje actual

        Returns:
            str: Valor formateado como "XX.X%"
        """
        return f"{valor_actual:.1f}%"


def get_strategy(tipo_meta):
    """
    Factory function para obtener la estrategia correcta según el tipo de meta.

    Args:
        tipo_meta (str): Tipo de meta ('ventas', 'costo', 'ingreso_real', 'ingreso_real_nominal')

    Returns:
        MetaStrategy: Instancia de la estrategia correspondiente

    Raises:
        ValueError: Si el tipo de meta no es reconocido
    """
    if tipo_meta in ['ventas', 'ingreso_real_nominal']:
        return MetaAbsolutaStrategy()
    elif tipo_meta == 'costo':
        return MetaRangoStrategy(48, 54)
    elif tipo_meta == 'ingreso_real':
        return MetaRangoStrategy(10, 15)
    else:
        raise ValueError(f"Tipo de meta desconocido: '{tipo_meta}'")
