"""ETL de XML DTE chileno hacia estructura homogénea para persistencia."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

import xmltodict


def _buscar_nodo_recursivo(data: Any, clave_objetivo: str) -> Any:
    """Busca una clave anidada de forma robusta para soportar variaciones del XML DTE."""
    if isinstance(data, dict):
        for key, value in data.items():
            if key == clave_objetivo:
                return value
            found = _buscar_nodo_recursivo(value, clave_objetivo)
            if found is not None:
                return found
    elif isinstance(data, list):
        for item in data:
            found = _buscar_nodo_recursivo(item, clave_objetivo)
            if found is not None:
                return found
    return None


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def parsear_dte_xml(xml_bytes: bytes, nombre_archivo: str = "sin_nombre.xml") -> Dict[str, Any]:
    """Parsea un XML DTE y devuelve un diccionario estándar de documento.

    Se encapsula en try/except porque en escenarios reales llegan XML truncados o con namespaces
    no homogéneos; devolver un error manejado evita cortar toda la carga masiva.
    """
    try:
        data = xmltodict.parse(xml_bytes)

        encabezado = _buscar_nodo_recursivo(data, "Encabezado")
        if not encabezado:
            raise ValueError("No se encontró nodo 'Encabezado' en el XML")

        id_doc = encabezado.get("IdDoc", {})
        emisor = encabezado.get("Emisor", {})
        totales = encabezado.get("Totales", {})

        fecha = id_doc.get("FchEmis")
        fecha_emision = datetime.strptime(fecha, "%Y-%m-%d").date()

        return {
            "folio": str(id_doc.get("Folio", "")).strip(),
            "tipo_dte": str(id_doc.get("TipoDTE", "")).strip(),
            "fecha_emision": fecha_emision,
            "rut_emisor": str(emisor.get("RUTEmisor", "")).strip(),
            "razon_social": str(emisor.get("RznSoc", "Proveedor sin nombre")).strip(),
            "monto_neto": _safe_float(totales.get("MntNeto")),
            "monto_iva": _safe_float(totales.get("IVA")),
            "monto_total": _safe_float(totales.get("MntTotal")),
            "url_archivo": nombre_archivo,
        }
    except Exception as exc:
        raise ValueError(f"No se pudo procesar {nombre_archivo}: {exc}") from exc
