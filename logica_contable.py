"""Motor contable: transforma documentos de compras en asientos contables."""

from __future__ import annotations

from typing import Any, Dict

from sqlalchemy.exc import IntegrityError

from db_config import (
    SessionLocal,
    TblAsientos,
    TblDocumentos,
    TblMovimientosContables,
    TblPlanCuentas,
    TblProveedores,
)


def _obtener_cuenta_por_nombre(session, nombre: str) -> TblPlanCuentas:
    cuenta = session.query(TblPlanCuentas).filter(TblPlanCuentas.nombre == nombre).first()
    if not cuenta:
        raise ValueError(f"No existe la cuenta obligatoria: {nombre}")
    return cuenta


def generar_asiento(documento: Dict[str, Any]) -> Dict[str, Any]:
    """Genera asiento contable de compras con partida doble.

    Reglas:
    1) Proveedor con cuenta default -> usarla; si no, Gastos Generales (Por Clasificar).
    2) Debe: gasto neto + IVA crédito fiscal.
    3) Haber: proveedores por pagar por el total.

    La operación se ejecuta en una sola transacción para garantizar atomicidad:
    o se escriben documento/asiento/movimientos completos o no se escribe nada.
    """
    with SessionLocal.begin() as session:
        proveedor = session.get(TblProveedores, documento["rut_emisor"])

        cuenta_gastos_default = _obtener_cuenta_por_nombre(session, "Gastos Generales (Por Clasificar)")
        cuenta_iva = _obtener_cuenta_por_nombre(session, "IVA Crédito Fiscal")
        cuenta_proveedores = _obtener_cuenta_por_nombre(session, "Proveedores por Pagar")

        if proveedor is None:
            proveedor = TblProveedores(
                rut=documento["rut_emisor"],
                razon_social=documento.get("razon_social", "Proveedor sin nombre"),
                cuenta_contable_default_id=cuenta_gastos_default.id_cuenta,
            )
            session.add(proveedor)
            session.flush()

        id_cuenta_gasto = proveedor.cuenta_contable_default_id or cuenta_gastos_default.id_cuenta

        doc_db = TblDocumentos(**documento)
        session.add(doc_db)
        session.flush()

        asiento = TblAsientos(id_documento=doc_db.id, fecha=documento["fecha_emision"])
        session.add(asiento)
        session.flush()

        glosa_base = f"Compra DTE {documento['tipo_dte']} Folio {documento['folio']} - {proveedor.razon_social}"
        movimientos = [
            TblMovimientosContables(
                id_asiento=asiento.id,
                id_cuenta=id_cuenta_gasto,
                debe=documento["monto_neto"],
                haber=0.0,
                glosa=f"{glosa_base} | Gasto Neto",
            ),
            TblMovimientosContables(
                id_asiento=asiento.id,
                id_cuenta=cuenta_iva.id_cuenta,
                debe=documento["monto_iva"],
                haber=0.0,
                glosa=f"{glosa_base} | IVA Crédito Fiscal",
            ),
            TblMovimientosContables(
                id_asiento=asiento.id,
                id_cuenta=cuenta_proveedores.id_cuenta,
                debe=0.0,
                haber=documento["monto_total"],
                glosa=f"{glosa_base} | Proveedores por Pagar",
            ),
        ]
        session.add_all(movimientos)

        return {"status": "ok", "documento_id": doc_db.id, "asiento_id": asiento.id}


def procesar_documento_con_control_duplicado(documento: Dict[str, Any]) -> Dict[str, Any]:
    """Wrapper de negocio para detectar duplicados según constraint único."""
    try:
        return generar_asiento(documento)
    except IntegrityError:
        return {"status": "duplicado", "motivo": "Documento ya existe (folio, rut, tipo_dte)"}
