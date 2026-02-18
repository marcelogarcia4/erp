"""Configuración de base de datos y modelos ORM para Contab-PY."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Optional

from sqlalchemy import (
    Date,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "contab_py.db"
PLAN_CUENTAS_CSV = BASE_DIR / "plan_cuentas.csv"


class Base(DeclarativeBase):
    """Clase base para los modelos ORM."""


class TblPlanCuentas(Base):
    __tablename__ = "Tbl_PlanCuentas"

    id_cuenta: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    codigo: Mapped[str] = mapped_column(String(20), unique=True, nullable=False, index=True)
    nombre: Mapped[str] = mapped_column(String(120), nullable=False)
    tipo: Mapped[str] = mapped_column(String(20), nullable=False)


class TblProveedores(Base):
    __tablename__ = "Tbl_Proveedores"

    rut: Mapped[str] = mapped_column(String(20), primary_key=True)
    razon_social: Mapped[str] = mapped_column(String(180), nullable=False)
    cuenta_contable_default_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("Tbl_PlanCuentas.id_cuenta"), nullable=True
    )

    cuenta_default: Mapped[Optional[TblPlanCuentas]] = relationship("TblPlanCuentas")


class TblDocumentos(Base):
    __tablename__ = "Tbl_Documentos"
    __table_args__ = (
        UniqueConstraint("folio", "rut_emisor", "tipo_dte", name="uq_doc_folio_rut_tipo"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    folio: Mapped[str] = mapped_column(String(30), nullable=False)
    tipo_dte: Mapped[str] = mapped_column(String(10), nullable=False)
    fecha_emision: Mapped[Date] = mapped_column(Date, nullable=False)
    rut_emisor: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    monto_neto: Mapped[float] = mapped_column(Float, nullable=False)
    monto_iva: Mapped[float] = mapped_column(Float, nullable=False)
    monto_total: Mapped[float] = mapped_column(Float, nullable=False)
    url_archivo: Mapped[str] = mapped_column(String(255), nullable=False)


class TblAsientos(Base):
    __tablename__ = "Tbl_Asientos"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    id_documento: Mapped[int] = mapped_column(ForeignKey("Tbl_Documentos.id"), nullable=False)
    fecha: Mapped[Date] = mapped_column(Date, nullable=False)


class TblMovimientosContables(Base):
    __tablename__ = "Tbl_Movimientos_Contables"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    id_asiento: Mapped[int] = mapped_column(ForeignKey("Tbl_Asientos.id"), nullable=False)
    id_cuenta: Mapped[int] = mapped_column(ForeignKey("Tbl_PlanCuentas.id_cuenta"), nullable=False)
    debe: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    haber: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    glosa: Mapped[str] = mapped_column(String(255), nullable=False)


engine = create_engine(f"sqlite:///{DB_PATH}", future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def _upsert_cuenta(codigo: str, nombre: str, tipo: str) -> None:
    """Inserta cuentas base si no existen para dejar la app lista para usar."""
    with SessionLocal() as session:
        exists = session.query(TblPlanCuentas).filter(TblPlanCuentas.codigo == codigo).first()
        if not exists:
            session.add(TblPlanCuentas(codigo=codigo, nombre=nombre, tipo=tipo))
            session.commit()


def seed_plan_cuentas() -> None:
    """Carga el plan de cuentas semilla desde CSV para separar datos maestros del código."""
    if not PLAN_CUENTAS_CSV.exists():
        return

    with PLAN_CUENTAS_CSV.open("r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            _upsert_cuenta(row["codigo"], row["nombre"], row["tipo"])


def init_db() -> None:
    """Crea tablas y datos mínimos al inicio de la app."""
    Base.metadata.create_all(engine)
    seed_plan_cuentas()


if __name__ == "__main__":
    init_db()
    print(f"Base inicializada en {DB_PATH}")
