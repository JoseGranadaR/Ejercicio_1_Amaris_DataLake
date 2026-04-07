"""
tests/test_etl.py
==================
Pruebas unitarias para las transformaciones del ETL.
Validan la lógica de negocio sin necesitar una cuenta de AWS real.

Ejecutar con:
    make test
    pytest tests/ -v
    pytest tests/test_etl.py -v -k "test_normalizar"
"""

import pytest
import pandas as pd
import numpy as np
from io import StringIO


# ── Helpers que replican la lógica del ETL ────────────────────
# (en el ETL real esto corre en Spark; aquí lo replicamos en Pandas para testear)

def normalizar_proveedores(df: pd.DataFrame) -> pd.DataFrame:
    """Replica la Transformación 1 sobre proveedores."""
    df = df.copy()
    df["nombre_proveedor"] = df["nombre_proveedor"].str.strip()
    df["tipo_energia"]     = df["tipo_energia"].str.strip().str.lower()
    df["pais_origen"]      = df["pais_origen"].str.strip()
    df["capacidad_mw"]     = pd.to_numeric(df["capacidad_mw"], errors="coerce")
    df["fecha_registro"]   = pd.to_datetime(df["fecha_registro"], errors="coerce")
    df["activo"]           = df["activo"].map({"true": True, "false": False, True: True, False: False})
    df = df[df["tipo_energia"].isin(["eolica", "hidroelectrica", "nuclear"])]
    df = df.drop_duplicates(subset=["nombre_proveedor"])
    return df


def normalizar_clientes(df: pd.DataFrame) -> pd.DataFrame:
    """Replica la Transformación 1 sobre clientes."""
    df = df.copy()
    df["tipo_identificacion"] = df["tipo_identificacion"].str.strip().str.upper()
    df["nombre"]              = df["nombre"].str.strip()
    df["ciudad"]              = df["ciudad"].str.strip().str.title()
    df["segmento"]            = df["segmento"].str.strip().str.lower()
    df["consumo_promedio_kwh"]= pd.to_numeric(df["consumo_promedio_kwh"], errors="coerce")
    df["fecha_vinculacion"]   = pd.to_datetime(df["fecha_vinculacion"], errors="coerce")
    df["antiguedad_dias"]     = (pd.Timestamp.now() - df["fecha_vinculacion"]).dt.days
    df["identificacion"]      = df["identificacion"].astype(str)
    df["identificacion_enmascarada"] = "****" + df["identificacion"].str[-4:]
    df = df[df["tipo_identificacion"].isin(["CC", "NIT", "CE", "PP", "TI"])]
    df = df.drop_duplicates(subset=["tipo_identificacion", "identificacion"])
    return df


def enriquecer_transacciones(df: pd.DataFrame) -> pd.DataFrame:
    """Replica la Transformación 2 sobre transacciones."""
    df = df.copy()
    df["tipo_transaccion"]         = df["tipo_transaccion"].str.strip().str.lower()
    df["tipo_energia"]             = df["tipo_energia"].str.strip().str.lower()
    df["estado"]                   = df["estado"].str.strip().str.lower()
    df["cantidad_kwh"]             = pd.to_numeric(df["cantidad_kwh"], errors="coerce")
    df["precio_cop_kwh"]           = pd.to_numeric(df["precio_cop_kwh"], errors="coerce")
    df["valor_total_cop"]          = (df["cantidad_kwh"] * df["precio_cop_kwh"]).round(2)
    df["clasificacion_volumen"]    = pd.cut(
        df["cantidad_kwh"],
        bins=[-1, 1_000, 100_000, float("inf")],
        labels=["pequeña", "mediana", "grande"],
    )
    # Eliminar duplicados ANTES de calcular promedios (evita sesgo por registros repetidos)
    df = df.drop_duplicates(subset=["id_transaccion"])
    precio_promedio = df.groupby("tipo_energia")["precio_cop_kwh"].transform("mean")
    df["precio_promedio_tipo_energia"] = precio_promedio.round(2)
    df["sobre_precio_promedio"]        = df["precio_cop_kwh"] > precio_promedio
    df["desviacion_precio_pct"]        = (
        (df["precio_cop_kwh"] - precio_promedio) / precio_promedio * 100
    ).round(2)
    return df


# ══════════════════════════════════════════════════════════════
# FIXTURES
# ══════════════════════════════════════════════════════════════

@pytest.fixture
def df_proveedores_raw():
    csv = """nombre_proveedor,tipo_energia,pais_origen,capacidad_mw,fecha_registro,activo
EnerSol Caribe  ,eolica,Colombia,350,2021-03-15,true
HidroAndes S.A.,HIDROELECTRICA ,Colombia,1200,2019-07-22,true
NuclearPower Co.,nuclear,Francia,2000,2020-01-10,true
VientoNorte Ltda.,eolica,Colombia,180,2022-05-30,true
EnerSol Caribe  ,eolica,Colombia,350,2021-03-15,true
InvalidType Corp,carbon,Colombia,500,2023-01-01,true"""
    return pd.read_csv(StringIO(csv))


@pytest.fixture
def df_clientes_raw():
    csv = """tipo_identificacion,identificacion,nombre,ciudad,segmento,fecha_vinculacion,consumo_promedio_kwh
cc,1023456789,  María López  ,bogotá,RESIDENCIAL,2020-01-15,350
NIT,900123456-1,Industrias S.A.,medellín,industrial,2018-06-20,45000
CC,987654321,Carlos Ruiz,Cali,residencial,2021-03-10,280
CC,1023456789,Duplicado,Bogotá,residencial,2022-01-01,100
INVALIDO,000000,Nadie,Ninguna,ninguna,2023-01-01,0"""
    return pd.read_csv(StringIO(csv))


@pytest.fixture
def df_transacciones_raw():
    csv = """id_transaccion,tipo_transaccion,nombre_cliente_proveedor,cantidad_kwh,precio_cop_kwh,tipo_energia,fecha_transaccion,estado,mercado
TRX-001,compra,EnerSol Caribe,500000,285.50,eolica,2024-01-05,completada,mayorista
TRX-002,venta,María López,350,410.00,eolica,2024-01-05,completada,minorista
TRX-003,compra,HidroAndes S.A.,1200000,260.00,hidroelectrica,2024-01-06,completada,mayorista
TRX-004,venta,Industrias S.A.,45000,395.00,hidroelectrica,2024-01-06,completada,minorista
TRX-005,COMPRA, NuclearPower,800000,210.50,nuclear,2024-01-07,pendiente,mayorista
TRX-001,compra,EnerSol Caribe,500000,285.50,eolica,2024-01-05,completada,mayorista"""
    return pd.read_csv(StringIO(csv))


# ══════════════════════════════════════════════════════════════
# TESTS — TRANSFORMACIÓN 1: NORMALIZACIÓN PROVEEDORES
# ══════════════════════════════════════════════════════════════

class TestNormalizacionProveedores:

    def test_elimina_espacios_en_nombre(self, df_proveedores_raw):
        result = normalizar_proveedores(df_proveedores_raw)
        assert "EnerSol Caribe" in result["nombre_proveedor"].values
        assert "EnerSol Caribe  " not in result["nombre_proveedor"].values

    def test_tipo_energia_en_minusculas(self, df_proveedores_raw):
        result = normalizar_proveedores(df_proveedores_raw)
        assert all(result["tipo_energia"] == result["tipo_energia"].str.lower())

    def test_filtra_tipo_energia_invalido(self, df_proveedores_raw):
        result = normalizar_proveedores(df_proveedores_raw)
        assert "carbon" not in result["tipo_energia"].values
        # 6 filas en el CSV: 1 carbon (filtrado) + 1 duplicado (EnerSol) = 4 válidos únicos
        assert len(result) == 4

    def test_elimina_duplicados_por_nombre(self, df_proveedores_raw):
        result = normalizar_proveedores(df_proveedores_raw)
        assert result["nombre_proveedor"].nunique() == len(result)

    def test_capacidad_mw_es_numerico(self, df_proveedores_raw):
        result = normalizar_proveedores(df_proveedores_raw)
        # Pandas infiere int64 cuando todos los valores son enteros, float64 si hay decimales.
        # Ambos son tipos numéricos válidos — el test acepta los dos.
        assert result["capacidad_mw"].dtype in [
            float, np.float64, int, np.int64, np.int32
        ], f"Se esperaba tipo numérico, se obtuvo: {result['capacidad_mw'].dtype}"

    def test_fecha_registro_es_datetime(self, df_proveedores_raw):
        result = normalizar_proveedores(df_proveedores_raw)
        assert pd.api.types.is_datetime64_any_dtype(result["fecha_registro"])

    def test_activo_es_booleano(self, df_proveedores_raw):
        result = normalizar_proveedores(df_proveedores_raw)
        assert result["activo"].dtype == bool


# ══════════════════════════════════════════════════════════════
# TESTS — TRANSFORMACIÓN 1: NORMALIZACIÓN CLIENTES
# ══════════════════════════════════════════════════════════════

class TestNormalizacionClientes:

    def test_tipo_identificacion_en_mayusculas(self, df_clientes_raw):
        result = normalizar_clientes(df_clientes_raw)
        assert all(result["tipo_identificacion"] == result["tipo_identificacion"].str.upper())

    def test_ciudad_en_title_case(self, df_clientes_raw):
        result = normalizar_clientes(df_clientes_raw)
        assert "Bogotá" in result["ciudad"].values
        assert "bogotá" not in result["ciudad"].values

    def test_segmento_en_minusculas(self, df_clientes_raw):
        result = normalizar_clientes(df_clientes_raw)
        assert all(result["segmento"] == result["segmento"].str.lower())

    def test_elimina_duplicados_por_identificacion(self, df_clientes_raw):
        result = normalizar_clientes(df_clientes_raw)
        combo  = result["tipo_identificacion"] + "_" + result["identificacion"].astype(str)
        assert combo.nunique() == len(result)

    def test_filtra_tipo_id_invalido(self, df_clientes_raw):
        result = normalizar_clientes(df_clientes_raw)
        assert "INVALIDO" not in result["tipo_identificacion"].values

    def test_identificacion_enmascarada(self, df_clientes_raw):
        result = normalizar_clientes(df_clientes_raw)
        assert all(result["identificacion_enmascarada"].str.startswith("****"))

    def test_antiguedad_dias_es_positivo(self, df_clientes_raw):
        result = normalizar_clientes(df_clientes_raw)
        assert (result["antiguedad_dias"] >= 0).all()

    def test_nombre_sin_espacios_extra(self, df_clientes_raw):
        result = normalizar_clientes(df_clientes_raw)
        assert "María López" in result["nombre"].values
        assert "  María López  " not in result["nombre"].values


# ══════════════════════════════════════════════════════════════
# TESTS — TRANSFORMACIÓN 2: ENRIQUECIMIENTO TRANSACCIONES
# ══════════════════════════════════════════════════════════════

class TestEnriquecimientoTransacciones:

    def test_calcula_valor_total_cop(self, df_transacciones_raw):
        result = enriquecer_transacciones(df_transacciones_raw)
        row    = result[result["id_transaccion"] == "TRX-001"].iloc[0]
        expected = round(500000 * 285.50, 2)
        assert row["valor_total_cop"] == expected

    def test_clasificacion_grande(self, df_transacciones_raw):
        result = enriquecer_transacciones(df_transacciones_raw)
        grandes = result[result["cantidad_kwh"] > 100_000]
        assert all(grandes["clasificacion_volumen"] == "grande")

    def test_clasificacion_mediana(self, df_transacciones_raw):
        result = enriquecer_transacciones(df_transacciones_raw)
        medianas = result[(result["cantidad_kwh"] >= 1_000) & (result["cantidad_kwh"] <= 100_000)]
        assert all(medianas["clasificacion_volumen"] == "mediana")

    def test_clasificacion_pequena(self, df_transacciones_raw):
        result = enriquecer_transacciones(df_transacciones_raw)
        pequenas = result[result["cantidad_kwh"] < 1_000]
        assert all(pequenas["clasificacion_volumen"] == "pequeña")

    def test_tipo_transaccion_en_minusculas(self, df_transacciones_raw):
        result = enriquecer_transacciones(df_transacciones_raw)
        assert "COMPRA" not in result["tipo_transaccion"].values
        assert all(result["tipo_transaccion"].isin(["compra", "venta"]))

    def test_elimina_duplicados_por_id(self, df_transacciones_raw):
        result = enriquecer_transacciones(df_transacciones_raw)
        assert result["id_transaccion"].nunique() == len(result)

    def test_precio_promedio_calculado_por_tipo(self, df_transacciones_raw):
        result = enriquecer_transacciones(df_transacciones_raw)
        # El promedio se calcula DESPUÉS del dedup (comportamiento correcto del ETL).
        # Eólica en el fixture tras dedup: TRX-001 (285.50) y TRX-002 (410.00)
        # Media esperada: (285.50 + 410.00) / 2 = 347.75
        eolicas = result[result["tipo_energia"] == "eolica"]
        promedio_real = eolicas["precio_cop_kwh"].mean()
        promedio_calculado = eolicas["precio_promedio_tipo_energia"].iloc[0]
        assert abs(promedio_calculado - round(promedio_real, 2)) < 0.01, (
            f"Promedio esperado {round(promedio_real, 2)}, "
            f"obtenido {promedio_calculado}"
        )

    def test_sobre_precio_promedio_es_booleano(self, df_transacciones_raw):
        result = enriquecer_transacciones(df_transacciones_raw)
        assert result["sobre_precio_promedio"].dtype == bool

    def test_desviacion_precio_en_porcentaje(self, df_transacciones_raw):
        result = enriquecer_transacciones(df_transacciones_raw)
        # La desviación debe ser coherente con sobre_precio_promedio
        positivas = result[result["sobre_precio_promedio"]]
        assert all(positivas["desviacion_precio_pct"] > 0)


# ══════════════════════════════════════════════════════════════
# TESTS — DATOS DE MUESTRA (integridad de los CSV de prueba)
# ══════════════════════════════════════════════════════════════

class TestDatosDeMuestra:
    """Verifica que los archivos CSV de prueba tienen el formato correcto."""

    REQUIRED_COLS = {
        "proveedores":   ["nombre_proveedor", "tipo_energia", "capacidad_mw", "activo"],
        "clientes":      ["tipo_identificacion", "identificacion", "nombre", "segmento"],
        "transacciones": ["id_transaccion", "tipo_transaccion", "cantidad_kwh", "precio_cop_kwh"],
    }

    @pytest.mark.parametrize("dataset", ["proveedores", "clientes", "transacciones"])
    def test_archivo_existe(self, dataset):
        path = f"data/sample/{dataset}.csv"
        import os
        assert os.path.exists(path), f"Archivo no encontrado: {path}"

    @pytest.mark.parametrize("dataset,cols", REQUIRED_COLS.items())
    def test_columnas_requeridas(self, dataset, cols):
        df = pd.read_csv(f"data/sample/{dataset}.csv")
        for col in cols:
            assert col in df.columns, f"Columna '{col}' faltante en {dataset}.csv"

    @pytest.mark.parametrize("dataset", ["proveedores", "clientes", "transacciones"])
    def test_no_esta_vacio(self, dataset):
        df = pd.read_csv(f"data/sample/{dataset}.csv")
        assert len(df) > 0, f"{dataset}.csv no tiene filas"

    def test_tipo_energia_validos_en_proveedores(self):
        df = pd.read_csv("data/sample/proveedores.csv")
        validos = {"eolica", "hidroelectrica", "nuclear"}
        assert set(df["tipo_energia"].unique()).issubset(validos)

    def test_tipo_transaccion_validos(self):
        df = pd.read_csv("data/sample/transacciones.csv")
        validos = {"compra", "venta"}
        assert set(df["tipo_transaccion"].unique()).issubset(validos)

    def test_precios_son_positivos(self):
        df = pd.read_csv("data/sample/transacciones.csv")
        assert (df["precio_cop_kwh"] > 0).all()

    def test_cantidades_son_positivas(self):
        df = pd.read_csv("data/sample/transacciones.csv")
        assert (df["cantidad_kwh"] > 0).all()
