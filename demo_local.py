"""
demo_local.py
=============
Demuestra el pipeline completo del Data Lake sin necesitar cuenta de AWS.
Simula S3, Glue y Athena en memoria usando la librería moto.

Ejecutar desde la raíz del proyecto:
    python demo_local.py

Tiempo de ejecución estimado: 15-30 segundos
"""

import io
import os
import sys
import json
import time
import textwrap

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from moto import mock_aws
from colorama import Fore, Style, init

init(autoreset=True)

# ── Helpers de consola ─────────────────────────────────────────
WIDTH = 60

def hdr(fase, titulo):
    print(f"\n{Fore.BLUE}{'═' * WIDTH}{Style.RESET_ALL}")
    print(f"  {Fore.BLUE}{fase}{Style.RESET_ALL}  {Fore.WHITE}{titulo}{Style.RESET_ALL}")
    print(f"{Fore.BLUE}{'═' * WIDTH}{Style.RESET_ALL}")

def ok(msg):
    print(f"  {Fore.GREEN}✅{Style.RESET_ALL}  {msg}")

def info(msg):
    print(f"  {Fore.CYAN}→{Style.RESET_ALL}  {msg}")

def warn(msg):
    print(f"  {Fore.YELLOW}⚠{Style.RESET_ALL}   {msg}")

def resultado(label, df, max_rows=6):
    """Imprime un DataFrame con bordes simples."""
    print(f"\n  {Fore.YELLOW}{label}{Style.RESET_ALL}")
    cols   = list(df.columns)
    widths = {c: max(len(str(c)), df[c].astype(str).str.len().max()) for c in cols}
    sep    = "  +" + "+".join("-" * (widths[c] + 2) for c in cols) + "+"
    hrow   = "  |" + "|".join(f" {str(c).ljust(widths[c])} " for c in cols) + "|"
    print(sep)
    print(hrow)
    print(sep)
    for _, row in df.head(max_rows).iterrows():
        line = "  |" + "|".join(f" {str(row[c]).ljust(widths[c])} " for c in cols) + "|"
        print(line)
    print(sep)
    if len(df) > max_rows:
        print(f"  ... y {len(df) - max_rows} filas más")

# ── Configuración ──────────────────────────────────────────────
REGION    = "us-east-1"
PARTITION = "year=2024/month=01/day=15/hour=00"
BUCKETS   = {
    "landing":   "amaris-dev-landing",
    "raw":       "amaris-dev-raw",
    "processed": "amaris-dev-processed",
    "curated":   "amaris-dev-curated",
    "athena":    "amaris-dev-athena-results",
}
DATABASE = "amaris_datalake_dev_processed"
DATASETS = ["proveedores", "clientes", "transacciones"]


# ══════════════════════════════════════════════════════════════
# PIPELINE PRINCIPAL
# ══════════════════════════════════════════════════════════════

@mock_aws
def ejecutar_pipeline():

    start_total = time.time()

    # ── FASE 1: Infraestructura ────────────────────────────────
    hdr("FASE 1/6", "Creación de infraestructura S3 y catálogo Glue")
    info("Simulando terraform apply en memoria con moto...")

    s3   = boto3.client("s3",   region_name=REGION)
    glue = boto3.client("glue", region_name=REGION)

    for nombre, bucket in BUCKETS.items():
        if REGION == "us-east-1":
            # us-east-1 es la región por defecto de S3 — no acepta LocationConstraint
            s3.create_bucket(Bucket=bucket)
        else:
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": REGION},
            )
        ok(f"Bucket {nombre}: s3://{bucket}")

    glue.create_database(DatabaseInput={"Name": DATABASE})
    ok(f"Catálogo Glue: {DATABASE}")

    # ── FASE 2: Ingesta de CSVs ────────────────────────────────
    hdr("FASE 2/6", "Ingesta de CSVs → Landing Zone (Lambda)")
    info("Cargando archivos de data/sample/ al bucket Landing...")

    dfs = {}
    for dataset in DATASETS:
        path = os.path.join("data", "sample", f"{dataset}.csv")
        if not os.path.exists(path):
            warn(f"Archivo no encontrado: {path}")
            sys.exit(1)

        with open(path, "rb") as f:
            contenido = f.read()

        key = f"raw/{dataset}/{PARTITION}/{dataset}.csv"
        s3.put_object(
            Bucket=BUCKETS["landing"],
            Key=key,
            Body=contenido,
            ContentType="text/csv",
            Metadata={"dataset": dataset, "fuente": "demo_local.py"},
        )
        dfs[dataset] = pd.read_csv(io.BytesIO(contenido))
        ok(f"{dataset}.csv → {len(dfs[dataset])} filas, {len(contenido):,} bytes")

    # ── FASE 3: Transformaciones ETL ──────────────────────────
    hdr("FASE 3/6", "Transformaciones ETL — Glue Spark (simulado en Pandas)")

    # ── T1: Normalización proveedores ──
    info("T1 — Normalización y limpieza: proveedores")
    prov = dfs["proveedores"].copy()
    antes = len(prov)
    prov["nombre_proveedor"] = prov["nombre_proveedor"].str.strip()
    prov["tipo_energia"]     = prov["tipo_energia"].str.strip().str.lower()
    prov["pais_origen"]      = prov["pais_origen"].str.strip()
    prov["capacidad_mw"]     = pd.to_numeric(prov["capacidad_mw"], errors="coerce")
    prov["fecha_registro"]   = pd.to_datetime(prov["fecha_registro"], errors="coerce")
    prov = prov[prov["tipo_energia"].isin(["eolica", "hidroelectrica", "nuclear"])]
    prov = prov.drop_duplicates(subset=["nombre_proveedor"])
    prov["_fecha_ingesta"] = "2024-01-15"
    ok(f"Proveedores: {antes} → {len(prov)} (tipo_energia normalizado, duplicados eliminados)")

    # ── T1: Normalización clientes ──
    info("T1 — Normalización y limpieza: clientes")
    cli = dfs["clientes"].copy()
    cli["tipo_identificacion"] = cli["tipo_identificacion"].str.strip().str.upper()
    cli["nombre"]              = cli["nombre"].str.strip()
    cli["ciudad"]              = cli["ciudad"].str.strip().str.title()
    cli["segmento"]            = cli["segmento"].str.strip().str.lower()
    cli["consumo_promedio_kwh"]= pd.to_numeric(cli["consumo_promedio_kwh"], errors="coerce")
    cli["fecha_vinculacion"]   = pd.to_datetime(cli["fecha_vinculacion"], errors="coerce")
    cli["antiguedad_dias"]     = (pd.Timestamp.now() - cli["fecha_vinculacion"]).dt.days.astype(int)
    cli["identificacion_enmascarada"] = "****" + cli["identificacion"].astype(str).str[-4:]
    cli = cli.drop_duplicates(subset=["tipo_identificacion", "identificacion"])
    cli["_fecha_ingesta"] = "2024-01-15"
    ok(f"Clientes: {len(cli)} registros, identificaciones enmascaradas ****XXXX")

    # ── T2: Enriquecimiento transacciones ──
    info("T2 — Enriquecimiento: calcular valor_total_cop y clasificación por volumen")
    trx = dfs["transacciones"].copy()
    trx["tipo_transaccion"]  = trx["tipo_transaccion"].str.strip().str.lower()
    trx["tipo_energia"]      = trx["tipo_energia"].str.strip().str.lower()
    trx["estado"]            = trx["estado"].str.strip().str.lower()
    trx["cantidad_kwh"]      = pd.to_numeric(trx["cantidad_kwh"],   errors="coerce")
    trx["precio_cop_kwh"]    = pd.to_numeric(trx["precio_cop_kwh"], errors="coerce")
    trx["valor_total_cop"]   = (trx["cantidad_kwh"] * trx["precio_cop_kwh"]).round(2)
    trx["clasificacion_volumen"] = pd.cut(
        trx["cantidad_kwh"],
        bins=[-1, 1_000, 100_000, float("inf")],
        labels=["pequeña", "mediana", "grande"],
    ).astype(str)
    precio_prom = trx.groupby("tipo_energia")["precio_cop_kwh"].transform("mean")
    trx["precio_promedio_tipo_energia"] = precio_prom.round(2)
    trx["sobre_precio_promedio"]        = trx["precio_cop_kwh"] > precio_prom
    trx["desviacion_precio_pct"]        = (
        (trx["precio_cop_kwh"] - precio_prom) / precio_prom * 100
    ).round(2)
    trx = trx.drop_duplicates(subset=["id_transaccion"])
    trx["_fecha_ingesta"] = "2024-01-15"
    vol = trx["clasificacion_volumen"].value_counts().to_dict()
    ok(f"Transacciones: valor_total_cop calculado | volumen → {vol}")

    # ── T3: Métricas por tipo de energía (integración) ──
    info("T3 — Integración referencial: métricas por tipo de energía y segmento")
    met_energia = (
        trx.groupby(["tipo_energia", "tipo_transaccion"])
        .agg(
            num_transacciones=("id_transaccion", "count"),
            total_kwh=("cantidad_kwh", "sum"),
            total_cop=("valor_total_cop", "sum"),
            precio_promedio_kwh=("precio_cop_kwh", "mean"),
        )
        .round(2)
        .reset_index()
    )
    met_energia["_fecha_ingesta"] = "2024-01-15"

    # Unir transacciones de venta con segmento del cliente
    trx_venta = trx[trx["tipo_transaccion"] == "venta"].copy()
    trx_venta = trx_venta.merge(
        cli[["nombre", "segmento", "ciudad"]].rename(columns={"nombre": "nombre_cliente_proveedor"}),
        on="nombre_cliente_proveedor",
        how="left",
    )
    met_segmento = (
        trx_venta[trx_venta["segmento"].notna()]
        .groupby(["segmento", "tipo_energia"])
        .agg(
            num_transacciones=("id_transaccion", "count"),
            total_kwh=("cantidad_kwh", "sum"),
            total_cop=("valor_total_cop", "sum"),
            ticket_promedio_cop=("valor_total_cop", "mean"),
        )
        .round(2)
        .reset_index()
    )
    ok(f"Métricas energía: {len(met_energia)} combinaciones tipo × transacción")
    ok(f"Métricas segmento: {len(met_segmento)} combinaciones segmento × energía")

    # ── FASE 4: Escribir Parquet ───────────────────────────────
    hdr("FASE 4/6", "Escribir Parquet comprimido (Snappy) → Processed Zone")

    tablas = {
        "proveedores":       prov,
        "clientes":          cli,
        "transacciones":     trx,
        "metricas_energia":  met_energia,
        "metricas_segmento": met_segmento,
    }
    type_map = {
        "int64":   "bigint",
        "int32":   "int",
        "float64": "double",
        "bool":    "boolean",
        "object":  "string",
    }

    for nombre, df in tablas.items():
        buf   = io.BytesIO()
        table = pa.Table.from_pandas(df.reset_index(drop=True), preserve_index=False)
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        raw_size = len(buf.getvalue())
        key = f"processed/{nombre}/{PARTITION}/{nombre}.parquet"
        s3.put_object(Bucket=BUCKETS["processed"], Key=key, Body=buf.getvalue())

        # Registrar en Glue
        cols = [
            {"Name": c, "Type": type_map.get(str(t), "string")}
            for c, t in df.dtypes.items()
        ]
        try:
            glue.create_table(
                DatabaseName=DATABASE,
                TableInput={
                    "Name": nombre,
                    "StorageDescriptor": {
                        "Columns": cols,
                        "Location": f"s3://{BUCKETS['processed']}/processed/{nombre}/",
                        "InputFormat":  "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        "SerdeInfo": {
                            "SerializationLibrary":
                                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        },
                    },
                    "PartitionKeys": [
                        {"Name": "year",  "Type": "string"},
                        {"Name": "month", "Type": "string"},
                        {"Name": "day",   "Type": "string"},
                    ],
                    "TableType": "EXTERNAL_TABLE",
                },
            )
        except Exception:
            pass

        ok(f"{nombre}.parquet — {len(df)} filas, {raw_size:,} bytes (Snappy)")

    # ── FASE 5: Catálogo Glue ──────────────────────────────────
    hdr("FASE 5/6", "Catálogo de datos automático (Glue Crawlers)")

    tablas_glue = glue.get_tables(DatabaseName=DATABASE)["TableList"]
    for t in tablas_glue:
        ncols = len(t["StorageDescriptor"]["Columns"])
        ok(f"Tabla catalogada: {t['Name']:<22} {ncols} columnas, particionada por year/month/day")

    # ── FASE 6: Consultas analíticas ──────────────────────────
    hdr("FASE 6/6", "Consultas analíticas (equivalente a Amazon Athena)")

    # Q1 — Total energía comprada
    info("Q1 — Total de energía comprada por tipo")
    q1 = (
        trx[trx["tipo_transaccion"] == "compra"]
        .groupby("tipo_energia")
        .agg(
            num_transacciones=("id_transaccion", "count"),
            total_kwh=("cantidad_kwh", "sum"),
            total_cop=("valor_total_cop", "sum"),
            precio_promedio_kwh=("precio_cop_kwh", "mean"),
        )
        .round(2)
        .reset_index()
        .sort_values("total_kwh", ascending=False)
    )
    resultado("Resultado Q1:", q1)

    # Q3 — Precio promedio por tipo y mercado
    info("Q3 — Precio promedio por tipo de energía y mercado")
    q3 = (
        trx[trx["estado"] == "completada"]
        .groupby(["tipo_energia", "mercado", "tipo_transaccion"])
        .agg(
            precio_promedio=("precio_cop_kwh", "mean"),
            precio_min=("precio_cop_kwh", "min"),
            precio_max=("precio_cop_kwh", "max"),
            num_operaciones=("id_transaccion", "count"),
        )
        .round(2)
        .reset_index()
    )
    resultado("Resultado Q3:", q3)

    # Q4 — Transacciones pendientes
    info("Q4 — Transacciones pendientes (alerta operacional)")
    q4 = trx[trx["estado"] == "pendiente"][
        ["id_transaccion", "tipo_transaccion", "nombre_cliente_proveedor",
         "cantidad_kwh", "valor_total_cop", "tipo_energia", "fecha_transaccion"]
    ]
    resultado("Resultado Q4:", q4)

    # Q5 — Margen compra vs venta (la consulta más relevante de negocio)
    info("Q5 — Margen estimado compra vs venta por tipo de energía")
    compras_avg = (
        trx[trx["tipo_transaccion"] == "compra"]
        .groupby("tipo_energia")["precio_cop_kwh"]
        .mean()
    )
    ventas_avg = (
        trx[trx["tipo_transaccion"] == "venta"]
        .groupby("tipo_energia")["precio_cop_kwh"]
        .mean()
    )
    q5 = pd.DataFrame({
        "precio_compra_kwh": compras_avg,
        "precio_venta_kwh":  ventas_avg,
    }).dropna().reset_index()
    q5["margen_cop_kwh"] = (q5["precio_venta_kwh"] - q5["precio_compra_kwh"]).round(2)
    q5["margen_pct"]     = (
        (q5["margen_cop_kwh"] / q5["precio_compra_kwh"]) * 100
    ).round(2)
    q5 = q5.sort_values("margen_pct", ascending=False)
    resultado("Resultado Q5 — MARGEN DE NEGOCIO:", q5)

    # ── RESUMEN FINAL ──────────────────────────────────────────
    elapsed = time.time() - start_total
    print(f"\n{Fore.GREEN}{'═' * WIDTH}{Style.RESET_ALL}")
    print(f"  {Fore.GREEN}PIPELINE COMPLETO — RESUMEN{Style.RESET_ALL}")
    print(f"{Fore.GREEN}{'═' * WIDTH}{Style.RESET_ALL}")

    # Contar objetos en S3 Processed
    resp_s3 = s3.list_objects_v2(Bucket=BUCKETS["processed"])
    n_parquet = resp_s3.get("KeyCount", 0)

    ok(f"Buckets S3 creados:          {len(BUCKETS)}")
    ok(f"CSVs ingeridos:              {len(DATASETS)} archivos")
    ok(f"Transformaciones aplicadas:  3 (T1 normalización, T2 enriquecimiento, T3 integración)")
    ok(f"Tablas Parquet en Processed: {n_parquet}")
    ok(f"Tablas en catálogo Glue:     {len(tablas_glue)}")
    ok(f"Consultas SQL ejecutadas:    4 (Q1, Q3, Q4, Q5)")
    ok(f"Tiempo total:                {elapsed:.1f} segundos")
    print(f"\n  {Fore.GREEN}Sin cuenta AWS. Sin Docker. Sin configuración.{Style.RESET_ALL}")
    print(f"  {Fore.GREEN}Todo simulado en memoria con moto + pandas + pyarrow.{Style.RESET_ALL}\n")


# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print(f"\n{Fore.WHITE}{'═' * WIDTH}")
    print(f"  DATA LAKE — COMERCIALIZADORA DE ENERGÍA")
    print(f"  Demo local  |  Prueba Técnica Amaris")
    print(f"{'═' * WIDTH}{Style.RESET_ALL}\n")

    # Verificar que los datos de prueba existen antes de empezar
    for ds in ["proveedores", "clientes", "transacciones"]:
        path = os.path.join("data", "sample", f"{ds}.csv")
        if not os.path.exists(path):
            print(f"  ❌  Archivo no encontrado: {path}")
            print(f"      Asegúrese de ejecutar este script desde la raíz del proyecto.")
            print(f"      La raíz es la carpeta que contiene README.md y Makefile.\n")
            sys.exit(1)

    ejecutar_pipeline()
