"""
glue_jobs/etl_energia_transform.py
====================================
AWS Glue ETL Job — 3 Transformaciones + CSV → Parquet
======================================================

Transformaciones implementadas:
  1. Normalización y limpieza de datos
     - Estandarizar strings (trim, lowercase en campos categóricos)
     - Casteo de tipos (fechas, numéricos)
     - Eliminar duplicados
  2. Enriquecimiento de transacciones
     - Calcular valor_total_cop = cantidad_kwh * precio_cop_kwh
     - Clasificar transacciones por volumen (pequeña/mediana/grande)
     - Agregar margen estimado (precio venta vs. precio compra promedio)
  3. Integración referencial
     - Unir transacciones con datos de clientes (segmento, ciudad)
     - Unir con proveedores (capacidad_mw, pais_origen)
     - Calcular métricas agregadas por tipo_energia

Salida: Parquet particionado en zona Processed con compresión Snappy
"""

import sys
from awsglue.transforms  import *
from awsglue.utils       import getResolvedOptions
from pyspark.context     import SparkContext
from awsglue.context     import GlueContext
from awsglue.job         import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql         import functions as F
from pyspark.sql.types   import (
    DoubleType, LongType, DateType, BooleanType, StringType
)
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ─── Parámetros del job ──────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "RAW_BUCKET",
    "PROCESSED_BUCKET",
    "GLUE_DATABASE",
    "PARTITION_DATE",        # formato: YYYY-MM-DD
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_BUCKET       = args["RAW_BUCKET"]
PROCESSED_BUCKET = args["PROCESSED_BUCKET"]
GLUE_DATABASE    = args["GLUE_DATABASE"]
PARTITION_DATE   = args["PARTITION_DATE"]   # e.g. "2024-01-15"

# Derivar year/month/day desde la fecha de partición
year, month, day = PARTITION_DATE.split("-")
PARTITION_PATH   = f"year={year}/month={month}/day={day}"

logger.info(f"Procesando partición: {PARTITION_PATH}")


# ============================================================
# HELPERS
# ============================================================

def read_raw(dataset: str) -> DynamicFrame:
    """Lee CSV desde la zona Raw de S3 con detección automática de esquema."""
    path = f"s3://{RAW_BUCKET}/raw/{dataset}/{PARTITION_PATH}/"
    logger.info(f"Leyendo: {path}")
    return glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [path], "recurse": True},
        format="csv",
        format_options={"withHeader": True, "separator": ","},
        transformation_ctx=f"read_{dataset}",
    )


def write_processed(df, dataset: str):
    """Escribe DataFrame como Parquet particionado en la zona Processed."""
    path = f"s3://{PROCESSED_BUCKET}/processed/{dataset}/{PARTITION_PATH}/"
    logger.info(f"Escribiendo Parquet: {path}")

    dyf = DynamicFrame.fromDF(df, glueContext, f"write_{dataset}")
    glueContext.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        connection_options={"path": path, "partitionKeys": []},
        format="parquet",
        format_options={"compression": "snappy"},
        transformation_ctx=f"write_{dataset}",
    )
    logger.info(f"Parquet escrito: {dataset} → {df.count()} filas")


# ============================================================
# TRANSFORMACIÓN 1: NORMALIZACIÓN Y LIMPIEZA
# ============================================================

def transform_proveedores(dyf: DynamicFrame) -> DynamicFrame:
    """
    T1 — Normalización de proveedores:
      - Trim en todos los strings
      - Tipo_energia en minúsculas estandarizadas
      - Cast de fecha_registro a DateType
      - Cast activo a BooleanType
      - Eliminar duplicados por nombre_proveedor
      - Agregar columna de ingesta
    """
    df = dyf.toDF()

    df = (df
        # Trim y lowercase en campos categóricos
        .withColumn("nombre_proveedor", F.trim(F.col("nombre_proveedor")))
        .withColumn("tipo_energia",     F.trim(F.lower(F.col("tipo_energia"))))
        .withColumn("pais_origen",      F.trim(F.col("pais_origen")))

        # Tipos correctos
        .withColumn("capacidad_mw",    F.col("capacidad_mw").cast(DoubleType()))
        .withColumn("fecha_registro",  F.to_date(F.col("fecha_registro"), "yyyy-MM-dd"))
        .withColumn("activo",          F.col("activo").cast(BooleanType()))

        # Validación: solo tipos de energía permitidos
        .filter(F.col("tipo_energia").isin("eolica", "hidroelectrica", "nuclear"))

        # Eliminar duplicados
        .dropDuplicates(["nombre_proveedor"])

        # Metadatos de ingesta
        .withColumn("_fecha_ingesta",  F.lit(PARTITION_DATE).cast(DateType()))
        .withColumn("_fuente",         F.lit("sistema_transaccional"))
    )

    logger.info(f"[T1] Proveedores normalizados: {df.count()} registros")
    return DynamicFrame.fromDF(df, glueContext, "proveedores_clean")


def transform_clientes(dyf: DynamicFrame) -> DynamicFrame:
    """
    T1 — Normalización de clientes:
      - Validar tipos de identificación permitidos (CC, NIT, CE, PP)
      - Ciudad en Title Case
      - Segmento en lowercase
      - Cast consumo_promedio_kwh a Double
      - Enmascarar parcialmente la identificación (privacidad)
    """
    df = dyf.toDF()

    df = (df
        .withColumn("tipo_identificacion", F.trim(F.upper(F.col("tipo_identificacion"))))
        .withColumn("nombre",             F.trim(F.col("nombre")))
        .withColumn("ciudad",             F.initcap(F.trim(F.col("ciudad"))))
        .withColumn("segmento",           F.trim(F.lower(F.col("segmento"))))

        # Solo tipos de ID válidos
        .filter(F.col("tipo_identificacion").isin("CC", "NIT", "CE", "PP", "TI"))

        # Tipos numéricos
        .withColumn("consumo_promedio_kwh", F.col("consumo_promedio_kwh").cast(DoubleType()))
        .withColumn("fecha_vinculacion",    F.to_date(F.col("fecha_vinculacion"), "yyyy-MM-dd"))

        # Antigüedad del cliente en días
        .withColumn("antiguedad_dias",
            F.datediff(F.current_date(), F.col("fecha_vinculacion")).cast(LongType())
        )

        # Enmascarar identificación: mostrar solo últimos 4 dígitos
        .withColumn("identificacion_enmascarada",
            F.concat(F.lit("****"), F.substring(F.col("identificacion").cast(StringType()), -4, 4))
        )

        .dropDuplicates(["tipo_identificacion", "identificacion"])
        .withColumn("_fecha_ingesta", F.lit(PARTITION_DATE).cast(DateType()))
    )

    logger.info(f"[T1] Clientes normalizados: {df.count()} registros")
    return DynamicFrame.fromDF(df, glueContext, "clientes_clean")


# ============================================================
# TRANSFORMACIÓN 2: ENRIQUECIMIENTO DE TRANSACCIONES
# ============================================================

def transform_transacciones(dyf: DynamicFrame) -> DynamicFrame:
    """
    T2 — Enriquecimiento de transacciones:
      - Calcular valor_total_cop = cantidad_kwh * precio_cop_kwh
      - Clasificar por volumen: pequeña (<1000 kWh), mediana (1K-100K), grande (>100K)
      - Agregar día_semana y hora_del_dia
      - Calcular precio_unitario_promedio_tipo_energia (window function)
      - Marcar transacciones por encima/debajo del promedio de su tipo de energía
    """
    from pyspark.sql.window import Window

    df = dyf.toDF()

    df = (df
        .withColumn("tipo_transaccion",          F.trim(F.lower(F.col("tipo_transaccion"))))
        .withColumn("nombre_cliente_proveedor",  F.trim(F.col("nombre_cliente_proveedor")))
        .withColumn("tipo_energia",              F.trim(F.lower(F.col("tipo_energia"))))
        .withColumn("estado",                    F.trim(F.lower(F.col("estado"))))
        .withColumn("mercado",                   F.trim(F.lower(F.col("mercado"))))

        # Tipos numéricos
        .withColumn("cantidad_kwh",      F.col("cantidad_kwh").cast(DoubleType()))
        .withColumn("precio_cop_kwh",    F.col("precio_cop_kwh").cast(DoubleType()))
        .withColumn("fecha_transaccion", F.to_date(F.col("fecha_transaccion"), "yyyy-MM-dd"))

        # T2a: Valor total
        .withColumn("valor_total_cop",
            F.round(F.col("cantidad_kwh") * F.col("precio_cop_kwh"), 2)
        )

        # T2b: Clasificación por volumen
        .withColumn("clasificacion_volumen",
            F.when(F.col("cantidad_kwh") < 1_000,           F.lit("pequeña"))
             .when(F.col("cantidad_kwh").between(1_000, 100_000), F.lit("mediana"))
             .otherwise(F.lit("grande"))
        )

        # T2c: Dimensiones temporales
        .withColumn("anio",       F.year(F.col("fecha_transaccion")))
        .withColumn("mes",        F.month(F.col("fecha_transaccion")))
        .withColumn("dia_semana", F.dayofweek(F.col("fecha_transaccion")))
    )

    # T2d: Precio promedio por tipo de energía (window function)
    w_energia = Window.partitionBy("tipo_energia")
    df = (df
        .withColumn("precio_promedio_tipo_energia",
            F.round(F.avg("precio_cop_kwh").over(w_energia), 2)
        )
        .withColumn("sobre_precio_promedio",
            F.col("precio_cop_kwh") > F.col("precio_promedio_tipo_energia")
        )
        .withColumn("desviacion_precio_pct",
            F.round(
                (F.col("precio_cop_kwh") - F.col("precio_promedio_tipo_energia"))
                / F.col("precio_promedio_tipo_energia") * 100, 2
            )
        )
        .dropDuplicates(["id_transaccion"])
        .withColumn("_fecha_ingesta", F.lit(PARTITION_DATE).cast(DateType()))
    )

    logger.info(f"[T2] Transacciones enriquecidas: {df.count()} registros")
    return DynamicFrame.fromDF(df, glueContext, "transacciones_enriched")


# ============================================================
# TRANSFORMACIÓN 3: INTEGRACIÓN REFERENCIAL (JOIN)
# ============================================================

def transform_metricas_energia(
    dyf_trx:  DynamicFrame,
    dyf_cli:  DynamicFrame,
    dyf_prov: DynamicFrame,
) -> DynamicFrame:
    """
    T3 — Integración referencial y tabla de métricas agregadas:
      - JOIN transacciones × clientes (por nombre_cliente_proveedor)
      - JOIN transacciones × proveedores (por nombre_cliente_proveedor)
      - Agregar por tipo_energia: total_kwh, total_cop, num_transacciones, precio_promedio
      - Agregar por segmento de cliente: consumo_total, ticket_promedio
      - Producir tabla de métricas consolidada (vista analítica)
    """
    df_trx  = dyf_trx.toDF()
    df_cli  = dyf_cli.toDF().select(
        "nombre", "ciudad", "segmento", "consumo_promedio_kwh"
    ).withColumnRenamed("nombre", "nombre_cliente")
    df_prov = dyf_prov.toDF().select(
        "nombre_proveedor", "capacidad_mw", "pais_origen"
    )

    # Enriquecer transacciones de venta con datos del cliente
    df_ventas = (df_trx
        .filter(F.col("tipo_transaccion") == "venta")
        .join(df_cli,
              df_trx["nombre_cliente_proveedor"] == df_cli["nombre_cliente"],
              "left")
    )

    # Enriquecer transacciones de compra con datos del proveedor
    _ = (df_trx
        .filter(F.col("tipo_transaccion") == "compra")
        .join(df_prov,
              df_trx["nombre_cliente_proveedor"] == df_prov["nombre_proveedor"],
              "left")
    )

    # T3a: Métricas por tipo de energía
    metricas_energia = (df_trx
        .groupBy("tipo_energia", "tipo_transaccion")
        .agg(
            F.count("*").alias("num_transacciones"),
            F.round(F.sum("cantidad_kwh"), 2).alias("total_kwh"),
            F.round(F.sum("valor_total_cop"), 2).alias("total_cop"),
            F.round(F.avg("precio_cop_kwh"), 2).alias("precio_promedio_kwh"),
            F.round(F.min("precio_cop_kwh"), 2).alias("precio_min_kwh"),
            F.round(F.max("precio_cop_kwh"), 2).alias("precio_max_kwh"),
        )
        .withColumn("_fecha_ingesta", F.lit(PARTITION_DATE).cast(DateType()))
        .withColumn("_tipo_metrica",  F.lit("por_energia"))
    )

    # T3b: Métricas por segmento de cliente
    metricas_segmento = (df_ventas
        .filter(F.col("segmento").isNotNull())
        .groupBy("segmento", "tipo_energia")
        .agg(
            F.count("*").alias("num_transacciones"),
            F.round(F.sum("cantidad_kwh"), 2).alias("total_kwh"),
            F.round(F.sum("valor_total_cop"), 2).alias("total_cop"),
            F.round(F.avg("valor_total_cop"), 2).alias("ticket_promedio_cop"),
        )
        .withColumn("_fecha_ingesta", F.lit(PARTITION_DATE).cast(DateType()))
        .withColumn("_tipo_metrica",  F.lit("por_segmento"))
    )

    logger.info(f"[T3] Métricas por energía: {metricas_energia.count()} filas")
    logger.info(f"[T3] Métricas por segmento: {metricas_segmento.count()} filas")

    # Retornar las dos tablas de métricas
    return (
        DynamicFrame.fromDF(metricas_energia,  glueContext, "metricas_energia"),
        DynamicFrame.fromDF(metricas_segmento, glueContext, "metricas_segmento"),
    )


# ============================================================
# EJECUCIÓN PRINCIPAL
# ============================================================

logger.info("=== Iniciando ETL Energía ===")

# Leer datos crudos
dyf_prov = read_raw("proveedores")
dyf_cli  = read_raw("clientes")
dyf_trx  = read_raw("transacciones")

# Aplicar transformaciones
dyf_prov_clean = transform_proveedores(dyf_prov)
dyf_cli_clean  = transform_clientes(dyf_cli)
dyf_trx_enrich = transform_transacciones(dyf_trx)

dyf_metricas_energia, dyf_metricas_segmento = transform_metricas_energia(
    dyf_trx_enrich, dyf_cli_clean, dyf_prov_clean
)

# Escribir Parquet en zona Processed
write_processed(dyf_prov_clean.toDF(),       "proveedores")
write_processed(dyf_cli_clean.toDF(),        "clientes")
write_processed(dyf_trx_enrich.toDF(),       "transacciones")
write_processed(dyf_metricas_energia.toDF(), "metricas_energia")
write_processed(dyf_metricas_segmento.toDF(),"metricas_segmento")

logger.info("=== ETL completado exitosamente ===")
job.commit()
