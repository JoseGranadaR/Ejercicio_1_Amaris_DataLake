"""
glue_jobs/redshift_pipeline.py
================================
Pipeline: Data Lake Processed → Redshift Data Warehouse
Carga las tablas Parquet transformadas en Redshift usando COPY desde S3

Tablas cargadas:
  - dim_proveedores
  - dim_clientes
  - fact_transacciones
  - agg_metricas_energia
  - agg_metricas_segmento
"""

import sys
import logging
from awsglue.utils       import getResolvedOptions
from pyspark.context     import SparkContext
from awsglue.context     import GlueContext
from awsglue.job         import Job
from awsglue.dynamicframe import DynamicFrame

logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "PROCESSED_BUCKET",
    "REDSHIFT_CONNECTION",
    "REDSHIFT_DB",
    "REDSHIFT_SCHEMA",
    "PARTITION_DATE",
    "IAM_ROLE_ARN",
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

PROCESSED_BUCKET    = args["PROCESSED_BUCKET"]
REDSHIFT_CONNECTION = args["REDSHIFT_CONNECTION"]
REDSHIFT_DB         = args["REDSHIFT_DB"]
REDSHIFT_SCHEMA     = args["REDSHIFT_SCHEMA"]
PARTITION_DATE      = args["PARTITION_DATE"]
IAM_ROLE_ARN        = args["IAM_ROLE_ARN"]

year, month, day = PARTITION_DATE.split("-")
PARTITION_PATH   = f"year={year}/month={month}/day={day}"

REDSHIFT_TABLES = {
    "proveedores":        "dim_proveedores",
    "clientes":           "dim_clientes",
    "transacciones":      "fact_transacciones",
    "metricas_energia":   "agg_metricas_energia",
    "metricas_segmento":  "agg_metricas_segmento",
}


def read_processed(dataset: str) -> DynamicFrame:
    path = f"s3://{PROCESSED_BUCKET}/processed/{dataset}/{PARTITION_PATH}/"
    logger.info(f"Leyendo Parquet: {path}")
    return glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [path], "recurse": True},
        format="parquet",
        transformation_ctx=f"read_{dataset}",
    )


def write_redshift(dyf: DynamicFrame, table_name: str):
    """Escribe en Redshift usando JDBC a través de la conexión Glue."""
    full_table = f"{REDSHIFT_SCHEMA}.{table_name}"
    logger.info(f"Cargando en Redshift: {full_table} ({dyf.count()} filas)")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dyf,
        catalog_connection=REDSHIFT_CONNECTION,
        connection_options={
            "dbtable":      full_table,
            "database":     REDSHIFT_DB,
            "preactions":   f"TRUNCATE TABLE {full_table};",
            "extracopyoptions": f"IAM_ROLE '{IAM_ROLE_ARN}' FORMAT AS PARQUET",
        },
        redshift_tmp_dir=f"s3://{PROCESSED_BUCKET}/tmp/redshift/",
        transformation_ctx=f"write_{table_name}",
    )
    logger.info(f"Carga exitosa: {full_table}")


logger.info("=== Iniciando pipeline Processed → Redshift ===")

for dataset, redshift_table in REDSHIFT_TABLES.items():
    try:
        dyf = read_processed(dataset)
        if dyf.count() == 0:
            logger.warning(f"Sin datos para {dataset} en {PARTITION_DATE}, omitiendo.")
            continue
        write_redshift(dyf, redshift_table)
    except Exception as e:
        logger.error(f"Error cargando {dataset}: {e}")
        raise

logger.info("=== Pipeline Redshift completado ===")
job.commit()
