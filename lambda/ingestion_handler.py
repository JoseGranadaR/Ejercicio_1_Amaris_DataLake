"""
ingestion_handler.py
Lambda: recibe eventos SQS ← S3 put en landing bucket.
Copia el CSV a Raw con partición fecha=YYYY-MM-DD y dispara el Glue Job.
"""
import json
import os
import urllib.parse
import logging
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3   = boto3.client("s3")
glue = boto3.client("glue")

RAW_BUCKET = os.environ["RAW_BUCKET"]
GLUE_MAP   = {
    "proveedores":   os.environ.get("GLUE_JOB_PROV",     ""),
    "clientes":      os.environ.get("GLUE_JOB_CLIENTES", ""),
    "transacciones": os.environ.get("GLUE_JOB_TX",       ""),
}


def handler(event, context):
    results = {"success": 0, "failed": 0}
    for record in event["Records"]:
        try:
            body     = json.loads(record["body"])
            s3_event = json.loads(body.get("Message", json.dumps(body)))
            for s3rec in s3_event.get("Records", []):
                bucket = s3rec["s3"]["bucket"]["name"]
                key    = urllib.parse.unquote_plus(s3rec["s3"]["object"]["key"])
                _process_file(bucket, key)
                results["success"] += 1
        except Exception as e:
            logger.error(f"Error procesando record: {e}", exc_info=True)
            results["failed"] += 1
            raise  # Re-lanza para que SQS reintente

    logger.info(f"Batch finalizado: {results}")
    return results


def _process_file(src_bucket: str, src_key: str):
    filename  = os.path.basename(src_key)
    table     = filename.replace(".csv", "").lower()
    today     = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dest_key  = f"{table}/fecha={today}/{filename}"

    logger.info(f"Copiando: s3://{src_bucket}/{src_key} → s3://{RAW_BUCKET}/{dest_key}")

    # Copiar CSV a Raw con partición por fecha
    s3.copy_object(
        CopySource        = {"Bucket": src_bucket, "Key": src_key},
        Bucket            = RAW_BUCKET,
        Key               = dest_key,
        MetadataDirective = "REPLACE",
        Metadata          = {
            "source-bucket": src_bucket,
            "source-key":    src_key,
            "ingested-at":   datetime.now(timezone.utc).isoformat(),
            "tabla":         table,
        },
    )
    logger.info(f"Archivo disponible en Raw: s3://{RAW_BUCKET}/{dest_key}")

    # Disparar Glue Job de transformación
    glue_job = GLUE_MAP.get(table)
    if glue_job:
        resp = glue.start_job_run(
            JobName   = glue_job,
            Arguments = {
                "--INPUT_PATH": f"s3://{RAW_BUCKET}/{dest_key}",
                "--PARTITION":  today,
            },
        )
        logger.info(f"Glue Job '{glue_job}' iniciado → RunId: {resp['JobRunId']}")
    else:
        logger.warning(f"Sin Glue Job mapeado para tabla='{table}'. Solo se copió a Raw.")
