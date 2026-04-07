"""
scripts/run_glue_job.py
=======================
Lanza el job ETL de AWS Glue, espera a que termine y reporta el resultado.
Lee la configuración desde el archivo .env del proyecto.

Uso:
    python scripts/run_glue_job.py                         # usa .env, fecha=hoy
    python scripts/run_glue_job.py --date 2024-01-15       # partición específica
    python scripts/run_glue_job.py --no-wait               # lanza sin esperar
"""

import os
import sys
import time
import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

POLL_INTERVAL_SEC = 15
TIMEOUT_SEC       = 600  # 10 minutos


def parse_args():
    p = argparse.ArgumentParser(description="Lanza el job ETL de Glue")
    p.add_argument("--env-file", default=".env")
    p.add_argument("--date",     default=None, help="Fecha de partición YYYY-MM-DD")
    p.add_argument("--no-wait",  action="store_true", help="No esperar al resultado")
    return p.parse_args()


def load_config(env_file: str) -> dict:
    load_dotenv(Path(env_file), override=True)
    job_name = os.getenv("GLUE_JOB_NAME")
    if not job_name:
        log.error("La variable GLUE_JOB_NAME no está definida en .env")
        log.error("Después de 'make deploy', copie el valor de glue_etl_job al .env")
        sys.exit(1)
    return {
        "job_name": job_name,
        "region":   os.getenv("AWS_REGION", "us-east-1"),
        "profile":  os.getenv("AWS_PROFILE") or None,
        "env":      os.getenv("ENVIRONMENT", "dev"),
    }


def main():
    args   = parse_args()
    config = load_config(args.env_file)
    date   = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    import boto3
    session = boto3.Session(region_name=config["region"], profile_name=config["profile"])
    glue    = session.client("glue")

    log.info(f"Lanzando job: {config['job_name']}")
    log.info(f"Partición:    {date}")

    try:
        response   = glue.start_job_run(
            JobName=config["job_name"],
            Arguments={"--PARTITION_DATE": date, "--ENVIRONMENT": config["env"]},
        )
        run_id = response["JobRunId"]
        log.info(f"JobRunId:     {run_id}")
    except Exception as e:
        log.error(f"Error lanzando el job: {e}")
        sys.exit(1)

    if args.no_wait:
        log.info("Modo --no-wait: el job sigue ejecutándose en AWS.")
        log.info("Verifique el estado en la consola de Glue o con:")
        log.info(f"  aws glue get-job-run --job-name {config['job_name']} --run-id {run_id}")
        return

    # Esperar al resultado
    log.info("Esperando resultado (esto puede tardar 3-8 minutos)...")
    elapsed = 0

    while elapsed < TIMEOUT_SEC:
        time.sleep(POLL_INTERVAL_SEC)
        elapsed += POLL_INTERVAL_SEC

        try:
            run_info = glue.get_job_run(JobName=config["job_name"], RunId=run_id)
            state    = run_info["JobRun"]["JobRunState"]
        except Exception as e:
            log.warning(f"Error consultando estado: {e}. Reintentando...")
            continue

        if state == "SUCCEEDED":
            duration = run_info["JobRun"].get("ExecutionTime", 0)
            log.info(f"✅  Job completado en {duration} segundos")
            log.info("    Los archivos Parquet están disponibles en el bucket Processed.")
            log.info("    Ejecute ahora: make crawl  y luego: make query")
            return

        elif state in ("FAILED", "ERROR", "TIMEOUT", "STOPPED"):
            msg = run_info["JobRun"].get("ErrorMessage", "Sin detalle")
            log.error(f"❌  Job terminó con estado: {state}")
            log.error(f"    Error: {msg}")
            log.error("    Ver logs completos en CloudWatch:")
            log.error(f"    /aws-glue/jobs/output → buscar RunId: {run_id}")
            sys.exit(1)

        else:
            log.info(f"Estado: {state} ({elapsed}s transcurridos)")

    log.error(f"Timeout: el job no terminó en {TIMEOUT_SEC} segundos.")
    log.error("Verifique manualmente en la consola de AWS Glue.")
    sys.exit(1)


if __name__ == "__main__":
    main()
