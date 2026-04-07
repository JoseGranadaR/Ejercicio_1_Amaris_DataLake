"""
scripts/deploy_data.py
======================
Sube los archivos CSV de data/sample/ al bucket Landing de S3
con la estructura de partición correcta (year/month/day/hour).

Lee la configuración desde el archivo .env del proyecto.

Uso:
    python scripts/deploy_data.py                         # usa .env por defecto
    python scripts/deploy_data.py --env-file .env.prod    # usa otro archivo .env
    python scripts/deploy_data.py --date 2024-03-01       # partición específica
    python scripts/deploy_data.py --dry-run               # simula sin subir
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone

# Cargar .env antes de importar boto3
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DATASETS = ["proveedores", "clientes", "transacciones"]


def parse_args():
    p = argparse.ArgumentParser(description="Sube CSVs al Data Lake Landing")
    p.add_argument("--source",   default="data/sample",  help="Carpeta con los CSVs")
    p.add_argument("--env-file", default=".env",         help="Archivo .env de configuración")
    p.add_argument("--date",     default=None,           help="Fecha de partición YYYY-MM-DD (default: hoy)")
    p.add_argument("--hour",     default="00",           help="Hora de partición HH (default: 00)")
    p.add_argument("--dry-run",  action="store_true",    help="Muestra qué subiría sin ejecutar")
    return p.parse_args()


def load_config(env_file: str) -> dict:
    """Lee el archivo .env y retorna la configuración como diccionario."""
    env_path = Path(env_file)
    if not env_path.exists():
        log.error(f"Archivo .env no encontrado: {env_file}")
        log.error("Ejecute primero: cp .env.example .env  y edite los valores.")
        sys.exit(1)

    load_dotenv(env_path, override=True)

    bucket = os.getenv("LANDING_BUCKET")
    region = os.getenv("AWS_REGION", "us-east-1")

    if not bucket:
        log.error("La variable LANDING_BUCKET no está definida en el archivo .env")
        log.error("Después de 'make deploy', copie el valor de landing_bucket al .env")
        sys.exit(1)

    return {
        "bucket": bucket,
        "region": region,
        "profile": os.getenv("AWS_PROFILE") or None,
    }


def build_s3_key(dataset: str, date_str: str, hour: str, filename: str) -> str:
    """
    Construye la clave S3 con estructura de partición Hive.
    Ejemplo: raw/ventas/year=2024/month=01/day=15/hour=00/ventas.csv
    """
    year, month, day = date_str.split("-")
    return f"raw/{dataset}/year={year}/month={month}/day={day}/hour={hour}/{filename}"


def upload_file(s3_client, local_path: Path, bucket: str, s3_key: str, dry_run: bool) -> bool:
    """Sube un archivo a S3. Retorna True si fue exitoso, False si el archivo no existe o hay error."""
    # Verificar existencia antes de cualquier operación
    if not local_path.exists():
        log.error(f"Archivo no encontrado: {local_path}")
        return False

    size_kb = local_path.stat().st_size / 1024

    if dry_run:
        log.info(f"[DRY-RUN] {local_path.name} ({size_kb:.1f} KB) → s3://{bucket}/{s3_key}")
        return True

    try:
        s3_client.upload_file(
            str(local_path),
            bucket,
            s3_key,
            ExtraArgs={
                "ContentType": "text/csv",
                "Metadata": {
                    "dataset":     local_path.stem,
                    "uploaded-at": datetime.now(timezone.utc).isoformat(),
                    "source":      "deploy_data.py",
                },
            },
        )
        log.info(f"✅  {local_path.name} ({size_kb:.1f} KB) → s3://{bucket}/{s3_key}")
        return True

    except Exception as e:
        log.error(f"❌  Error subiendo {local_path.name}: {e}")
        return False


def main():
    args = parse_args()
    config = load_config(args.env_file)

    # Determinar fecha de partición
    date_str = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        log.error(f"Formato de fecha inválido: {date_str}. Use YYYY-MM-DD")
        sys.exit(1)

    source_dir = Path(args.source)
    if not source_dir.exists():
        log.error(f"Carpeta de datos no encontrada: {source_dir}")
        sys.exit(1)

    # Cliente S3
    import boto3

    session = boto3.Session(
        region_name=config["region"],
        profile_name=config["profile"],
    )
    s3 = session.client("s3")

    # Verificar acceso al bucket antes de empezar
    if not args.dry_run:
        try:
            s3.head_bucket(Bucket=config["bucket"])
        except Exception as e:
            log.error(f"No se puede acceder al bucket '{config['bucket']}': {e}")
            log.error("Verifique que ejecutó 'make deploy' y que el .env está actualizado.")
            sys.exit(1)

    log.info(f"Partición: {date_str} / hour={args.hour}")
    log.info(f"Bucket:    {config['bucket']}")
    log.info(f"Dry-run:   {args.dry_run}")
    log.info("")

    resultados = {"ok": 0, "error": 0, "omitido": 0}

    for dataset in DATASETS:
        csv_path = source_dir / f"{dataset}.csv"

        if not csv_path.exists():
            log.warning(f"⚠️  Archivo no encontrado, omitiendo: {csv_path}")
            resultados["omitido"] += 1
            continue

        s3_key = build_s3_key(dataset, date_str, args.hour, csv_path.name)
        success = upload_file(s3, csv_path, config["bucket"], s3_key, args.dry_run)
        resultados["ok" if success else "error"] += 1

    log.info("")
    log.info(f"Resultado: {resultados['ok']} subidos, {resultados['error']} errores, {resultados['omitido']} omitidos")

    if resultados["error"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
