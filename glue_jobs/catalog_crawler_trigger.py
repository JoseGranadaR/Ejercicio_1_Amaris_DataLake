"""
glue_jobs/catalog_crawler_trigger.py
=====================================
Script Python para disparar los Glue Crawlers programáticamente
(complemento al Crawler configurado vía Terraform/IaC)

Uso:
  python catalog_crawler_trigger.py --environment dev --wait
  python catalog_crawler_trigger.py --zone processed
"""

import boto3
import time
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


CRAWLERS = {
    "raw":       "amaris-{env}-raw-crawler",
    "processed": "amaris-{env}-processed-crawler",
    "curated":   "amaris-{env}-curated-crawler",
}


def start_crawler(glue_client, crawler_name: str) -> bool:
    """Inicia un Crawler de Glue. Retorna True si se inició correctamente."""
    try:
        state = glue_client.get_crawler(Name=crawler_name)["Crawler"]["State"]
        if state == "RUNNING":
            logger.info(f"Crawler ya en ejecución: {crawler_name}")
            return True

        glue_client.start_crawler(Name=crawler_name)
        logger.info(f"Crawler iniciado: {crawler_name}")
        return True

    except glue_client.exceptions.EntityNotFoundException:
        logger.error(f"Crawler no encontrado: {crawler_name}")
        return False
    except Exception as e:
        logger.error(f"Error iniciando {crawler_name}: {e}")
        return False


def wait_for_crawler(glue_client, crawler_name: str, timeout_sec: int = 600) -> str:
    """Espera a que el Crawler termine. Retorna estado final."""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        time.sleep(15)
        response = glue_client.get_crawler(Name=crawler_name)
        state    = response["Crawler"]["State"]
        logger.info(f"  {crawler_name}: {state}")
        if state in ("READY", "STOPPING"):
            last_crawl = response["Crawler"].get("LastCrawl", {})
            status     = last_crawl.get("Status", "UNKNOWN")
            logger.info(f"  Resultado: {status} — {last_crawl.get('MessagePrefix', '')}")
            return status
    logger.warning(f"Timeout esperando: {crawler_name}")
    return "TIMEOUT"


def run_crawlers(environment: str, zone: str = "all", wait: bool = True):
    """Ejecuta los crawlers del Data Lake y opcionalmente espera su finalización."""
    glue   = boto3.client("glue")
    zones  = [zone] if zone != "all" else list(CRAWLERS.keys())
    report = []

    for z in zones:
        name = CRAWLERS[z].format(env=environment)
        started = start_crawler(glue, name)
        if started and wait:
            status = wait_for_crawler(glue, name)
            report.append({"zone": z, "crawler": name, "status": status})
        else:
            report.append({"zone": z, "crawler": name, "status": "STARTED" if started else "FAILED"})

    logger.info("\n=== Resumen Crawlers ===")
    for r in report:
        emoji = "✅" if r["status"] in ("SUCCEEDED", "STARTED") else "❌"
        logger.info(f"  {emoji} {r['zone']:<12} {r['status']}")

    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dispara Glue Crawlers del Data Lake Amaris")
    parser.add_argument("--environment", default="dev",  help="dev | staging | prod")
    parser.add_argument("--zone",        default="all",  help="raw | processed | curated | all")
    parser.add_argument("--wait",        action="store_true", help="Esperar a que terminen")
    args = parser.parse_args()

    run_crawlers(args.environment, args.zone, args.wait)
