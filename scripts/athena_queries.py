"""
scripts/athena_queries.py
==========================
Ejecuta consultas SQL sobre los datos procesados del Data Lake usando Amazon Athena.
Lee la configuración desde el archivo .env del proyecto.

Consultas disponibles:
  q1  Total de energía comprada por tipo (kWh y COP)
  q2  Top 5 clientes por consumo
  q3  Precio promedio por tipo de energía y mercado
  q4  Transacciones pendientes (alerta operacional)
  q5  Margen estimado compra vs venta por tipo de energía
  q6  Distribución de clientes por segmento y ciudad
  q7  Proveedores activos con mayor capacidad instalada

Uso:
    python scripts/athena_queries.py                        # todas las consultas
    python scripts/athena_queries.py --run q5               # solo la consulta Q5
    python scripts/athena_queries.py --run q1,q3,q5         # varias consultas
    python scripts/athena_queries.py --run all --output csv # guardar como CSV
"""

import os
import sys
import csv
import json
import time
import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Consultas SQL ──────────────────────────────────────────────
QUERIES = {
    "q1": {
        "titulo": "Total de energía comprada por tipo (kWh y COP)",
        "sql": """
            SELECT
                tipo_energia,
                COUNT(*)                          AS num_transacciones,
                ROUND(SUM(cantidad_kwh), 2)       AS total_kwh,
                ROUND(SUM(valor_total_cop), 2)    AS total_cop,
                ROUND(AVG(precio_cop_kwh), 2)     AS precio_promedio_kwh
            FROM processed.transacciones
            WHERE tipo_transaccion = 'compra'
              AND estado = 'completada'
            GROUP BY tipo_energia
            ORDER BY total_kwh DESC
        """,
    },
    "q2": {
        "titulo": "Top 5 clientes por consumo",
        "sql": """
            SELECT
                nombre_cliente_proveedor          AS cliente,
                COUNT(*)                          AS num_compras,
                ROUND(SUM(cantidad_kwh), 2)       AS total_kwh_consumido,
                ROUND(SUM(valor_total_cop), 2)    AS total_pagado_cop,
                clasificacion_volumen
            FROM processed.transacciones
            WHERE tipo_transaccion = 'venta'
              AND estado = 'completada'
            GROUP BY nombre_cliente_proveedor, clasificacion_volumen
            ORDER BY total_kwh_consumido DESC
            LIMIT 5
        """,
    },
    "q3": {
        "titulo": "Precio promedio por tipo de energía y mercado",
        "sql": """
            SELECT
                tipo_energia,
                mercado,
                tipo_transaccion,
                ROUND(AVG(precio_cop_kwh), 2)     AS precio_promedio,
                ROUND(MIN(precio_cop_kwh), 2)     AS precio_min,
                ROUND(MAX(precio_cop_kwh), 2)     AS precio_max,
                COUNT(*)                          AS num_operaciones
            FROM processed.transacciones
            WHERE estado = 'completada'
            GROUP BY tipo_energia, mercado, tipo_transaccion
            ORDER BY tipo_energia, mercado
        """,
    },
    "q4": {
        "titulo": "Transacciones pendientes (alerta operacional)",
        "sql": """
            SELECT
                id_transaccion,
                tipo_transaccion,
                nombre_cliente_proveedor,
                ROUND(cantidad_kwh, 2)            AS kwh,
                ROUND(valor_total_cop, 2)         AS valor_cop,
                tipo_energia,
                fecha_transaccion,
                DATEDIFF(current_date, fecha_transaccion) AS dias_pendiente
            FROM processed.transacciones
            WHERE estado = 'pendiente'
            ORDER BY dias_pendiente DESC, valor_cop DESC
        """,
    },
    "q5": {
        "titulo": "Margen estimado compra vs venta por tipo de energía",
        "sql": """
            WITH compras AS (
                SELECT tipo_energia,
                       ROUND(AVG(precio_cop_kwh), 4) AS precio_compra,
                       SUM(cantidad_kwh)             AS kwh_comprado
                FROM processed.transacciones
                WHERE tipo_transaccion = 'compra' AND estado = 'completada'
                GROUP BY tipo_energia
            ),
            ventas AS (
                SELECT tipo_energia,
                       ROUND(AVG(precio_cop_kwh), 4) AS precio_venta,
                       SUM(cantidad_kwh)             AS kwh_vendido
                FROM processed.transacciones
                WHERE tipo_transaccion = 'venta' AND estado = 'completada'
                GROUP BY tipo_energia
            )
            SELECT
                c.tipo_energia,
                c.precio_compra,
                v.precio_venta,
                ROUND(v.precio_venta - c.precio_compra, 4)                    AS margen_cop_kwh,
                ROUND((v.precio_venta - c.precio_compra) / c.precio_compra * 100, 2) AS margen_pct,
                c.kwh_comprado,
                v.kwh_vendido
            FROM compras c
            JOIN ventas  v ON c.tipo_energia = v.tipo_energia
            ORDER BY margen_pct DESC
        """,
    },
    "q6": {
        "titulo": "Distribución de clientes por segmento y ciudad",
        "sql": """
            SELECT
                segmento,
                ciudad,
                COUNT(*)                               AS num_clientes,
                ROUND(AVG(consumo_promedio_kwh), 2)    AS consumo_promedio_kwh,
                ROUND(SUM(consumo_promedio_kwh), 2)    AS consumo_total_kwh
            FROM processed.clientes
            GROUP BY segmento, ciudad
            ORDER BY segmento, consumo_total_kwh DESC
        """,
    },
    "q7": {
        "titulo": "Proveedores activos con mayor capacidad instalada",
        "sql": """
            SELECT
                nombre_proveedor,
                tipo_energia,
                pais_origen,
                capacidad_mw,
                fecha_registro
            FROM processed.proveedores
            WHERE activo = true
            ORDER BY capacidad_mw DESC
        """,
    },
}


# ── Configuración ──────────────────────────────────────────────

def load_config(env_file: str) -> dict:
    load_dotenv(Path(env_file), override=True)

    required = ["ATHENA_WORKGROUP", "GLUE_DATABASE", "ATHENA_RESULTS_BUCKET"]
    missing  = [k for k in required if not os.getenv(k)]
    if missing:
        log.error(f"Variables no definidas en {env_file}: {', '.join(missing)}")
        log.error("Ejecute 'make deploy' y copie los outputs al archivo .env")
        sys.exit(1)

    return {
        "database":       os.getenv("GLUE_DATABASE"),
        "workgroup":      os.getenv("ATHENA_WORKGROUP"),
        "results_bucket": os.getenv("ATHENA_RESULTS_BUCKET"),
        "region":         os.getenv("AWS_REGION", "us-east-1"),
        "profile":        os.getenv("AWS_PROFILE") or None,
    }


# ── Cliente Athena ─────────────────────────────────────────────

class AthenaRunner:
    def __init__(self, config: dict):
        import boto3
        session    = boto3.Session(region_name=config["region"], profile_name=config["profile"])
        self.client = session.client("athena")
        self.config = config

    def run(self, sql: str, query_id: str, timeout: int = 120) -> list[dict] | None:
        # Reemplazar 'processed.' por el nombre real de la base de datos
        sql_final = sql.replace("processed.", f"{self.config['database']}.")

        response = self.client.start_query_execution(
            QueryString=sql_final,
            WorkGroup=self.config["workgroup"],
            QueryExecutionContext={"Database": self.config["database"]},
            ResultConfiguration={
                "OutputLocation": f"s3://{self.config['results_bucket']}/results/{query_id}/",
            },
        )
        exec_id  = response["QueryExecutionId"]
        deadline = time.time() + timeout

        while time.time() < deadline:
            time.sleep(2)
            status = self.client.get_query_execution(QueryExecutionId=exec_id)
            state  = status["QueryExecution"]["Status"]["State"]

            if state == "SUCCEEDED":
                return self._fetch(exec_id)
            elif state in ("FAILED", "CANCELLED"):
                reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
                log.error(f"  Query falló: {reason}")
                return None

        log.error("  Timeout esperando resultado de Athena")
        return None

    def _fetch(self, exec_id: str) -> list[dict]:
        rows, headers, kwargs = [], None, {"QueryExecutionId": exec_id, "MaxResults": 1000}
        while True:
            r       = self.client.get_query_results(**kwargs)
            r_rows  = r["ResultSet"]["Rows"]
            if headers is None:
                headers = [c["VarCharValue"] for c in r_rows[0]["Data"]]
                r_rows  = r_rows[1:]
            for row in r_rows:
                rows.append(dict(zip(headers, [c.get("VarCharValue", "") for c in row["Data"]])))
            if not (token := r.get("NextToken")):
                break
            kwargs["NextToken"] = token
        return rows


# ── Presentación de resultados ─────────────────────────────────

def print_table(rows: list[dict], max_rows: int = 25):
    if not rows:
        print("  (sin resultados)")
        return
    disp   = rows[:max_rows]
    cols   = list(disp[0].keys())
    widths = {c: max(len(c), max((len(str(r.get(c, ""))) for r in disp), default=0)) for c in cols}
    sep    = "┼".join("─" * (widths[c] + 2) for c in cols)
    hdr    = "│".join(f" {c.ljust(widths[c])} " for c in cols)
    print(f"┌{sep}┐")
    print(f"│{hdr}│")
    print(f"├{sep}┤")
    for row in disp:
        line = "│".join(f" {str(row.get(c, '')).ljust(widths[c])} " for c in cols)
        print(f"│{line}│")
    print(f"└{sep}┘")
    if len(rows) > max_rows:
        print(f"  ... y {len(rows) - max_rows} filas más")


def save_csv(rows: list[dict], filename: str):
    if not rows:
        return
    with open(filename, "w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=rows[0].keys()).writeheader()
        csv.DictWriter(f, fieldnames=rows[0].keys()).writerows(rows)
    log.info(f"  Guardado: {filename}")


# ── Main ───────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Consultas Athena — Data Lake Amaris")
    p.add_argument("--env-file", default=".env")
    p.add_argument("--run",      default="all", help="all | q1,q3,q5 | q2")
    p.add_argument("--output",   default="table", choices=["table", "csv", "json"])
    return p.parse_args()


def main():
    args    = parse_args()
    config  = load_config(args.env_file)
    runner  = AthenaRunner(config)
    ids     = list(QUERIES.keys()) if args.run == "all" else [q.strip() for q in args.run.split(",")]
    invalid = [q for q in ids if q not in QUERIES]
    if invalid:
        log.error(f"Consultas no reconocidas: {invalid}. Opciones: {list(QUERIES.keys())}")
        sys.exit(1)

    print(f"\n  Base de datos: {config['database']}")
    print(f"  Workgroup:     {config['workgroup']}")
    print(f"  Consultas:     {len(ids)}\n")

    for qid in ids:
        q = QUERIES[qid]
        print(f"\n{'═'*65}")
        print(f"  {qid.upper()}  —  {q['titulo']}")
        print(f"{'═'*65}")

        rows = runner.run(q["sql"].strip(), query_id=qid)
        if rows is None:
            continue

        if args.output == "table":
            print_table(rows)
        elif args.output == "json":
            print(json.dumps(rows, indent=2, ensure_ascii=False))
        elif args.output == "csv":
            filename = f"resultado_{qid}.csv"
            save_csv(rows, filename)
            print(f"  {len(rows)} filas guardadas en {filename}")

    print("\n  ✅  Consultas completadas\n")


if __name__ == "__main__":
    main()
