# Ejercicio 1 — Data Lake en AWS para Comercializadora de Energía

Pipeline de datos completo para procesar información de proveedores, clientes y
transacciones de energía eléctrica. Carga automática de CSVs, tres transformaciones
con AWS Glue, catálogo automático y consultas analíticas con Amazon Athena.

---

## Estructura del proyecto

```
Ejercicio_1_Amaris_DataLake/
├── .github/
│   └── workflows/
│       └── deploy.yml             # CI/CD: lint → test → plan → apply
├── terraform/
│   ├── main.tf                    # Infraestructura principal
│   ├── variables.tf               # Definición de variables
│   ├── outputs.tf                 # Valores exportados al finalizar
│   ├── modules/
│   │   ├── s3/main.tf             # Módulo S3 (4 zonas del Data Lake)
│   │   ├── iam/main.tf            # Roles y políticas de mínimo privilegio
│   │   ├── glue/main.tf           # Crawlers + ETL Job
│   │   ├── lakeformation/main.tf  # Gobierno de datos centralizado
│   │   └── redshift/main.tf       # Data Warehouse (plus)
│   └── environments/
│       └── dev/
│           └── terraform.tfvars   # ← ÚNICO ARCHIVO QUE DEBE EDITAR
├── glue_jobs/
│   ├── etl_energia_transform.py   # 3 transformaciones CSV → Parquet (Glue Spark)
│   ├── catalog_crawler_trigger.py # Dispara los Crawlers de catalogación
│   └── redshift_pipeline.py       # Carga Processed → Redshift (plus)
├── lambda/
│   └── ingestion_handler.py       # Ingesta periódica de CSVs cada hora
├── scripts/
│   ├── deploy_data.py             # Sube los CSVs de prueba a S3
│   ├── run_glue_job.py            # Lanza y monitorea el job ETL
│   ├── athena_queries.py          # 7 consultas analíticas sobre los datos
│   └── redshift_ddl.sql           # Schema del Data Warehouse
├── data/
│   └── sample/
│       ├── proveedores.csv        # Datos ficticios de generadores
│       ├── clientes.csv           # Datos ficticios de clientes
│       └── transacciones.csv      # Datos ficticios de compras y ventas
├── tests/
│   ├── __init__.py
│   ├── test_etl.py                # Tests unitarios de las 3 transformaciones
│   └── test_deploy_data.py        # Tests del script de carga (con moto)
├── docs/
│   └── pipeline_documentation.md  # Documentación técnica completa
├── .env.example                   # Plantilla de variables de entorno
├── .gitignore
├── Makefile                       # Comandos simplificados
├── pytest.ini                     # Configuración de tests
├── requirements.txt               # Dependencias de producción
└── requirements-dev.txt           # Dependencias de desarrollo y testing
```

---

## Requisitos previos

| Herramienta | Versión mínima | Cómo verificar         |
|-------------|---------------|------------------------|
| Python      | 3.10          | `python3 --version`    |
| Terraform   | 1.5.0         | `terraform --version`  |
| AWS CLI     | 2.x           | `aws --version`        |
| Git         | 2.x           | `git --version`        |
| Make        | 3.x           | `make --version`       |

> **Windows:** instale Make con `winget install GnuWin32.Make` o use Git Bash.

---

## Despliegue desde cero — 6 pasos

### Paso 0 — Clonar y configurar AWS

```bash
git clone https://github.com/JoseGranadaR/Ejercicio_1_Amaris_DataLake
cd Ejercicio_1_Amaris_DataLake

# Configurar credenciales de AWS (solo la primera vez)
aws configure
```

### Paso 1 — Configurar las variables del proyecto

Abra el archivo `terraform/environments/dev/terraform.tfvars` y edite las cuatro líneas:

```hcl
aws_region   = "us-east-1"      # Región donde se desplegará todo
project_name = "amaris"         # Prefijo para todos los recursos en AWS
environment  = "dev"            # dev | staging | prod
alert_email  = "su@email.com"   # Email para alertas de CloudWatch
```

> Este es el **único archivo** que debe editar antes del despliegue.

### Paso 2 — Preparar el entorno Python

```bash
make setup
```

Esto crea el entorno virtual `.venv/`, instala todas las dependencias y copia
`.env.example` a `.env`. A partir de aquí puede activar el entorno:

```bash
# Mac / Linux
source .venv/bin/activate

# Windows (PowerShell)
.venv\Scripts\Activate.ps1
```

### Paso 3 — Ejecutar los tests

```bash
make test
```

Todos los tests deben pasar antes de desplegar. Los tests no necesitan AWS real,
usan `moto` para simular los servicios de Amazon localmente.

### Paso 4 — Desplegar la infraestructura en AWS

```bash
make deploy
```

Este comando empaqueta el código Lambda y ejecuta `terraform apply`. Tarda entre
5 y 10 minutos. Al finalizar muestra los nombres de todos los recursos creados.

**Copie los valores del output al archivo `.env`:**

```bash
# El output de terraform se verá así:
# landing_bucket     = "amaris-dev-landing"
# processed_bucket   = "amaris-dev-processed"
# glue_etl_job       = "amaris-dev-etl-energia-transform"
# athena_workgroup   = "amaris-dev"
# glue_database      = "amaris_datalake_dev_processed"
```

### Paso 5 — Ejecutar el pipeline completo

```bash
# Subir los CSVs de prueba al Data Lake
make upload

# Ejecutar las transformaciones ETL (tarda 3-5 minutos)
make run-etl

# Actualizar el catálogo de datos
make crawl

# Consultar los datos con Athena
make query
```

Para consultas individuales:

```bash
make query Q=q5          # Solo el análisis de margen
make query Q=q1,q3       # Energía comprada + precios
```

### Paso 6 — Eliminar todo al terminar

```bash
make destroy
```

---

## Consultas disponibles

| Código | Pregunta que responde |
|--------|-----------------------|
| q1 | ¿Cuántos kWh se compraron de cada tipo de energía y cuánto se pagó? |
| q2 | ¿Cuáles son los 5 clientes con mayor consumo? |
| q3 | ¿Cuál es el precio promedio por tipo de energía y mercado? |
| q4 | ¿Qué transacciones están pendientes y cuántos días llevan? |
| q5 | ¿Cuál es el margen entre precio de compra y precio de venta? |
| q6 | ¿Cómo se distribuyen los clientes por ciudad y segmento? |
| q7 | ¿Cuáles son los proveedores activos con mayor capacidad instalada? |

---

## CI/CD con GitHub Actions

El archivo `.github/workflows/deploy.yml` automatiza el ciclo completo:

| Evento | Acciones |
|--------|----------|
| Pull Request | Lint + Tests + Terraform Plan (publicado como comentario en el PR) |
| Merge a `main` | Lint + Tests + Terraform Apply + Upload de datos |

**Configurar en GitHub → Settings → Secrets:**

| Secret | Valor |
|--------|-------|
| `AWS_ROLE_ARN` | ARN del rol IAM con permisos de despliegue |
| `AWS_REGION` | Región AWS (ej: `us-east-1`) |
| `TF_VAR_alert_email` | Email para alertas |

---

## Solución de problemas comunes

| Síntoma | Causa | Solución |
|---------|-------|----------|
| `make: command not found` | Make no instalado | Mac: `brew install make` / Windows: `winget install GnuWin32.Make` |
| `ModuleNotFoundError: dotenv` | Entorno virtual no activado | Ejecutar `source .venv/bin/activate` |
| `LANDING_BUCKET not defined in .env` | .env no actualizado tras el despliegue | Copiar los outputs de `make deploy` al archivo `.env` |
| `terraform: command not found` | Terraform no instalado | Seguir instrucciones en terraform.io/downloads |
| `NoCredentialsError` | AWS no configurado | Ejecutar `aws configure` |
| Glue job estado `FAILED` | CSVs no subidos o ruta incorrecta | Ejecutar `make upload` antes de `make run-etl` |
| Athena `TABLE_NOT_FOUND` | Crawlers no ejecutados | Ejecutar `make crawl` después del ETL |
