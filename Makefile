# ══════════════════════════════════════════════════════════════
# Makefile — Comandos simplificados del Data Lake
# Uso: make <comando>
#
# Ejemplos:
#   make setup       → configura el entorno Python por primera vez
#   make deploy      → despliega toda la infraestructura en AWS
#   make upload      → sube los CSVs de prueba a S3
#   make run-etl     → ejecuta el job de transformación Glue
#   make query       → ejecuta las 7 consultas analíticas
#   make test        → corre los tests unitarios
#   make destroy     → elimina toda la infraestructura de AWS
# ══════════════════════════════════════════════════════════════

# ── Detección automática del sistema operativo ────────────────
ifeq ($(OS),Windows_NT)
    PYTHON   := python
    VENV_BIN := .venv\Scripts
    SEP      := \\
else
    PYTHON   := python3
    VENV_BIN := .venv/bin
    SEP      := /
endif

TERRAFORM    := terraform
TF_VARS      := terraform/environments/dev/terraform.tfvars
ENV_FILE     := .env

.PHONY: help setup install install-dev layer deploy plan upload run-etl crawl query test lint format check destroy clean

# ── Ayuda ─────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  DATA LAKE AMARIS — Comandos disponibles"
	@echo "  ─────────────────────────────────────────────────"
	@echo "  PRIMERA VEZ:"
	@echo "    make setup         Crea entorno virtual e instala dependencias"
	@echo ""
	@echo "  DESPLIEGUE:"
	@echo "    make layer         Empaqueta el código Lambda para subir a AWS"
	@echo "    make plan          Previsualiza cambios en AWS (sin crear nada)"
	@echo "    make deploy        Despliega toda la infraestructura en AWS"
	@echo ""
	@echo "  PIPELINE DE DATOS:"
	@echo "    make upload        Sube los CSVs de prueba al bucket Landing"
	@echo "    make run-etl       Ejecuta el job ETL de transformación"
	@echo "    make crawl         Actualiza el catálogo de datos (Crawlers)"
	@echo "    make query         Ejecuta las 7 consultas analíticas en Athena"
	@echo "    make query Q=q5    Ejecuta solo la consulta Q5 (margen)"
	@echo ""
	@echo "  DESARROLLO:"
	@echo "    make test          Ejecuta los tests unitarios"
	@echo "    make lint          Verifica el estilo del código"
	@echo "    make format        Formatea el código automáticamente"
	@echo ""
	@echo "  LIMPIEZA:"
	@echo "    make destroy       Elimina TODA la infraestructura de AWS"
	@echo "    make clean         Limpia archivos temporales locales"
	@echo ""

# ── Configuración inicial ─────────────────────────────────────
setup:
	@echo "→ Creando entorno virtual Python..."
	$(PYTHON) -m venv .venv
	@echo "→ Instalando dependencias..."
	$(VENV_BIN)$(SEP)pip install --upgrade pip --quiet
	$(VENV_BIN)$(SEP)pip install -r requirements-dev.txt --quiet
	@echo "→ Copiando plantilla de variables..."
	@test -f $(ENV_FILE) || cp .env.example $(ENV_FILE)
	@echo ""
	@echo "✅ Entorno listo."
	@echo "   Activar entorno en Mac/Linux: source .venv/bin/activate"
	@echo "   Activar entorno en Windows:   .venv\Scripts\activate"
	@echo ""
	@echo "   Edite el archivo .env con sus valores de AWS antes de continuar."

install:
	$(VENV_BIN)$(SEP)pip install -r requirements.txt

install-dev:
	$(VENV_BIN)$(SEP)pip install -r requirements-dev.txt

# ── Empaquetado Lambda ────────────────────────────────────────
layer:
	@echo "→ Empaquetando código Lambda..."
	@cd lambda && zip -r ../terraform/lambda_ingestion.zip ingestion_handler.py
	@echo "✅ Lambda empaquetada: terraform/lambda_ingestion.zip"

# ── Terraform ─────────────────────────────────────────────────
plan:
	@echo "→ Inicializando Terraform..."
	@cd terraform && $(TERRAFORM) init -input=false
	@echo "→ Calculando cambios..."
	@cd terraform && $(TERRAFORM) plan -var-file=environments/dev/terraform.tfvars -input=false

deploy: layer
	@echo "→ Inicializando Terraform..."
	@cd terraform && $(TERRAFORM) init -input=false
	@echo "→ Desplegando infraestructura en AWS..."
	@cd terraform && $(TERRAFORM) apply \
		-var-file=environments/dev/terraform.tfvars \
		-input=false \
		-auto-approve
	@echo ""
	@echo "✅ Infraestructura desplegada."
	@echo "   Actualice el archivo .env con los valores de los Outputs mostrados arriba."

destroy:
	@echo "⚠️  Se eliminarán TODOS los recursos de AWS del proyecto."
	@echo "    Presione Ctrl+C para cancelar o espere 5 segundos..."
	@sleep 5
	@cd terraform && $(TERRAFORM) destroy \
		-var-file=environments/dev/terraform.tfvars \
		-input=false \
		-auto-approve
	@echo "✅ Infraestructura eliminada."

# ── Pipeline de datos ─────────────────────────────────────────
upload:
	@echo "→ Subiendo CSVs de prueba al bucket Landing..."
	$(VENV_BIN)$(SEP)python scripts/deploy_data.py --source data/sample --env-file $(ENV_FILE)
	@echo "✅ Datos subidos correctamente."

run-etl:
	@echo "→ Lanzando job ETL de Glue..."
	$(VENV_BIN)$(SEP)python scripts/run_glue_job.py --env-file $(ENV_FILE)

crawl:
	@echo "→ Ejecutando Crawlers de catalogación..."
	$(VENV_BIN)$(SEP)python glue_jobs/catalog_crawler_trigger.py \
		--environment dev --zone all --wait

Q ?= all
query:
	@echo "→ Ejecutando consultas Athena ($(Q))..."
	$(VENV_BIN)$(SEP)python scripts/athena_queries.py \
		--env-file $(ENV_FILE) --run $(Q)

# ── Calidad de código ─────────────────────────────────────────
test:
	@echo "→ Ejecutando tests unitarios..."
	$(VENV_BIN)$(SEP)pytest tests/ -v --cov=. --cov-report=term-missing

lint:
	$(VENV_BIN)$(SEP)flake8 scripts/ glue_jobs/ lambda/ tests/ \
		--max-line-length=100 --exclude=.venv
	$(VENV_BIN)$(SEP)mypy scripts/ --ignore-missing-imports

format:
	$(VENV_BIN)$(SEP)black scripts/ glue_jobs/ lambda/ tests/ --line-length=100
	$(VENV_BIN)$(SEP)isort scripts/ glue_jobs/ lambda/ tests/

check: lint test

# ── Limpieza ──────────────────────────────────────────────────
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.log" -delete 2>/dev/null || true
	rm -f terraform/lambda_ingestion.zip
	rm -f resultado_q*.csv
	@echo "✅ Archivos temporales eliminados."
