# terraform/modules/glue/main.tf
# Catálogo de datos, Crawlers y ETL Job de AWS Glue.

variable "prefix"           { description = "Prefijo del proyecto" }
variable "region"           { description = "Región AWS" }
variable "glue_role_arn"    { description = "ARN del rol IAM para Glue" }
variable "raw_bucket"       { description = "Nombre del bucket Raw" }
variable "processed_bucket" { description = "Nombre del bucket Processed" }
variable "curated_bucket"   { description = "Nombre del bucket Curated" }
variable "scripts_bucket"   { description = "Nombre del bucket de scripts" }
variable "etl_script_key"   { description = "Ruta del script ETL en S3" }
variable "kms_key_arn"      { description = "ARN de la llave KMS" }

# ── Bases de datos del catálogo (una por zona) ─────────────────
resource "aws_glue_catalog_database" "raw" {
  name        = "${replace(var.prefix, "-", "_")}_raw"
  description = "Zona Raw — CSVs originales catalogados"
}

resource "aws_glue_catalog_database" "processed" {
  name        = "${replace(var.prefix, "-", "_")}_processed"
  description = "Zona Processed — Parquet transformado"
}

resource "aws_glue_catalog_database" "curated" {
  name        = "${replace(var.prefix, "-", "_")}_curated"
  description = "Zona Curated — métricas y agregados"
}

# ── Crawlers — detección automática de esquemas ────────────────
resource "aws_glue_crawler" "raw" {
  database_name = aws_glue_catalog_database.raw.name
  name          = "${var.prefix}-raw-crawler"
  role          = var.glue_role_arn
  description   = "Cataloga esquemas CSV en zona Raw"
  schedule      = "cron(0 * * * ? *)"   # Cada hora en punto

  s3_target { path = "s3://${var.raw_bucket}/raw/" }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}

resource "aws_glue_crawler" "processed" {
  database_name = aws_glue_catalog_database.processed.name
  name          = "${var.prefix}-processed-crawler"
  role          = var.glue_role_arn
  description   = "Cataloga tablas Parquet en zona Processed"
  schedule      = "cron(30 * * * ? *)"  # Cada hora a los :30

  s3_target { path = "s3://${var.processed_bucket}/processed/" }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}

resource "aws_glue_crawler" "curated" {
  database_name = aws_glue_catalog_database.curated.name
  name          = "${var.prefix}-curated-crawler"
  role          = var.glue_role_arn
  schedule      = "cron(0 2 * * ? *)"   # Diario a las 2 AM

  s3_target { path = "s3://${var.curated_bucket}/curated/" }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "DEPRECATE_IN_DATABASE"
  }
}

# ── ETL Job — aplica las 3 transformaciones ────────────────────
resource "aws_glue_job" "etl_transform" {
  name        = "${var.prefix}-etl-energia-transform"
  role_arn    = var.glue_role_arn
  description = "ETL: CSV Raw → Parquet Processed (3 transformaciones)"

  command {
    script_location = "s3://${var.scripts_bucket}/${var.etl_script_key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--RAW_BUCKET"                       = var.raw_bucket
    "--PROCESSED_BUCKET"                 = var.processed_bucket
    "--GLUE_DATABASE"                    = aws_glue_catalog_database.processed.name
    "--PARTITION_DATE"                   = "2024-01-15"
    "--TempDir"                          = "s3://${var.scripts_bucket}/tmp/"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  max_retries       = 1
  timeout           = 60
}

# ── EventBridge: dispara el ETL cuando la Lambda termina ───────
resource "aws_cloudwatch_event_rule" "trigger_etl" {
  name        = "${var.prefix}-trigger-etl"
  description = "Dispara el job ETL cuando la Lambda de ingesta completa"
  event_pattern = jsonencode({
    source      = ["amaris.datalake.ingestion"]
    detail-type = ["CSVIngestionCompleted"]
  })
}

resource "aws_iam_role" "eventbridge_glue" {
  name = "${var.prefix}-eventbridge-glue"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "events.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "eventbridge_glue" {
  name = "start-glue-job"
  role = aws_iam_role.eventbridge_glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = ["glue:StartJobRun"], Resource = aws_glue_job.etl_transform.arn }]
  })
}

resource "aws_cloudwatch_event_target" "etl_job" {
  rule     = aws_cloudwatch_event_rule.trigger_etl.name
  arn      = aws_glue_job.etl_transform.arn
  role_arn = aws_iam_role.eventbridge_glue.arn
}

output "raw_database"      { value = aws_glue_catalog_database.raw.name }
output "processed_database"{ value = aws_glue_catalog_database.processed.name }
output "curated_database"  { value = aws_glue_catalog_database.curated.name }
output "etl_job_name"      { value = aws_glue_job.etl_transform.name }
