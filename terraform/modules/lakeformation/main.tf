# terraform/modules/lakeformation/main.tf
# AWS Lake Formation — Gobierno centralizado del Data Lake.
# Controla quién puede ver qué tabla y qué columnas.

variable "prefix"                {}
variable "account_id"            {}
variable "glue_role_arn"         {}
variable "lambda_role_arn"       {}
variable "analyst_role_arn"      {}
variable "raw_bucket_arn"        {}
variable "processed_bucket_arn"  {}
variable "curated_bucket_arn"    {}
variable "glue_database_name"    {}

# ── Configurar administradores de Lake Formation ───────────────
resource "aws_lakeformation_data_lake_settings" "main" {
  admins = ["arn:aws:iam::${var.account_id}:role/${var.prefix}-glue-role"]

  create_database_default_permissions {
    permissions = ["ALL"]
    principal   = "IAM_ALLOWED_PRINCIPALS"
  }
  create_table_default_permissions {
    permissions = ["ALL"]
    principal   = "IAM_ALLOWED_PRINCIPALS"
  }
}

# ── Registrar los buckets S3 como recursos del Data Lake ───────
resource "aws_lakeformation_resource" "raw" {
  arn      = var.raw_bucket_arn
  role_arn = var.glue_role_arn
}

resource "aws_lakeformation_resource" "processed" {
  arn      = var.processed_bucket_arn
  role_arn = var.glue_role_arn
}

resource "aws_lakeformation_resource" "curated" {
  arn      = var.curated_bucket_arn
  role_arn = var.glue_role_arn
}

# ── Permisos para el rol Glue (ETL y Crawlers) ─────────────────
resource "aws_lakeformation_permissions" "glue_database" {
  principal   = var.glue_role_arn
  permissions = ["CREATE_TABLE", "ALTER", "DROP", "DESCRIBE"]
  database { name = var.glue_database_name }
}

resource "aws_lakeformation_permissions" "glue_s3_raw" {
  principal   = var.glue_role_arn
  permissions = ["DATA_LOCATION_ACCESS"]
  data_location { arn = var.raw_bucket_arn }
}

resource "aws_lakeformation_permissions" "glue_s3_processed" {
  principal   = var.glue_role_arn
  permissions = ["DATA_LOCATION_ACCESS"]
  data_location { arn = var.processed_bucket_arn }
}

# ── Permisos analista: lectura de todas las tablas ─────────────
resource "aws_lakeformation_permissions" "analyst_tables" {
  principal   = var.analyst_role_arn
  permissions = ["SELECT", "DESCRIBE"]
  table {
    database_name = var.glue_database_name
    wildcard      = true
  }
}

# ── Seguridad a nivel de columna: clientes ─────────────────────
# El analista puede ver el número enmascarado pero no la identificación real.
resource "aws_lakeformation_permissions" "analyst_clientes_columns" {
  principal   = var.analyst_role_arn
  permissions = ["SELECT"]
  table_with_columns {
    database_name = var.glue_database_name
    name          = "clientes"
    column_names  = [
      "tipo_identificacion", "nombre", "ciudad", "segmento",
      "consumo_promedio_kwh", "fecha_vinculacion", "antiguedad_dias",
      "identificacion_enmascarada", "_fecha_ingesta",
    ]
  }
}

# ── Etiquetas de clasificación para gobierno de datos ──────────
resource "aws_lakeformation_tag" "sensitivity" {
  key    = "sensitivity"
  values = ["publico", "interno", "confidencial", "restringido"]
}

resource "aws_lakeformation_tag" "dominio" {
  key    = "dominio"
  values = ["energia", "clientes", "transacciones", "proveedores"]
}
