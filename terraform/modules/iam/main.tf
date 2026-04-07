# terraform/modules/iam/main.tf
# Roles IAM con principio de mínimo privilegio para cada componente del Data Lake.

variable "prefix"            { description = "Prefijo del proyecto (ej: amaris-dev)" }
variable "account_id"        { description = "ID de la cuenta AWS" }
variable "region"            { description = "Región AWS" }
variable "kms_key_arn"       { description = "ARN de la llave KMS" }
variable "landing_bucket"    { description = "ARN del bucket Landing" }
variable "raw_bucket"        { description = "ARN del bucket Raw" }
variable "processed_bucket"  { description = "ARN del bucket Processed" }
variable "curated_bucket"    { description = "ARN del bucket Curated" }
variable "scripts_bucket"    { description = "ARN del bucket de scripts Glue" }
variable "athena_bucket"     { description = "ARN del bucket de resultados Athena" }

# ── Rol Lambda: solo puede escribir en Landing ──────────────────
resource "aws_iam_role" "lambda" {
  name = "${var.prefix}-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_s3" {
  name = "landing-write"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"],
        Resource = [var.landing_bucket, "${var.landing_bucket}/*"] },
      { Effect = "Allow", Action = ["kms:GenerateDataKey*", "kms:Decrypt"], Resource = var.kms_key_arn },
      { Effect = "Allow", Action = ["events:PutEvents"],
        Resource = "arn:aws:events:${var.region}:${var.account_id}:event-bus/default" },
    ]
  })
}

# ── Rol Glue: lee de Raw/Landing, escribe en Processed/Curated ─
resource "aws_iam_role" "glue" {
  name = "${var.prefix}-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "glue.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "datalake-rw"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetObject", "s3:ListBucket"],
        Resource = [var.landing_bucket, "${var.landing_bucket}/*", var.raw_bucket, "${var.raw_bucket}/*",
                    var.scripts_bucket, "${var.scripts_bucket}/*"] },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
        Resource = [var.processed_bucket, "${var.processed_bucket}/*", var.curated_bucket, "${var.curated_bucket}/*"] },
      { Effect = "Allow", Action = ["kms:Decrypt", "kms:GenerateDataKey*", "kms:DescribeKey"], Resource = var.kms_key_arn },
    ]
  })
}

# ── Rol Analista: solo lectura en Processed y Curated ──────────
resource "aws_iam_role" "analyst" {
  name = "${var.prefix}-analyst-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::${var.account_id}:root" }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "sts:ExternalId" = "${var.prefix}-analyst" } }
    }]
  })
}

resource "aws_iam_role_policy" "analyst" {
  name = "athena-read"
  role = aws_iam_role.analyst.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults"],
        Resource = "arn:aws:athena:${var.region}:${var.account_id}:workgroup/${var.prefix}" },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:ListBucket"],
        Resource = [var.processed_bucket, "${var.processed_bucket}/*", var.curated_bucket, "${var.curated_bucket}/*"] },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
        Resource = [var.athena_bucket, "${var.athena_bucket}/*"] },
      { Effect = "Allow", Action = ["glue:GetDatabase", "glue:GetTable", "glue:GetPartitions"], Resource = "*" },
      { Effect = "Allow", Action = ["kms:Decrypt", "kms:GenerateDataKey*"], Resource = var.kms_key_arn },
    ]
  })
}

output "lambda_role_arn"  { value = aws_iam_role.lambda.arn }
output "glue_role_arn"    { value = aws_iam_role.glue.arn }
output "analyst_role_arn" { value = aws_iam_role.analyst.arn }
