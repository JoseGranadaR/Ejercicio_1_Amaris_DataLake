###############################################################
# AMARIS — DATA LAKE ENERGÍA ELÉCTRICA
# main.tf  |  Infraestructura completa
# Capas S3: landing → raw → processed → curated
###############################################################

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = "amaris-datalake"
      Environment = var.environment
      ManagedBy   = "Terraform"
      Owner       = "DataEngineering"
    }
  }
}

###############################################################
# KMS
###############################################################
resource "aws_kms_key" "datalake" {
  description             = "KMS DataLake Amaris Energia"
  deletion_window_in_days = 7
  enable_key_rotation     = true
}
resource "aws_kms_alias" "datalake" {
  name          = "alias/amaris-${var.environment}-datalake"
  target_key_id = aws_kms_key.datalake.key_id
}

###############################################################
# S3 BUCKETS — 7 buckets con propósito definido
###############################################################
locals {
  buckets = {
    landing   = "${var.project_name}-${var.environment}-landing"
    raw       = "${var.project_name}-${var.environment}-raw"
    processed = "${var.project_name}-${var.environment}-processed"
    curated   = "${var.project_name}-${var.environment}-curated"
    scripts   = "${var.project_name}-${var.environment}-scripts"
    athena    = "${var.project_name}-${var.environment}-athena-results"
    logs      = "${var.project_name}-${var.environment}-logs"
  }
}

resource "aws_s3_bucket" "datalake" {
  for_each = local.buckets
  bucket   = each.value
}

resource "aws_s3_bucket_versioning" "datalake" {
  for_each = { for k, v in local.buckets : k => v if k != "athena" && k != "logs" }
  bucket   = aws_s3_bucket.datalake[each.key].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.datalake[each.key].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.datalake.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "datalake" {
  for_each                = local.buckets
  bucket                  = aws_s3_bucket.datalake[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket = aws_s3_bucket.datalake["landing"].id
  rule {
    id     = "archive-old"
    status = "Enabled"
    filter { prefix = "" }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    expiration { days = 365 }
  }
}
resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.datalake["raw"].id
  rule {
    id     = "tiered-storage"
    status = "Enabled"
    filter { prefix = "" }
    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 180
      storage_class = "GLACIER"
    }
  }
}
resource "aws_s3_bucket_lifecycle_configuration" "athena" {
  bucket = aws_s3_bucket.datalake["athena"].id
  rule {
    id     = "clean-results"
    status = "Enabled"
    filter { prefix = "" }
    expiration { days = 7 }
  }
}

# S3 Event → SQS cuando llega CSV a landing
resource "aws_s3_bucket_notification" "landing_csv" {
  bucket = aws_s3_bucket.datalake["landing"].id
  queue {
    queue_arn     = aws_sqs_queue.csv_ingestion.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".csv"
  }
  depends_on = [aws_sqs_queue_policy.csv_ingestion]
}

###############################################################
# SQS
###############################################################
resource "aws_sqs_queue" "csv_ingestion" {
  name                       = "${var.project_name}-${var.environment}-csv-ingestion"
  visibility_timeout_seconds = 300
  message_retention_seconds  = 86400
  kms_master_key_id          = aws_kms_key.datalake.arn
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 3
  })
}
resource "aws_sqs_queue" "dlq" {
  name              = "${var.project_name}-${var.environment}-csv-dlq"
  kms_master_key_id = aws_kms_key.datalake.arn
}
resource "aws_sqs_queue_policy" "csv_ingestion" {
  queue_url = aws_sqs_queue.csv_ingestion.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "s3.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.csv_ingestion.arn
      Condition = { ArnLike = { "aws:SourceArn" = aws_s3_bucket.datalake["landing"].arn } }
    }]
  })
}

###############################################################
# IAM ROLES
###############################################################

# Lambda
resource "aws_iam_role" "lambda_ingestion" {
  name = "${var.project_name}-${var.environment}-lambda-ingestion"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}
resource "aws_iam_role_policy" "lambda_ingestion" {
  name = "datalake-access"
  role = aws_iam_role.lambda_ingestion.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetObject", "s3:HeadObject"],
        Resource = "${aws_s3_bucket.datalake["landing"].arn}/*" },
      { Effect = "Allow", Action = ["s3:PutObject"],
        Resource = "${aws_s3_bucket.datalake["raw"].arn}/*" },
      { Effect = "Allow", Action = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"],
        Resource = aws_sqs_queue.csv_ingestion.arn },
      { Effect = "Allow", Action = ["kms:Decrypt", "kms:GenerateDataKey"],
        Resource = aws_kms_key.datalake.arn },
      { Effect = "Allow", Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
        Resource = "arn:aws:logs:*:*:*" },
      { Effect = "Allow", Action = ["glue:StartJobRun", "glue:GetJobRun"],
        Resource = "*" },
    ]
  })
}

# Glue
resource "aws_iam_role" "glue" {
  name = "${var.project_name}-${var.environment}-glue"
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
  name = "s3-full-datalake"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = [for k, v in local.buckets : "${aws_s3_bucket.datalake[k].arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [for k, v in local.buckets : aws_s3_bucket.datalake[k].arn]
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
        Resource = aws_kms_key.datalake.arn
      },
    ]
  })
}

# Redshift (plus)
resource "aws_iam_role" "redshift" {
  name = "${var.project_name}-${var.environment}-redshift"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "redshift.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}
resource "aws_iam_role_policy" "redshift_s3" {
  name = "s3-read-processed"
  role = aws_iam_role.redshift.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetObject", "s3:ListBucket"],
        Resource = ["${aws_s3_bucket.datalake["processed"].arn}", "${aws_s3_bucket.datalake["processed"].arn}/*"] },
      { Effect = "Allow", Action = ["kms:Decrypt"],
        Resource = aws_kms_key.datalake.arn },
    ]
  })
}

###############################################################
# AWS GLUE — Databases + Crawlers + Jobs + Workflow
###############################################################
resource "aws_glue_catalog_database" "raw" {
  name        = "${replace(var.project_name, "-", "_")}_${var.environment}_raw"
  description = "Raw Zone — CSV originales particionados"
}
resource "aws_glue_catalog_database" "processed" {
  name        = "${replace(var.project_name, "-", "_")}_${var.environment}_processed"
  description = "Processed Zone — Parquet transformado"
}

resource "aws_glue_crawler" "raw" {
  database_name = aws_glue_catalog_database.raw.name
  name          = "${var.project_name}-${var.environment}-crawler-raw"
  role          = aws_iam_role.glue.arn
  schedule      = "cron(0 1 * * ? *)"
  s3_target { path = "s3://${aws_s3_bucket.datalake["raw"].bucket}/" }
  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}
resource "aws_glue_crawler" "processed" {
  database_name = aws_glue_catalog_database.processed.name
  name          = "${var.project_name}-${var.environment}-crawler-processed"
  role          = aws_iam_role.glue.arn
  schedule      = "cron(30 1 * * ? *)"
  s3_target { path = "s3://${aws_s3_bucket.datalake["processed"].bucket}/" }
  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}

resource "aws_glue_job" "transform_proveedores" {
  name = "${var.project_name}-${var.environment}-transform-proveedores"
  role_arn         = aws_iam_role.glue.arn
  glue_version     = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  default_arguments = {
    "--TempDir"          = "s3://${aws_s3_bucket.datalake["scripts"].bucket}/temp/"
    "--RAW_BUCKET"       = aws_s3_bucket.datalake["raw"].bucket
    "--PROCESSED_BUCKET" = aws_s3_bucket.datalake["processed"].bucket
    "--GLUE_DATABASE"    = aws_glue_catalog_database.processed.name
    "--TABLE_NAME"       = "proveedores"
    "--enable-metrics"   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }
  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.datalake["scripts"].bucket}/glue_jobs/transform_proveedores.py"
  }
}
resource "aws_glue_job" "transform_clientes" {
  name = "${var.project_name}-${var.environment}-transform-clientes"
  role_arn         = aws_iam_role.glue.arn
  glue_version     = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  default_arguments = {
    "--TempDir"          = "s3://${aws_s3_bucket.datalake["scripts"].bucket}/temp/"
    "--RAW_BUCKET"       = aws_s3_bucket.datalake["raw"].bucket
    "--PROCESSED_BUCKET" = aws_s3_bucket.datalake["processed"].bucket
    "--GLUE_DATABASE"    = aws_glue_catalog_database.processed.name
    "--TABLE_NAME"       = "clientes"
  }
  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.datalake["scripts"].bucket}/glue_jobs/transform_clientes.py"
  }
}
resource "aws_glue_job" "transform_transacciones" {
  name = "${var.project_name}-${var.environment}-transform-transacciones"
  role_arn         = aws_iam_role.glue.arn
  glue_version     = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  default_arguments = {
    "--TempDir"          = "s3://${aws_s3_bucket.datalake["scripts"].bucket}/temp/"
    "--RAW_BUCKET"       = aws_s3_bucket.datalake["raw"].bucket
    "--PROCESSED_BUCKET" = aws_s3_bucket.datalake["processed"].bucket
    "--GLUE_DATABASE"    = aws_glue_catalog_database.processed.name
    "--TABLE_NAME"       = "transacciones"
  }
  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.datalake["scripts"].bucket}/glue_jobs/transform_transacciones.py"
  }
}

# Glue Workflow
resource "aws_glue_workflow" "etl_pipeline" {
  name        = "${var.project_name}-${var.environment}-etl-pipeline"
  description = "Pipeline ETL diario: Raw → Processed"
}
resource "aws_glue_trigger" "start_etl" {
  name = "${var.project_name}-${var.environment}-trigger-start"
  type          = "SCHEDULED"
  schedule      = "cron(0 2 * * ? *)"
  workflow_name = aws_glue_workflow.etl_pipeline.name
  actions { job_name = aws_glue_job.transform_proveedores.name }
  actions { job_name = aws_glue_job.transform_clientes.name }
  actions { job_name = aws_glue_job.transform_transacciones.name }
}
resource "aws_glue_trigger" "post_etl_crawler" {
  name          = "${var.project_name}-${var.environment}-trigger-crawler"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.etl_pipeline.name
  predicate {
    logical = "AND"
    conditions {
      job_name         = aws_glue_job.transform_proveedores.name
      logical_operator = "EQUALS"
      state            = "SUCCEEDED"
    }
    conditions {
      job_name         = aws_glue_job.transform_clientes.name
      logical_operator = "EQUALS"
      state            = "SUCCEEDED"
    }
    conditions {
      job_name         = aws_glue_job.transform_transacciones.name
      logical_operator = "EQUALS"
      state            = "SUCCEEDED"
    }
  }
  actions { crawler_name = aws_glue_crawler.processed.name }
}

###############################################################
# LAMBDA — Ingestion (Landing → Raw + dispara Glue)
###############################################################
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../lambda/ingestion_handler.py"
  output_path = "${path.module}/ingestion_lambda.zip"
}
resource "aws_lambda_function" "csv_ingestion" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = "${var.project_name}-${var.environment}-csv-ingestion"
  role             = aws_iam_role.lambda_ingestion.arn
  handler          = "ingestion_handler.handler"
  runtime          = "python3.12"
  timeout          = 120
  memory_size      = 512
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  environment {
    variables = {
      RAW_BUCKET         = aws_s3_bucket.datalake["raw"].bucket
      GLUE_JOB_PROV      = aws_glue_job.transform_proveedores.name
      GLUE_JOB_CLIENTES  = aws_glue_job.transform_clientes.name
      GLUE_JOB_TX        = aws_glue_job.transform_transacciones.name
      ENVIRONMENT        = var.environment
    }
  }
}
resource "aws_lambda_event_source_mapping" "sqs_trigger" {
  event_source_arn = aws_sqs_queue.csv_ingestion.arn
  function_name    = aws_lambda_function.csv_ingestion.arn
  batch_size       = 5
}

###############################################################
# ATHENA WORKGROUP
###############################################################
resource "aws_athena_workgroup" "main" {
  name = "${var.project_name}-${var.environment}"
  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    result_configuration {
      output_location = "s3://${aws_s3_bucket.datalake["athena"].bucket}/results/"
      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = aws_kms_key.datalake.arn
      }
    }
  }
}

###############################################################
# LAKE FORMATION (Plus)
###############################################################
resource "aws_lakeformation_data_lake_settings" "main" {
  admins = [aws_iam_role.glue.arn]
}
resource "aws_lakeformation_resource" "raw_bucket" {
  arn      = aws_s3_bucket.datalake["raw"].arn
  role_arn = aws_iam_role.glue.arn
}
resource "aws_lakeformation_resource" "processed_bucket" {
  arn      = aws_s3_bucket.datalake["processed"].arn
  role_arn = aws_iam_role.glue.arn
}
resource "aws_lakeformation_permissions" "glue_raw_db" {
  principal   = aws_iam_role.glue.arn
  permissions = ["ALL"]
  database { name = aws_glue_catalog_database.raw.name }
}
resource "aws_lakeformation_permissions" "glue_processed_db" {
  principal   = aws_iam_role.glue.arn
  permissions = ["ALL"]
  database { name = aws_glue_catalog_database.processed.name }
}

###############################################################
# REDSHIFT (Plus)
###############################################################
resource "aws_redshift_subnet_group" "main" {
  name       = "${var.project_name}-${var.environment}-subnet-group"
  subnet_ids = var.subnet_ids
}
resource "aws_redshift_cluster" "dw" {
  cluster_identifier     = "${var.project_name}-${var.environment}-dw"
  database_name          = "energia_dw"
  master_username        = "admin"
  master_password        = var.redshift_password
  node_type              = "dc2.large"
  cluster_type           = "single-node"
  iam_roles              = [aws_iam_role.redshift.arn]
  cluster_subnet_group_name = aws_redshift_subnet_group.main.name
  skip_final_snapshot    = var.environment != "prod"
  encrypted              = true
  kms_key_id             = aws_kms_key.datalake.arn
  publicly_accessible    = false
}

###############################################################
# CLOUDWATCH ALARMS
###############################################################
resource "aws_sns_topic" "alerts" {
  name              = "${var.project_name}-${var.environment}-alerts"
  kms_master_key_id = aws_kms_key.datalake.arn
}
resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${var.project_name}-${var.environment}-lambda-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 5
  alarm_actions       = [aws_sns_topic.alerts.arn]
  dimensions          = { FunctionName = aws_lambda_function.csv_ingestion.function_name }
}
resource "aws_cloudwatch_metric_alarm" "dlq_messages" {
  alarm_name          = "${var.project_name}-${var.environment}-dlq-depth"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  alarm_actions       = [aws_sns_topic.alerts.arn]
  dimensions          = { QueueName = aws_sqs_queue.dlq.name }
}
