# terraform/modules/redshift/main.tf
# Amazon Redshift Serverless — Data Warehouse (requisito plus).
# Recibe datos desde la zona Processed mediante el job redshift_pipeline.py.

variable "prefix"            {}
variable "region"            {}
variable "kms_key_arn"       {}
variable "redshift_password" { sensitive = true }

# ── Namespace (base de datos y credenciales) ────────────────────
resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = "${var.prefix}-dwh"
  db_name             = "energia_dwh"
  admin_username      = "admin_energia"
  admin_user_password = var.redshift_password
  kms_key_id          = var.kms_key_arn
  iam_roles           = [aws_iam_role.redshift.arn]
}

# ── Workgroup (cómputo) ────────────────────────────────────────
resource "aws_redshiftserverless_workgroup" "main" {
  namespace_name      = aws_redshiftserverless_namespace.main.namespace_name
  workgroup_name      = "${var.prefix}-workgroup"
  base_capacity       = 8      # RPUs mínimos (escala automáticamente)
  publicly_accessible = false  # Solo acceso interno desde VPC
}

# ── Rol IAM para que Redshift lea de S3 (comando COPY) ─────────
resource "aws_iam_role" "redshift" {
  name = "${var.prefix}-redshift-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "redshift.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "redshift_s3" {
  name = "s3-parquet-read"
  role = aws_iam_role.redshift.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"], Resource = "*" },
      { Effect = "Allow", Action = ["kms:Decrypt"], Resource = var.kms_key_arn },
      { Effect = "Allow", Action = ["glue:GetDatabase", "glue:GetTable", "glue:GetPartitions"], Resource = "*" },
    ]
  })
}

output "namespace_name"    { value = aws_redshiftserverless_namespace.main.namespace_name }
output "workgroup_name"    { value = aws_redshiftserverless_workgroup.main.workgroup_name }
output "redshift_role_arn" { value = aws_iam_role.redshift.arn }
