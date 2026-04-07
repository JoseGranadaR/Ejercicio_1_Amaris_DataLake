# terraform/modules/s3/main.tf
# Módulo reutilizable para crear un bucket S3 con cifrado, versionamiento y lifecycle.

variable "bucket_name"            { description = "Nombre del bucket S3" }
variable "kms_key_arn"            { description = "ARN de la llave KMS para cifrado" }
variable "lifecycle_days_ia"      { default = 90;  description = "Días hasta mover a Standard-IA" }
variable "lifecycle_days_glacier" { default = 365; description = "Días hasta mover a Glacier" }
variable "enable_versioning"      { default = true }

resource "aws_s3_bucket" "this" {
  bucket        = var.bucket_name
  force_destroy = false
}

resource "aws_s3_bucket_versioning" "this" {
  bucket = aws_s3_bucket.this.id
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Suspended"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  bucket = aws_s3_bucket.this.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  bucket                  = aws_s3_bucket.this.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "this" {
  bucket = aws_s3_bucket.this.id
  rule {
    id     = "tiered-storage"
    status = "Enabled"
    filter { prefix = "" }
    transition {
      days          = var.lifecycle_days_ia
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = var.lifecycle_days_glacier
      storage_class = "GLACIER"
    }
  }
}

output "bucket_name" { value = aws_s3_bucket.this.bucket }
output "bucket_arn"  { value = aws_s3_bucket.this.arn }
