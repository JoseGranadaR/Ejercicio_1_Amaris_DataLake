output "landing_bucket"    { value = aws_s3_bucket.datalake["landing"].bucket }
output "raw_bucket"        { value = aws_s3_bucket.datalake["raw"].bucket }
output "processed_bucket"  { value = aws_s3_bucket.datalake["processed"].bucket }
output "scripts_bucket"    { value = aws_s3_bucket.datalake["scripts"].bucket }
output "glue_db_raw"       { value = aws_glue_catalog_database.raw.name }
output "glue_db_processed" { value = aws_glue_catalog_database.processed.name }
output "athena_workgroup"  { value = aws_athena_workgroup.main.name }
output "glue_workflow"     { value = aws_glue_workflow.etl_pipeline.name }
output "redshift_endpoint" { value = aws_redshift_cluster.dw.endpoint }
output "kms_key_arn" {
  value     = aws_kms_key.datalake.arn
  sensitive = true
}
