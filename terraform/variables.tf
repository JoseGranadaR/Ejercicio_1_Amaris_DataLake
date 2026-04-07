variable "aws_region"        { default = "us-east-1" }
variable "project_name"      { default = "amaris-energia" }
variable "environment"       { default = "dev" }
variable "alert_email"       { default = "" }
variable "redshift_password" {
  sensitive = true
  default   = "ChangeMe123!"
}
variable "subnet_ids" {
  type    = list(string)
  default = []
}
