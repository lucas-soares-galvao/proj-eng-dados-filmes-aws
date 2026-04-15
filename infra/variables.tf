locals {
  glue_src_path = "${path.root}/../app"
}

variable "env" {
  description = "The environment for the Glue job (e.g., dev, hom, prod)"
  type        = string
}

variable "glue_job_name" {
  description = "The name of the Glue job to create"
  type        = string
  default     = "my-glue-etl-job"
}

variable "iam_role_name" {
  description = "The name of the IAM role for Glue jobs"
  type        = string
  default     = "glue-job-role"
}