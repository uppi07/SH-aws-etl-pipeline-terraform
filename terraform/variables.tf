variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "CodingChallenge@SceneHealth"
  type        = string
  default     = "patient-etl"
}

variable "bucket_name" {
  description = "Coding Challenge: AWS ETL Pipeline with Terraform"
  type        = string
  default     = "patient-etl-pipeline-bucket-demo"
}

variable "glue_job_name" {
  description = "Glue job name"
  type        = string
  default     = "patient-adherence-etl-job"
}
