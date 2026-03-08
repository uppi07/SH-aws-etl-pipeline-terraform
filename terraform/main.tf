resource "aws_s3_bucket" "etl_bucket" {
  bucket = var.bucket_name

  tags = {
    Project = var.project_name
    Owner   = "Upendra Dommaraju"
    Purpose = "CodingChallenge@SceneHealth"
  }
}

# Lock down the bucket since this pipeline is internal-only.
resource "aws_s3_bucket_public_access_block" "etl_bucket_pab" {
  bucket = aws_s3_bucket.etl_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "etl_bucket_sse" {
  bucket = aws_s3_bucket.etl_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.etl_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_s3_access" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload"
    ]
    # Glue overwrite writes can require delete + multipart cleanup permissions.
    resources = ["${aws_s3_bucket.etl_bucket.arn}/*"]
  }

  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.etl_bucket.arn]
  }
}

resource "aws_iam_policy" "s3_access_policy" {
  name = "${var.project_name}-s3-access"

  policy = data.aws_iam_policy_document.glue_s3_access.json
}

resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.s3_access_policy.arn
}

resource "aws_glue_job" "etl_job" {
  name        = var.glue_job_name
  role_arn    = aws_iam_role.glue_role.arn
  description = "ETL job for patient adherence dataset (raw CSV to processed Parquet)"

  command {
    script_location = "s3://${var.bucket_name}/scripts/patient_adherence_etl.py"
    python_version  = "3"
  }

  depends_on = [
    aws_s3_object.glue_script
  ]

  glue_version = "4.0"
  max_retries  = 1
  timeout      = 10

  # Keep input/output paths configurable through job args instead of hardcoding in script.
  default_arguments = {
    "--job-language" = "python"
    "--input_path"   = "s3://${var.bucket_name}/raw/patient_adherence_dataset.csv"
    "--output_path"  = "s3://${var.bucket_name}/processed/"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = {
    Project = var.project_name
    Owner   = "Upendra Dommaraju"
    Purpose = "CodingChallenge@SceneHealth"
  }
}

resource "aws_s3_object" "dataset" {
  bucket = aws_s3_bucket.etl_bucket.id

  key = "raw/patient_adherence_dataset.csv"

  source = "../data/patient_adherence_dataset.csv"

  etag = filemd5("../data/patient_adherence_dataset.csv")
}

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.etl_bucket.id

  key = "scripts/patient_adherence_etl.py"

  source = "../glue/patient_adherence_etl.py"

  etag = filemd5("../glue/patient_adherence_etl.py")
}
