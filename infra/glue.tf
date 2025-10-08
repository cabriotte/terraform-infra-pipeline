# IAM Role para o Glue Job
resource "aws_iam_role" "glue_role" {
  name = "glue_service_role"

  assume_role_policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
POLICY
}

# Permissões básicas para Glue
resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Glue Job
resource "aws_glue_job" "challenge2_glue_s3_process_files" {
  name     = "challenge2-glue-s3-process-files"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://aws-glue-assets-196724157578-sa-east-1/scripts/glue_process_files.py"
    python_version  = "3"
  }

  max_retries = 0
  glue_version = "4.0"
  number_of_workers = 2
  worker_type = "G.1X"
}