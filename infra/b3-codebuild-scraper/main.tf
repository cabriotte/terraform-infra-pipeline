module "b3_codebuild_scraper" {
  source           = "./b3_codebuild_scraper"
  role_name        = var.role_name
  project_name     = var.project_name
  github_repo_url  = var.github_repo_url
  buildspec_path   = var.buildspec_path
  bucket_name      = var.bucket_name
  bucket_arn       = var.bucket_arn
}

resource "aws_iam_role_policy" "codebuild_s3_access" {
  name = "codebuild_s3_access"
  role = var.role_name 
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:PutObject",
          "s3:GetObject"
        ],
        Resource = "${var.bucket_arn}/*"
      }
    ]
  })
}