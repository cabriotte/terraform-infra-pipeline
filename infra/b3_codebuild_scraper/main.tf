resource "aws_codebuild_project" "scraper" {
  name         = var.project_name
  description  = "Projeto de scraping da B3"
  service_role = var.role_arn

  source {
    type      = "GITHUB"
    location  = var.github_repo_url
    buildspec = var.buildspec_path
  }

  environment {
    compute_type    = "BUILD_GENERAL1_SMALL"  # ✅ compatível com Free Tier
    image           = "aws/codebuild/standard:7.0"
    type            = "LINUX_CONTAINER"
    privileged_mode = true

    environment_variable {
      name  = "BUCKET_NAME"
      value = var.bucket_name
    }
  }

  artifacts {
    type = "NO_ARTIFACTS"
  }
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