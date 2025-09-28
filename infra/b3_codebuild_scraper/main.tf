resource "aws_codebuild_project" "aws_scraper_codebuild" {
  name         = var.project_name
  description  = "Projeto de scraping da B3"
  service_role = var.role_arn
  build_timeout = 15 # tempo em minutos

  source {
    type      = "GITHUB"
    location  = var.github_repo_url
    buildspec = var.buildspec_path
  }

  environment {
    compute_type    = "BUILD_GENERAL1_SMALL"  # compatível com Free Tier
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

resource "aws_iam_role_policy" "codebuild_permissions" {
  name = "codebuild_full_permissions"
  role = var.role_name
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid    = "CodeBuildCoreAccess",
        Effect = "Allow",
        Action = [
          "codebuild:CreateReportGroup",
          "codebuild:CreateProject",
          "codebuild:CreateReport",
          "codebuild:UpdateReport",
          "codebuild:BatchPutTestCases",
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "cloudwatch:PutMetricData"
        ],
        Resource = "*"
      },
      {
        Sid    = "S3AccessForArtifacts",
        Effect = "Allow",
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:GetObject",
          "s3:ListBucket"
        ],
        Resource = [
          "${var.bucket_arn}",
          "${var.bucket_arn}/*"
        ]
      }
    ]
  })
}