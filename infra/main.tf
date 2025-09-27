module "b3_codebuild_scraper" {
  source           = "./b3_codebuild_scraper"
  role_name        = var.role_name
  role_arn         = var.role_arn
  project_name     = var.project_name
  github_repo_url  = var.github_repo_url
  buildspec_path   = var.buildspec_path
  bucket_name      = var.bucket_name
  bucket_arn       = var.bucket_arn
}
