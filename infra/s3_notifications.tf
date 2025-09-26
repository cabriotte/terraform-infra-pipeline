resource "aws_lambda_permission" "allow_s3" {
    statement_id  = "AllowS3Invoke"
    action        = "lambda:InvokeFunction"
    function_name = aws_lambda_function.challenge2-lambda-s3-process-files.function_name
    principal     = "s3.amazonaws.com"
    source_arn    = "arn:aws:s3:::dev-challenge2-files"
}

resource "aws_s3_bucket_notification" "bucket_notify" {
    bucket = var.bucket_name

    lambda_function {
        lambda_function_arn = aws_lambda_function.challenge2-lambda-s3-process-files.arn
        events              = ["s3:ObjectCreated:*"]
        # Opcional: filtros
        ###filter_prefix       = "uploads/"
        ###filter_suffix       = ".txt"
    }

    depends_on = [aws_lambda_permission.allow_s3]
}