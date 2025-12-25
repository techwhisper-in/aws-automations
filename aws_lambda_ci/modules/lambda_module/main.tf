resource "aws_lambda_function" "lambda_function" {
  function_name = var.lambda_function_name
  role = var.lambda_execution_role
  handler = var.lambda_function_handler
  runtime = var.lambda_runtime_environment
  timeout = var.timeout
  memory_size = var.memory_size
  filename = data.archive_file.lambda_zip.output_path
  #s3_bucket = var.lambda_bucket
  #s3_key = var.lambda_package
  kms_key_arn = var.kms_key
  tracing_config {
    mode = var.tracing_configuration
  }
  layers = var.layer_arns
  environment {
    variables = var.lambda_env_variable
  }
  tags = var.lambda_tags
  vpc_config {
    security_group_ids = [data.aws_ssm_parameter.security_group_id.value]
    subnet_ids = [
      data.aws_ssm_parameter.subnet_a_id.value,
      data.aws_ssm_parameter.subnet_b_id.value,
      data.aws_ssm_parameter.subnet_c_id.value
      ]
  }
}