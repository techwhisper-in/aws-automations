module "lambda" {
  #count = var.infra_create=="true" ? 1 : 0
  source     = "../modules/lambda_module"
  assume_role_arn = var.assume_role_arn
  infra_region = var.infra_region
  lambda_function_name = var.lambda_function_name
  lambda_execution_role = var.lambda_execution_role
  memory_size = var.memory_size
  timeout = var.timeout
  layer_arns = var.layer_arns
  kms_key = var.kms_key
  tracing_configuration = var.tracing_configuration
  #lambda_bucket = var.lambda_bucket
  #lambda_package = var.lambda_package
  lambda_function_handler = var.lambda_function_handler
  lambda_runtime_environment = var.lambda_runtime_environment
  security_group_id = var.security_group_id
  vpc_id = var.vpc_id
  subnet_a_id = var.subnet_a_id
  subnet_b_id = var.subnet_b_id
  subnet_c_id = var.subnet_c_id
  lambda_tags = var.lambda_tags
  lambda_env_variable = var.lambda_env_variable
}
