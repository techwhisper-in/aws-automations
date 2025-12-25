variable "assume_role_arn" {
  type = string
  description = "The role ARN that has access to resource"
}
variable "infra_region" {
  type = string
  default = "ap-south-1"
  description = "Region where resource will be created"
}
variable "lambda_function_name" {
  description = "Name of lambda function"
  type = string
}
variable "lambda_execution_role" {
  description = "Execution Role for Lambda Function"
  type = string
}
variable "memory_size" {
  description = "Memory size for Lambda function"
  type = number
  default = 128
}
variable "timeout" {
    description = "Timeout for Lambda Function"
    type = number
    default = 20
}
variable "layer_arns" {
    description = "ARNs of Lambda layers"
    type = list(any)
}
/*variable "layer2_arn" {
    description = "ARN of second Lambda layer"
    type = string
}*/
variable "kms_key" {
  description = "KMS key ARN to encrypt/decrypt"
  type = string
}
variable "tracing_configuration" {
  description = "Tracing Configuration for Lambda"
  type = string
}
/*variable "lambda_bucket" {
  description = "S3 bucket where Lambda code is stored"
  type = string
}
variable "lambda_package" {
  description = "S3 key for lambda code package"
  type = string
}*/
variable "lambda_function_handler" {
  description = "Handler for lambda function"
  type = string
}
variable "lambda_runtime_environment" {
  description = "Runtime environment for Lambda function"
  type = string
}
variable "security_group_id" {
    description = "SSM parameter value for Security group attached to Lambda"
    type = string
}
variable "vpc_id" {
  description = "SSM parameter value for VPC ID"
  type = string
}
variable "subnet_a_id" {
    description = "SSM parameter for Subnet A id"
    type = string
}
variable "subnet_b_id" {
    description = "SSM parameter for Subnet B id"
    type = string
}
variable "subnet_c_id" {
    description = "SSM parameter for Subnet C id"
    type = string
}
variable "lambda_tags" {
  type = map(any)
}
variable "lambda_env_variable" {
  type = map(any)
}

