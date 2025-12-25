variable "bucket_name" {
  type = string
  description = "S3 bucket name to be created"
}
variable "infra_region" {
  type        = string
  default     = "us-east-1"
  description = "Region where resource to be deployed"
}
variable "aws_account_id" {
  type = string
  description = "AWS account id where infra will be created"
}
variable "kms_key_arn" {
  type = string
  default = ""
  description = "KMS key ARN for encrypt/decrypt of bucket resources "
}
variable "infra_tags" {
  type = map(any)
  default = {

  }
}
variable "assume_role_arn" {
  type = string
  description = "The role arn that has access to create resource"
}
