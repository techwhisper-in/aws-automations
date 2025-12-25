module "S3" {
  source     = "../modules/s3_bucket"
  bucket_name = var.bucket_name
  infra_region = var.infra_region
  infra_tags = var.infra_tags
  aws_account_id = var.aws_account_id
  kms_key_arn = var.kms_key_arn
  assume_role_arn = var.assume_role_arn
}
