/*data "aws_caller_identity" "current" {
    #count = var.infra_create ? 1 : 0
}
data "aws_region" "current" {
    #count = var.infra_create ? 1 : 0
}*/

resource "aws_s3_bucket" "s3_bucket" {
  bucket = var.bucket_name
  tags   = merge({ Name = var.bucket_name }, var.infra_tags)

}

resource "aws_s3_bucket_ownership_controls" "s3_ownership" {
  bucket = aws_s3_bucket.s3_bucket.id
  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_acl" "s3_acl" {
  depends_on = [aws_s3_bucket_ownership_controls.s3_ownership]
  bucket = aws_s3_bucket.s3_bucket.id
  acl    = "private"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "s3_encryption" {
  depends_on = [aws_s3_bucket.s3_bucket]
  bucket     = aws_s3_bucket.s3_bucket.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = var.kms_key_arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

