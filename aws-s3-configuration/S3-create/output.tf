output "bucket-name" {
  value = try(module.S3.bucket-configuration.id,"(Not Created)")
}