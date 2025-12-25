terraform {
  backend "s3" {
    bucket         = "techwhisper-ami-snapshot-ebs-volume-inventory"
    key            = "file/create-lambda.tfstate"
    region         = "ap-south-1"
  }
}
