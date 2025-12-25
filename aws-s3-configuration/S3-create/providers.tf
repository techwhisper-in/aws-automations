terraform {
  backend "s3" {}
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.33"
    }
  }
  
}

provider "aws" {
  region = var.infra_region
  assume_role {
    role_arn = var.assume_role_arn
  }
}