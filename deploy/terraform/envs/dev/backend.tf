# Local backend for now — switch to an S3 backend once an AWS account
# exists and this environment is actually applied (Workstream I).
terraform {
  required_version = ">= 1.7"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}
