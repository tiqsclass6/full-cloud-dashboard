terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.7" # ephemeral random_password (never stored in state)
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.14" # time_sleep: IAM changes need a moment to reach AgentCore
    }
  }

  # The bucket name contains the account id, so it is passed at init: -backend-config="bucket=lakehouse-tfstate-<account-id>"
  backend "s3" {
    key          = "lakehouse/envs/dev/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
      Stack       = "envs/${var.environment}"
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
