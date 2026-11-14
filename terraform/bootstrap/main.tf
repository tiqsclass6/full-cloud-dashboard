terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
      Stack     = "bootstrap"
    }
  }
}

# Separate stack, so destroying an environment never removes the cost alerts.
module "cost_guardrails" {
  source = "../modules/cost-guardrails"

  name_prefix                   = var.project
  monthly_limit_usd             = var.monthly_budget_usd
  forecasted_thresholds_percent = [90, 95]
  enable_actual_backstop        = true
  alert_emails                  = var.budget_alert_emails
  kms_key_arn                   = aws_kms_key.account.arn
  tags                          = { Project = var.project }
}
