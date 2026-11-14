variable "aws_region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project name, used as a name prefix and the Project cost-allocation tag."
  type        = string
  default     = "cloud-dashboard"
}

variable "monthly_budget_usd" {
  description = "Monthly cost budget in USD."
  type        = number
  default     = 100
}

variable "budget_alert_emails" {
  description = "Email addresses for budget alerts (each must confirm the SNS subscription)."
  type        = list(string)
}

variable "audit_log_retention_days" {
  description = "Days to keep CloudTrail and Config history in the audit bucket."
  type        = number
  default     = 365
}

variable "config_resource_types" {
  description = "Resource types the AWS Config recorder records (only while an environment has recording on)."
  type        = list(string)
  default = [
    "AWS::S3::Bucket",
    "AWS::RDS::DBInstance",
    "AWS::EKS::Cluster",
    "AWS::EC2::Volume",
    "AWS::KMS::Key",
    "AWS::ECR::Repository",
  ]
}
