output "budget_alerts_topic_arn" {
  description = "SNS topic for budget alerts."
  value       = module.cost_guardrails.sns_topic_arn
}

output "budget_name" {
  description = "Monthly cost budget name."
  value       = module.cost_guardrails.budget_name
}

output "state_bucket" {
  description = "S3 bucket holding Terraform state for every stack."
  value       = aws_s3_bucket.state.bucket
}

output "account_kms_key_arn" {
  description = "Account-level key (state, alert topics, audit logs)."
  value       = aws_kms_key.account.arn
}

output "audit_bucket" {
  description = "CloudTrail and Config history."
  value       = aws_s3_bucket.audit.bucket
}

output "config_recorder_name" {
  description = "The account's Config recorder; environments switch recording on and off."
  value       = aws_config_configuration_recorder.default.name
}

output "security_alerts_topic_arn" {
  description = "SNS topic for HIGH GuardDuty findings."
  value       = aws_sns_topic.security_alerts.arn
}

output "ops_agent_local_role_arn" {
  description = "Role local/scripts/ai-creds.sh assumes for the ops agent on kind (Bedrock model access only)."
  value       = aws_iam_role.ops_agent_local.arn
}
