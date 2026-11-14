output "sns_topic_arn" {
  description = "SNS topic that receives budget alerts. Subscribe Slack or other endpoints to it."
  value       = aws_sns_topic.budget_alerts.arn
}

output "budget_name" {
  description = "Name of the monthly cost budget."
  value       = aws_budgets_budget.monthly.name
}
