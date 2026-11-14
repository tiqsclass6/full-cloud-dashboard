variable "name_prefix" {
  description = "Prefix for resource names."
  type        = string
  default     = "cloud-dashboard"
}

variable "monthly_limit_usd" {
  description = "Monthly cost budget in USD."
  type        = number
  default     = 100

  validation {
    condition     = var.monthly_limit_usd > 0
    error_message = "monthly_limit_usd must be greater than 0."
  }
}

variable "forecasted_thresholds_percent" {
  description = "Percentages of the budget at which a FORECASTED alert is sent."
  type        = list(number)
  default     = [90, 95]

  validation {
    condition     = alltrue([for t in var.forecasted_thresholds_percent : t > 0 && t <= 1000])
    error_message = "Each threshold must be between 0 and 1000 percent."
  }
}

variable "enable_actual_backstop" {
  description = "Also alert when ACTUAL spend reaches actual_backstop_percent. Forecasts need usage history, so this is a safety net."
  type        = bool
  default     = true
}

variable "actual_backstop_percent" {
  description = "Percentage of the budget at which the ACTUAL backstop alert is sent."
  type        = number
  default     = 100
}

variable "alert_emails" {
  description = "Email addresses subscribed to the budget SNS topic. Each address must confirm the subscription."
  type        = list(string)
  default     = []
}

variable "cost_filter_tag" {
  description = "Optional cost-allocation tag to scope the budget, as { key, value }. Null budgets the whole account. The tag must be activated in Billing."
  type = object({
    key   = string
    value = string
  })
  default = null
}

variable "tags" {
  description = "Tags applied to taggable resources."
  type        = map(string)
  default     = {}
}

variable "kms_key_arn" {
  description = "Customer-managed KMS key for the SNS topic (null: unencrypted). Its policy must let budgets.amazonaws.com use it."
  type        = string
  default     = null
}
