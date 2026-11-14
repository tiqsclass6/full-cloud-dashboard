variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project tag (cost allocation)."
  type        = string
  default     = "cloud-dashboard"
}

variable "environment" {
  description = "Environment name; also the Argo CD cluster label lakehouse.io/env."
  type        = string
  default     = "dev"
}

variable "kubernetes_version" {
  description = "EKS Kubernetes version."
  type        = string
  default     = "1.37"
}

variable "vpc_cidr" {
  description = "VPC address range."
  type        = string
  default     = "10.40.0.0/16"
}

variable "node_instance_types" {
  description = "Spot instance types for the workers (several, so spot capacity is easier to find)."
  type        = list(string)
  default     = ["m6a.xlarge", "m6i.xlarge", "m5.xlarge", "m5a.xlarge"]
}

variable "node_count" {
  description = "Worker nodes: one per availability zone, matching Kafka's per-zone placement."
  type        = number
  default     = 3
}

variable "athena_bytes_scanned_cutoff" {
  description = "Per-query scan limit for the Athena workgroup (cost guard)."
  type        = number
  default     = 1073741824 # 1 GiB
}

variable "api_allowed_cidrs" {
  description = "Addresses allowed to reach the public Kubernetes API endpoint (the operator's IP as a /32)."
  type        = list(string)

  # Checked at plan time: EKS rejects a malformed CIDR only when it creates the cluster, minutes into an apply.
  validation {
    condition     = length(var.api_allowed_cidrs) > 0 && alltrue([for c in var.api_allowed_cidrs : can(cidrnetmask(c))])
    error_message = "api_allowed_cidrs must hold valid IPv4 CIDR blocks, e.g. [\"203.0.113.10/32\"]."
  }
}

variable "grafana_admin_password_version" {
  description = "Increment to generate and store a new Grafana admin password (write-only secret)."
  type        = number
  default     = 1
}

variable "ecr_tag_mutability" {
  description = "ECR tag mutability: MUTABLE in dev, IMMUTABLE for staging and prod."
  type        = string
  default     = "MUTABLE"
}

variable "config_recorder_name" {
  description = "The account's AWS Config recorder (managed in terraform/bootstrap)."
  type        = string
  default     = "default"
}

variable "agentcore_runtime" {
  description = "Create the ops agent on AgentCore Runtime. Second apply, once the arm64 image is in ECR (local/scripts/aws-images.sh)."
  type        = bool
  default     = false
}

variable "agentcore_image_tag" {
  description = "Tag of the arm64 ai-ops image AgentCore Runtime runs (AgentCore requires linux/arm64)."
  type        = string
  default     = "0.1.0-arm64" # global.imageTag in helm/values/dev.yaml, plus -arm64
}

variable "agentcore_az_ids" {
  description = "Availability Zone IDs where AgentCore Runtime supports VPC networking (us-east-1, from the CreateAgentRuntime error of 2026-11-01)."
  type        = list(string)
  default     = ["use1-az1", "use1-az2", "use1-az4"]
}
