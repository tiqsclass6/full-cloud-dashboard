output "region" {
  value = var.region
}

output "cluster_name" {
  value = module.eks.cluster_name
}

output "ecr_registry" {
  value = "${local.account_id}.dkr.ecr.${var.region}.amazonaws.com"
}

output "warehouse" {
  description = "Iceberg warehouse location."
  value       = "s3://${aws_s3_bucket.lakehouse["warehouse"].bucket}/iceberg"
}

output "checkpoints_bucket" {
  value = aws_s3_bucket.lakehouse["checkpoints"].bucket
}

output "glue_database" {
  value = aws_glue_catalog_database.lakehouse.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.lakehouse.name
}

output "flink_jobs_role_arn" {
  value = aws_iam_role.irsa["flink-jobs"].arn
}

output "spark_jobs_role_arn" {
  value = aws_iam_role.irsa["spark-jobs"].arn
}

output "postgres_endpoint" {
  value = aws_db_instance.postgres.address
}

output "postgres_secret_arn" {
  description = "Secrets Manager secret holding the RDS master credentials."
  value       = aws_db_instance.postgres.master_user_secret[0].secret_arn
}

output "external_secrets_role_arn" {
  value = aws_iam_role.irsa["external-secrets"].arn
}

output "grafana_admin_secret_name" {
  value = aws_secretsmanager_secret.grafana_admin.name
}

output "image_signing_key" {
  description = "KMS key that signs the platform images (Cosign URI form)."
  value       = "awskms:///${aws_kms_alias.image_signing.arn}"
}

output "kyverno_admission_role_arn" {
  value = aws_iam_role.irsa["kyverno-admission"].arn
}

output "iceberg_exporter_role_arn" {
  value = aws_iam_role.irsa["iceberg-exporter"].arn
}

output "ops_agent_role_arn" {
  description = "IRSA role of ops-agent (Claude through Mantle, InvokeAgentRuntime)."
  value       = aws_iam_role.irsa["ops-agent"].arn
}

output "hermes_role_arn" {
  description = "IRSA role of Hermes (Converse through the US inference profile only)."
  value       = aws_iam_role.irsa["hermes"].arn
}

output "aws_load_balancer_controller_role_arn" {
  description = "IRSA role of the AWS Load Balancer Controller."
  value       = aws_iam_role.irsa["aws-load-balancer-controller"].arn
}

output "vpc_id" {
  value = module.vpc.vpc_id
}

output "vpc_cidr" {
  value = var.vpc_cidr
}

output "private_subnets" {
  value = join(",", module.vpc.private_subnets)
}

output "agentcore_gateway_id" {
  value = aws_bedrockagentcore_gateway.ops_tools.gateway_id
}

output "agentcore_gateway_url" {
  value = aws_bedrockagentcore_gateway.ops_tools.gateway_url
}

output "agentcore_gateway_egress_sg" {
  value = aws_security_group.agentcore_gateway_egress.id
}

output "mcp_ca_secret_arn" {
  value = aws_secretsmanager_secret.mcp_ca.arn
}

output "agentcore_runtime_arn" {
  description = "Empty until the second apply (agentcore_runtime = true)."
  value       = try(aws_bedrockagentcore_agent_runtime.ops_agent[0].agent_runtime_arn, "")
}
