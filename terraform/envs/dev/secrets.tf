# Ephemeral and write-only: never in state or plan output. Bump grafana_admin_password_version to rotate.
ephemeral "random_password" "grafana_admin" {
  length  = 32
  special = false
}

resource "aws_secretsmanager_secret" "grafana_admin" {
  name        = "${local.name}/grafana-admin"
  description = "Grafana admin login (synced to monitoring/grafana-admin)"
  kms_key_id  = aws_kms_key.lakehouse.arn
  # Ephemeral environment: delete immediately on destroy instead of a 7-30 day recovery window.
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "grafana_admin" {
  secret_id = aws_secretsmanager_secret.grafana_admin.id
  secret_string_wo = jsonencode({
    "admin-user"     = "admin"
    "admin-password" = ephemeral.random_password.grafana_admin.result
  })
  secret_string_wo_version = var.grafana_admin_password_version
}
