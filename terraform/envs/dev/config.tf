# The recorder lives in the bootstrap stack; this stack turns recording on and adds the rules.
resource "aws_config_configuration_recorder_status" "this" {
  name       = var.config_recorder_name
  is_enabled = true
}

locals {
  config_rules = {
    "s3-bucket-ssl-requests-only"        = "S3_BUCKET_SSL_REQUESTS_ONLY"
    "s3-bucket-sse-enabled"              = "S3_BUCKET_SERVER_SIDE_ENCRYPTION_ENABLED"
    "rds-storage-encrypted"              = "RDS_STORAGE_ENCRYPTED"
    "rds-instance-public-access-check"   = "RDS_INSTANCE_PUBLIC_ACCESS_CHECK"
    "eks-secrets-encrypted"              = "EKS_SECRETS_ENCRYPTED"
    "eks-endpoint-no-public-access"      = "EKS_ENDPOINT_NO_PUBLIC_ACCESS" # expected NON_COMPLIANT in dev (see .trivyignore.yaml AWS-0040)
    "encrypted-volumes"                  = "ENCRYPTED_VOLUMES"
    "ecr-private-image-scanning-enabled" = "ECR_PRIVATE_IMAGE_SCANNING_ENABLED"
    "cmk-backing-key-rotation-enabled"   = "CMK_BACKING_KEY_ROTATION_ENABLED"
  }
}

resource "aws_config_config_rule" "platform" {
  for_each = local.config_rules
  name     = "${local.name}-${each.key}"
  source {
    owner             = "AWS"
    source_identifier = each.value
  }
  depends_on = [aws_config_configuration_recorder_status.this]
}
