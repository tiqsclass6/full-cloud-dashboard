locals {
  account_id = data.aws_caller_identity.current.account_id
  # Bucket names are global; the account id keeps them unique.
  buckets = {
    warehouse      = "${local.name}-warehouse-${local.account_id}"      # Iceberg data and metadata
    checkpoints    = "${local.name}-checkpoints-${local.account_id}"    # Flink checkpoints, savepoints, HA
    athena-results = "${local.name}-athena-results-${local.account_id}" # query results
  }
}

resource "aws_s3_bucket" "lakehouse" {
  for_each = local.buckets
  bucket   = each.value
  # Ephemeral environment: destroy removes the data with the bucket.
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  for_each                = aws_s3_bucket.lakehouse
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "lakehouse" {
  for_each = aws_s3_bucket.lakehouse
  bucket   = each.value.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# S3 Bucket Key: one KMS call per bucket key, not per object (many small Iceberg and checkpoint files).
resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  for_each = aws_s3_bucket.lakehouse
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lakehouse.arn
    }
    bucket_key_enabled = true
  }
}

data "aws_iam_policy_document" "tls_only" {
  for_each = aws_s3_bucket.lakehouse
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    resources = [each.value.arn, "${each.value.arn}/*"]
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "lakehouse" {
  for_each   = aws_s3_bucket.lakehouse
  bucket     = each.value.id
  policy     = data.aws_iam_policy_document.tls_only[each.key].json
  depends_on = [aws_s3_bucket_public_access_block.lakehouse]
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  for_each = aws_s3_bucket.lakehouse
  bucket   = each.value.id

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  # Query results are disposable.
  dynamic "rule" {
    for_each = each.key == "athena-results" ? [1] : []
    content {
      id     = "expire-results"
      status = "Enabled"
      filter {}
      expiration {
        days = 7
      }
    }
  }
}

# The Iceberg catalog: Flink and Spark register tables here (GlueCatalog); Athena reads them.
resource "aws_glue_catalog_database" "lakehouse" {
  name         = "lakehouse"
  description  = "Iceberg tables written by the streaming and batch jobs (${var.environment})"
  location_uri = "s3://${aws_s3_bucket.lakehouse["warehouse"].bucket}/iceberg/lakehouse.db"
}

resource "aws_athena_workgroup" "lakehouse" {
  name          = local.name
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lakehouse["athena-results"].bucket}/results/"
      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = aws_kms_key.lakehouse.arn
      }
    }
  }
}
