# One role per service account. The OIDC subject is that account and no other.
locals {
  irsa = {
    ebs-csi                      = { namespace = "kube-system", service_account = "ebs-csi-controller-sa" }
    flink-jobs                   = { namespace = "apps", service_account = "flink" }
    spark-jobs                   = { namespace = "apps", service_account = "spark-operator-spark" }
    external-secrets             = { namespace = "external-secrets", service_account = "external-secrets" }
    kyverno-admission            = { namespace = "kyverno", service_account = "kyverno-admission-controller" }
    iceberg-exporter             = { namespace = "apps", service_account = "iceberg-exporter" }
    ops-agent                    = { namespace = "ai-ops", service_account = "ops-agent" }
    hermes                       = { namespace = "ai-ops", service_account = "hermes" }
    aws-load-balancer-controller = { namespace = "kube-system", service_account = "aws-load-balancer-controller" }
  }
  oidc_issuer = replace(module.eks.cluster_oidc_issuer_url, "https://", "")
}

data "aws_iam_policy_document" "irsa_trust" {
  for_each = local.irsa

  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer}:sub"
      values   = ["system:serviceaccount:${each.value.namespace}:${each.value.service_account}"]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "irsa" {
  for_each           = local.irsa
  name               = "${local.name}-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.irsa_trust[each.key].json
}

resource "aws_iam_role_policy_attachment" "ebs_csi" {
  role       = aws_iam_role.irsa["ebs-csi"].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}

data "aws_iam_policy_document" "lakehouse_jobs" {
  statement {
    sid       = "ListBuckets"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lakehouse["warehouse"].arn, aws_s3_bucket.lakehouse["checkpoints"].arn]
  }
  statement {
    sid = "ReadWriteObjects"
    actions = [
      "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
      "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.lakehouse["warehouse"].arn}/*", "${aws_s3_bucket.lakehouse["checkpoints"].arn}/*"]
  }
  statement {
    sid       = "UseDataKey"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
    resources = [aws_kms_key.lakehouse.arn]
  }
  # No glue:DeleteTable: the jobs never drop a table.
  statement {
    sid = "IcebergGlueCatalog"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases",
      "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable",
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:glue:${var.region}:${local.account_id}:catalog",
      aws_glue_catalog_database.lakehouse.arn,
      "arn:${data.aws_partition.current.partition}:glue:${var.region}:${local.account_id}:table/${aws_glue_catalog_database.lakehouse.name}/*",
    ]
  }
}

resource "aws_iam_policy" "lakehouse_jobs" {
  name   = "${local.name}-lakehouse-jobs"
  policy = data.aws_iam_policy_document.lakehouse_jobs.json
}

resource "aws_iam_role_policy_attachment" "lakehouse_jobs" {
  for_each   = toset(["flink-jobs", "spark-jobs"])
  role       = aws_iam_role.irsa[each.key].name
  policy_arn = aws_iam_policy.lakehouse_jobs.arn
}

data "aws_iam_policy_document" "external_secrets" {
  statement {
    sid       = "ReadPlatformSecrets"
    actions   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
    resources = [aws_db_instance.postgres.master_user_secret[0].secret_arn, aws_secretsmanager_secret.grafana_admin.arn]
  }
  statement {
    sid       = "DecryptGrafanaSecret"
    actions   = ["kms:Decrypt"]
    resources = [aws_kms_key.lakehouse.arn]
  }
}

resource "aws_iam_policy" "external_secrets" {
  name   = "${local.name}-external-secrets"
  policy = data.aws_iam_policy_document.external_secrets.json
}

resource "aws_iam_role_policy_attachment" "external_secrets" {
  role       = aws_iam_role.irsa["external-secrets"].name
  policy_arn = aws_iam_policy.external_secrets.arn
}

data "aws_iam_policy_document" "iceberg_exporter" {
  statement {
    sid       = "ListWarehouse"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lakehouse["warehouse"].arn]
  }
  statement {
    sid       = "ReadMetadataAndManifests"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lakehouse["warehouse"].arn}/*"]
  }
  statement {
    sid       = "DecryptWarehouse"
    actions   = ["kms:Decrypt", "kms:DescribeKey"]
    resources = [aws_kms_key.lakehouse.arn]
  }
  statement {
    sid     = "ReadGlueCatalog"
    actions = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables"]
    resources = [
      "arn:${data.aws_partition.current.partition}:glue:${var.region}:${local.account_id}:catalog",
      aws_glue_catalog_database.lakehouse.arn,
      "arn:${data.aws_partition.current.partition}:glue:${var.region}:${local.account_id}:table/${aws_glue_catalog_database.lakehouse.name}/*",
    ]
  }
}

resource "aws_iam_policy" "iceberg_exporter" {
  name   = "${local.name}-iceberg-exporter"
  policy = data.aws_iam_policy_document.iceberg_exporter.json
}

resource "aws_iam_role_policy_attachment" "iceberg_exporter" {
  role       = aws_iam_role.irsa["iceberg-exporter"].name
  policy_arn = aws_iam_policy.iceberg_exporter.arn
}
