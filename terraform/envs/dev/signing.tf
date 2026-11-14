# Cosign signs with this key at push time. The private key stays in KMS.
resource "aws_kms_key" "image_signing" {
  description              = "${local.name} container image signing (Cosign)"
  key_usage                = "SIGN_VERIFY"
  customer_master_key_spec = "ECC_NIST_P256"
  deletion_window_in_days  = 7
}

resource "aws_kms_alias" "image_signing" {
  name          = "alias/${local.name}-image-signing"
  target_key_id = aws_kms_key.image_signing.key_id
}

data "aws_iam_policy_document" "kyverno_ecr" {
  statement {
    sid       = "EcrLogin"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }
  statement {
    sid = "ReadPlatformRepositories"
    actions = [
      "ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer", "ecr:BatchCheckLayerAvailability", "ecr:DescribeImages",
    ]
    resources = [for repo in aws_ecr_repository.app : repo.arn]
  }
}

resource "aws_iam_policy" "kyverno_ecr" {
  name   = "${local.name}-kyverno-ecr-read"
  policy = data.aws_iam_policy_document.kyverno_ecr.json
}

resource "aws_iam_role_policy_attachment" "kyverno_ecr" {
  role       = aws_iam_role.irsa["kyverno-admission"].name
  policy_arn = aws_iam_policy.kyverno_ecr.arn
}
