# Kind only: local/scripts/ai-creds.sh assumes this role (EKS uses IRSA in envs/dev).

data "aws_partition" "current" {}

locals {
  ops_agent_models = ["anthropic.claude-sonnet-4-6", "anthropic.claude-opus-5-5"]
  # The agent and Hermes call the models through the US cross-region inference profiles (InvokeModel).
  ops_agent_profile_arns = [for m in local.ops_agent_models : "arn:${data.aws_partition.current.partition}:bedrock:${var.aws_region}:${data.aws_caller_identity.current.account_id}:inference-profile/us.${m}"]
}

data "aws_iam_policy_document" "ops_agent_local_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [data.aws_caller_identity.current.arn] # the identity that applies this stack
    }
  }
}

resource "aws_iam_role" "ops_agent_local" {
  name                 = "${var.project}-ops-agent-local"
  description          = "Ops agent on kind: invoke the agent's Claude models in Amazon Bedrock only"
  assume_role_policy   = data.aws_iam_policy_document.ops_agent_local_trust.json
  max_session_duration = 14400
}

data "aws_iam_policy_document" "ops_agent_models" {
  statement {
    sid       = "InferenceProfiles"
    actions   = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"]
    resources = local.ops_agent_profile_arns
  }
  # The models in the routed regions, only through those profiles.
  statement {
    sid       = "ModelsThroughProfilesOnly"
    actions   = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"]
    resources = [for m in local.ops_agent_models : "arn:${data.aws_partition.current.partition}:bedrock:us-*::foundation-model/${m}"]
    condition {
      test     = "StringEquals"
      variable = "bedrock:InferenceProfileArn"
      values   = local.ops_agent_profile_arns
    }
  }
}

resource "aws_iam_role_policy" "ops_agent_local" {
  name   = "invoke-agent-models"
  role   = aws_iam_role.ops_agent_local.id
  policy = data.aws_iam_policy_document.ops_agent_models.json
}
