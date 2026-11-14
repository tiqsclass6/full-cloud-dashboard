# Kind only: local/scripts/ai-creds.sh assumes this role (EKS uses IRSA in envs/dev).

data "aws_partition" "current" {}

locals {
  ops_agent_models   = ["anthropic.claude-sonnet-4-6", "anthropic.claude-opus-5-5"]
  hermes_profile_arn = "arn:${data.aws_partition.current.partition}:bedrock:${var.aws_region}:${data.aws_caller_identity.current.account_id}:inference-profile/us.${local.ops_agent_models[0]}"
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
  # Claude on Bedrock (bedrock-mantle) authorizes CreateInference; resource scoping is not documented yet (docs/ai-ops.md).
  statement {
    sid     = "InvokeAgentModels"
    actions = ["bedrock-mantle:CreateInference"]
    resources = concat(
      [for m in local.ops_agent_models : "arn:${data.aws_partition.current.partition}:bedrock:${var.aws_region}::foundation-model/${m}"],
      ["arn:${data.aws_partition.current.partition}:bedrock-mantle:${var.aws_region}:${data.aws_caller_identity.current.account_id}:project/*"],
    )
  }
  # Hermes uses the US cross-region inference profile; the model in the routed regions only through that profile.
  statement {
    sid       = "ConverseForHermes"
    actions   = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"]
    resources = [local.hermes_profile_arn]
  }
  statement {
    sid       = "ModelThroughProfileOnly"
    actions   = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"]
    resources = ["arn:${data.aws_partition.current.partition}:bedrock:us-*::foundation-model/${local.ops_agent_models[0]}"]
    condition {
      test     = "StringEquals"
      variable = "bedrock:InferenceProfileArn"
      values   = [local.hermes_profile_arn]
    }
  }
}

resource "aws_iam_role_policy" "ops_agent_local" {
  name   = "invoke-agent-models"
  role   = aws_iam_role.ops_agent_local.id
  policy = data.aws_iam_policy_document.ops_agent_models.json
}
