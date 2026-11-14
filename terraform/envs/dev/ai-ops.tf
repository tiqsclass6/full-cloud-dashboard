# The Gateway target comes from aws-agentcore.sh and the Runtime from a second apply; ARN patterns allow AgentCore's id suffix.

locals {
  # Sonnet investigates; Opus answers only when Sonnet declines.
  ops_agent_models       = ["anthropic.claude-sonnet-4-6", "anthropic.claude-opus-5-5"]
  partition              = data.aws_partition.current.partition
  agentcore_arn          = "arn:${local.partition}:bedrock-agentcore:${var.region}:${local.account_id}"
  agentcore_runtime_name = replace("${local.name}_ops_agent", "-", "_")
  agentcore_gateway_name = "${local.name}-ops-tools"
  agentcore_runtime_arns = "${local.agentcore_arn}:runtime/${local.agentcore_runtime_name}-*"
  agentcore_gateway_arns = "${local.agentcore_arn}:gateway/${local.agentcore_gateway_name}-*"
  # The API-key provider aws-agentcore.sh creates for the Gateway target (named after the cluster).
  agentcore_api_key_provider = "${local.agentcore_arn}:token-vault/default/apikeycredentialprovider/${local.name}-mcp-token"
  hermes_profile_arn         = "arn:${local.partition}:bedrock:${var.region}:${local.account_id}:inference-profile/us.${local.ops_agent_models[0]}"
  # Runtime is offered in a subset of zone IDs, which are stable across accounts; zone names are not.
  agentcore_subnets = [for s in data.aws_subnet.private : s.id if contains(var.agentcore_az_ids, s.availability_zone_id)]
}

data "aws_iam_policy_document" "ops_agent_models" {
  statement {
    sid     = "MessagesApiOnBedrockMantle"
    actions = ["bedrock-mantle:CreateInference"]
    resources = concat(
      [for m in local.ops_agent_models : "arn:${local.partition}:bedrock:${var.region}::foundation-model/${m}"],
      ["arn:${local.partition}:bedrock-mantle:${var.region}:${local.account_id}:project/*"],
    )
  }
}

# Hermes (its own role): Converse through the US inference profile, and the model only through that profile.
data "aws_iam_policy_document" "hermes" {
  statement {
    sid       = "InferenceProfile"
    actions   = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"]
    resources = [local.hermes_profile_arn]
  }
  statement {
    sid       = "ModelThroughProfileOnly"
    actions   = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"]
    resources = ["arn:${local.partition}:bedrock:us-*::foundation-model/${local.ops_agent_models[0]}"]
    condition {
      test     = "StringEquals"
      variable = "bedrock:InferenceProfileArn"
      values   = [local.hermes_profile_arn]
    }
  }
}
resource "aws_iam_policy" "hermes" {
  name   = "${local.name}-hermes"
  policy = data.aws_iam_policy_document.hermes.json
}
resource "aws_iam_role_policy_attachment" "hermes" {
  role       = aws_iam_role.irsa["hermes"].name
  policy_arn = aws_iam_policy.hermes.arn
}

data "aws_iam_policy_document" "ops_agent" {
  source_policy_documents = [data.aws_iam_policy_document.ops_agent_models.json]
  statement {
    sid       = "InvokeAgentRuntime"
    actions   = ["bedrock-agentcore:InvokeAgentRuntime"]
    resources = [local.agentcore_runtime_arns, "${local.agentcore_runtime_arns}/runtime-endpoint/*"]
  }
}
resource "aws_iam_policy" "ops_agent" {
  name   = "${local.name}-ops-agent"
  policy = data.aws_iam_policy_document.ops_agent.json
}
resource "aws_iam_role_policy_attachment" "ops_agent" {
  role       = aws_iam_role.irsa["ops-agent"].name
  policy_arn = aws_iam_policy.ops_agent.arn
}

resource "aws_iam_policy" "aws_load_balancer_controller" {
  name   = "${local.name}-aws-load-balancer-controller"
  policy = file("${path.module}/policies/aws-load-balancer-controller-v3.6.0.json")
}
resource "aws_iam_role_policy_attachment" "aws_load_balancer_controller" {
  role       = aws_iam_role.irsa["aws-load-balancer-controller"].name
  policy_arn = aws_iam_policy.aws_load_balancer_controller.arn
}

resource "aws_secretsmanager_secret" "mcp_ca" {
  name                    = "${local.name}/ai-ops/mcp-ca-certificate"
  description             = "PEM CA certificate AgentCore Gateway trusts for the MCP server's TLS endpoint (public, not a key)"
  kms_key_id              = aws_kms_key.lakehouse.arn
  recovery_window_in_days = 0
}

# SourceArn is required: SourceAccount alone would let any AgentCore resource in the account assume the role.
data "aws_iam_policy_document" "agentcore_trust" {
  for_each = { gateway = local.agentcore_gateway_arns, runtime = local.agentcore_runtime_arns }
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["bedrock-agentcore.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = [each.value]
    }
  }
}

resource "aws_iam_role" "agentcore_gateway" {
  name               = "${local.name}-agentcore-gateway"
  assume_role_policy = data.aws_iam_policy_document.agentcore_trust["gateway"].json
}

data "aws_iam_policy_document" "agentcore_gateway" {
  statement {
    sid       = "ReadMcpCaCertificate"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.mcp_ca.arn]
  }
  # The key is shared with S3 and RDS: usable only through AgentCore (Gateway configuration) and Secrets Manager.
  statement {
    sid       = "UseLakehouseKeyViaServices"
    actions   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.lakehouse.arn]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["bedrock-agentcore.${var.region}.amazonaws.com", "secretsmanager.${var.region}.amazonaws.com"]
    }
  }
  statement {
    sid       = "DescribeLakehouseKey"
    actions   = ["kms:DescribeKey"]
    resources = [aws_kms_key.lakehouse.arn]
  }
  statement {
    sid     = "WorkloadAccessToken"
    actions = ["bedrock-agentcore:GetWorkloadAccessToken"]
    resources = [
      "${local.agentcore_arn}:workload-identity-directory/default",
      "${local.agentcore_arn}:workload-identity-directory/default/workload-identity/${local.agentcore_gateway_name}-*",
    ]
  }
  statement {
    sid     = "OutboundApiKey"
    actions = ["bedrock-agentcore:GetResourceApiKey"]
    resources = [
      "${local.agentcore_arn}:workload-identity-directory/default",
      "${local.agentcore_arn}:workload-identity-directory/default/workload-identity/${local.agentcore_gateway_name}-*",
      "${local.agentcore_arn}:token-vault/default",
      local.agentcore_api_key_provider,
    ]
  }
  statement {
    sid       = "ReadApiKeyFromTokenVault"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:${local.partition}:secretsmanager:${var.region}:${local.account_id}:secret:bedrock-agentcore-identity!*"]
  }
}
resource "aws_iam_role_policy" "agentcore_gateway" {
  name   = "gateway"
  role   = aws_iam_role.agentcore_gateway.id
  policy = data.aws_iam_policy_document.agentcore_gateway.json
}

# IAM changes reach AgentCore with a delay: CreateGateway failed twice on kms:GenerateDataKey despite depends_on.
resource "time_sleep" "agentcore_gateway_iam" {
  create_duration = "30s"
  triggers        = { policy = sha1(aws_iam_role_policy.agentcore_gateway.policy) }
  depends_on      = [aws_iam_role_policy.agentcore_gateway]
}

resource "aws_bedrockagentcore_gateway" "ops_tools" {
  name            = local.agentcore_gateway_name
  description     = "The platform's read-only MCP tools for the ops agent"
  role_arn        = aws_iam_role.agentcore_gateway.arn
  protocol_type   = "MCP"
  authorizer_type = "AWS_IAM"
  kms_key_arn     = aws_kms_key.lakehouse.arn
  depends_on      = [time_sleep.agentcore_gateway_iam]
}

resource "aws_security_group" "agentcore_gateway_egress" {
  name        = "${local.name}-agentcore-gateway-egress"
  description = "AgentCore Gateway to the MCP endpoint (internal NLB) inside the VPC"
  vpc_id      = module.vpc.vpc_id
}
resource "aws_vpc_security_group_egress_rule" "agentcore_gateway_to_mcp" {
  security_group_id = aws_security_group.agentcore_gateway_egress.id
  cidr_ipv4         = var.vpc_cidr
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  description       = "TLS to the MCP endpoint"
}

resource "aws_iam_role" "agentcore_runtime" {
  name               = "${local.name}-agentcore-runtime"
  assume_role_policy = data.aws_iam_policy_document.agentcore_trust["runtime"].json
}

data "aws_iam_policy_document" "agentcore_runtime" {
  source_policy_documents = [data.aws_iam_policy_document.ops_agent_models.json]
  statement {
    sid       = "PullAgentImage"
    actions   = ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"]
    resources = [aws_ecr_repository.app["lakehouse/ai-ops"].arn]
  }
  statement {
    sid       = "EcrToken"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }
  # Image layers are stored through ECR in S3; the key cannot be used directly.
  statement {
    sid       = "DecryptImageLayers"
    actions   = ["kms:Decrypt"]
    resources = [aws_kms_key.lakehouse.arn]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["ecr.${var.region}.amazonaws.com", "s3.${var.region}.amazonaws.com"]
    }
  }
  # Only this Runtime's log groups (the prefix aws-down.sh deletes).
  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"]
    resources = ["arn:${local.partition}:logs:${var.region}:${local.account_id}:log-group:/aws/bedrock-agentcore/runtimes/${local.agentcore_runtime_name}-*"]
  }
  statement {
    sid       = "UseTheToolGateway"
    actions   = ["bedrock-agentcore:InvokeGateway"]
    resources = [aws_bedrockagentcore_gateway.ops_tools.gateway_arn]
  }
}
resource "aws_iam_role_policy" "agentcore_runtime" {
  name   = "runtime"
  role   = aws_iam_role.agentcore_runtime.id
  policy = data.aws_iam_policy_document.agentcore_runtime.json
}

resource "aws_security_group" "agentcore_runtime" {
  name        = "${local.name}-agentcore-runtime"
  description = "The ops agent on AgentCore Runtime: HTTPS out (Bedrock, the Gateway) through the NAT gateway"
  vpc_id      = module.vpc.vpc_id
}
resource "aws_vpc_security_group_egress_rule" "agentcore_runtime_https" {
  security_group_id = aws_security_group.agentcore_runtime.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  description       = "HTTPS to Amazon Bedrock and AgentCore Gateway"
}

data "aws_subnet" "private" {
  count = length(module.vpc.private_subnets)
  id    = module.vpc.private_subnets[count.index]
}

resource "aws_bedrockagentcore_agent_runtime" "ops_agent" {
  count              = var.agentcore_runtime ? 1 : 0
  agent_runtime_name = local.agentcore_runtime_name
  description        = "Platform ops agent: alert triage with Claude, read-only tools through AgentCore Gateway"
  role_arn           = aws_iam_role.agentcore_runtime.arn

  agent_runtime_artifact {
    container_configuration {
      container_uri = "${aws_ecr_repository.app["lakehouse/ai-ops"].repository_url}:${var.agentcore_image_tag}"
    }
  }
  network_configuration {
    network_mode = "VPC"
    network_mode_config {
      subnets         = local.agentcore_subnets
      security_groups = [aws_security_group.agentcore_runtime.id]
    }
  }
  protocol_configuration {
    server_protocol = "HTTP"
  }
  environment_variables = {
    OPS_AGENT_MODE    = "runtime"
    AWS_REGION        = var.region
    MODEL_ID          = local.ops_agent_models[0]
    FALLBACK_MODEL_ID = local.ops_agent_models[1]
    GATEWAY_URL       = "${aws_bedrockagentcore_gateway.ops_tools.gateway_url}"
  }
  depends_on = [aws_iam_role_policy.agentcore_runtime]
}
