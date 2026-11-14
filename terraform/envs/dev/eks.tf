# One node group per zone: an EBS volume needs its replacement node in the same zone.
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "21.29.0"

  name               = local.name
  kubernetes_version = var.kubernetes_version

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  endpoint_public_access       = true
  endpoint_public_access_cidrs = var.api_allowed_cidrs
  endpoint_private_access      = true

  authentication_mode                      = "API"
  enable_cluster_creator_admin_permissions = true
  enable_irsa                              = true

  create_kms_key    = false
  encryption_config = { provider_key_arn = aws_kms_key.lakehouse.arn, resources = ["secrets"] }

  enabled_log_types                      = ["audit", "authenticator"]
  cloudwatch_log_group_retention_in_days = 7

  addons = {
    # enableNetworkPolicy: without it EKS accepts NetworkPolicies but ignores them.
    vpc-cni = {
      before_compute       = true
      configuration_values = jsonencode({ enableNetworkPolicy = "true" })
    }
    kube-proxy         = {}
    coredns            = {}
    aws-ebs-csi-driver = { service_account_role_arn = aws_iam_role.irsa["ebs-csi"].arn }
    metrics-server     = {}
  }

  eks_managed_node_groups = {
    for i, az in local.azs : "spot-${substr(az, -1, 1)}" => {
      subnet_ids     = [module.vpc.private_subnets[i]]
      ami_type       = "AL2023_x86_64_STANDARD"
      capacity_type  = "SPOT"
      instance_types = var.node_instance_types
      min_size       = 1
      max_size       = 2
      desired_size   = 1

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = 50
            volume_type           = "gp3"
            encrypted             = true
            delete_on_termination = true
          }
        }
      }

      # IMDSv2 with a hop limit of 1: pods cannot borrow the node's role and must use IRSA.
      metadata_options = {
        http_endpoint               = "enabled"
        http_tokens                 = "required"
        http_put_response_hop_limit = 1
      }

      labels = { "lakehouse.io/pool" = "general" }
    }
  }
}
