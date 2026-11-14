resource "aws_db_subnet_group" "postgres" {
  name       = local.name
  subnet_ids = module.vpc.private_subnets
}

resource "aws_security_group" "postgres" {
  name        = "${local.name}-postgres"
  description = "Postgres, reachable from the EKS nodes only"
  vpc_id      = module.vpc.vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "postgres_from_nodes" {
  security_group_id            = aws_security_group.postgres.id
  referenced_security_group_id = module.eks.node_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  description                  = "EKS nodes"
}

resource "aws_db_parameter_group" "postgres" {
  name   = "${local.name}-postgres17"
  family = "postgres17"

  # Static parameters. AWS reports apply_method as pending-reboot, so a shorter method diffs forever.
  parameter {
    name         = "rds.force_ssl"
    apply_method = "pending-reboot"
    value        = "1"
  }
  parameter {
    name         = "ssl_min_protocol_version"
    apply_method = "pending-reboot"
    value        = "TLSv1.2"
  }
  parameter {
    name  = "log_connections"
    value = "1"
  }
  parameter {
    name  = "log_disconnections"
    value = "1"
  }
}

resource "aws_db_instance" "postgres" {
  identifier     = local.name
  engine         = "postgres"
  engine_version = "17"
  instance_class = "db.t4g.micro"

  allocated_storage = 20
  storage_type      = "gp3"
  storage_encrypted = true
  kms_key_id        = aws_kms_key.lakehouse.arn

  db_name  = "apicurio"
  username = "lakehouse"
  # RDS generates the password and keeps it in Secrets Manager; it never enters Terraform state.
  manage_master_user_password = true

  parameter_group_name   = aws_db_parameter_group.postgres.name
  db_subnet_group_name   = aws_db_subnet_group.postgres.name
  vpc_security_group_ids = [aws_security_group.postgres.id]
  publicly_accessible    = false
  multi_az               = false

  backup_retention_period    = 1
  auto_minor_version_upgrade = true
  apply_immediately          = true
  # Ephemeral environment: no final snapshot, no deletion protection.
  skip_final_snapshot = true
  deletion_protection = false
}
