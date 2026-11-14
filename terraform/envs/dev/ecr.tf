resource "aws_ecr_repository" "app" {
  for_each = toset([
    "lakehouse/ingestion-service",
    "lakehouse/flink-iceberg-job",
    "lakehouse/spark-jobs",
    "lakehouse/iceberg-exporter",
    "lakehouse/ai-ops",
    "lakehouse/mcp-platform-tools",
    "lakehouse/hermes",
  ])
  name = each.key
  # Mutable tags in dev only, so a rebuilt image can keep its version during a demo.
  image_tag_mutability = var.ecr_tag_mutability
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
  encryption_configuration {
    encryption_type = "KMS"
    kms_key         = aws_kms_key.lakehouse.arn
  }
}

resource "aws_ecr_lifecycle_policy" "app" {
  for_each   = aws_ecr_repository.app
  repository = each.value.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the 10 most recent images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 10 }
      action       = { type = "expire" }
    }]
  })
}
