resource "aws_kms_key" "lakehouse" {
  description             = "${local.name} data encryption"
  enable_key_rotation     = true
  deletion_window_in_days = 7
}

resource "aws_kms_alias" "lakehouse" {
  name          = "alias/${local.name}"
  target_key_id = aws_kms_key.lakehouse.key_id
}
