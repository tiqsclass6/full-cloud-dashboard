# The bucket name contains the account id, so it is passed at init: -backend-config="bucket=lakehouse-tfstate-<account-id>"
terraform {
  backend "s3" {
    key          = "lakehouse/bootstrap/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}
