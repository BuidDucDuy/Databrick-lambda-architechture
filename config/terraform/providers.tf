terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
    databricks = {
      source = "databricks/databricks"
    }
  }
}

provider "aws" {
  region = "ap-southeast-1"
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}
