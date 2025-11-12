terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.9.0"
    }
  }
}


provider "aws" {
  region = "eu-west-1" # Chamge this to your region
}



module "s3" {
  source         = "../../modules/s3"
  s3_bucket_name = "zalihat-ecommerce-raw-dev" # Change this to your bucket name

}
