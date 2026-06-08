terraform {
  backend "gcs" {
    bucket = "diploma-balancer-tfstate"
    prefix = "terraform/state"
  }
}