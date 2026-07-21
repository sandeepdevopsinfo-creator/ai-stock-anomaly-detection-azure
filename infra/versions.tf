terraform {
  required_version = ">= 1.8.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.81"
    }
  }

  backend "azurerm" {
    resource_group_name  = "stock-anomaly-rg"
    storage_account_name = "sandeepstockad20260713"
    container_name       = "tfstate"
    key                  = "stock-anomaly-production.tfstate"
    use_azuread_auth     = true
  }
}

