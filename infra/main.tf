# ============================================================
# Resource Group
# ============================================================

resource "azurerm_resource_group" "rg" {
  name     = "stock-anomaly-rg"
  location = "East US"
}


# ============================================================
# Storage Account
# ============================================================

resource "azurerm_storage_account" "storage" {
  name                     = "sandeepstockad20260713"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"

  allow_nested_items_to_be_public = false
  min_tls_version                 = "TLS1_0"
}


# ============================================================
# Event Hub Namespace
# ============================================================

resource "azurerm_eventhub_namespace" "events" {
  name                = "sandeep-stock-events-2026"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "Standard"
  capacity            = 1
}


# ============================================================
# Event Hub
# ============================================================

resource "azurerm_eventhub" "anomaly_events" {
  name            = "stock-anomaly-events"
  namespace_id    = azurerm_eventhub_namespace.events.id
  partition_count = 1

  retention_description {
    cleanup_policy          = "Delete"
    retention_time_in_hours = 1
  }
}


# ============================================================
# Event Hub Consumer Group
# ============================================================

resource "azurerm_eventhub_consumer_group" "anomaly_consumer" {
  name                = "anomaly-alert-consumer"
  namespace_name      = azurerm_eventhub_namespace.events.name
  eventhub_name       = azurerm_eventhub.anomaly_events.name
  resource_group_name = azurerm_resource_group.rg.name
}


# ============================================================
# Existing Log Analytics Workspace
# ============================================================

data "azurerm_log_analytics_workspace" "app_insights_workspace" {
  name                = "DefaultWorkspace-7d914a90-ba64-4ffb-9f29-ba271fb2f0a1-EUS"
  resource_group_name = "DefaultResourceGroup-EUS"
}


# ============================================================
# Application Insights
# ============================================================

resource "azurerm_application_insights" "app_insights" {
  name                = "Sandeep-stock-anomaly-func-2026-v2"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  application_type    = "web"
  workspace_id        = data.azurerm_log_analytics_workspace.app_insights_workspace.id

  sampling_percentage = 0
}


# ============================================================
# Flex Consumption Service Plan
# ============================================================

resource "azurerm_service_plan" "function_plan" {
  name                = "ASP-stockanomalyrg-9f27"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  os_type             = "Linux"
  sku_name            = "FC1"

  lifecycle {
    prevent_destroy = true
  }
}


# ============================================================
# Flex Consumption Function App
# ============================================================

resource "azurerm_function_app_flex_consumption" "function_app" {
  name                = "Sandeep-stock-anomaly-func-2026-v2"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  service_plan_id     = azurerm_service_plan.function_plan.id

  storage_container_type     = "blobContainer"
  storage_container_endpoint = "https://sandeepstockad20260713.blob.core.windows.net/app-package-sandeep-stock-anomaly-func-2026-v-0215662"

  storage_authentication_type = "StorageAccountConnectionString"
  storage_access_key          = azurerm_storage_account.storage.primary_access_key

  runtime_name    = "python"
  runtime_version = "3.12"

  instance_memory_in_mb  = 2048
  maximum_instance_count = 100

  identity {
    type = "SystemAssigned"
  }

  site_config {}

  lifecycle {
    prevent_destroy = true

    ignore_changes = [
      app_settings
    ]
  }
}
data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "stock_anomaly_kv" {
  name                = "sandeep-stock-kv-2026"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  rbac_authorization_enabled    = true
  public_network_access_enabled = true
  soft_delete_retention_days    = 7
  purge_protection_enabled      = false

  lifecycle {
    prevent_destroy = true
  }
}
# ============================================================
# Function App permission to read Key Vault secrets
# ============================================================

resource "azurerm_role_assignment" "function_key_vault_secrets_user" {
  scope                = azurerm_key_vault.stock_anomaly_kv.id
  role_definition_name = "Key Vault Secrets User"

  principal_id = azurerm_function_app_flex_consumption.function_app.identity[0].principal_id

  principal_type = "ServicePrincipal"

  skip_service_principal_aad_check = true

  depends_on = [
    azurerm_key_vault.stock_anomaly_kv,
    azurerm_function_app_flex_consumption.function_app
  ]
}
