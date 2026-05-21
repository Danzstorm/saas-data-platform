# Infraestructura — Terraform para onboarding de un tenant nuevo

> Este documento describe **qué provisionaría Terraform** para soportar el onboarding de un nuevo tenant en la plataforma SAAS multi-tenant, y muestra un snippet del módulo principal. **No se requiere que `terraform plan` corra contra una cuenta real** — el snippet es ilustrativo, alineado con la arquitectura de las secciones 5.2 y 5.3 de la prueba.

## 1. Qué se provisiona

El onboarding de un tenant nuevo (ej. `gt` Guatemala) en un ambiente (ej. `dev`) requiere los siguientes recursos:

### 1.1 Unity Catalog

| Recurso | Nombre / convención | Responsabilidad |
|---|---|---|
| `databricks_schema` | `saas_dev.bronze_gt` | Tablas Bronze del tenant |
| `databricks_schema` | `saas_dev.silver_gt` | Tablas Silver del tenant |
| `databricks_schema` | `saas_dev.gold_gt` | Tablas Gold del tenant |
| `databricks_grant` (×3) | grant a grupo `saas_${env}_${tenant}_engineer` | RW sobre los 3 schemas del tenant |
| `databricks_grant` | grant a grupo `saas_${env}_${tenant}_consumer` | READ sólo sobre `gold_${tenant}` |
| `databricks_external_location` | `saas-${env}-${tenant}` | Punto de montaje sobre ADLS Gen2 (un container por tenant) |
| `databricks_storage_credential` | `saas-${env}` | Credencial compartida; el aislamiento real lo da la external location |

### 1.2 ADLS Gen2

| Recurso | Nombre / convención | Responsabilidad |
|---|---|---|
| `azurerm_storage_container` | `bronze-${tenant}` en cuenta `saas${env}` | Storage Bronze (raw + Delta) |
| `azurerm_storage_container` | `silver-${tenant}` | Storage Silver |
| `azurerm_storage_container` | `gold-${tenant}` | Storage Gold |
| `azurerm_role_assignment` | Storage Blob Data Contributor → SP de Databricks | Permite a Databricks escribir |
| Lifecycle policy (opcional) | sobre `bronze-${tenant}` | Move-to-cool tras N días |

### 1.3 Secretos y configuración

| Recurso | Nombre / convención |
|---|---|
| `databricks_secret_scope` | `saas-${env}-${tenant}` |
| `databricks_secret` | Connection strings de orígenes operacionales del tenant (Mongo, Couchbase si aplica) |
| Entry en `config/tenants/${tenant}.yaml` | Archivo nuevo en el repo |

### 1.4 Identity (Entra ID / Azure AD)

| Recurso | Nombre |
|---|---|
| `azuread_group` | `saas-${env}-${tenant}-engineer` |
| `azuread_group` | `saas-${env}-${tenant}-consumer` |
| `databricks_group` (synced) | mismo nombre, federado |

### 1.5 Jobs / orquestación

| Recurso | Nombre |
|---|---|
| `databricks_job` | `saas-${env}-${tenant}-bronze-to-gold` |
| `databricks_job_run_now` (opcional) | smoke run inmediato post-onboarding |

---

## 2. Snippet ilustrativo del módulo

```hcl
# modules/tenant/main.tf — onboardea un tenant nuevo en un ambiente dado.
# No es un plan corrible contra una cuenta real; es ilustrativo.

terraform {
  required_version = ">= 1.6.0"
  required_providers {
    databricks = { source = "databricks/databricks", version = "~> 1.50" }
    azurerm    = { source = "hashicorp/azurerm",    version = "~> 3.110" }
    azuread    = { source = "hashicorp/azuread",    version = "~> 2.50" }
  }
}

variable "env"               { type = string }                 # dev | qa | main
variable "tenant"            { type = string }                 # sv, hn, gt, ...
variable "catalog_name"      { type = string }                 # saas_dev
variable "storage_account"   { type = string }                 # saasdevadls
variable "databricks_sp_id"  { type = string }                 # service principal de Databricks

locals {
  layers = ["bronze", "silver", "gold"]
  prefix = "saas-${var.env}-${var.tenant}"
}

# --- 1. Schemas por capa en Unity Catalog -----------------------------------
resource "databricks_schema" "layers" {
  for_each      = toset(local.layers)
  catalog_name  = var.catalog_name
  name          = "${each.value}_${var.tenant}"
  comment       = "Layer ${each.value} for tenant ${var.tenant}"
  properties = { tenant = var.tenant, layer = each.value }
}

# --- 2. Containers ADLS por capa --------------------------------------------
resource "azurerm_storage_container" "layers" {
  for_each              = toset(local.layers)
  name                  = "${each.value}-${var.tenant}"
  storage_account_name  = var.storage_account
  container_access_type = "private"
}

# --- 3. External location por capa (URI abfss://) ----------------------------
resource "databricks_external_location" "layers" {
  for_each       = toset(local.layers)
  name           = "${local.prefix}-${each.value}"
  url            = "abfss://${each.value}-${var.tenant}@${var.storage_account}.dfs.core.windows.net/"
  credential_name = "saas-${var.env}"                  # storage credential compartida del ambiente
}

# --- 4. Grupos de acceso + grants -------------------------------------------
resource "azuread_group" "engineer" {
  display_name     = "${local.prefix}-engineer"
  security_enabled = true
}

resource "databricks_grant" "engineer_schemas" {
  for_each   = databricks_schema.layers
  securable_type = "SCHEMA"
  securable_name = "${var.catalog_name}.${each.value.name}"
  principal      = azuread_group.engineer.display_name
  privileges     = ["ALL_PRIVILEGES"]
}

# --- 5. Secret scope para credenciales del tenant ---------------------------
resource "databricks_secret_scope" "tenant" {
  name                     = local.prefix
  initial_manage_principal = "users"
}

output "schemas" { value = [for s in databricks_schema.layers : s.name] }
output "secret_scope" { value = databricks_secret_scope.tenant.name }
```

**Uso del módulo desde un environment root:**

```hcl
module "tenant_gt_dev" {
  source           = "../modules/tenant"
  env              = "dev"
  tenant           = "gt"
  catalog_name     = "saas_dev"
  storage_account  = "saasdevadls"
  databricks_sp_id = data.azuread_service_principal.databricks.object_id
}
```

---

## 3. Lo que NO está en el snippet (y por qué)

- **Provider configuration** (`provider "databricks" { ... }`): vive en el root, no en el módulo, porque los providers se configuran con auth tokens que vienen del entorno (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`) o de un Service Principal compartido.
- **Backend remoto** (`terraform { backend "azurerm" { ... } }`): también en el root, normalmente un container `tfstate` separado por ambiente.
- **Workspaces de Databricks**: se asumen pre-existentes (un workspace por ambiente). Crear workspaces se hace fuera de este módulo, manualmente o con un módulo `platform` separado.
- **`workspaces_users` y SCIM sync**: en una organización con Entra ID, el sync es continuo; los grupos de aquí terminan reflejados en Databricks vía SCIM automáticamente.

## 4. Flujo de onboarding (resumen operativo)

1. **Repo de IaC** (separado del repo del pipeline): un PR agrega una nueva instancia del módulo `tenant` con `tenant = "gt"` en `environments/dev/main.tf`.
2. **CI de IaC**: `terraform fmt` + `terraform validate` + `terraform plan -out=tfplan`. El plan se sube como artefacto del PR para revisión.
3. **Apply manual** tras aprobación, vía un job de GitHub Actions con OIDC federation a Azure (no PAT en secrets).
4. **PR en este repo** (`saas-data-platform`): agrega `config/tenants/gt.yaml`, actualiza la lista `tenants.known` en `config/base.yaml`.
5. **Smoke run**: `make run-all TENANT=gt ENV=dev` localmente, luego un manual run del job `saas-dev-gt-bronze-to-gold` en Databricks.
6. **Verificación**: query a `gold_gt.daily_metrics_by_delivery_type` debe devolver filas para las fechas del smoke run.

> Ver `docs/onboarding-tenant.md` para la versión paso a paso enfocada al ingeniero que hace el onboarding.
