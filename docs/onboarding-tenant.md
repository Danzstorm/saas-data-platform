# Onboarding de un tenant nuevo — guía operativa

> Cómo agregar un tenant nuevo a la plataforma SAAS, paso a paso. Asume que la infraestructura del ambiente (catálogo, storage account, SP de Databricks) ya está provisionada — ver `docs/infra.md` para el aprovisionamiento de capa de plataforma.

## 0. Pre-requisitos

- Acceso de escritura al repo `Danzstorm/saas-data-platform`.
- Acceso al repo de Terraform de la plataforma (uno por ambiente).
- Permisos para hacer merge en `develop` (PR con revisión).
- Código del tenant en minúsculas, de 2-3 caracteres. Ejemplos válidos: `sv`, `hn`, `mx`, `bra`. Inválidos: `MX`, `México`, `mexico-norte`.

## 1. En el repo de IaC (Terraform)

1. **Crea un nuevo bloque `module`** en `environments/${env}/main.tf`:

   ```hcl
   module "tenant_mx_dev" {
     source       = "../modules/tenant"
     env          = "dev"
     tenant       = "mx"           # el código de 2-3 chars del tenant
     catalog_name = "saas_dev"
     storage_account = "saasdevadls"
   }
   ```

2. **Abre un PR**. CI corre `terraform fmt`, `validate` y `plan -out=tfplan`. El plan queda como artefacto.
3. **Revisar el plan**: deberían aparecer exactamente:
   - 3 `databricks_schema` (`bronze_mx`, `silver_mx`, `gold_mx`)
   - 3 `azurerm_storage_container`
   - 3 `databricks_external_location`
   - 1 `azuread_group` (engineer)
   - 1 `databricks_secret_scope`
   - 3 `databricks_grant` (un schema cada uno)
4. **Merge** y dispara el apply manual desde GitHub Actions.

## 2. En este repo (`saas-data-platform`)

1. **Crea el archivo `config/tenants/${tenant}.yaml`**:

   ```yaml
   # config/tenants/mx.yaml
   tenant:
     id: mx
     display_name: "México"
     timezone: "America/Mexico_City"
     currency: "MXN"
   ```

2. **Agrega el código a `config/base.yaml`** en `tenants.known`:

   ```yaml
   tenants:
     known: [sv, hn, jm, ec, pe, gt, mx]   # ← añade el nuevo
   ```

3. **(Opcional) Override por ambiente** si el tenant tiene un comportamiento distinto:

   ```yaml
   # config/env/qa.yaml — sólo si aplica
   tenants:
     mx_specific: { fail_on_critical: true }
   ```

4. **Commit en una rama `feat/onboard-${tenant}`**, PR contra `develop`.

   ```powershell
   git checkout -b feat/onboard-mx
   git add config/tenants/mx.yaml config/base.yaml
   git commit -m "feat: onboard tenant mx (México)"
   git push -u origin feat/onboard-mx
   gh pr create --title "Onboard tenant mx (México)" --body "Adds Mexico tenant config..."
   ```

5. **CI debe pasar** (lint + tests + validación YAML).

## 3. Smoke test local

Antes de tocar Databricks, valida localmente que el tenant nuevo no rompa nada:

```powershell
# Smoke run con un rango chico
uv run saas-pipeline run --layer all --tenant mx --env dev --start-date 2025-01-01 --end-date 2025-01-07
```

Verifica:
- No hubo errores.
- `data/bronze/mx/deliveries/` contiene particiones.
- `data/silver/mx/{fact_deliveries,dim_materials}/` contiene particiones.
- `data/gold/mx/daily_metrics_by_delivery_type/` contiene particiones.
- `data/shared/quality_logs/` tiene rows con `tenant_id='mx'`.

## 4. Smoke run en Databricks (dev)

1. **Despliega el bundle** (DAB):

   ```bash
   databricks bundle deploy --target dev --var tenant=mx
   ```

2. **Dispara el job** manualmente:

   ```bash
   databricks bundle run saas_pipeline --target dev --params '{"tenant":"mx","start_date":"2025-01-01","end_date":"2025-01-07"}'
   ```

3. **Verifica** con SQL:

   ```sql
   SELECT * FROM saas_dev.gold_mx.daily_metrics_by_delivery_type LIMIT 10;
   SELECT * FROM saas_dev.shared.quality_logs WHERE tenant_id = 'mx' ORDER BY executed_at DESC LIMIT 20;
   ```

## 5. Promoción a QA y main

Repite los pasos 1-4 cambiando `env=qa` y luego `env=main`. La promoción a `main` debe pasar antes por una revisión del PM y del equipo de Data Governance — los criterios están en `docs/observations.md` (sec 4 sobre retención).

## 6. Rollback

Si algo falla:

1. **En Databricks**: cancela el job, no hay nada que limpiar (Delta es ACID).
2. **En el repo**: revertir el PR de `config/tenants/mx.yaml` (sólo si el tenant ya no se va a usar).
3. **En Terraform**: `terraform destroy -target=module.tenant_mx_dev` (cuidado: borra storage y schemas; no usar en `main`).

## 7. Checklist final

- [ ] PR de Terraform mergeado y applied
- [ ] PR de config mergeado en `develop`
- [ ] Smoke run local OK
- [ ] Smoke run Databricks dev OK
- [ ] Quality logs muestran rows del tenant nuevo
- [ ] Job programado en Databricks (job schedule en el bundle)
- [ ] Notificación al canal del equipo del tenant
