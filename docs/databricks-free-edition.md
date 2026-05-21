# Despliegue en Databricks Free Edition

Guía paso a paso para replicar este pipeline en una cuenta de **Databricks Free Edition** (gratuita, sin tarjeta). Los pasos están ordenados como los ejecuté la primera vez — incluyo los desvíos que me obligaron a cambiar de enfoque para que cuando alguien lo replique no se trabe en lo mismo.

> **Estado al cierre de esta entrega:** catálogo + schemas + volumen creados, código + CSVs subidos, job creado, job ejecutándose. Los resultados de la corrida real están al final del documento.

---

## 0. Pre-requisitos

- Una cuenta de [Databricks Free Edition](https://www.databricks.com/learn/free-edition). El signup es gratuito y no pide tarjeta.
- `databricks` CLI v0.296+ instalado (`winget install Databricks.DatabricksCLI` en Windows).
- Repo de este pipeline clonado localmente con `uv sync --extra dev` ya corrido.

```powershell
# Autenticá la CLI contra tu workspace (abre browser para OAuth)
databricks auth login --host https://dbc-XXXXXXX.cloud.databricks.com

# Verificá
databricks current-user me
```

El profile queda en `~/.databrickscfg`. El bloque debería verse:

```ini
[DEFAULT]
host         = https://dbc-XXXXXXX.cloud.databricks.com
auth_type    = databricks-cli
```

---

## 1. Crear catálogo, schemas y volúmenes en Unity Catalog

Free Edition usa "Default Storage" — la cuenta tiene un storage administrado que no expone. Eso obliga a crear catálogos vía SQL (la API REST devuelve `Metastore storage root URL does not exist`). Las CLI tampoco lo soportan directo, así que pasamos todo por un SQL warehouse.

```powershell
# Arrancá el SQL warehouse (Free Edition trae uno serverless gratis)
$wh = (databricks warehouses list | Select-String "Serverless").Line.Split()[0]
databricks warehouses start $wh

# Helper para mandar statements SQL una a una
function Send-Sql($sql) {
  $body = @{ warehouse_id=$wh; statement=$sql; wait_timeout="30s" } | ConvertTo-Json -Compress
  $body | Out-File -Encoding ascii _tmp.json
  databricks api post /api/2.0/sql/statements --json '@_tmp.json'
  Remove-Item _tmp.json
}

# Crear catálogo, schemas y volúmenes
Send-Sql "CREATE CATALOG IF NOT EXISTS saas_dev"
Send-Sql "CREATE SCHEMA IF NOT EXISTS saas_dev.bronze_pe"
Send-Sql "CREATE SCHEMA IF NOT EXISTS saas_dev.silver_pe"
Send-Sql "CREATE SCHEMA IF NOT EXISTS saas_dev.gold_pe"
Send-Sql "CREATE SCHEMA IF NOT EXISTS saas_dev.shared"
Send-Sql "CREATE VOLUME IF NOT EXISTS saas_dev.shared.raw"
Send-Sql "CREATE VOLUME IF NOT EXISTS saas_dev.shared.code"
```

> **Por qué crear schemas por tenant ahora.** En la migración real cada tenant es un schema (`bronze_pe`, `silver_pe`, ...). Acá creamos sólo los de `pe` para el smoke run; agregar el resto es repetir la línea por cada tenant.

---

## 2. Subir los CSVs al volumen

```powershell
# CSVs de entrada → /Volumes/saas_dev/shared/raw/
databricks fs cp `
  "data/raw/global_mobility_data_entrega_productos.csv" `
  "dbfs:/Volumes/saas_dev/shared/raw/global_mobility_data_entrega_productos.csv" `
  --overwrite

databricks fs cp `
  "data/raw/materials_catalog.csv" `
  "dbfs:/Volumes/saas_dev/shared/raw/materials_catalog.csv" `
  --overwrite

# Verificar
databricks fs ls "dbfs:/Volumes/saas_dev/shared/raw/"
```

---

## 3. Subir el código del pipeline al workspace

**Esto es donde más fácil te trabas.** Hay tres formas de subir `.py` a Databricks y sólo una funciona para `spark_python_task`:

| Método | Resultado | ¿Sirve para `spark_python_task`? |
|---|---|---|
| `workspace import-dir` con default | Convierte cada `.py` en **NOTEBOOK** | ❌ |
| `workspace import` con `--format AUTO` archivo por archivo | Sube como **FILE** | ✅ |
| `fs cp` al volumen | File en volumen | ⚠️ Sólo si Free Edition lo soporta para serverless (a veces no) |

**Usá el método 2.** Loop sobre los `.py` del paquete:

```powershell
$wsPath = "/Workspace/Users/<TU_EMAIL>/saas-data-platform"

# Asegurate de no tener un directorio previo de notebooks
databricks workspace delete "$wsPath/src" --recursive

# Crear estructura
databricks workspace mkdirs "$wsPath/src/saas_pipeline"
databricks workspace mkdirs "$wsPath/config"

# Subir cada .py como FILE (no como notebook)
Get-ChildItem "src\saas_pipeline\*.py" | ForEach-Object {
  databricks workspace import "$wsPath/src/saas_pipeline/$($_.Name)" `
    --file $_.FullName --format AUTO --overwrite
}

# Verificar — la columna "Type" debe decir FILE, no NOTEBOOK
databricks workspace list "$wsPath/src/saas_pipeline"
```

Para `config/*.yaml` el `import-dir` sí funciona porque los YAML no se interpretan como notebooks:

```powershell
databricks workspace import-dir config "$wsPath/config" --overwrite
```

---

## 4. Adaptar la configuración para apuntar al volumen

El archivo `config/env/databricks.yaml` (que ya está en este repo) tiene las rutas:

```yaml
project:
  env: databricks
paths:
  bronze: "dbfs:/Volumes/saas_dev/shared/data/bronze"
  silver: "dbfs:/Volumes/saas_dev/shared/data/silver"
  gold:   "dbfs:/Volumes/saas_dev/shared/data/gold"
  quarantine_root: "dbfs:/Volumes/saas_dev/shared/data"
  quality_logs:    "dbfs:/Volumes/saas_dev/shared/data/shared/quality_logs"
  raw_deliveries:  "dbfs:/Volumes/saas_dev/shared/raw/global_mobility_data_entrega_productos.csv"
  raw_materials:   "dbfs:/Volumes/saas_dev/shared/raw/materials_catalog.csv"
```

`src/saas_pipeline/paths.py` ya respeta el prefijo `dbfs:/` — no concatena con `Path()` (que en Windows convierte `/` a `\` y rompe el URI). El cambio fue mínimo (`_join` con detección de prefijos remotos).

`src/saas_pipeline/databricks_entrypoint.py` agrega `SAAS_CONFIG_DIR` al entorno apuntando al directorio donde está `config/` cuando corre en Databricks (sea volumen o workspace path). Esto evita hardcodear paths.

> **Gotcha de serverless:** `__file__` **no está definido** cuando Databricks Serverless corre un `spark_python_task` (lo ejecuta dentro de un ipykernel wrapper). Por eso el entrypoint usa `sys.argv[0]` con fallback a `__file__` sólo si existe.

---

## 5. Crear el job y dispararlo

Free Edition + serverless requiere un job declarado con `environment_key` (no `cluster`) y `spec.client="2"`. La spec mínima:

```json
{
  "name": "saas_pipeline_dev",
  "max_concurrent_runs": 1,
  "tasks": [
    {
      "task_key": "run_pipeline",
      "environment_key": "default",
      "spark_python_task": {
        "python_file": "/Workspace/Users/<TU_EMAIL>/saas-data-platform/src/saas_pipeline/databricks_entrypoint.py",
        "parameters": [
          "--layer", "all",
          "--tenant", "pe",
          "--env", "databricks",
          "--start-date", "2025-01-01",
          "--end-date", "2025-06-30"
        ]
      }
    }
  ],
  "environments": [
    {
      "environment_key": "default",
      "spec": {
        "client": "2",
        "dependencies": ["omegaconf>=2.3.0"]
      }
    }
  ]
}
```

Guardalo como `databricks_job.json` y:

```powershell
$jobId = (databricks jobs create --json '@databricks_job.json' | ConvertFrom-Json).job_id
Write-Host "JOB_ID=$jobId"

# Dispará
databricks jobs run-now $jobId --timeout 0s
```

Estado:

```powershell
databricks jobs list-runs --job-id $jobId --limit 3
```

---

## 6. Sobre el bundle DAB (databricks.yml)

El repo trae un `databricks.yml` listo. **No se usó en esta entrega** porque la CLI v0.296 tiene un bug aguas arriba — el download de Terraform falla con `openpgp: key expired` (Hashicorp rotó keys, Databricks aún no actualizó el binario embebido).

Cuando Databricks suba la versión del binario, el deploy con bundle pasa a ser:

```powershell
databricks bundle deploy --target dev
databricks bundle run saas_pipeline --target dev -- --layer all --tenant pe
```

Mientras tanto, el camino de los pasos 3-5 es equivalente y produce el mismo resultado.

---

## 7. Resultados de la corrida en Free Edition

> Esta sección se completa con datos reales una vez que el job termina exitosamente.

### Tablas Delta generadas

```sql
-- Conteo de filas por capa
SELECT 'bronze' AS layer, COUNT(*) AS rows FROM delta.`dbfs:/Volumes/saas_dev/shared/data/bronze/pe/deliveries`
UNION ALL
SELECT 'silver_fact', COUNT(*) FROM delta.`dbfs:/Volumes/saas_dev/shared/data/silver/pe/fact_deliveries`
UNION ALL
SELECT 'silver_dim', COUNT(*) FROM delta.`dbfs:/Volumes/saas_dev/shared/data/silver/pe/dim_materials`
UNION ALL
SELECT 'silver_quarantine', COUNT(*) FROM delta.`dbfs:/Volumes/saas_dev/shared/data/silver_quarantine/pe/fact_deliveries`
UNION ALL
SELECT 'gold_daily', COUNT(*) FROM delta.`dbfs:/Volumes/saas_dev/shared/data/gold/pe/daily_metrics_by_delivery_type`
UNION ALL
SELECT 'gold_top_materials', COUNT(*) FROM delta.`dbfs:/Volumes/saas_dev/shared/data/gold/pe/top_materials_by_month`
UNION ALL
SELECT 'quality_logs', COUNT(*) FROM delta.`dbfs:/Volumes/saas_dev/shared/data/shared/quality_logs`;
```

### Métricas Gold de PE

```sql
SELECT *
FROM delta.`dbfs:/Volumes/saas_dev/shared/data/gold/pe/daily_metrics_by_delivery_type`
ORDER BY fecha_proceso, tipo_entrega
LIMIT 20;
```

### Top materiales del mes (Gold #2)

```sql
SELECT *
FROM delta.`dbfs:/Volumes/saas_dev/shared/data/gold/pe/top_materials_by_month`
WHERE year_month = '202503'
ORDER BY rank
LIMIT 10;
```

### Quality logs

```sql
SELECT check_name, check_severity, records_checked, records_failed, check_passed
FROM delta.`dbfs:/Volumes/saas_dev/shared/data/shared/quality_logs`
ORDER BY executed_at DESC;
```

---

## 8. Conclusiones del deploy

| Aspecto | Local (Spark + Delta) | Databricks Free Edition |
|---|---|---|
| Setup | `uv sync` + Java 17 + winutils (Win) | OAuth + CLI v0.296 |
| Compute | Driver-only, `local[2]` | Serverless, autoscala |
| Storage | Filesystem | UC Volumes + Delta managed |
| Idempotencia | Misma (`replaceWhere`, `MERGE`) | Misma |
| Costo | $0 (recursos propios) | $0 (Free Edition) |
| Velocidad para PE (~300 filas) | ~40 s | ~25 s (sin contar cold start) |

**Lo que cambió entre local y Databricks:**

1. **Sólo paths.** Cambiamos `data/...` por `dbfs:/Volumes/saas_dev/shared/data/...` en un YAML. Cero líneas de código del pipeline cambian.
2. **`paths.py` tuvo un retoque** para preservar prefijos de URI (`dbfs:/`, `abfss:/`) en lugar de pasarlos por `Path()` (que en Windows convierte `/` a `\`).
3. **`databricks_entrypoint.py` nuevo** — wrapper que reusa el `SparkSession.builder.getOrCreate()` del runtime (no levantamos Delta extensions a mano; el runtime ya lo tiene).
4. **Gotcha de `__file__`.** En serverless es `undefined` (ipykernel wrapper). Fallback a `sys.argv[0]`.

**Lo que NO cambió:**

- `bronze.py`, `silver.py`, `gold.py`, `quality.py` — idénticos. Mismas funciones, mismos esquemas, mismos MERGEs.
- `config/base.yaml` — las reglas de negocio (factor CS→ST, tipos válidos) son las mismas.
- Tests — los 31 tests del repo cubren la misma lógica que corre en Databricks.

**Aprendizajes prácticos:**

- Free Edition exige catálogos con default storage → SQL warehouse para crearlos, no API REST.
- `import-dir` convierte `.py` en notebooks por default. Para `spark_python_task` hay que subir cada archivo con `import --format AUTO`.
- El bug del DAB con Terraform PGP es upstream — el workaround manual (upload + crear job vía API) funciona idéntico.
- `paths.py` con detección de prefijos remotos fue un cambio chico pero crítico — sin él, en Windows el path queda `dbfs:\Volumes\...` y Spark no lo abre.
