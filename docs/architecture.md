# Arquitectura del pipeline

> Este documento describe la arquitectura del pipeline SAAS multi-tenant **parte por parte**, con diagramas Mermaid que GitHub renderiza nativamente. Es la referencia visual del README — léelo de arriba a abajo para entender cómo viajan los datos.

---

## 1. Flujo end-to-end (vista 30 000 pies)

```mermaid
flowchart LR
    subgraph RAW[RAW · CSV en disco]
        A1[global_mobility_data_entrega_productos.csv<br/>~3 100 filas · 6 tenants · Q1-Q2 2025<br/>~4 %25 anomalías intencionales]
        A2[materials_catalog.csv<br/>SCD Type 2<br/>versiones por SKU]
    end

    subgraph BRONZE[BRONZE · Delta · particionado fecha + tenant]
        B1[data/bronze/&lt;tenant&gt;/deliveries/<br/>fecha_proceso=YYYYMMDD/_tenant_id=&lt;t&gt;]
    end

    subgraph SILVER[SILVER · Delta · idempotente vía MERGE]
        S1[silver/&lt;tenant&gt;/dim_materials<br/>SCD Type 2]
        S2[silver/&lt;tenant&gt;/fact_deliveries<br/>limpio · enriquecido · CS→ST]
        SQ[silver_quarantine/&lt;tenant&gt;/fact_deliveries<br/>filas con anomalías + _quarantine_reason]
    end

    subgraph GOLD[GOLD · Delta · recompute por partición]
        G1[gold/&lt;tenant&gt;/daily_metrics_by_delivery_type<br/>total_units · total_revenue · routes · transports]
    end

    subgraph SHARED[SHARED · Delta]
        Q1[shared/quality_logs<br/>_run_id · check_name · severity · passed]
    end

    A1 -->|replaceWhere<br/>tenant + fecha_proceso| B1
    A2 -->|MERGE<br/>material + valid_from| S1
    B1 -->|anomaly classification<br/>dedup · enrich temporal| S2
    B1 -->|filas con _quarantine_reason| SQ
    S1 -.->|join temporal<br/>fecha BETWEEN valid_from AND valid_to| S2
    S2 -->|recompute con replaceWhere| G1
    S2 -.->|4 checks DQ| Q1
    S1 -.->|1 check SCD2| Q1
```

---

## 2. Capa por capa — qué hace cada `.py`

```mermaid
flowchart TB
    CLI[cli.py · Typer<br/>--layer --tenant --env --start-date --end-date]

    CFG[config.py · OmegaConf<br/>base → env → tenant → CLI overrides]
    PATHS[paths.py<br/>compone data/&lt;layer&gt;/&lt;tenant&gt;/&lt;table&gt;/]
    SPARK[spark.py<br/>SparkSession + Delta extensions]
    UTILS[utils.py<br/>run_id · batch_id · date_range]
    SCHEMAS[schemas.py<br/>esquemas explícitos de CSV]

    BRONZE[bronze.py<br/>· lee CSV con esquema<br/>· filtra por tenant<br/>· añade columnas técnicas<br/>· replaceWhere por partición]

    SILVER[silver.py<br/>· run_dim_materials = MERGE SCD2<br/>· run_fact_deliveries =<br/>  ① clasificar anomalías<br/>  ② dedup exacto<br/>  ③ join temporal con dim<br/>  ④ marcar orphans<br/>  ⑤ separar clean / quarantine<br/>  ⑥ normalizar CS→ST · flags<br/>  ⑦ MERGE sobre clave de negocio]

    QUALITY[quality.py<br/>4 checks Silver:<br/>· no_orphan_in_fact crítico<br/>· cantidad_positive crítico<br/>· revenue_non_negative warn<br/>· dim_one_current warn<br/>→ append a quality_logs]

    GOLD[gold.py<br/>· lee Silver fact<br/>· groupBy tenant+fecha+tipo<br/>· total_units · total_revenue<br/>  active_routes · active_transports<br/>· replaceWhere por rango fechas]

    CLI --> CFG
    CLI --> SPARK
    CLI --> BRONZE
    CLI --> SILVER
    CLI --> QUALITY
    CLI --> GOLD

    BRONZE --> PATHS
    SILVER --> PATHS
    GOLD --> PATHS
    QUALITY --> PATHS

    BRONZE --> SCHEMAS
    SILVER --> SCHEMAS

    BRONZE --> UTILS
    SILVER --> UTILS
```

---

## 3. Detalle Bronze — qué hace una corrida

```mermaid
sequenceDiagram
    autonumber
    participant CLI
    participant Bronze as bronze.py
    participant Spark
    participant Delta as data/bronze/&lt;tenant&gt;/deliveries

    CLI->>Bronze: run(tenant='pe', start='2025-01-01', end='2025-06-30')
    Bronze->>Spark: read.csv(raw_deliveries, schema=RAW_DELIVERIES_SCHEMA)
    Note over Spark: Esquema explícito — sin inferencia
    Bronze->>Spark: filter(pais.upper() == 'PE')
    Bronze->>Spark: filter(fecha in range OR fecha is invalid)
    Note over Bronze,Spark: Las filas con fecha nula van a la partición sintética __invalid__<br/>para que Silver pueda cuarentenarlas
    Bronze->>Spark: withColumn(_tenant_id='pe')
    Bronze->>Spark: withColumn(_ingestion_timestamp, _source_file, _batch_id)
    Bronze->>Delta: write.delta(replaceWhere='_tenant_id=pe AND fecha BETWEEN ...')
    Note over Delta: Idempotente: una reejecución del mismo rango sobrescribe esa ventana<br/>sin duplicar y sin tocar particiones fuera del rango
```

---

## 4. Detalle Silver — el corazón del pipeline

```mermaid
flowchart TB
    B[Bronze<br/>data/bronze/&lt;t&gt;/deliveries<br/>filas crudas con columnas técnicas]

    B --> CLA[_classify_anomalies<br/>añade _quarantine_reason:<br/>· null/__invalid__ fecha<br/>· cantidad null o ≤ 0<br/>· precio null<br/>· tipo_entrega no en valid set → '__discard__'<br/>· null/None = fila limpia]

    CLA --> DED[_dedup_exact<br/>dropDuplicates sobre 9 cols originales<br/>conserva una copia por fila idéntica]

    DED --> JOIN[_temporal_join_materials<br/>LEFT join con silver/&lt;t&gt;/dim_materials<br/>fact.fecha_proceso BETWEEN dim.valid_from AND dim.valid_to<br/>NO usa is_current ←]

    JOIN --> ORPH[_flag_orphans<br/>si row es 'clean' Y dim.descripcion IS NULL<br/>→ _quarantine_reason = 'orphan_material']

    ORPH --> SPLIT{¿_quarantine_reason?}

    SPLIT -->|NULL = clean| NORM[_normalize_units<br/>CS × 20 = ST<br/>añade cantidad_st]
    SPLIT -->|'__discard__'| DISCARD[contar y descartar<br/>no se persiste]
    SPLIT -->|cualquier otra| QUAR[_write_quarantine<br/>OVERWRITE por tenant_id<br/>silver_quarantine/&lt;t&gt;/fact_deliveries]

    NORM --> FLAGS[_add_flags<br/>is_routine_delivery in {ZPRE,ZVE1}<br/>is_bonus_delivery in {Z04,Z05}]

    FLAGS --> TECH[añade columnas técnicas:<br/>_silver_run_id<br/>_silver_batch_id<br/>_silver_ingestion_timestamp]

    TECH --> MERGE[_write_fact<br/>MERGE INTO silver/&lt;t&gt;/fact_deliveries<br/>ON tenant + fecha + transporte + ruta + material + tipo_entrega]

    MERGE --> DONE[Silver fact_deliveries listo<br/>~89%25 de las filas raw para tenants normales]
```

### ¿Por qué este orden?

1. **Clasificar antes de deduplicar.** Si deduplicamos primero, perdemos info de cuántos duplicados había.
2. **Dedup antes del join.** Hace el join más barato (menos filas) y el resultado es el mismo (los duplicados son idénticos en columnas).
3. **Join antes de marcar orphans.** No podemos saber qué es orphan hasta que el left join no encuentra match.
4. **Marcar orphans antes de filtrar limpios.** Una fila con material huérfano pero todo lo demás bien debe ir a cuarentena, no a Silver.
5. **Normalizar y flagear DESPUÉS de filtrar limpios.** No tiene sentido normalizar filas que vamos a tirar.
6. **MERGE al final.** Sólo lo limpio entra a la tabla autoritativa.

---

## 5. SCD Type 2 — el join temporal explicado visualmente

El cat catálogo tiene SKUs con historia. Ejemplo real: `AA004003` (Cola Regular 600 ml):

```mermaid
gantt
    title Versiones de AA004003 (Cola Regular 600ml)
    dateFormat YYYY-MM-DD
    axisFormat %b %y

    section AA004003
    v1 · precio 31.95 · is_current=false : a1, 2024-01-01, 2025-03-31
    v2 · precio 33.80 · is_current=true  : a2, 2025-04-01, 9999-12-31
```

Una transacción del **2025-03-01** debe matchear **v1** (precio 31.95). Una del **2025-05-01** debe matchear **v2** (33.80). Si usáramos `is_current=true`, **TODAS** las transacciones históricas matchearían la versión actual y el revenue histórico saldría inflado.

```mermaid
flowchart LR
    F[fact_deliveries<br/>fecha_proceso=20250301<br/>material=AA004003]

    D1[dim v1<br/>valid_from=2024-01-01<br/>valid_to=2025-03-31<br/>precio_base=31.95]
    D2[dim v2<br/>valid_from=2025-04-01<br/>valid_to=9999-12-31<br/>precio_base=33.80]

    F -->|fecha BETWEEN v1| D1
    F -.->|fecha NO está en v2| D2

    style D1 stroke:#0a0,stroke-width:3px
    style D2 stroke:#aaa,stroke-dasharray:5
```

Por eso el join es **literalmente**:

```sql
fact.fecha_proceso BETWEEN dim.valid_from AND dim.valid_to
```

y **NO**:

```sql
dim.is_current = true   -- ¡incorrecto! mata el histórico
```

El test `tests/test_scd2_temporal_join.py:test_temporal_join_picks_correct_version` cubre exactamente este escenario.

---

## 6. Manejo de anomalías — la regla por la regla

| Tipo de anomalía | Acción | Justificación de negocio |
|---|---|---|
| `fecha_proceso` null o inválida | **Cuarentena** | Sin fecha no se puede particionar ni atribuir. No se descarta porque puede haber error de origen recuperable. |
| `cantidad` null, negativa o cero | **Cuarentena** | Posible error de feed. Una cantidad cero en una entrega es sospechosa, no obvia. |
| `precio` null | **Cuarentena** | Sin precio el revenue no es calculable. Por consistencia con cantidad, no se asume cero. |
| `material` no en el catálogo | **Cuarentena** | Romper integridad referencial debe ser visible. Es el bug más fácil de pasar por alto (left-join silencioso = filas sin enrich). |
| `tipo_entrega` fuera de {ZPRE,ZVE1,Z04,Z05} | **Descarte** | Regla de negocio: COBR (cobranza), Z99 (lo que sea) no son entregas, no entran al modelo analítico. Sólo se cuentan. |
| Duplicado exacto | **Deduplicar** | El feed envía la misma fila dos veces. No es un error visible — sólo nos quedamos con una copia. |

```mermaid
flowchart LR
    R[fila raw de Bronze]

    R --> C1{fecha válida?}
    C1 -->|no| Q1[Cuarentena<br/>invalid_fecha_proceso]
    C1 -->|sí| C2{cantidad > 0?}

    C2 -->|no| Q2[Cuarentena<br/>invalid_cantidad]
    C2 -->|sí| C3{precio NOT NULL?}

    C3 -->|no| Q3[Cuarentena<br/>null_precio]
    C3 -->|sí| C4{tipo_entrega válido?}

    C4 -->|no| DISC[Descarte<br/>sólo se cuenta]
    C4 -->|sí| C5{material en catálogo?}

    C5 -->|no| Q4[Cuarentena<br/>orphan_material]
    C5 -->|sí| OK[Silver fact_deliveries<br/>fila persistida ✓]
```

---

## 7. Idempotencia capa por capa

| Capa | Estrategia | Qué pasa si re-corro el mismo rango |
|---|---|---|
| Bronze | `replaceWhere` por `_tenant_id` + `fecha_proceso` rango | Sólo se reescribe esa ventana. Particiones fuera del rango quedan intactas. |
| Silver `dim_materials` | `MERGE INTO` por `(material, valid_from)` | Las versiones existentes se actualizan si cambiaron sus atributos. Nuevas versiones se insertan. |
| Silver `fact_deliveries` | `MERGE INTO` por clave de negocio compuesta | Filas existentes se actualizan, nuevas se insertan. |
| Silver `quarantine` | `overwrite` con `replaceWhere` por `_tenant_id` | Toda la cuarentena del tenant se reconstruye en cada run (es reproducible desde raw). |
| Gold | `overwrite` con `replaceWhere` por rango de `fecha_proceso` | Se recomputan sólo las particiones del rango. |
| Quality logs | `append` | Histórico inmutable — cada run añade una fila por check. (Mejora propuesta en `observations.md`: particionar por `executed_date`.) |

```mermaid
flowchart LR
    R1[1ª corrida<br/>2025-01-01 → 2025-03-31] -->|escribe| D1[Delta state v1]
    R2[2ª corrida<br/>2025-01-01 → 2025-03-31<br/>mismo rango] -->|replaceWhere = mismo rango| D2[Delta state v2 ≡ v1]
    R3[3ª corrida<br/>2025-04-01 → 2025-06-30<br/>rango diferente] -->|replaceWhere = sólo Q2| D3[Delta state v3<br/>= v2 + nuevas particiones]

    style D1 fill:#dfd
    style D2 fill:#dfd
    style D3 fill:#dfd
```

---

## 8. Quality logs — el contrato del schema

Tabla Delta compartida en `data/shared/quality_logs/` (en Databricks: `<env>.shared.quality_logs`). Una fila por (run × tenant × check).

```mermaid
classDiagram
    class QualityLogRow {
        +string _run_id
        +string _batch_id
        +string tenant_id
        +string layer               // bronze | silver | gold
        +string table_name
        +string check_name
        +string check_severity      // critical | warning | info
        +long records_checked
        +long records_failed
        +boolean check_passed       // records_failed == 0
        +timestamp executed_at
    }
```

**Por qué este esquema:**

- `_run_id` permite **cross-layer trace**: un solo `run_id` cubre Bronze→Silver→Gold de un tenant.
- `_batch_id` es más fino: uno por `(run, tenant, layer)`.
- `tenant_id + layer + table_name` localizan **qué dato** se chequeó.
- `severity` clasifica la urgencia: `critical` puede abortar Gold (`--fail-on-critical`).
- `records_checked / records_failed` dan **magnitud** del problema, no sólo pass/fail.
- `executed_at` permite filtrar histórico y construir vistas agregadas.

---

## 9. Configuración jerárquica — qué wins qué

```mermaid
flowchart LR
    B[base.yaml<br/>defaults globales<br/>· reglas de negocio<br/>· paths defecto<br/>· lista de tenants known]

    E[env/dev.yaml<br/>· paralelismo<br/>· paths del ambiente<br/>· fail_on_critical]

    T[tenants/sv.yaml<br/>· timezone<br/>· currency<br/>· id]

    CLI[CLI overrides<br/>· start_date<br/>· end_date<br/>· fail_fast]

    B -.->|merge| E
    E -.->|merge| T
    T -.->|merge| CLI
    CLI --> CFG[cfg final<br/>passed to bronze/silver/gold]

    style B fill:#fef
    style E fill:#eff
    style T fill:#efe
    style CLI fill:#ffe
```

Precedencia: derecha gana. Si en `base.yaml` `shuffle_partitions=8` y en `env/qa.yaml` es `32`, en QA queda `32`. Si encima la CLI pasara un override, ganaría la CLI.

---

## 10. Multi-tenant — un solo código, N tenants

```mermaid
flowchart LR
    CLI[saas-pipeline run --tenant all]
    CLI --> R{tenant?}

    R -->|all| L[Lista de tenants known<br/>SV HN JM EC PE GT]
    R -->|sv| LSV[lista = sv]

    L -->|loop| PROC

    LSV --> PROC[Para cada tenant t:<br/>· bronze.run t<br/>· silver.run_dim_materials t<br/>· silver.run_fact_deliveries t<br/>· quality.run_silver_checks t<br/>· critical_failed? → skip Gold<br/>· gold.run_daily_metrics t]

    PROC -->|fail_fast=true Y falla| ABORT[Abortar toda la corrida]
    PROC -->|fail_fast=false Y falla| CONT[Loguear fallo · continuar con siguiente tenant]
    PROC -->|todos OK| OUT[Pipeline terminado · exit 0]
    ABORT --> EXIT1[exit 1 · lista de fallos en stderr]
    CONT --> OUT
```

El **aislamiento es por path**: cada tenant escribe en `data/<layer>/<tenant>/`. Los códigos vienen en mayúscula del CSV (`SV`, `PE`) y se normalizan a minúscula al entrar a Bronze (sec 5.3 de la spec).

---

## 11. Mapeo local → Databricks (Unity Catalog)

```mermaid
flowchart LR
    subgraph LOCAL[Local · filesystem]
        L1[data/bronze/sv/deliveries/...]
        L2[data/silver/sv/fact_deliveries/...]
        L3[data/silver/sv/dim_materials/...]
        L4[data/gold/sv/daily_metrics.../...]
        L5[data/shared/quality_logs/...]
    end

    subgraph UC[Databricks · Unity Catalog]
        U1[saas_dev.bronze_sv.deliveries]
        U2[saas_dev.silver_sv.fact_deliveries]
        U3[saas_dev.silver_sv.dim_materials]
        U4[saas_dev.gold_sv.daily_metrics_by_delivery_type]
        U5[saas_dev.shared.quality_logs]
    end

    L1 -.->|cambio de path en config| U1
    L2 -.-> U2
    L3 -.-> U3
    L4 -.-> U4
    L5 -.-> U5
```

**Punto clave:** la migración a Databricks NO requiere cambios de código. Sólo:
1. Cambiar los `paths.*` en `config/env/main.yaml` a paths `abfss://` o `dbfs:/Volumes/...`.
2. En lugar de `.save(path)`, usar `.saveAsTable(table_uc_name)` (cambio en `paths.py`).
3. Hacer deploy vía DAB (`databricks bundle deploy`).

---

## 12. Tests — qué cubre cada uno

```mermaid
flowchart TB
    subgraph FAST[No-Spark · &lt; 100 ms]
        T1[test_config.py · 13 tests<br/>YAML load · merge precedencia · tenant skip · env unknown raises]
        T2[test_utils.py · 5 tests<br/>date_range · yyyymmdd_to_iso · uniqueness de ids]
    end

    subgraph SPARK[Spark · ~25 s en total]
        T3[test_silver_transforms.py · 5 tests<br/>CS→ST · clasificación anomalías · dedup · flags · orphans]
        T4[test_scd2_temporal_join.py · 2 tests<br/>versión correcta por fecha · null en huérfanos]
        T5[test_quality.py · 4 tests<br/>uno por cada check de quality.py]
    end

    T1 --> CI
    T2 --> CI
    T3 --> CI
    T4 --> CI
    T5 --> CI

    CI[GitHub Actions<br/>setup-java@v4 · setup-uv · ruff · pytest]
```

**29 tests total · todos verdes en local · todos corren en CI.**

---

## 13. ¿Y la mentoría?

`mentoring/bad_code.py` es el código del Anexo A literal. `mentoring/good_code.py` es un refactor:

```mermaid
flowchart LR
    BAD[bad_code.py<br/>· pandas en lugar de Spark<br/>· iterrows<br/>· valores mágicos<br/>· filtro WHERE pais hardcoded<br/>· parquet plano sin idempotencia<br/>· print done]

    BAD -->|refactor| GOOD[good_code.py<br/>· Spark nativo end-to-end<br/>· DeliveryProcessingConfig dataclass<br/>· esquema explícito<br/>· soporte multi-tenant real<br/>· Delta particionado con replaceWhere<br/>· logging estructurado]

    BAD -.-> REVIEW[code_review.md<br/>6 observaciones documentadas<br/>+ Cómo se lo explicaría al junior]
```

Las 6 observaciones cubren los criterios negativos del eval explícitamente.

---

## 14. CI / CD

```mermaid
sequenceDiagram
    participant Dev as Daniel · localhost
    participant GH as GitHub
    participant CI as GitHub Actions

    Dev->>GH: git push (o PR a develop)
    GH->>CI: trigger ci.yml
    CI->>CI: checkout
    CI->>CI: setup-python@3.11
    CI->>CI: setup-java@temurin-17
    CI->>CI: setup-uv@0.4.x · uv sync --extra dev
    CI->>CI: ruff check src tests
    CI->>CI: validar YAMLs cargan
    CI->>CI: validar config merge
    CI->>CI: pytest -v (29 tests)
    alt todo pasa
        CI->>GH: ✅ merge OK
    else algo falla
        CI->>GH: ❌ block merge
    end
```

---

## 15. Onboarding de un tenant nuevo (resumen visual)

```mermaid
flowchart TB
    A[1 PR en repo Terraform<br/>módulo &quot;tenant&quot; nueva instancia]
    A --> B[2 terraform apply<br/>schemas · containers · external locations · grupos · secret scope]
    B --> C[3 PR en saas-data-platform<br/>config/tenants/&lt;t&gt;.yaml + actualizar known list]
    C --> D[4 CI verde]
    D --> E[5 Smoke run local<br/>make run-all TENANT=&lt;t&gt;]
    E --> F[6 Smoke run en Databricks dev<br/>databricks bundle deploy → run]
    F --> G[7 Validación SQL<br/>SELECT count * FROM gold_&lt;t&gt;.daily_metrics ...]
    G --> H[8 Job programado en main]
    H --> I[Tenant onboardeado ✓]
```

Detalle completo en [`onboarding-tenant.md`](onboarding-tenant.md).
