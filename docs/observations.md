# Observaciones a la arquitectura provista

Documento obligatorio según sec 9.2. La consigna fue clara: la arquitectura ya está definida (sec 5) y mi trabajo es implementarla, no rediseñarla. Cuando discrepé con algo, lo dejé acá para discutir en sustentación en lugar de cambiarlo unilateralmente. La sec 12 lo pide así también ("Cambios a la arquitectura provista sin registrarlos en observations.md" es criterio negativo).

Lo que viene son seis observaciones. Las dos primeras son discrepancias con trade-off explícito. La tercera y cuarta son discrepancias con propuesta concreta de mejora. La quinta y sexta son ambigüedades que tuve que resolver durante la implementación y vale la pena exponer.

---

## 1. Aislamiento por schema vs catálogo: la decisión correcta hoy, con costo mañana

La sec 5.2 elige aislar tenants por schema dentro de un catálogo único (`saas_<env>`). La justificación que da el documento — onboarding ágil, gobierno centralizado, vistas cross-tenant en Gold — es válida. **Estoy de acuerdo en que es la elección correcta para el estado actual del proyecto.**

Lo que me preocupa es el costo que esa decisión carga para el futuro y que el documento no menciona:

**Blast radius del IAM.** Un misclick a nivel de catálogo (`GRANT ALL ON CATALOG saas_dev TO ...`) afecta a todos los tenants a la vez. El modelo asume que el rol de admin del catálogo es altamente disciplinado y que los grupos están bien sincronizados con Entra ID. En una corporación con N unidades de negocio (CBC, Beliv, BIA, ...) y N teams de plataforma, ese supuesto se rompe a los 6 meses.

**Storage credentials y external locations.** Si en H2 aparece un tenant con compliance distinto (ej. CBC con requisitos de residencia de datos por país que Beliv no tiene), el aislamiento real hay que moverlo a `external location` por schema, que es menos auditable que tener un catálogo por tenant. Esa es la cirugía que después cuesta meses.

**Mi propuesta.** Mantener el modelo actual como default para el 90% de tenants. Pero dejar previsto un patrón "tenant aislado" (catálogo dedicado `saas_<env>_<tenant>`) para los casos con compliance / residencia / billing separados. La capa de configuración (`config/tenants/*.yaml`) ya lo soporta — agregando un campo `tenant.catalog_template`. En mi implementación lo dejé extensible para no cerrar la puerta.

No lo apliqué porque cambiar la arquitectura unilateralmente no es lo que pide el enunciado. Pero quería tenerlo en el radar.

---

## 2. La partición de Bronze por `(fecha_proceso, tenant_id)` es redundante con el path

La sec 5.4 pide particionar Bronze por **fecha_proceso Y tenant_id**. Pero la sec 5.2 ya separa por tenant en el path físico: `data/bronze/<tenant>/deliveries/fecha_proceso=YYYYMMDD/`. El resultado es que dentro de una tabla Bronze el `_tenant_id` tiene cardinalidad 1 — siempre es el mismo valor — y la columna de partición es "técnica" más que funcional.

Cuando lo implementé pensé en dos opciones: (a) ignorar el doble particionado, que para el caso local es lo más limpio, o (b) implementarlo tal como pide la spec, asumiendo que hay una razón mirando al futuro. Elegí (b) porque encontré la razón: si en algún momento la plataforma decide colapsar todos los tenants en una **sola tabla por capa** (modelo "Unity Catalog con row-level security" o "Delta Sharing entre catálogos"), el código ya queda preparado y sólo cambia el path raíz. No tener `_tenant_id` como columna de partición rompería ese camino migratorio.

**Recomendación para la guía de arquitectura.** Aclarar que el doble particionado es *opcional* en path-based isolation y *requerido* para single-table isolation. Hoy es overhead pequeño con beneficio futuro — vale la pena dejarlo documentado.

---

## 3. "tipo_entrega inválido → descarte" pierde trazabilidad

La sec 5.6 dice que las filas con `tipo_entrega` fuera de `{ZPRE, ZVE1, Z04, Z05}` se **descartan** (sólo se contabilizan, no se persisten). La justificación del documento: "COBR y Z99 no pertenecen al alcance analítico".

Acá tengo una discrepancia real. Esa justificación mezcla dos cosas:

1. *Estos tipos no pertenecen al modelo analítico de Silver.* — De acuerdo.
2. *Estos datos no nos sirven en ningún caso.* — No estoy de acuerdo.

COBR (cobranza) y Z99 son **filas legítimas del feed origen**. Si mañana finanzas pregunta "¿cuántas cobranzas procesamos en marzo?", la respuesta es "no sé, las descartamos". Y si en algún momento Z99 termina siendo un tipo de promoción que sí se incorpora al modelo (porque se acordó internamente), no hay histórico para recuperar — habría que pedir al feed que reenvíe.

**Implementación actual.** Respeté la spec — los descarto en Silver. Pero registro el conteo en `quality_logs` indirectamente (la métrica `discarded_invalid_type` aparece en el output del proceso).

**Propuesta para H2.** Reescribir la política como *"descarte para el alcance analítico de Silver, pero persistir en una tabla `bronze_descartes/<tenant>/` para auditoría"*. Costo: marginal (las filas ya están en memoria cuando se decide descartarlas). Beneficio: cuando alguien pregunta por COBR existe una respuesta. Y si en H2 se incorporan, el histórico está.

---

## 4. `quality_logs` sin retención ni particionado: bomba de tiempo

La sec 5.9 define el esquema de `quality_logs` y la ubicación (`data/shared/quality_logs/` local, `<env>.shared.quality_logs` en Databricks). No menciona retención ni particionado.

El cálculo rápido: 6 tenants × 4 checks × N corridas por día. Con corridas horarias = 576 filas por día = 210 000 por año. Sin particionar, cada query a `quality_logs` escanea el histórico completo. Y la mayoría de las queries reales son "últimos 7 días" — leer un año entero para mostrar una semana es ineficiente y caro.

**Implementación actual.** Sin particionar (siguiendo la spec literal). Pero el esquema incluye `executed_at` como `timestamp`, lo cual deja el camino libre para particionar por `executed_date = date(executed_at)` cuando se decida elevar a productivo.

**Propuesta para H2.**

1. Particionar `quality_logs` por `executed_date`.
2. Política de retención Delta: `TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 30 days')` + un `VACUUM` semanal.
3. Tabla agregada `quality_logs_daily` por `(date, tenant, layer, check_name)` para dashboards.
4. **Alerting declarativo.** Hoy `fail_on_critical` aborta el job pero no avisa a nadie. Un job de alertas (Lakehouse Monitoring o equivalente) que mire los checks `critical` y dispare notificación cierra el loop.

---

## 5. Ambigüedad: backfill masivo vs reproceso incremental

La sec 5.5 habla de idempotencia. La ejecuté correctamente para el caso "ayer falló, lo vuelvo a correr". Pero no aclara la diferencia operativa con el caso "se descubrió un bug en la lógica CS→ST y hay que reprocesar 6 meses para los 6 tenants".

Técnicamente mi CLI lo soporta — `--start-date 2025-01-01 --end-date 2025-06-30 --tenant all` reescribe todo. Pero son 6 × 180 días = 1 080 reescrituras de partición, en serie. En local con 3 000 filas dura un minuto; en Databricks con 100 millones de filas dura horas y el cluster cuesta.

**Cómo lo resolví.** Dejé el CLI capaz de procesar cualquier rango. Documenté la limitación.

**Propuesta para H2.** Un Job de Databricks con `for_each_task` (un task por tenant) y subdivisión por mes paraleliza el backfill sin cambiar la lógica. O un modo "backfill" que use `repartitionByRange` para distribuir mejor la carga. Pero esto se conversa con el equipo, no se decide unilateralmente.

---

## 6. Ambigüedad: `dim_materials` global vs por tenant

El catálogo `materials_catalog.csv` es **uno solo**, pero la spec habla de `silver_<tenant>.dim_materials` (una dimensión por tenant). Eso significa que cada tenant tiene una copia idéntica del catálogo.

Es duplicación de datos pero respeta el modelo de aislamiento. La pregunta es si en el futuro (Beliv vs CBC) los catálogos van a ser parcialmente solapados pero con precios distintos por tenant. Si la respuesta es sí, el modelo actual no escala bien.

**Cómo lo resolví.** Hoy escribo la misma dimensión en cada schema de tenant (siguiendo la spec literal). Es lo correcto bajo el supuesto actual.

**Propuesta para H2.** Considerar un schema `shared/dim_materials` o un catálogo `saas_<env>_shared`. Cada `silver_<tenant>` mantiene sólo materiales realmente usados por ese tenant (vista filtrada o tabla derivada). Reduce duplicación y aclara la pregunta "¿este precio aplica a este tenant?".

Pero esto depende del modelo de gobierno del catálogo de materiales. Si negocio dice "el catálogo es global y los precios son por SKU", el modelo actual está bien y mi propuesta es ruido. Es una conversación con el dueño del producto, no una decisión técnica.

---

## Resumen ejecutivo

| # | Observación | Tipo | Estado en código |
|---|---|---|---|
| 1 | Aislamiento schema vs catálogo | Discrepancia con propuesta | Reservé el camino en config, no aplicado |
| 2 | Partición Bronze redundante con path | Ambigüedad resuelta | Implementado tal cual spec, documentado |
| 3 | Descarte sin trazabilidad | Discrepancia con propuesta | Spec respetada, métrica en logs |
| 4 | quality_logs sin retención | Discrepancia con propuesta | Esquema preparado, no aplicado |
| 5 | Backfill no definido | Ambigüedad | CLI lo soporta, doc operativa pendiente |
| 6 | dim_materials global vs por tenant | Ambigüedad | Duplicación según spec |

Si tuviera que ranquear cuáles aplicaría primero en H2: **#4** (quality_logs sin retención) primero — porque el costo crece linealmente con el tiempo y es trivial de aplicar. Después **#3** (descartes con auditoría) — porque cierra un hueco de gobierno con costo marginal. **#1** (schema vs catálogo) lo dejaría para cuando aparezca el primer caso real que lo motive, no antes — over-engineering antes de tener el caso de uso es el otro error frecuente.
