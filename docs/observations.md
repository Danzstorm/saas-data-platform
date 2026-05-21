# Observaciones a la arquitectura provista

> Archivo obligatorio según sección 9.2 de la prueba. Contiene observaciones sustantivas sobre la arquitectura SAAS provista: decisiones con las que no estoy completamente de acuerdo, ambigüedades resueltas durante la implementación y mejoras propuestas para iteraciones siguientes. La regla de oro fue: **no cambiar la arquitectura unilateralmente**; cuando discrepé, lo dejé registrado aquí para discutirlo en la sustentación.

---

## 1. Aislamiento por schema dentro de un catálogo único: trade-off real, no decisión obvia

**Lo que dice la arquitectura.** Sec 5.2: un sólo catálogo por ambiente (`saas_dev`, `saas_qa`, `saas_main`) y un schema por tenant. Justificación: onboarding ágil, gobierno centralizado y vistas cross-tenant en Gold.

**Mi discrepancia.** Estoy de acuerdo en que es la elección correcta **hoy**, pero la decisión tiene un costo que vale la pena documentar para que el equipo no se sorprenda más adelante:

- **Blast radius del IAM.** Un misclick a nivel de catálogo (`GRANT ALL ON CATALOG saas_dev TO ...`) impacta a todos los tenants. La elección por schema asume que el rol de admin del catálogo es altamente disciplinado. En una corporación con N unidades de negocio, ese supuesto se rompe rápido.
- **Storage credentials y external locations.** Si en el futuro un tenant exige aislamiento de storage (ej. CBC con compliance distinto a Beliv), forzar todo bajo un mismo catálogo obliga a mover el aislamiento a `external location` por schema, que es más oscuro y menos auditable que tener un catálogo por tenant.
- **Lakehouse Federation y data sharing.** Compartir datos cross-tenant entre catálogos (Delta Sharing) tiene una semántica más natural que entre schemas. Si en H2 aparece un caso de monetización del dato (otro tenant paga por leer agregados), el modelo schema lo complica.

**Mi propuesta alternativa para Horizonte 2.** Mantener el modelo actual para tenants nuevos sin requisitos especiales, pero dejar previsto un patrón "tenant aislado" (catálogo dedicado `saas_<env>_<tenant>`) para los casos con compliance, residencia de datos o billing separado. La capa de configuración (`config/tenants/*.yaml`) ya soporta esto añadiendo un campo `tenant.catalog_template`; en mi implementación lo dejé extensible para no cerrar la puerta.

**Trade-off honesto.** La arquitectura provista optimiza por velocidad de onboarding (90% de los casos). Mi propuesta agrega complejidad solo para el 10% que la necesita. Vale la pena tener el patrón listo, no la implementación.

---

## 2. La partición de Bronze por `(fecha_proceso, tenant_id)` mezcla dos cosas distintas

**Lo que dice la arquitectura.** Sec 5.4: Bronze particionado por `fecha_proceso` Y `tenant_id`.

**Ambigüedad / discrepancia.** En la estructura de paths definida en sec 5.2, el tenant ya es parte del **path físico**: `data/bronze/<tenant>/deliveries/fecha_proceso=YYYYMMDD/`. Si encima particiono por `tenant_id` dentro de la tabla, termino con un path tipo `data/bronze/sv/deliveries/fecha_proceso=20250301/_tenant_id=sv/...` — la columna `_tenant_id` queda **siempre constante** dentro de una tabla porque el path ya separa por tenant.

**Cómo lo resolví.** Implementé el doble particionado tal como pide la spec, pero documentando que `_tenant_id` es de cardinalidad 1 por tabla (overhead de partición es despreciable). El beneficio real: en Databricks, si en algún momento se decide colapsar todos los tenants en una sola tabla por capa (modelo "Unity con vistas filtradas por row-level security"), el código ya está preparado y sólo cambia el path raíz. No tener `_tenant_id` como columna de partición rompería ese path migratorio.

**Recomendación.** Mantener el `_tenant_id` partition column en la spec, pero aclarar en la guía de arquitectura que es **opcional para path-based isolation** y **requerida para single-table isolation**. Hoy queda como un costo pequeño con un beneficio futuro.

---

## 3. La política de "tipo_entrega inválido → descarte" no registra trazabilidad

**Lo que dice la arquitectura.** Sec 5.6: filas con `tipo_entrega` fuera de `{ZPRE, ZVE1, Z04, Z05}` se **descartan** (sólo se contabilizan, no se persisten). Resto de anomalías van a cuarentena.

**Mi discrepancia.** La justificación del documento ("COBR y Z99 no pertenecen al alcance analítico") es una **decisión de scope analítico**, pero no es lo mismo que "estos datos no nos sirven en ningún caso". COBR (cobranza) y Z99 (lo que sea Z99) son **datos legítimos del feed origen**. Si mañana el equipo de finanzas pregunta cuántas cobranzas se procesaron por país, no podemos contestar — los descartamos sin dejar rastro.

**Cómo lo resolví.** Implementé el comportamiento exacto que pide la spec (descarte en Silver), pero registro el conteo en `quality_logs` indirectamente al persistir el resultado de `_classify_anomalies` (la métrica `discarded_invalid_type` queda en el log del proceso). Como mejora propuesta, no la apliqué unilateralmente.

**Mi propuesta para Horizonte 2.** Renombrar la política a *"descarte para el alcance analítico de Silver, persistir en una tabla `bronze_descartes/<tenant>` para auditoría"*. Costo marginal (escribir filas que ya tenemos en memoria), beneficio claro: cuando alguien pregunta por COBR existe una respuesta. Si en algún momento se incorporan al modelo (ej. Z99 termina siendo un tipo de promoción), está todo el histórico recuperable sin reprocesos.

---

## 4. Quality logs sin retención ni particionado: bomba de tiempo

**Lo que dice la arquitectura.** Sec 5.9: tabla `quality_logs` con esquema definido y ubicación `data/shared/quality_logs/` (o `<env>.shared.quality_logs` en Databricks). No se menciona retención ni particionado.

**Mi discrepancia / ambigüedad.** Con 6 tenants × 3 capas × 4 checks × N corridas por día, la tabla crece rápido. Sin particionar, cada lectura escanea todo el histórico. Sin retención, después de 6 meses tenemos millones de filas para algo que mayormente se consulta "últimos 7 días".

**Cómo lo resolví.** En mi implementación dejé la tabla **sin particionar** (siguiendo la spec literal), pero el esquema incluye `executed_at` como `timestamp`, lo cual deja el camino libre para añadir `partitionBy(F.to_date("executed_at"))` cuando se eleve a productivo. No lo apliqué por respeto a la arquitectura provista.

**Mi propuesta para Horizonte 2.**
1. **Particionar** `quality_logs` por `executed_date = date(executed_at)`.
2. **Política de retención** Delta (`VACUUM` + `tblproperties("delta.logRetentionDuration"='interval 30 days')`).
3. **Tabla agregada** `quality_logs_daily` por (date, tenant, layer, check_name) para dashboards rápidos.
4. **Alerting** declarativo: un check `critical` que falle dispara una notificación (Databricks Lakehouse Monitoring o un Job de alertas). La spec habla de `fail_on_critical` para abortar el job, pero abortar sin alertar es la mitad del trabajo.

---

## 5. (Bonus) Faltan convenciones para reproceso histórico vs. backfill

**Ambigüedad.** La spec habla de idempotencia (sec 5.5) — perfecto. Pero no aclara la diferencia operativa entre:

- **Reproceso incremental:** ayer falló un día, lo vuelvo a correr. Funciona con `replaceWhere` actual.
- **Backfill masivo:** se descubrió un bug en la lógica CS→ST y hay que reprocesar 6 meses para todos los tenants. ¿Esto se hace cómo? ¿Un job que itera día por día? ¿Un override del rango con un flag de "modo backfill"?

**Cómo lo resolví.** El CLI acepta cualquier `--start-date --end-date`, así que técnicamente sirve para ambos. Pero un backfill de 6 meses para 6 tenants es 6 × 180 = 1080 reescrituras de partición — sin paralelismo a nivel de driver es lento.

**Propuesta H2.** Documentar el patrón "backfill" como un Job de Databricks con `for_each_task` (un task por tenant) y subdividir por mes. O usar `repartitionByRange` + `replaceWhere` con un rango más grande. Pero esto se discute con el equipo, no se decide unilateralmente.

---

## 6. (Bonus) `dim_materials` global vs. por tenant es una decisión sin discutir

**Ambigüedad.** El cat catálogo `materials_catalog.csv` es uno solo y la spec habla de `silver_<tenant>.dim_materials`. ¿Significa que cada tenant tiene una copia idéntica del catálogo? ¿Y si en el futuro Beliv y CBC tienen catálogos parcialmente solapados pero con precios distintos?

**Cómo lo resolví.** Hoy escribo la misma dimensión en cada schema de tenant (siguiendo la spec literal). Es duplicación de datos, pero respeta el modelo de aislamiento.

**Propuesta H2.** Considerar un schema `shared/dim_materials` o un catálogo `saas_<env>_shared`, y que cada `silver_<tenant>` mantenga sólo materiales realmente usados por ese tenant (vista filtrada o tabla derivada). Reduce duplicación y aclara la pregunta "¿este precio aplica a este tenant?" sin tener que mantener N copias.

---

## Resumen ejecutivo de las observaciones

| # | Observación | Tipo | Aplicado en el código |
|---|---|---|---|
| 1 | Aislamiento schema vs. catálogo | Discrepancia con propuesta | Reserva el camino en config, no aplicado |
| 2 | Partición Bronze redundante con path | Ambigüedad resuelta | Implementado tal cual la spec, documentado |
| 3 | Descarte sin trazabilidad | Discrepancia con propuesta | Spec respetada, métrica en logs |
| 4 | quality_logs sin retención/partición | Discrepancia con propuesta | Esquema preparado, no aplicado |
| 5 | Backfill no definido | Ambigüedad | CLI lo soporta, doc operativa pendiente |
| 6 | dim_materials global vs. por tenant | Ambigüedad | Duplicación según spec |
