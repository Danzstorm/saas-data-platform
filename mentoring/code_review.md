# Code Review — `bad_code.py`

> Revisión del código provisto en el Anexo A de la prueba. Asumimos que es una entrega de un ingeniero junior del equipo. El refactor completo vive en `good_code.py`.

A continuación, las observaciones priorizadas. Cada una sigue el formato **qué está mal → por qué importa → cómo se corrige**.

---

## 1. Uso de `pandas` para mover el dataset completo a memoria del driver

**Qué está mal.** El código hace `pd.read_csv(file_path)` y luego itera fila por fila con `df.iterrows()`. Recién al final convierte el resultado a Spark con `spark.createDataFrame(out)`. Spark queda relegado a "escritor de archivos".

**Por qué importa.**
- Pandas carga TODO el CSV en RAM del driver. Si el archivo crece a millones de filas (escenario realista en la corporación), revienta por OOM. Spark distribuiría la lectura entre executors.
- `iterrows()` es notoriamente lento (~100x más lento que operaciones vectorizadas) y rompe completamente el modelo de ejecución perezosa de Spark.
- Pierde paralelismo, predicate pushdown, optimización del Catalyst y compresión columnar.

**Cómo se corrige.** Leer directo con `spark.read.csv()`, aplicar las transformaciones como expresiones de columna (`F.when`, `F.col`, etc.) y no salir nunca del API de DataFrame hasta el `.write`. Ver `good_code.py:read_deliveries` y `normalize_units`.

---

## 2. Reglas de negocio hardcoded y mágicas (literales sin nombre)

**Qué está mal.**
```python
if row["tipo_entrega"] == "ZPRE" or row["tipo_entrega"] == "ZVE1":
    if row["unidad"] == "CS":
        qty = row["cantidad"] * 20
```

`"ZPRE"`, `"ZVE1"`, `"CS"`, `20`, e incluso `/tmp/output/` están enterrados en el código.

**Por qué importa.**
- Si negocio agrega un nuevo tipo de entrega (`Z04`, `Z05`), hay que tocar código en N lugares.
- El factor `20` es información de dominio que debería vivir en una configuración auditable, no en una expresión.
- `/tmp/output/` no es portátil (Windows / Databricks no tienen `/tmp` por defecto). Tampoco es testeable: cualquier prueba unitaria contamina ese path.

**Cómo se corrige.** Mover las reglas a un objeto de configuración (dataclass, OmegaConf, etc.). Ver `DeliveryProcessingConfig` en `good_code.py`: tipos válidos, factor CS→ST y path de salida son parámetros.

---

## 3. Inferencia de tipos del CSV + ausencia total de validaciones

**Qué está mal.** `pd.read_csv()` infiere los dtypes a ojo. `precio` y `cantidad`, que son decimales con anomalías intencionales (negativos, nulos), pueden terminar como `object` (string) si pandas encuentra valores extraños. Tampoco se valida que `unidad` sea CS o ST — si llega un valor desconocido (`KG`, vacío), la rama `else` lo trata como ST silenciosamente.

**Por qué importa.**
- Errores de tipo se manifiestan tarde y como bugs silenciosos. `qty * row["precio"]` con `precio` como string produce concatenación, no error claro.
- Filas con unidades desconocidas inflan métricas de ingreso sin alerta.
- El criterio de evaluación de la prueba penaliza "materiales no presentes silenciosamente". El mismo principio aplica a unidades desconocidas.

**Cómo se corrige.** (a) Esquema explícito en `spark.read.schema(...)` (ver `DELIVERIES_SCHEMA` en `good_code.py`). (b) Validación post-normalización que cuente las unidades inesperadas y registre warning. (c) En un pipeline serio: tabla de cuarentena y `quality_logs`, como hace el resto del repo.

---

## 4. Escritura no idempotente + sin partición + en formato Parquet plano

**Qué está mal.**
```python
sdf.write.mode("overwrite").parquet("/tmp/output/" + country)
```

`.mode("overwrite")` borra y reescribe TODA la salida del country. No hay particionado, no hay Delta, no hay clave de negocio para reconciliar.

**Por qué importa.**
- Reejecutar el job con un rango de fechas chico borra los datos de los rangos anteriores → pérdida de información.
- Sin Delta perdemos ACID, time travel, MERGE, `replaceWhere`. La prueba exige Delta (sec 3 + 5.5).
- Sin particionado los downstream barren todo el dataset para cada query.

**Cómo se corrige.** Escribir Delta particionado por `(_tenant_id, fecha_proceso)` y usar `replaceWhere` para que la reescritura sólo afecte el rango procesado:
```python
df.write.format("delta") \
   .mode("overwrite") \
   .option("replaceWhere", f"fecha_proceso BETWEEN '{start}' AND '{end}'") \
   .partitionBy("_tenant_id", "fecha_proceso") \
   .save(out_path)
```

Ver `write_idempotent` en `good_code.py`.

---

## 5. (Bonus) Soporte multi-tenant simulado con un `WHERE pais == country`

**Qué está mal.** El argumento `country` es un literal pasado a la función; la única "isolation" es filtrar el DataFrame.

**Por qué importa.** El criterio explícito de la prueba dice: *"Configuración multi-tenant implementada como un filtro WHERE pais = X, sin la estructura jerárquica definida"* es una **causa de evaluación negativa**. El proyecto SAAS exige aislamiento por schema / paths por tenant.

**Cómo se corrige.** La función debe poder operar sobre todos los tenants o sobre una lista, normalizando el código del país a minúscula (`pais` → `_tenant_id`) y particionando físicamente la salida por tenant. Ver `select_tenants` + `partitionBy("_tenant_id", ...)` en `good_code.py`.

---

## 6. (Bonus) `print("done")` como sistema de observabilidad

**Qué está mal.** `print` no estructurado, sin nivel, sin contexto (tenant, run_id, conteo de filas).

**Por qué importa.** En Databricks Jobs los `print` van al driver log pero son difíciles de filtrar. No hay forma de medir si el job procesó 10 o 10 millones de filas. Y si el job falla a medias, nadie sabe en qué quedó.

**Cómo se corrige.** `logging.getLogger(__name__)`, log a INFO con métricas (`rows_written=...`, `tenant=...`, `batch_id=...`), y considerar emitir a `quality_logs` los conteos. Ver `logger.info(...)` en `good_code.py`.

---

## Cómo se lo explicaría al junior

Lo primero que le diría es que **el código funciona**: la lógica está bien razonada y se entiende a la primera. Eso ya es mucho. Pero hay una diferencia entre código que funciona y código que aguanta en producción multi-tenant; el ejercicio es discutir esa diferencia, no señalar errores.

Le pediría que pensara en tres dimensiones cuando refactorice: **escala** (qué pasa si esto procesa 100 millones de filas en lugar de 100), **operación** (qué pasa cuando hay que reejecutar un día porque el feed venía mal), y **gobierno** (cómo otro equipo entiende y mantiene esto sin preguntarte). Cada observación de arriba cae en una de esas tres categorías y, mirado así, la respuesta deja de ser "está mal" y empieza a ser "esto tiene un costo en X dimensión, ¿lo asumimos?".

Como tarea de auto-investigación le pediría que leyera la documentación de **Delta Lake** (particularmente `MERGE INTO` y `replaceWhere`), el patrón **Medallion** de Databricks, y **SCD Type 2** con join temporal. También que se diera una vuelta por **Catalyst** y por qué `iterrows()` y `pandas` no escalan: entender el "porqué" del API de Spark es lo que evita escribir código así en el futuro.

Cerraría diciendo: *"esto que cambiamos hoy lo vas a ver en cada PR los próximos meses; vale más que lo entiendas que que lo memorices. Si dudás de algo del refactor, marcalo en el code review y lo discutimos"*. El estilo de feedback que aplico es nombrar el problema una vez, mostrar el patrón correcto, y dejar que el siguiente PR lo aplique solo — repetir la misma corrección es señal de que el feedback no se entendió, no de que el junior es lento.
