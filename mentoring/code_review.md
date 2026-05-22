# Code review — `bad_code.py`

Lo que viene es la revisión del código del Anexo A. La consigna fue tratarlo como si fuera una entrega de un junior del equipo: leer, marcar lo que mejoraría, refactorizar en `good_code.py` y dejar una nota corta sobre cómo le daría el feedback.

Una aclaración importante antes de empezar: **el código funciona**. Hace lo que dice. La revisión no es para enumerar errores — es para identificar qué de eso no aguanta en un pipeline multi-tenant que va a producción con datos sensibles. La diferencia entre "anda" y "anda bien" es lo que esta revisión intenta poner en palabras.

Son seis observaciones, ordenadas por impacto.

---

## 1. Usar `pandas` para procesar el dataset entero en el driver

```python
df = pd.read_csv(file_path)
df = df[df["pais"] == country]
result = []
for i, row in df.iterrows():
    ...
```

El código carga todo el CSV en memoria con pandas e itera fila por fila. Spark queda como un escritor de archivos al final. Es el patrón que se ve cuando alguien aprendió pandas primero y después tuvo que "adaptarlo" a Spark.

**Por qué importa.** Hoy con 3 000 filas anda. Cuando este pipeline corra con el dataset real de una corporación de bebidas (millones de filas mensuales) el driver no resiste — OOM. Y `iterrows()` es notoriamente lento, ~100x más que operaciones vectorizadas. Pero el problema más grave no es la performance: es que el modelo de ejecución perezosa de Spark queda desactivado. No hay predicate pushdown, no hay paralelismo, no hay optimización del Catalyst.

**Cómo se corrige.** Leer directo con `spark.read.csv()`, aplicar las transformaciones como expresiones de columna (`F.when`, `F.col`), no salir nunca del API de DataFrame hasta el `.write`. En `good_code.py:read_deliveries` está la versión correcta.

---

## 2. Reglas de negocio escondidas como literales en el código

```python
if row["tipo_entrega"] == "ZPRE" or row["tipo_entrega"] == "ZVE1":
    if row["unidad"] == "CS":
        qty = row["cantidad"] * 20
```

`"ZPRE"`, `"ZVE1"`, `"CS"`, el `20` que es el factor CS→ST, y hasta el `/tmp/output/` están enterrados en expresiones.

**Por qué importa.** Si negocio mañana agrega `Z04` como tipo de rutina (o cambia el factor a 24 porque cambió el packaging), hay que tocar código en N lugares. El factor `20` es **información de dominio** que pertenece a un YAML auditable, no a una multiplicación. Y `/tmp/output/` no funciona en Windows ni en Databricks (donde no hay `/tmp`), así que el código directamente no es portable.

**Cómo se corrige.** Mover las reglas a un objeto de configuración. En `good_code.py` está `DeliveryProcessingConfig` como dataclass — los tipos válidos, el factor, el path raíz son atributos. En el repo "grande" lo mismo está hecho con OmegaConf jerárquico en `config/base.yaml`. La idea es la misma: el código consume reglas, no las contiene.

---

## 3. Inferencia de tipos del CSV + cero validaciones

`pd.read_csv()` infiere los dtypes mirando una muestra. `precio` y `cantidad`, que son decimales con anomalías intencionales (negativos, nulos, ceros), pueden terminar como `object` (string en pandas) si pandas encuentra valores extraños en las primeras filas. Y `unidad` puede llegar como `"KG"` o vacío — el `else` del código lo trata como ST silenciosamente.

**Por qué importa.** Errores de tipo se manifiestan tarde y como bugs silenciosos. `qty * row["precio"]` con `precio` como string en pandas hace concatenación, no error claro. Y filas con unidades desconocidas inflan métricas de ingreso sin alerta. El criterio de evaluación de la prueba completa habla de "materiales perdidos silenciosamente" como negativo — el mismo principio aplica a unidades desconocidas.

**Cómo se corrige.** Tres cosas: (a) esquema explícito en `spark.read.schema(...)`, (b) validación post-normalización que cuente las unidades inesperadas y registre warning, (c) en un pipeline serio: tabla de cuarentena y `quality_logs`. En `good_code.py` está (a) y (b); (c) está en el repo grande.

---

## 4. Escritura no idempotente, sin partición y en formato no ACID

```python
sdf.write.mode("overwrite").parquet("/tmp/output/" + country)
```

`overwrite` borra y reescribe TODO el output del country. No hay particionado, no hay Delta, no hay clave de negocio para reconciliar.

**Por qué importa.** Reejecutar el job con un rango de fechas chico borra los datos de los rangos anteriores — pérdida de información. Sin Delta no hay ACID, time travel, MERGE, ni `replaceWhere`. La prueba completa exige Delta explícitamente (sec 3 y 5.5). Y sin particionado los queries downstream barren todo el dataset.

**Cómo se corrige.** Escribir Delta particionado por `(_tenant_id, fecha_proceso)` y usar `replaceWhere` para que la reescritura sólo afecte el rango procesado:

```python
df.write.format("delta") \
   .mode("overwrite") \
   .option("replaceWhere", f"fecha_proceso BETWEEN '{start}' AND '{end}'") \
   .partitionBy("_tenant_id", "fecha_proceso") \
   .save(out_path)
```

Está en `write_idempotent` de `good_code.py`.

---

## 5. Multi-tenant simulado con un `WHERE pais == country`

El argumento `country` es un literal pasado a la función; la única "isolation" es filtrar el DataFrame. Esto en producción multi-tenant es un antipatrón conocido.

**Por qué importa.** El criterio explícito de la prueba completa lo marca como negativo: *"Configuración multi-tenant implementada como un filtro WHERE pais = X, sin la estructura jerárquica definida"*. La razón es operativa: cuando aparezca el primer tenant con compliance distinto y permisos que no pueden ver datos de otros tenants, ese WHERE es la primera fuga de información — basta con olvidarlo en una query downstream.

**Cómo se corrige.** El código debe poder operar sobre todos los tenants o sobre una lista. Normalizar el código del país a minúscula (`pais` → `_tenant_id`) y particionar físicamente la salida por tenant. Está en `select_tenants` + `partitionBy("_tenant_id", ...)` de `good_code.py`. En el repo grande está en el path: cada tenant escribe en `data/<layer>/<tenant>/`, físicamente separados.

---

## 6. `print("done")` como sistema de observabilidad

```python
sdf.write.mode("overwrite").parquet("/tmp/output/" + country)
print("done")
return out
```

**Por qué importa.** En Databricks Jobs los `print` van al driver log pero son difíciles de filtrar y no hay forma de medir si el job procesó 10 o 10 millones de filas. Si el job falla a medias, nadie sabe en qué quedó. Si se necesita contestar "cuánto procesamos ayer" hay que ir manualmente a la tabla output y contar.

**Cómo se corrige.** `logging.getLogger(__name__)` con nivel INFO, contexto del tenant + batch_id + conteo de filas, y considerar emitir a `quality_logs` los conteos para que queden auditables. En `good_code.py` está `logger.info(...)`. El repo grande usa Rich para output local + `print` con métricas estructuradas + persistencia en `quality_logs`.

---

## Cómo se lo explicaría al junior

Lo primero que le diría es que **el código funciona y se entiende a la primera**. Eso ya es mucho — hay código en producción que no cumple ninguno de los dos. La diferencia entre "anda" y "aguanta en producción multi-tenant" es lo que estamos discutiendo, no si el código está mal.

Le pediría que pensara las observaciones en tres dimensiones cuando refactorice: **escala** (qué pasa si esto procesa 100 millones de filas en lugar de 100), **operación** (qué pasa cuando hay que reejecutar un día porque el feed venía mal) y **gobierno** (cómo otro equipo entiende y mantiene esto sin preguntarte). Cada observación de arriba cae en alguna de las tres y, vista así, deja de ser una lista de "está mal" y empieza a ser "esto tiene un costo en X dimensión, ¿lo asumimos?".

Como tarea de auto-investigación le pediría que leyera la documentación de Delta Lake (en especial `MERGE INTO` y `replaceWhere`), el patrón Medallion de Databricks, y SCD Type 2 con join temporal. También que se diera una vuelta por Catalyst y entendiera por qué `iterrows()` y `pandas` no escalan — entender el "porqué" del API de Spark es lo que evita escribir código así en el futuro. Y lo apuntaría a `docs/architecture.md` del repo grande, donde está lo mismo aplicado.

El estilo de feedback que aplico es: nombrar el problema una vez, mostrar el patrón correcto, y dejar que el siguiente PR lo aplique solo. Si veo la misma corrección dos veces es señal de que el feedback no se entendió, no de que la persona sea lenta — la responsabilidad de explicar mejor es mía. Y cerraría con algo concreto: *"de lo que hablamos, lo más alto-impacto que cambiarías hoy es el patrón Delta + replaceWhere. ¿Lo intentas como refactor y lo revisamos el viernes?"*. Feedback abstracto desaparece; tarea concreta con fecha queda.
