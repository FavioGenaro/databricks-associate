# Databricks notebook source
# MAGIC %md
# MAGIC # Sesión 02 - Lab 1: Patrones de ingesta y Lakeflow Connect

# COMMAND ----------

# MAGIC %md
# MAGIC Este laboratorio compara una carga batch completa (full load) contra una carga incremental sobre el mismo conjunto de datos, recorre de forma conceptual cómo se configura un conector gestionado de Lakeflow Connect, y cierra con un paso sí ejecutable en Free Edition (crear una tabla vía Add Data / file upload). Antes de correrlo, sube `pedidos_full.csv` y `pedidos_incremental.csv` al volume `/Volumes/dbassociate/default/vol_landing/sesion_02/` (arrastrando los archivos en el explorador de Catalog, o con `dbutils.fs.cp` si ya están en el workspace).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación del entorno

# COMMAND ----------

dbutils.fs.ls("/Volumes/dbassociate/default/vol_landing/sesion_02") # lee los archivos de la ruta


# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 1A: Ingesta batch completa (full load)
# MAGIC
# MAGIC Se define un `StructType` explícito en vez de `inferSchema=True`, el mismo criterio exigido desde la Sesión 01. La carga agrega columnas técnicas de auditoría: `ingestion_timestamp`, `source_file` (vía `_metadata.file_name`, nunca `input_file_name()`), `source_system` y `batch_id`.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, current_timestamp, lit

schema_pedidos = StructType([
    StructField("pedido_id", IntegerType(), False),
    StructField("fecha_pedido", DateType(), False),
    StructField("cliente_id", IntegerType(), True),
    StructField("canal", StringType(), True),
    StructField("producto", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("cantidad", IntegerType(), True),
    StructField("precio_unitario", DoubleType(), True),
    StructField("monto_total", DoubleType(), True),
    StructField("region", StringType(), True),
])

path_full = "/Volumes/dbassociate/default/vol_landing/sesion_02/pedidos_full.csv"

df_full = (
    spark.read.option("header", True).schema(schema_pedidos).csv(path_full)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_name"))
    .withColumn("source_system", lit("erp_pedidos"))
    .withColumn("batch_id", lit("carga_full_2026-06-30"))
)

df_full.write.mode("overwrite").saveAsTable("dbassociate.default.pedidos_lab1")

print("Filas cargadas en la carga full:", df_full.count())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 1B: Ingesta incremental (append)
# MAGIC
# MAGIC `pedidos_incremental.csv` representa únicamente los pedidos nuevos de un día posterior, ya recortados en el origen. La ingesta incremental no relee el archivo completo del día anterior: solo agrega (`append`) lo nuevo, con un `batch_id` propio para poder distinguir esta corrida de la anterior.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- el %sql es una comand magic, para ejecutar otros lenguajes de programación diferentes al por defecto del notebook

# COMMAND ----------

path_incremental = "/Volumes/dbassociate/default/vol_landing/sesion_02/pedidos_incremental.csv"

df_incremental = (
    spark.read.option("header", True).schema(schema_pedidos).csv(path_incremental)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_name"))
    .withColumn("source_system", lit("erp_pedidos")) # valor constante
    .withColumn("batch_id", lit("carga_incremental_2026-07-01"))
)

# lo guardmaos como tabla. catalogo, esquema y tabla
# internamente se guarda como formato delta.
df_incremental.write.mode("append").saveAsTable("dbassociate.default.pedidos_lab1")

# ingesta de tipo batch, fullload.
print("Filas agregadas en la carga incremental:", df_incremental.count())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 1C: Comparar ambas cargas en la misma tabla
# MAGIC
# MAGIC Nota de idempotencia: si esta celda de `append` se corriera dos veces por error, los pedidos incrementales quedarían duplicados, porque un `append` no verifica si esos `pedido_id` ya existen. Ese control de idempotencia (que sí lo resuelven `COPY INTO` y Auto Loader) se cubre en la Sesión 03.

# COMMAND ----------

# comparamos los datos ingestados
spark.sql("""
    SELECT batch_id, source_system, COUNT(*) AS num_pedidos, MIN(fecha_pedido) AS fecha_min, MAX(fecha_pedido) AS fecha_max
    FROM dbassociate.default.pedidos_lab1
    GROUP BY batch_id, source_system
    ORDER BY fecha_min
""").show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 1D: Configurar el conector de SQL Server hacia una Azure SQL Database (demo en vivo, pendiente de tier Premium)
# MAGIC
# MAGIC Los conectores gestionados de Lakeflow Connect requieren credenciales reales de un sistema fuente. La mayoría (Salesforce, HubSpot, Workday, ServiceNow) dependen de una cuenta SaaS de terceros que este curso no tiene. El conector de **SQL Server** es distinto: soporta Azure SQL Database como fuente, y una Azure SQL Database es un recurso que el propio docente puede provisionar en su suscripción de Azure para hacer una demo real en clase.
# MAGIC
# MAGIC Ese "real" tiene una condición: el workspace del curso corre hoy sobre Databricks Free Edition, que bloquea esta demo por dos motivos distintos. Primero, por defecto restringe el acceso saliente a internet a un conjunto reducido de dominios de confianza (se amplía solo si el alumno verifica su identidad con LinkedIn, y aun así el problema de fondo seguiría siendo del workspace, no del alumno). Segundo, y más determinante: la ingestion gateway de este conector necesita compute clásico, y Free Edition no da acceso a compute clásico bajo ninguna circunstancia, aunque la pipeline en sí corra en serverless. Premium resuelve ambos puntos: en Premium el acceso saliente a internet queda abierto por defecto (la restricción es una configuración opcional, no el estado por defecto), y Premium sí habilita compute clásico.
# MAGIC
# MAGIC Mientras esa decisión de tier no se tome, este paso documenta la secuencia real de configuración, para ejecutarla en vivo el día que el workspace pase a Premium:
# MAGIC
# MAGIC **Del lado de la Azure SQL Database, antes de tocar Databricks:**
# MAGIC
# MAGIC 1. Abrir el firewall de la base de datos para permitir la conexión desde Databricks.
# MAGIC 2. Crear un usuario SQL dedicado a la ingesta, con privilegios mínimos, usando el script de utilidades que provee Databricks.
# MAGIC 3. Habilitar Change Tracking (recomendado si la tabla tiene primary key, es más liviano) o CDC (si no la tiene) sobre las tablas a ingerir.
# MAGIC
# MAGIC **En Databricks:**
# MAGIC
# MAGIC 1. **Catalog Explorer → Add data → Data Ingestion**: elegir el conector "SQL Server".
# MAGIC 2. **Crear la `Connection`**: host y puerto de la Azure SQL Database, autenticación por usuario/contraseña (el método más simple y confiable para este conector), credenciales gestionadas como securable de Unity Catalog.
# MAGIC 3. **Elegir el origen**: seleccionar las tablas de la Azure SQL Database a ingerir.
# MAGIC 4. **Elegir el destino**: catalog y schema de Unity Catalog donde va a escribir el pipeline.
# MAGIC 5. **Ingestion gateway**: se crea sobre compute clásico y corre en modo continuo, sin detenerla manualmente, para no perder cambios por retención de logs; la ingestion pipeline en sí corre en serverless.
# MAGIC 6. **Configurar el schedule** de la pipeline: triggered (batch) o continuous, según la frecuencia requerida.
# MAGIC 7. **Monitorear la sincronización**: event logs de la gateway, métricas de salud de la pipeline (última corrida, filas procesadas, errores).
# MAGIC
# MAGIC Antipatrón a marcar: nunca guardar el usuario/contraseña de la Azure SQL Database en una celda del notebook. La `Connection` de Unity Catalog cumple, para conectores de Lakeflow Connect, el mismo rol que un Secret Scope respaldado por Key Vault cumple para credenciales usadas directamente en código: centralizar el secreto fuera del código, aunque son dos mecanismos distintos que no deben confundirse entre sí.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 1E: Paso ejecutable en clase (Free Edition): crear una tabla vía Add Data / file upload
# MAGIC
# MAGIC No es un conector gestionado de Lakeflow Connect, pero comparte el mismo punto de entrada en la UI (**Catalog Explorer → Add data**) y sí funciona en Free Edition sin credenciales ni acceso a internet: el origen es un archivo del propio computador, subido a un staging interno de Databricks, no a un sistema externo.
# MAGIC
# MAGIC 1. En el workspace, ir a **New → Add or upload data**.
# MAGIC 2. Click en **Create or modify a table**.
# MAGIC 3. Arrastrar `pedidos_incremental.csv` al drop zone (o el archivo que suba cada alumno).
# MAGIC 4. Elegir catalog `dbassociate` y schema `default`. Revisar el preview de 50 filas y los tipos de columna detectados automáticamente.
# MAGIC 5. Nombrar la tabla `pedidos_upload_demo` y click en **Create**.
# MAGIC
# MAGIC La celda siguiente valida, desde código, que la tabla se creó y quedó gobernada por Unity Catalog igual que cualquier otra tabla de este notebook.

# COMMAND ----------

spark.sql("DESCRIBE TABLE dbassociate.default.pedidos_upload_demo").show(truncate=False)

print("Filas en la tabla creada vía file upload:", spark.table("dbassociate.default.pedidos_upload_demo").count())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpieza

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS dbassociate.default.pedidos_lab1")
spark.sql("DROP TABLE IF EXISTS dbassociate.default.pedidos_upload_demo")

print("Tablas temporales de este laboratorio eliminadas.")
