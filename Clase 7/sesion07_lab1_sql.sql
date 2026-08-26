-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Sesión 07 - Lab 1 (SQL): Objetos de la capa Gold
-- MAGIC
-- MAGIC **Este archivo corre conectado a un SQL Warehouse (Serverless o Pro)**, no a un cluster ni a compute serverless de propósito general. `CREATE MATERIALIZED VIEW` y `CREATE STREAMING TABLE` fallan ahí, incluso vía `%sql`, con `MATERIALIZED_VIEW_OPERATION_NOT_ALLOWED.MV_NOT_ENABLED_ON_SERVERLESS_GENERIC_COMPUTE`: el error no depende del lenguaje de la celda, depende de a qué compute está conectado el notebook. Antes de abrir este archivo, corré `sesion07_lab1.ipynb` completo (deja armadas las tablas Silver y el primer lote de la Streaming Table).
-- MAGIC
-- MAGIC Cambiá el selector de compute, arriba, de tu cluster/serverless habitual a un SQL Warehouse antes de ejecutar la primera celda de acá abajo.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lab 1A: CREATE VIEW (objeto sin precómputo)
-- MAGIC
-- MAGIC Una `VIEW` no guarda datos propios: cada consulta vuelve a ejecutar la definición contra la tabla base.

-- COMMAND ----------

-- Creamos la vista en gold, en el catalogo solo estan los metadatos de la visa, no se guardan en parquet
CREATE OR REPLACE VIEW dbassociate.gold.vw_tickets_abiertos AS
SELECT ticket_id, cliente_id, canal, prioridad, estado, fecha_apertura
FROM dbassociate.silver.tickets_soporte
WHERE estado != 'cerrado';

-- COMMAND ----------

-- Cantidad de registros
SELECT COUNT(*) AS tickets_no_cerrados FROM dbassociate.gold.vw_tickets_abiertos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Confirmar que la View no tiene retraso:** insertamos un ticket nuevo directo en la tabla Silver y volvemos a consultar la View sin tocar su definición. El resultado tiene que reflejar el cambio de inmediato.

-- COMMAND ----------

INSERT INTO dbassociate.silver.tickets_soporte
VALUES ('TCK-4900', 5900, DATE'2026-07-09', NULL, 'chat', 'alta', 'abierto', NULL, 89.90);

-- COMMAND ----------

SELECT COUNT(*) AS tickets_no_cerrados FROM dbassociate.gold.vw_tickets_abiertos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lab 1B: CREATE MATERIALIZED VIEW (objeto precomputado, con broadcast a una tabla de referencia)
-- MAGIC
-- MAGIC A diferencia de la View, una `MATERIALIZED VIEW` sí guarda su resultado. `sla_prioridad` es chica (4 filas): Spark la transmite automáticamente a cada nodo sin que haga falta forzar `broadcast()` como en un DataFrame. La MV se define en SQL declarativo, no en código PySpark.

-- COMMAND ----------

-- No podemos crear una vista materializada con un cluster serverless, pero si con un sql warehouse
CREATE OR REPLACE MATERIALIZED VIEW dbassociate.gold.mv_metricas_tickets_diarias AS
SELECT
    t.fecha_apertura,
    t.canal,
    t.prioridad,
    COUNT(*) AS total_tickets,
    AVG(t.tiempo_resolucion_horas) AS tiempo_promedio_horas,
    s.sla_horas,
    s.departamento_responsable
FROM dbassociate.silver.tickets_soporte t
JOIN dbassociate.silver.sla_prioridad s
    ON t.prioridad = s.prioridad
GROUP BY t.fecha_apertura, t.canal, t.prioridad, s.sla_horas, s.departamento_responsable;

-- COMMAND ----------

SELECT SUM(total_tickets) AS total_tickets_mv FROM dbassociate.gold.mv_metricas_tickets_diarias;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lab 1C: la Materialized View se queda vieja hasta que se refresca
-- MAGIC
-- MAGIC Insertamos un lote de 10 tickets nuevos (el mismo lote incremental del Setup, acá como `INSERT` literal porque este archivo no lee CSV). La View va a reflejar el cambio de inmediato; la Materialized View se va a quedar con el número anterior hasta que alguien la refresque.

-- COMMAND ----------

-- Agregamos nuevos registros
INSERT INTO dbassociate.silver.tickets_soporte VALUES
    ('TCK-4036', 5036, DATE'2026-07-10', DATE'2026-07-10', 'email', 'media', 'resuelto', 6.0, 49.90),
    ('TCK-4037', 5037, DATE'2026-07-10', NULL, 'portal', 'baja', 'abierto', NULL, 29.90),
    ('TCK-4038', 5038, DATE'2026-07-10', DATE'2026-07-11', 'chat', 'alta', 'cerrado', 15.0, 89.90),
    ('TCK-4039', 5039, DATE'2026-07-10', DATE'2026-07-12', 'telefono', 'critica', 'cerrado', 37.5, 129.90),
    ('TCK-4040', 5040, DATE'2026-07-11', DATE'2026-07-11', 'email', 'baja', 'cerrado', 3.0, 29.90),
    ('TCK-4041', 5041, DATE'2026-07-11', DATE'2026-07-11', 'portal', 'media', 'resuelto', 8.5, 49.90),
    ('TCK-4042', 5042, DATE'2026-07-11', NULL, 'chat', 'alta', 'en_progreso', NULL, 89.90),
    ('TCK-4043', 5043, DATE'2026-07-11', DATE'2026-07-13', 'telefono', 'critica', 'cerrado', 39.0, 129.90),
    ('TCK-4044', 5044, DATE'2026-07-12', DATE'2026-07-12', 'email', 'media', 'resuelto', 7.0, 49.90),
    ('TCK-4045', 5045, DATE'2026-07-12', DATE'2026-07-12', 'portal', 'baja', 'resuelto', 2.5, 29.90);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC View (siempre al día) vs. Materialized View (todavía no refrescada):

-- COMMAND ----------

-- Aumento 6 registros más
SELECT COUNT(*) AS tickets_no_cerrados FROM dbassociate.gold.vw_tickets_abiertos;

-- COMMAND ----------

-- vemos que no se actualiza la cantidad, se tiene que refrescar.
SELECT SUM(total_tickets) AS total_tickets_mv FROM dbassociate.gold.mv_metricas_tickets_diarias;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lab 1D: REFRESH MATERIALIZED VIEW
-- MAGIC
-- MAGIC Recalcula el contenido contra el estado actual de la tabla Silver. Después de refrescar, el total tiene que coincidir con el de la View.

-- COMMAND ----------

-- Se tiene que hacer un refresh a la vista
REFRESH MATERIALIZED VIEW dbassociate.gold.mv_metricas_tickets_diarias;

-- COMMAND ----------

-- se actualiza el conteo a 45
SELECT SUM(total_tickets) AS total_tickets_mv FROM dbassociate.gold.mv_metricas_tickets_diarias;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lab 1E: CREATE STREAMING TABLE (ingesta incremental por archivo)
-- MAGIC
-- MAGIC El origen es la carpeta `sesion_07/tickets_stream/` del Volume, donde `sesion07_lab1.ipynb` ya dejó el primer lote (`tickets_stream_lote1.csv`). Cada refresco solo procesa los archivos que todavía no había visto.

-- COMMAND ----------

-- creamos una columna de tipo streamming
CREATE OR REFRESH STREAMING TABLE dbassociate.gold.st_tickets_stream AS
SELECT * FROM STREAM read_files(
    '/Volumes/dbassociate/default/vol_landing/sesion_07/tickets_stream/',
    format => 'csv',
    header => 'true'
);

-- COMMAND ----------

--  hay 8 registros
SELECT COUNT(*) AS total_tickets_stream FROM dbassociate.gold.st_tickets_stream;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Llega un segundo lote:** volvé a `sesion07_lab1.ipynb`, corré la celda del "Paso intermedio" (aterriza `tickets_stream_lote2.csv` en la misma carpeta) y volvé acá. En producción ese paso lo haría un proceso de ingesta aparte (Auto Loader, un job); acá lo simulamos a mano. Pero la separación entre "quién deja el archivo" y "quién lo consume por SQL" es real, no solo una limitación de este laboratorio.

-- COMMAND ----------

-- Tiene un checkpoint interno y solo procesa los datos nuevos
-- Si bien hacemos clicks, podemos usar lake flows para automatizarlo.
REFRESH STREAMING TABLE dbassociate.gold.st_tickets_stream;

-- COMMAND ----------

SELECT COUNT(*) AS total_tickets_stream FROM dbassociate.gold.st_tickets_stream;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lab 1F: comparación de los tres objetos
-- MAGIC
-- MAGIC | Objeto | ¿Guarda datos propios? | ¿Cuándo refleja un cambio? | Costo de cómputo | Caso de uso típico |
-- MAGIC |---|---|---|---|---|
-- MAGIC | `VIEW` | No, recalcula en cada consulta | Siempre al día | Se paga en cada `SELECT` | Recorte simple, sin agregación pesada, que tiene que estar siempre actualizado |
-- MAGIC | `MATERIALIZED VIEW` | Sí | Solo tras `REFRESH` (manual o programado) | Se paga en el refresco, no en cada `SELECT` | Métrica de negocio agregada que varios consumidores consultan seguido |
-- MAGIC | `STREAMING TABLE` | Sí | Solo tras `REFRESH`, pero procesa incrementalmente (no recalcula todo) | Se paga solo por los datos nuevos en cada refresco | Origen que crece por archivos/eventos nuevos, sin reprocesar el historial completo |
-- MAGIC
-- MAGIC Los tres corren sobre un pipeline serverless administrado por Databricks: no hace falta programar ni mantener ese pipeline a mano, a diferencia de un job de orquestación tradicional.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Limpieza
-- MAGIC
-- MAGIC Las tablas Silver (`tickets_soporte`, `sla_prioridad`) se eliminan desde `sesion07_lab1.ipynb`, no acá.

-- COMMAND ----------

DROP TABLE IF EXISTS dbassociate.gold.st_tickets_stream;
DROP MATERIALIZED VIEW IF EXISTS dbassociate.gold.mv_metricas_tickets_diarias;
DROP VIEW IF EXISTS dbassociate.gold.vw_tickets_abiertos;