-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Sesión 07 - Lab 2 (SQL): Calidad de datos y capa semántica
-- MAGIC
-- MAGIC **Este archivo corre conectado a un SQL Warehouse (Serverless o Pro)**, no a un cluster ni a compute serverless de propósito general (ver la nota completa en `sesion07_lab2.ipynb`). Antes de abrir este archivo, corré `sesion07_lab2.ipynb` completo (deja armada `dbassociate.silver.tickets_soporte`, con problemas de calidad a propósito).
-- MAGIC
-- MAGIC Cambiá el selector de compute, arriba, a un SQL Warehouse antes de ejecutar la primera celda de acá abajo.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lab 2A: expectations con la acción por defecto (warn)
-- MAGIC
-- MAGIC `CONSTRAINT <nombre> EXPECT (<condición>)`, sin `ON VIOLATION`, no descarta ni bloquea nada: las filas que no cumplen se mantienen en el resultado, pero la violación queda registrada en el event log de la Materialized View.

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW dbassociate.gold.mv_tickets_calidad_warn (
    -- Veremos los expects
    -- es de tipo WARN por defecto,
    CONSTRAINT ticket_id_no_nulo EXPECT (ticket_id IS NOT NULL),
    CONSTRAINT prioridad_valida EXPECT (prioridad IN ('baja', 'media', 'alta', 'critica')),
    CONSTRAINT tiempo_resolucion_valido EXPECT (tiempo_resolucion_horas IS NULL OR tiempo_resolucion_horas >= 0),
    CONSTRAINT fechas_coherentes EXPECT (fecha_cierre IS NULL OR fecha_cierre >= fecha_apertura)
) AS
SELECT * FROM dbassociate.silver.tickets_soporte;

-- COMMAND ----------

-- Muestra todos los registros
SELECT COUNT(*) AS total_filas FROM dbassociate.gold.mv_tickets_calidad_warn;
-- Verificar con un select para ver como estan los datos que no cumplen

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lab 2B: consultar las métricas de calidad en el event log
-- MAGIC
-- MAGIC `event_log()` expone el log de eventos de la Materialized View, incluidas las métricas de expectations (`details:flow_progress.data_quality.expectations`). Solo el dueño del objeto puede consultarlo.

-- COMMAND ----------

-- El event_log deben registrarse todas las expectations detectadas
SELECT
    timestamp,
    details:flow_progress.data_quality.expectations AS metricas_expectations
FROM event_log(TABLE(dbassociate.gold.mv_tickets_calidad_warn))
WHERE event_type = 'flow_progress'
  AND details:flow_progress.data_quality.expectations IS NOT NULL -- Aqui consulta en base a un struct que se contiene en un campo details.
ORDER BY timestamp DESC
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lab 2C: descartar filas inválidas antes de publicar en Gold (ON VIOLATION DROP ROW)
-- MAGIC
-- MAGIC Mismas cuatro reglas, ahora con `ON VIOLATION DROP ROW`: la fila completa se descarta si no las cumple. El total tiene que ser menor al de `mv_tickets_calidad_warn`, porque las 4 filas problemáticas del origen ya no entran.

-- COMMAND ----------

-- Solo guarda los datos que cumplan los expects.
CREATE OR REPLACE MATERIALIZED VIEW dbassociate.gold.mv_tickets_validados (
    CONSTRAINT ticket_id_no_nulo EXPECT (ticket_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT prioridad_valida EXPECT (prioridad IN ('baja', 'media', 'alta', 'critica')) ON VIOLATION DROP ROW,
    CONSTRAINT tiempo_resolucion_valido EXPECT (tiempo_resolucion_horas IS NULL OR tiempo_resolucion_horas >= 0) ON VIOLATION DROP ROW,
    CONSTRAINT fechas_coherentes EXPECT (fecha_cierre IS NULL OR fecha_cierre >= fecha_apertura) ON VIOLATION DROP ROW
) AS
SELECT * FROM dbassociate.silver.tickets_soporte;

-- COMMAND ----------

SELECT COUNT(*) AS total_filas_validadas FROM dbassociate.gold.mv_tickets_validados;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lab 2D: bloquear la publicación por completo (ON VIOLATION FAIL UPDATE)
-- MAGIC
-- MAGIC `ON VIOLATION FAIL UPDATE` no descarta silenciosamente: hace fallar la creación (o el refresco) completo apenas encuentra una fila inválida. Se usa para una regla que se considera crítica, acá que no exista un `ticket_id` nulo, porque sin esa llave el ticket ni siquiera se puede identificar.
-- MAGIC
-- MAGIC **La siguiente celda tiene que fallar.** Es el comportamiento esperado de `FAIL UPDATE` frente a la fila con `ticket_id` nulo del origen. Correla, mirá el error, y seguí con la celda de abajo (la corrección).

-- COMMAND ----------

-- creamos uan nueva vista materializada
-- Intenta crearlo pero por el expectetion DROP ROW no se puede crear al encontrar errores en la data que no cumple con esa regla.
CREATE OR REPLACE MATERIALIZED VIEW dbassociate.gold.mv_tickets_estrictos (
    CONSTRAINT ticket_id_no_nulo EXPECT (ticket_id IS NOT NULL) ON VIOLATION FAIL UPDATE
) AS
SELECT * FROM dbassociate.silver.tickets_soporte;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Para destrabar una regla `FAIL UPDATE`, la intervención tiene que pasar por el origen (Silver), no por relajar la regla: si no, se pierde el propósito de marcarla como crítica. Filtramos la fila problemática y reintentamos.

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW dbassociate.gold.mv_tickets_estrictos (
    CONSTRAINT ticket_id_no_nulo EXPECT (ticket_id IS NOT NULL) ON VIOLATION FAIL UPDATE
) AS
SELECT * FROM dbassociate.silver.tickets_soporte WHERE ticket_id IS NOT NULL;

-- COMMAND ----------

SELECT COUNT(*) AS total_filas FROM dbassociate.gold.mv_tickets_estrictos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lab 2E: patrón de cuarentena (conservar lo descartado, no perderlo)
-- MAGIC
-- MAGIC `ON VIOLATION DROP ROW` (Lab 2C) es simple, pero las filas rechazadas desaparecen sin dejar rastro. Para poder investigarlas después, se escriben aparte, con el motivo del rechazo, en vez de descartarlas silenciosamente.

-- COMMAND ----------

CREATE OR REPLACE TABLE dbassociate.gold.tickets_cuarentena AS
SELECT
    *,
    -- recreamos los expectations en una columna nueva
    CASE
        WHEN ticket_id IS NULL THEN 'ticket_id_nulo'
        WHEN prioridad NOT IN ('baja', 'media', 'alta', 'critica') THEN 'prioridad_invalida'
        WHEN tiempo_resolucion_horas < 0 THEN 'tiempo_resolucion_invalido'
        WHEN fecha_cierre < fecha_apertura THEN 'fechas_incoherentes'
    END AS motivo_rechazo
FROM dbassociate.silver.tickets_soporte
WHERE ticket_id IS NULL
   OR prioridad NOT IN ('baja', 'media', 'alta', 'critica')
   OR tiempo_resolucion_horas < 0
   OR fecha_cierre < fecha_apertura;

-- COMMAND ----------

SELECT ticket_id, prioridad, tiempo_resolucion_horas, fecha_apertura, fecha_cierre, motivo_rechazo
FROM dbassociate.gold.tickets_cuarentena;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lab 2F: diseño de la capa semántica (vista publicada para BI)
-- MAGIC
-- MAGIC Una tabla Gold "cruda" (nombres técnicos, columnas de auditoría, sin documentar) no es lo mismo que un objeto pensado para que un analista de BI lo consuma con confianza. La capa semántica agrega una vista encima con nombres de negocio estables, comentarios, y permisos otorgados por dominio, no por tabla suelta a cada persona.

-- COMMAND ----------

-- Creamos las vistas para recrear las capas semanticas.
CREATE OR REPLACE VIEW dbassociate.gold.vw_metricas_soporte_bi
COMMENT 'Metricas diarias de tickets de soporte, publicadas para el equipo de BI. Contrato estable: no eliminar ni renombrar columnas sin coordinar con los consumidores.'
AS
SELECT
    fecha_apertura AS fecha,
    canal,
    prioridad,
    total_tickets,
    tiempo_promedio_horas,
    sla_horas AS sla_objetivo_horas,
    departamento_responsable
FROM dbassociate.gold.mv_metricas_tickets_diarias;

-- COMMAND ----------

SELECT * FROM dbassociate.gold.vw_metricas_soporte_bi ORDER BY fecha, canal LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Publicar por dominio, no por tabla suelta:** en vez de otorgar `SELECT` usuario por usuario, se le otorga a un grupo que representa al equipo consumidor (`analistas_bi`). Reemplazá el nombre del grupo por uno real de tu workspace antes de ejecutar. Si el grupo no existe, la sentencia falla; es esperable si todavía no lo creaste.

-- COMMAND ----------

-- Asigando permisos de SELECT a la vista al grupo analistas_bi
GRANT SELECT ON VIEW dbassociate.gold.vw_metricas_soporte_bi TO `analistas_bi`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Nota sobre versionado de contratos:** cuando un cambio en la vista publicada rompe una columna que un consumidor ya usa (renombrarla, cambiar su tipo, eliminarla), la práctica es publicar una nueva versión (`vw_metricas_soporte_bi_v2`, o un esquema `gold_v2` completo, según qué tan amplio sea el cambio) y dejar la anterior activa hasta que los consumidores migren, en vez de editar la vista existente sin avisar. La vista original actúa como el "contrato" con el equipo de BI: se puede *agregar* una columna nueva sin romperlo, pero no se puede *quitar* o *redefinir* una columna existente sin coordinarlo.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Limpieza
-- MAGIC
-- MAGIC La tabla Silver (`tickets_soporte`) se elimina desde `sesion07_lab2.ipynb`, no acá. `dbassociate.gold.mv_metricas_tickets_diarias` es del Lab 1 (`sesion07_lab1.sql`).

-- COMMAND ----------

DROP VIEW IF EXISTS dbassociate.gold.vw_metricas_soporte_bi;
DROP TABLE IF EXISTS dbassociate.gold.tickets_cuarentena;
DROP MATERIALIZED VIEW IF EXISTS dbassociate.gold.mv_tickets_estrictos;
DROP MATERIALIZED VIEW IF EXISTS dbassociate.gold.mv_tickets_validados;
DROP MATERIALIZED VIEW IF EXISTS dbassociate.gold.mv_tickets_calidad_warn;