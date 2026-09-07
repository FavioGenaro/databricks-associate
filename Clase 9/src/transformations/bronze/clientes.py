from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit, current_timestamp
# from src.project_sdp_etl.schemas.bronze.clientes import schema_clientes # importamos la función con el schema
from src.schemas.bronze.clientes import schema_clientes # importamos la función con el schema

 # esto construye una tabla y la ingesta en base a la definición que le pasemos, en este caso definimos la ingesta en bronze_table()
@dp.table(
    name="dbassociate.bronze.clientes_raw",
    comment="This is my table",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false", # bloqueamos el full refresh
        "delta.appendOnly": "true", # solo hace inserciones de datos, solo intertaciones
        # "pipelines.trigger.interval": "1 minute",
        # tmb se puede poner el schema de la tabla, pero por defecto le asignara uno al crear el schema.
    },
)
# lo recomendado es usar autoloader para cargar los datos, tambien es posible usar un spark.read sobre una tabla delta.
# podemos usar el autoloader como INGESTADOR, que es lo más recomendado
def bronze_table():
    df_reader = (
        spark.readStream
        .format("cloudFiles") # autoloader
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("delimiter", ",")
        .schema(schema_clientes()) # llamamos al schema de clientes de bronze
        # .load("/Volumes/dbassociate/default/vol_landing/sesion_09/")
        .load("abfss://metastore-data@saassociatedbkrs.dfs.core.windows.net/sesion09/")
        .withColumn("ingest_at", current_timestamp())
        .withColumn("metadata", col("_metadata")) # "_metadata" es una columna que habilita autoloader para tener detalles de la ejecución
    )

    return df_reader
