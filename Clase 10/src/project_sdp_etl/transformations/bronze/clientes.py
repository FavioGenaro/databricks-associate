from pyspark.sql.functions import col, current_timestamp
from pyspark import pipelines as dp
from src.project_sdp_etl.schemas.bronze.clientes import schema_clientes

# Leídos desde configuration (resources/project_sdp_etl.pipeline.yml), no
# hardcodeados: catalog y landing_path cambian según el target (dev/test).
# spark.conf.get("{name_var}") lee los configurarion
catalog = spark.conf.get("bundle.catalog")
schema_bronze = spark.conf.get("bundle.schema_bronze")
landing_path = spark.conf.get("bundle.landing_path")


@dp.table(
    name=f"{catalog}.{schema_bronze}.clientes_raw",
    comment="Tabla Bronze clientes_raw",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false",
        "delta.appendOnly": "true",
    },
)
def bronze_table():
    df_reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("delimiter", ",")
        .schema(schema_clientes())
        .load(landing_path)
        .withColumn("ingest_at", current_timestamp())
        .withColumn("metadata", col("_metadata"))
    )

    return df_reader
