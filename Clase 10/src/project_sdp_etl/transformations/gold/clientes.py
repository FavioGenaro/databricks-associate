from pyspark import pipelines as dp
from pyspark.sql.functions import col, date_trunc, count

catalog = spark.conf.get("bundle.catalog")
schema_silver = spark.conf.get("bundle.schema_silver")
schema_gold = spark.conf.get("bundle.schema_gold")


@dp.materialized_view(
    name=f"{catalog}.{schema_gold}.clientes_resumen_mensual",
    comment="Tabla Gold: conteo de clientes por ciudad y mes de registro",
    table_properties={"quality": "gold"},
)
def gold_clientes_resumen_mensual():
    df = spark.read.table(f"{catalog}.{schema_silver}.clientes")

    return (
        df.filter(col("ciudad").isNotNull() & col("fecha_registro").isNotNull())
        .withColumn("mes_registro", date_trunc("month", col("fecha_registro")))
        .groupBy("ciudad", "mes_registro")
        .agg(count("*").alias("total_clientes"))
    )
