from pyspark import pipelines as dp
from pyspark.sql.functions import initcap, trim, col, when, lower, lit, current_timestamp
from pyspark.sql.types import LongType, StringType, DateType
from datetime import datetime
from src.project_sdp_etl.schemas.silver.clientes import schema_clientes
from src.project_sdp_etl.utils.utils import parse_fecha_registro_safe

catalog = spark.conf.get("bundle.catalog")
schema_bronze = spark.conf.get("bundle.schema_bronze")
schema_silver = spark.conf.get("bundle.schema_silver")

valid_expects = {
    "warning_id_cliente_null": "id_cliente IS NOT NULL",
    "warning_email_valid": "email IS NOT NULL AND email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$'",
    "warning_fecha_valida": "fecha_registro IS NOT NULL AND fecha_registro >= '1900-01-01' AND fecha_registro <= current_date()"
}

@dp.temporary_view(
    name="view_clientes",
    comment="Clientes limpios con validaciones aplicadas"
)
@dp.expect_all(valid_expects)

def staging_clientes():

    EMAIL_PATTERN = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    MIN_DATE = "1900-01-01"
    today = datetime.now().date()

    df = (
        spark.readStream.table(f"{catalog}.{schema_bronze}.clientes_raw")
        .withColumn("nombre", initcap(trim(col("nombre"))))
        .withColumn(
            "ciudad",
            when(col("ciudad").isNull(), None).otherwise(initcap(trim(col("ciudad"))))
        )
        .withColumn(
            "email",
            when(col("email").isNull(), None)
            .when(lower(trim(col("email"))) == "null", None)
            .otherwise(lower(trim(col("email"))))
        )
        .withColumn(
            "email",
            when(col("email").rlike(EMAIL_PATTERN), col("email")).otherwise(None)
        )
        .withColumn("fecha_registro", parse_fecha_registro_safe("fecha_registro"))
        .withColumn(
            "fecha_registro",
            when(
                (col("fecha_registro") > lit(today)) |
                (col("fecha_registro") < lit(MIN_DATE)),
                None
            ).otherwise(col("fecha_registro"))
        )
        .withColumn("updated_at", current_timestamp())
        .select(
            col("id_cliente").cast(LongType()),
            col("nombre").cast(StringType()),
            col("email").cast(StringType()),
            col("ciudad").cast(StringType()),
            col("fecha_registro").cast(DateType()),
            col("updated_at")
        )
    )

    return df

dp.create_streaming_table(
    name=f"{catalog}.{schema_silver}.clientes",
    comment="Estado actual de clientes VÁLIDOS (SCD Tipo 1)",
    schema=schema_clientes()
)

dp.create_auto_cdc_flow(
    target=f"{catalog}.{schema_silver}.clientes",
    source="view_clientes",
    keys=["id_cliente"],
    sequence_by="updated_at",
    column_list = ["nombre", "email", "ciudad", "fecha_registro", "updated_at"],
    stored_as_scd_type=1,
    name="clientes_cdc_flow"
)
