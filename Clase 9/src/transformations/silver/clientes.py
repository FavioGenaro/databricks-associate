from pyspark import pipelines as dp
from pyspark.sql.functions import initcap, trim, col, when, lower, lit, current_timestamp
from pyspark.sql.types import LongType, StringType, DateType
from datetime import datetime
from src.schemas.silver.clientes import schema_clientes
from src.utils.utils import parse_fecha_registro_safe

# diccionario de validaciones
valid_expects = {
    "warning_id_cliente_null": "id_cliente IS NOT NULL",
    "warning_email_valid": "email IS NOT NULL AND email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$'",
    "warning_fecha_valida": "fecha_registro IS NOT NULL AND fecha_registro >= '1900-01-01' AND fecha_registro <= current_date()"
}

### PRIMERA PARTE
# definimos una vista temporal para aplicar validaciones
@dp.temporary_view(
    name="view_clientes",
    comment="Clientes limpios con validaciones aplicadas"
)
# definimos las validaciones o expects que se aplicaran a la vista antes de ingresarlos a la tabla
@dp.expect_all(valid_expects)
# definimos el ingestador de datos y aplicamos tranformaciones
def staging_clientes():
    # realizamos tranformaciones para ingestarlo sobre la vista temporal
    EMAIL_PATTERN = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    MIN_DATE = "1900-01-01"
    today = datetime.now().date()

    # Lectura desde Bronze
    df = (
        spark.readStream.table("dbassociate.bronze.clientes_raw")
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
        # para controlar el formato de la fecha.
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
# Python aplica los decoradores de abajo hacia arriba.
# Conceptualmente: def staging_clientes() -> @dp.expect_all -> @dp.temporary_view


### SEGUNDA PARTE
# create_streaming_table reemplaza a @dp.table, porque el auto CDC usa si o si streaming table
# que hace create_streaming_table?: crea la tabla si no existe, sino actualiza el esquema y agrega el trigger de streaming.
dp.create_streaming_table(
    name="dbassociate.silver.clientes",
    comment="Estado actual de clientes VÁLIDOS (SCD Tipo 1)", # Es metadata/documentación.
    schema=schema_clientes() # proporcionando explícitamente el esquema.
)

#  source es un delta table o vista y el target si o si es un delta table
dp.create_auto_cdc_flow(
    target="dbassociate.silver.clientes", # tabla de destino, es la tabla streaming
    source="view_clientes", # la vista temporal que definimos arriba, es el origen de los datos
    # AUTO CDC
    keys=["id_cliente"], # el id para comparar los registros e identificarlos
    sequence_by="updated_at", # Esto indica qué columna se utilizará para determinar el orden de los cambios. En caso de duplicados, se usa esta columna para controlar cual seleccionar (de menor a mayor)
    column_list = ["nombre", "email", "ciudad", "fecha_registro", "updated_at"], # columnas que insertará o actualizará. Columnas del source que deben mantenerse/aplicarse en el target como columnas de datos.
    stored_as_scd_type=1, # esto indica que es un SCD tipo 1
    name="clientes_cdc_flow" # nombre del auto scdc, el evento log monitoria los expectation y el cdc y para identificarlo dentro del eventlog necesita el nombre
)


