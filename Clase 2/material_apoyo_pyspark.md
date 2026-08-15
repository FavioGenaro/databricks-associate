# Material de apoyo: PySpark para quienes vienen de SQL

Este documento es una referencia de consulta rápida, no una lectura obligatoria antes de la Sesión 02. La mayoría de los alumnos del curso llegan con SQL sólido y poco o nada de PySpark, así que está organizado como una traducción: qué hacías en SQL, cómo se escribe lo mismo en PySpark. Tenlo abierto en una pestaña mientras resuelves los labs.

---

## 1. Lo esencial antes de empezar

Un DataFrame es una tabla: filas y columnas con nombre y tipo, igual que una tabla SQL. La diferencia que más confunde al principio es la **evaluación diferida (lazy evaluation)**: escribir `df.filter(...)` o `df.select(...)` no ejecuta nada todavía, solo arma un plan. Spark recién procesa los datos cuando llamás a una acción como `.show()`, `.display()`, `.count()` o `.collect()`, o cuando escribís el resultado con `.write`.

En la práctica, esto significa que podés encadenar varias transformaciones sin preocuparte por el costo de cada paso intermedio: Spark optimiza el plan completo antes de correrlo.

## 2. Tabla de equivalencias SQL → PySpark

| Querés hacer esto en SQL | Así se escribe en PySpark | Nota |
|---|---|---|
| `SELECT col1, col2 FROM t` | `df.select("col1", "col2")` | También podés pasar `col("col1")` en vez del string |
| `SELECT * FROM t WHERE cond` | `df.filter(cond)` o `df.where(cond)` | Son sinónimos exactos, `filter` es el más usado en el curso |
| `SELECT DISTINCT col FROM t` | `df.select("col").distinct()` | |
| `SELECT ... ORDER BY col DESC` | `df.orderBy(col("col").desc())` | Sin `.desc()` ordena ascendente |
| `SELECT ... GROUP BY col` | `df.groupBy("col").agg(...)` | `agg()` recibe las funciones de agregación |
| `t1 JOIN t2 ON t1.id = t2.id` | `df1.join(df2, on=df1["id"] == df2["id"], how="inner")` | `how` puede ser `inner`, `left`, `right`, `full`, entre otros |
| `col AS alias` | `col("col").alias("alias")` | |
| `CASE WHEN cond THEN a ELSE b END` | `when(cond, a).otherwise(b)` | Requiere importar `when` |
| `SELECT ... LIMIT n` | `df.limit(n)` | |
| Agregar una columna calculada | `df.withColumn("nueva", expr)` | No existe en SQL plano; es el equivalente a un `SELECT *, expr AS nueva` |
| Renombrar una columna | `df.withColumnRenamed("vieja", "nueva")` | |
| Quitar una columna | `df.drop("col")` | |

## 3. Sintaxis Python que vas a ver todo el tiempo en los labs

**Imports típicos.** Casi ninguna función de columna (`col`, `lit`, `when`, `explode`, `current_timestamp`, etc.) está disponible por defecto: hay que importarla de `pyspark.sql.functions`.

```python
from pyspark.sql.functions import col, lit, when, current_timestamp
```

**Encadenado de métodos (method chaining).** Los notebooks del curso arman transformaciones envolviendo toda la expresión entre paréntesis, un método por línea. Es el mismo DataFrame en cada paso, solo que Python permite leerlo como una secuencia:

```python
df_resultado = (
    df_origen
    .filter(col("region") == "Lima")
    .withColumn("monto_con_igv", col("monto_total") * 1.18)
    .select("pedido_id", "monto_con_igv")
)
```

**`col("nombre")` vs. `"nombre"` como string.** Muchos métodos aceptan ambas formas (`df.select("col1")` funciona igual que `df.select(col("col1"))`), pero en cuanto necesitás hacer una operación sobre la columna (compararla, sumarle algo, aplicarle una función), tenés que envolverla en `col(...)`. `df.filter("region == 'Lima'")` funciona porque es un string de expresión SQL, pero `df.filter(col("region") == "Lima")` es la forma que se usa en todo el curso porque es más fácil de leer y de depurar.

**f-strings para armar texto dinámico.** Se usan constantemente para mensajes de `print()` y para construir el texto de una consulta SQL dentro de `spark.sql(...)`, no para construir DataFrames directamente:

```python
print(f"Filas cargadas: {df_full.count()}")
```

**Listas y `StructField` para definir un schema.** Un `StructType` es una lista de `StructField`, cada uno con nombre, tipo y si acepta nulos. Esto ya lo vas a ver en el primer lab de la Sesión 02, pero conviene tenerlo identificado como estructura de datos Python común (una lista de objetos), no como sintaxis exclusiva de Spark.

## 4. Funciones de `pyspark.sql.functions` más usadas en este curso

| Función | Para qué sirve |
|---|---|
| `col("nombre")` | Referenciar una columna para operar sobre ella |
| `lit(valor)` | Convertir un valor fijo de Python en una columna constante |
| `current_timestamp()` | Timestamp del momento de ejecución, para columnas de auditoría |
| `when(cond, val).otherwise(val)` | Lógica condicional, equivalente a `CASE WHEN` |
| `explode(col)` | Convierte un array en filas independientes (Sesión 02, Lab 2) |
| `to_date(col, formato)` / `date_format(col, formato)` | Convertir entre string y fecha, y formatear fechas |
| `round(col, decimales)` | Redondear un número |
| `count()`, `sum()`, `avg()`, `min()`, `max()` | Agregaciones, se usan dentro de `.agg(...)` |

## 5. Errores de sintaxis frecuentes al empezar

- **Confundir `.show()` con `.display()`.** `.show()` imprime texto plano en la consola, funciona en cualquier entorno Spark. `.display()` es específico de Databricks y muestra una tabla interactiva con opciones de orden y gráficos. En este curso vas a ver ambos: `.show()` en el código que después correría igual fuera de Databricks, `.display()` cuando el punto es explorar el resultado en el notebook.
- **Olvidar el import de una función.** Si usás `when(...)` sin haber hecho `from pyspark.sql.functions import when`, Python tira `NameError`, no un error de Spark. Es el error más común en los primeros labs.
- **Usar `=` en vez de `==` dentro de un `filter` o un `when`.** Es una regla de Python, no de SQL: `=` asigna, `==` compara. `col("region") = "Lima"` ni siquiera es sintaxis válida de Python.
- **Paréntesis mal cerrados en un encadenado multilínea.** Si vas a escribir una transformación en varias líneas como en el ejemplo de la sección 3, toda la expresión tiene que estar envuelta en un único paréntesis exterior; si te falta uno, Python interpreta la línea siguiente como una instrucción nueva y tira un error de sintaxis.
- **Confundir un DataFrame con una tabla de Unity Catalog.** Un DataFrame vive solo en la sesión de Spark del notebook. Para que otra persona o notebook lo pueda consultar, hace falta escribirlo con `.write.saveAsTable(...)`, tal como se hace en cada lab del curso.

## 6. Para practicar y profundizar por tu cuenta

- [PySpark basics](https://learn.microsoft.com/azure/databricks/pyspark/basics) (Microsoft Learn): la referencia más directa, con los mismos patrones de esta guía pero más detallados.
- [Tutorial: Load and transform data using Apache Spark DataFrames](https://learn.microsoft.com/azure/databricks/getting-started/dataframes) (Microsoft Learn): tutorial guiado, se puede correr en el propio workspace del curso.
- [PySpark DataFrames QuickStart](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html) (documentación oficial de Apache Spark): más denso, útil una vez que ya te manejás con lo básico.
- Databricks Academy (gratuito, requiere crear una cuenta): curso self-paced "Apache Spark Programming with Databricks", más extenso que este documento, con ejercicios propios.

Ninguno de estos reemplaza la práctica: la forma más rápida de interiorizar la sintaxis es volver a escribir, sin copiar y pegar, las celdas de los labs ya resueltos de la Sesión 02.
