from pyspark.sql.functions import col, current_timestamp, trim, coalesce, when, to_date, substring, lit, to_timestamp, abs, hash
from pyspark.sql.types import DateType

def parse_fecha_registro_safe(fecha_col: str):
    col_trimmed = trim(col(fecha_col))
    
    return coalesce(
        
        when(col_trimmed.rlike("^\\d{4}-\\d{2}-\\d{2}$"),
               to_date(col_trimmed, 'yyyy-MM-dd')),
        
        
        when(col_trimmed.rlike("^\\d{2}/\\d{2}/\\d{4}$"),
               to_date(col_trimmed, 'dd/MM/yyyy')),
        
        
        when(col_trimmed.rlike("^\\d{2}-\\d{2}-\\d{4}$"),
               to_date(col_trimmed, 'MM-dd-yyyy')),
        
        
        when(col_trimmed.rlike("^\\d{4}/\\d{2}/\\d{2}"),
               to_date(substring(col_trimmed, 1, 10), 'yyyy/MM/dd')),
        
        
        when(col_trimmed.rlike("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$"),
               to_date(substring(col_trimmed, 1, 10), 'yyyy-MM-dd')),
        
        
        when(col_trimmed.rlike("^\\d{2}/\\d{2}/\\d{4} \\d{2}:\\d{2}:\\d{2}$"),
               to_date(substring(col_trimmed, 1, 10), 'dd/MM/yyyy')),
        
        
        when(col_trimmed.rlike("^\\d{2}/\\d{2}/\\d{4}$"),
               to_date(col_trimmed, 'MM/dd/yyyy')),
        
        lit(None).cast(DateType())
    )