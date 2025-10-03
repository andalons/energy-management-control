from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import datetime


def spark_open_session(lakehouse_name):
    """Abre sesión de Spark configurada para el lakehouse"""
    spark = (
        SparkSession.builder
        .appName("JSON_log")
        .config("spark.fabric.lakehouse.name", lakehouse_name)
        .getOrCreate()
    )
    return spark


def spark_write_table(df, table_name, write_mode="append"):
    """Escribe DataFrame en tabla Delta"""
    writer = df.write.format("delta").mode(write_mode)
    
    if write_mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    
    writer.saveAsTable(table_name)


def check_json_log_exists(
    lakehouse_name: str,
    medallion_short: str,
    source_file: str,
    api_name: str = None
) -> bool:
    """
    Verifica si un archivo ya fue procesado.
    
    Parameters
    ----------
    lakehouse_name : str
        Nombre del lakehouse (ej: 'lh_energy_bronze')
    medallion_short : str
        Prefijo de la tabla log (ej: 'brz')
    source_file : str
        Ruta del archivo JSON a verificar
    api_name : str, optional
        Categoría/widget para filtrar (ej: 'balance/balance-electrico')
    
    Returns
    -------
    bool
        True si el archivo ya existe en el log
    """
    spark = spark_open_session(lakehouse_name)
    log_table = f"{medallion_short}_json_log"
    
    if not spark._jsparkSession.catalog().tableExists(log_table):
        return False
    
    df = spark.table(log_table).filter(col("source_file") == source_file)
    
    if api_name:
        df = df.filter(col("api_name") == api_name)
    
    return df.limit(1).count() > 0


def save_json_log(
    api_name: str,
    source_file: str,
    target_table: str,
    ingestion_date: datetime.datetime,
    write_mode: str = "append",
    lakehouse_name: str = "lh_energy_bronze",
    medallion_short: str = "brz"
):
    """
    Registra archivo procesado en la tabla de log.
    
    Parameters
    ----------
    api_name : str
        Categoría/widget (ej: 'balance/balance-electrico')
    source_file : str
        Ruta completa del archivo JSON procesado
    target_table : str
        Nombre de la tabla Delta destino
    ingestion_date : datetime
        Timestamp del procesamiento
    write_mode : str
        Modo de escritura ('append' por defecto)
    lakehouse_name : str
        Nombre del lakehouse
    medallion_short : str
        Prefijo de la tabla log
    """
    spark = spark_open_session(lakehouse_name)
    
    schema = StructType([
        StructField("api_name", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("ingestion_date", TimestampType(), True),
    ])
    
    new_row = [(str(api_name), str(source_file), str(target_table), ingestion_date)]
    df_new = spark.createDataFrame(new_row, schema=schema)
    
    log_table = f"{medallion_short}_json_log"
    
    try:
        if spark._jsparkSession.catalog().tableExists(log_table):
            df_existing = spark.table(log_table)
            
            duplicate = (
                df_existing
                .filter(
                    (col("api_name") == api_name) &
                    (col("source_file") == source_file)
                )
                .limit(1)
                .count()
            )
            
            if duplicate > 0:
                print(f"\t⚠️  Ya procesado: {source_file}")
                return None
        
        spark_write_table(df_new, log_table, write_mode)
        print(f"\t✅ Log guardado: {source_file}")
        
    except Exception as e:
        print(f"\t⛔ Error guardando log para {source_file}")
        print(f"\t   {str(e)}")
        return e
