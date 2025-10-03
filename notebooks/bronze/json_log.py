from pyspark.sql import functions as F 
from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, TimestampType 
import datetime


def check_json_log_exists(
    lakehouse_name: str,
    medallion_short: str,
    log_table: str,
    source_file: str,
    api_name: str = None
) -> bool:
    """
    Check if a source_file (optionally filtered by api_name) already exists in a Delta log table.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    log_table : str
        Target Delta table name (e.g., 'brz_json_log').
    source_file : str
        File path or identifier to check.
    api_name : str, optional
        API name to filter on (if the table has that column).

    Returns
    -------
    bool
        True if the file already exists in the table, False otherwise.
    """
    
    spark = spark_open_session(lakehouse_name)
    log_table = f"{medallion_short}_json_log"
    if not spark._jsparkSession.catalog().tableExists(log_table):
        return False  # table not created yet

    df = spark.table(log_table).filter(F.col("source_file") == source_file)

    if api_name:
        df = df.filter(F.col("api_name") == api_name)

    return df.limit(1).count() > 0



def spark_open_session(lakehouse_name):
    spark = ( SparkSession.builder .appName("JSON_log") .config("spark.fabric.lakehouse.name", lakehouse_name) .getOrCreate() ) 
    return spark 

def spark_write_table(df,table_name, write_mode="append"): 
    writer = ( df.write.format("delta") .mode(write_mode) ) 
    #Only add overwriteSchema if mode is overwrite 
    if write_mode == "overwrite": 
        writer = writer.option("overwriteSchema", "true") 
    writer.saveAsTable(table_name)

def save_json_log(
    api_name,
    source_file,
    target_table,
    ingestion_date: datetime,
    write_mode="append",
    lakehouse_name="lh_bronze",
    medallion_short="brz"
):
    # -----------------------------
    # 1. Open pyspark session
    # -----------------------------
    spark = spark_open_session(lakehouse_name)

    # -----------------------------
    # 2. Define schema
    # -----------------------------
    schema = StructType([
        StructField("api_name", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("ingestion_date", TimestampType(), True),
    ])

    new_row = [(str(api_name), str(source_file), str(target_table), ingestion_date)]
    df_new = spark.createDataFrame(new_row, schema=schema)

    # -----------------------------
    # 3. Target log table
    # -----------------------------
    log_table = f"{medallion_short}_json_log"

    try:
        # Check if log table exists
        if spark._jsparkSession.catalog().tableExists(log_table):
            df_existing = spark.table(log_table)

            # Does this source_file already exist?
            duplicate = (
                df_existing
                .filter(
                    (F.col("api_name") == api_name) &
                    (F.col("source_file") == source_file)
                )
                .limit(1)
                .count()
            )

            if duplicate > 0:
                print(f"\t⚠️ Skipping log: entry already exists for api={api_name}, file={source_file}")
                return None

        # -----------------------------
        # 4. Write new log entry
        # -----------------------------
        spark_write_table(df_new, log_table, write_mode)
        print(f"\t✅ Log appended successfully at: {log_table}")

    except Exception as e:
        print(f"\t⛔ Error writing log for file={source_file} at table={target_table}")
        print(e)
        return e
