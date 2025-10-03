from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

base_paths = [
    "Files/bronze/OPEN-METEO/brz_ccaa_mensual_2023_2025/2023",
    "Files/bronze/OPEN-METEO/brz_territorial_diario_2023_2025/2023",
    "Files/bronze/OPEN-METEO/brz_ccaa_mensual_2023_2025/2024",
    "Files/bronze/OPEN-METEO/brz_territorial_diario_2023_2025/2024",
    "Files/bronze/OPEN-METEO/brz_ccaa_mensual_2023_2025/2025",
    "Files/bronze/OPEN-METEO/brz_territorial_diario_2023_2025/2025"
]

for path in base_paths:
    print(f"🔎 Listando archivos en {path}")
    files = mssparkutils.fs.ls(path)

    for f in files:
        if not f.name.endswith(".json"):
            continue

        file_path = f"{path}/{f.name}"

        # 🔑 nombre tabla = nombre archivo (sin .json, sin guiones, con OPEN_METEO)
        raw_name = f.name.replace(".json", "").replace("-", "_")
        table_name = raw_name.replace("brz_", "brz_OPEN_METEO_")

        print(f"📂 Procesando archivo: {file_path} → tabla: {table_name}")

        df_raw = spark.read.option("multiline", True).json(file_path)

        # Detectamos si es mensual o diario según el nombre del archivo
        if "mensual" in f.name:
            df = df_raw.select(
                F.lit(raw_name).alias("region"),   # guardamos nombre de la ccaa para referencia
                F.col("year").cast("int"),
                F.col("month").cast("int"),
                F.col("temperature_2m_max").cast("double"),
                F.col("temperature_2m_min").cast("double"),
                F.col("temperature_2m_mean").cast("double"),
                F.col("precipitation_sum").cast("double"),
                F.col("wind_speed_10m_max").cast("double")
            ).withColumn("ingestion_date", F.current_timestamp())

        else:  # diario
            df = df_raw.select(
                F.lit(raw_name).alias("territorio"),  # guardamos nombre del territorio para referencia
                F.col("date"),
                F.col("temperature_2m_max").cast("double"),
                F.col("temperature_2m_min").cast("double"),
                F.col("temperature_2m_mean").cast("double"),
                F.col("precipitation_sum").cast("double"),
                F.col("wind_speed_10m_max").cast("double")
            ).withColumn("ingestion_date", F.current_timestamp())

        # Guardamos en su propia tabla
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)

        print(f"✅ Tabla creada: {table_name}")
