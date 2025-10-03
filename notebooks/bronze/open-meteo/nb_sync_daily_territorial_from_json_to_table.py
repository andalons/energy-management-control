from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from notebookutils import mssparkutils
import json, os

spark = SparkSession.builder.getOrCreate()

# -------------------------------
# Recibir parámetros desde entorno
# -------------------------------
import os, json

param_value = os.environ.get("json_results", "{}")

try:
    outer = json.loads(param_value)  # primer nivel
    exit_val = outer.get("exitValue", "{}")
    parsed = json.loads(exit_val)    # segundo nivel
except Exception as e:
    raise SystemExit(f"❌ Error parseando parámetros: {e}")

start_date = parsed.get("start_date")
end_date = parsed.get("end_date")
years_touched = parsed.get("years_touched", [])

print(start_date, end_date, years_touched)


# -------------------------------
# Leer solo archivos JSON por año y filtrar por fecha
# -------------------------------
BASE_DIR = "Files/bronze/OPEN-METEO/brz_territorial_diario_2023_2025"
base_paths = [os.path.join(BASE_DIR, str(y)) for y in years_touched]

for path in base_paths:
    print(f"📂 Explorando carpeta: {path}")
    try:
        files = mssparkutils.fs.ls(path)
    except Exception as e:
        print(f"⚠️ No se pudo acceder a {path}: {e}")
        continue

    for f in files:
        if not f.name.endswith(".json"):
            continue

        file_path = f"{path}/{f.name}"
        raw_name = f.name.replace(".json", "").replace("-", "_")
        table_name = raw_name.replace("brz_", "brz_OPEN_METEO_")

        print(f"📥 Leyendo: {file_path}")
        df_raw = spark.read.option("multiline", True).json(file_path)

        # Solo si contiene columna "date"
        if "date" not in df_raw.columns:
            print(f"⚠️ Archivo {file_path} no tiene columna 'date'. Saltando.")
            continue

        # Filtrar por fecha (solo días modificados)
        df_filtered = df_raw \
            .withColumn("date", F.to_date("date")) \
            .filter((F.col("date") >= F.lit(start_date)) & (F.col("date") <= F.lit(end_date)))

        if df_filtered.rdd.isEmpty():
            print(f"⏭️ No hay registros en el rango en {f.name}")
            continue

        df_filtered = df_filtered.withColumn("ingestion_date", F.current_timestamp())

        # Añadir campo de partición si lo necesitas (por ejemplo, por año o territorio)
        # Realizar UPSERT a tabla Delta
        print(f"📝 Merge (upsert) a tabla: {table_name}")

        # Asumimos que las tablas ya existen (creadas previamente)
        # y que el campo clave es 'date' y 'territorio'
        from delta.tables import DeltaTable

        if not spark._jsparkSession.catalog().tableExists(table_name):
            print(f"❌ La tabla {table_name} no existe. Creándola.")
            df_filtered.write.format("delta").mode("overwrite").saveAsTable(table_name)
        else:
            delta_table = DeltaTable.forName(spark, table_name)

            (delta_table.alias("t")
             .merge(
                df_filtered.alias("s"),
                "t.date = s.date AND t.territorio = s.territorio"
             )
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())

        print(f"✅ Tabla actualizada: {table_name}")

print("🎉 Actualización por rango de fechas completada.")
