# Documentación Técnica - Sistema de Procesamiento de Datos ESIOS API

## 1. Resumen del Sistema

Este sistema procesa datos de la API de ESIOS (Red Eléctrica de España) para obtener información energética en tres categorías principales:
- **Mercados/Componentes-Precio**: Precios de mercado eléctrico
- **Demanda/Evolución**: Evolución de la demanda eléctrica
- **Balance/Balance-Eléctrico**: Balance de generación eléctrica por tecnologías

## 2. Arquitectura del Sistema

### 2.1. Componentes Principales

#### Notebooks de Ingesta (Ingest)
- `nb_brz_esios_precios_month_day_ingest.ipynb`: Descarga datos de precios
- `nb_brz_esios_demanda_month_ingest.ipynb`: Descarga datos de demanda
- `nb_brz_esios_balance_month_ingest.ipynb`: Descarga datos de balance eléctrico

#### Notebooks de Procesamiento
- `nb_brz_esios_precios_month_day.ipynb`: Procesa datos de precios
- `nb_brz_esios_demanda_month.ipynb`: Procesa datos de demanda
- `nb_brz_esios_balance_month.ipynb`: Procesa datos de balance eléctrico

### 2.2. Flujo de Datos

1. **Extracción**: Llamadas a API ESIOS → JSON raw
2. **Almacenamiento Bronze**: JSON en estructura organizada
3. **Transformación**: JSON → DataFrames Spark → Tablas Delta
4. **Almacenamiento Bronze**: Tablas estructuradas en Lakehouse

## 3. Proceso de Ejecución de Llamadas a la API

### 3.1. Configuración Inicial

#### Autenticación
```python
headers = {
    "Accept": "application/json; application/vnd.esios-api-v2+json",
    "Content-Type": "application/json",
    "Host": "api.esios.ree.es",
    "x-api-key": "[API_KEY]"  # Clave API requerida
}
```

#### Configuración Spark
```python
spark = SparkSession.builder.appName("ESIOS_Processing").getOrCreate()
mssparkutils.lakehouse.set("lh_bronze")
```

### 3.2. Parámetros de Consulta

#### Indicadores Configurados
```python
indicators = [
    {"name": "PVPC-T-2-0TD", "id": 1293},
    {"name": "Precio mercado SPOT Diario", "id": 600},
    {"name": "Mercado SPOT Diario España", "id": 614}
]
```

#### Rangos Temporales
- **Años**: 2023, 2024, 2025
- **Granularidades**: "day" (diario), "month" (mensual)

### 3.3. Construcción de URLs API

```python
# Estructura base de la URL
base_url = "https://api.esios.ree.es/indicators/{indicator_id}"

# Parámetros de consulta
params = {
    "start_date": "{start_date}T00:00:00Z",
    "end_date": "{end_date}T23:59:59Z",
    "time_trunc": "{granularity}"
}
```

### 3.4. Ejecución de Llamadas

```python
for indicator in indicators:
    for year in years:
        for trunc in granularities:
            # Construir URL completa
            url = f"{base_url.format(indicator_id=indicator['id'])}?{params}"
            
            # Ejecutar llamada HTTP
            response = requests.get(url, headers=headers)
            
            # Validar respuesta
            if response.status_code == 200:
                data = response.json()
                # Guardar JSON raw
```

## 4. Estructura de Datos Descargados

### 4.1. Formato JSON Raw

Los datos descargados siguen este esquema JSON:

```json
{
    "indicator": {
        "id": 1293,
        "name": "PVPC-T-2-0TD",
        "short_name": "PVPC T. 2.0TD",
        "values": [
            {
                "value": 123.45,
                "datetime": "2023-01-01T00:00:00.000+01:00",
                "datetime_utc": "2022-12-31T23:00:00.000Z",
                "geo_id": 3,
                "geo_name": "Península"
            }
        ]
    }
}
```

### 4.2. Campos Principales

- **value**: Valor numérico del indicador
- **datetime**: Fecha/hora en zona horaria local
- **datetime_utc**: Fecha/hora en UTC
- **geo_id**: Identificador geográfico
- **geo_name**: Nombre de la zona geográfica

### 4.3. Ejemplos de Datos por Categoría

#### Precios de Mercado
```json
{
    "value": 85.32,
    "datetime": "2023-01-01T01:00:00.000+01:00",
    "geo_id": 3,
    "geo_name": "Península"
}
```

#### Demanda Eléctrica
```json
{
    "value": 24567.89,
    "datetime": "2023-01-01T01:00:00.000+01:00",
    "geo_id": 3,
    "geo_name": "Península"
}
```

#### Balance por Tecnología
```json
{
    "value": 1234.56,
    "datetime": "2023-01-01T01:00:00.000+01:00",
    "geo_id": 3,
    "geo_name": "Península"
}
```

## 5. Proceso de Transformación JSON a Tablas

### 5.1. Lectura de Archivos JSON

```python
# Leer todos los archivos JSON del directorio
df = spark.read.json(bronze_base_physical)
```

### 5.2. Extracción y Explosión de Datos

```python
# Extraer metadatos del indicador
df_meta = df.select(
    col("indicator.id").alias("indicator_id"),
    col("indicator.short_name").alias("indicator_name")
)

# Explotar array de valores
df_values = df.select(explode("indicator.values").alias("value_data"))

# Extraer campos de valores
df_final = df_values.select(
    col("value_data.value").cast("double").alias("value"),
    col("value_data.datetime").alias("datetime_local"),
    col("value_data.datetime_utc").alias("datetime_utc"),
    col("value_data.geo_id").alias("geo_id"),
    col("value_data.geo_name").alias("geo_name")
)
```

### 5.3. Enriquecimiento con Metadatos

```python
# Unir metadatos con valores
df_complete = df_final.crossJoin(df_meta)

# Agregar metadatos de procesamiento
df_complete = df_complete.withColumn("processing_date", current_date()) \
    .withColumn("year", year(col("datetime_utc"))) \
    .withColumn("time_trunc", lit(trunc))
```

### 5.4. Persistencia en Delta Tables

```python
# Generar nombre de tabla usando slugify
table_name = f"{tables_prefix}{slugify(indicator_name)}_{trunc}"

# Guardar como tabla Delta
df_complete.write.format("delta").mode("overwrite") \
    .saveAsTable(table_name)
```

## 6. Dependencias y Requisitos del Sistema

### 6.1. Dependencias de Python

```txt
pyspark
requests
datetime
os
re
unicodedata
json
```

### 6.2. Configuración de Entorno

#### Variables de Entorno Requeridas
- `ESIOS_API_KEY`: Clave API de ESIOS
- `LAKEHOUSE_BRONZE`: Nombre del Lakehouse bronze

#### Configuración Spark
- Spark 3.0+
- Configuración de memoria optimizada para procesamiento de JSON

### 6.3. Requisitos de Infraestructura

#### Almacenamiento
- Espacio suficiente para JSON raw (~100MB por año por indicador)
- Espacio para tablas Delta procesadas

#### Procesamiento
- Cluster Spark configurado
- Acceso a internet para llamadas API

## 7. Convenciones de Nomenclatura

### 7.1. Estructura de Directorios

```
Files/bronze/ESIOS/data/
├── mercados/componentes-precio/
│   ├── day/2023/
│   ├── month/2023/
│   └── ...
├── demanda/evolucion/
│   ├── day/2023/
│   ├── month/2023/
│   └── ...
└── balance/balance-electrico/
    ├── day/2023/
    ├── month/2023/
    └── ...
```

### 7.2. Nomenclatura de Archivos JSON

```python
# Patrón: brz-{categoria}-{indicador_normalizado}-{granularidad}-{año}.json
filename = f"brz-{category}-{slugify(indicator_name)}-{trunc}-{year}.json"
```

#### Ejemplos:
- `brz-mercados-pvpc_t_2_0td-day-2023.json`
- `brz-demanda-demanda_b_c_-month-2024.json`
- `brz-balance-carbon-month-2023.json`

### 7.3. Nomenclatura de Tablas Delta

```python
# Patrón: brz_esios_{categoria}_{indicador}_{granularidad}
table_name = f"brz_esios_{category}_{slugify(indicator_name)}_{trunc}"
```

#### Ejemplos:
- `brz_esios_mercados_pvpc_t_2_0td_day`
- `brz_esios_demanda_demanda_b_c__month`
- `brz_esios_balance_carbon_month`

### 7.4. Función Slugify

```python
def slugify(text):
    """Normaliza texto para nombres de archivos/tablas"""
    text = unicodedata.normalize('NFKD', text)
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '_', text)
    return text.lower()
```

## 8. Procesamiento por Categoría

### 8.1. Mercados/Componentes-Precio

#### Indicadores Procesados:
- PVPC-T-2-0TD (ID: 1293)
- Precio mercado SPOT Diario (ID: 600)
- Mercado SPOT Diario España (ID: 614)

#### Estructura de Tabla:
```sql
CREATE TABLE brz_esios_mercados_{indicador}_{granularidad} (
    value DOUBLE,
    datetime_local STRING,
    datetime_utc STRING,
    geo_id INT,
    geo_name STRING,
    indicator_id INT,
    indicator_name STRING,
    processing_date DATE,
    year INT,
    time_trunc STRING
)
```

### 8.2. Demanda/Evolución

#### Indicadores Procesados:
- Demanda en tiempo real
- Evolución de demanda por zonas

### 8.3. Balance/Balance-Eléctrico

#### Tecnologías Procesadas:
- Carbón
- Ciclo combinado
- Cogeneración
- Consumo bombeo
- Eólica
- Hidráulica
- Otras renovables
- Residuos no renovables
- Saldo intercambios
- Solar fotovoltaica
- Solar térmica
- Turbinación bombeo

## 9. Manejo de Errores

### 9.1. Validación de Respuestas API

```python
if response.status_code == 200:
    data = response.json()
    # Procesamiento exitoso
elif response.status_code == 429:
    # Rate limiting - implementar retry con backoff
    time.sleep(60)
else:
    # Loggear error y continuar
    print(f"Error {response.status_code} para {indicator['name']}")
```

### 9.2. Validación de Datos

```python
# Validar que existen valores
df_count = df_complete.count()
if df_count == 0:
    print(f"Advertencia: No hay datos para {indicator_name}-{year}-{trunc}")
```

## 10. Monitorización y Logging

### 10.1. Métricas de Procesamiento

- Número de registros procesados por ejecución
- Tiempo de ejecución por indicador
- Estado de las llamadas API

### 10.2. Logging de Ejecución

```python
print(f"Procesados {df_count} registros para {indicator_name}")
print(f"Tabla creada: {table_name}")
```

## 11. Consideraciones de Rendimiento

### 11.1. Optimización de Llamadas API

- Uso de granularidad apropiada (day/month)
- Límites de rate limiting de ESIOS API
- Cacheo de respuestas cuando sea posible

### 11.2. Optimización Spark

- Particionamiento por año y granularidad
- Compresión Delta optimizada
- Configuración de memoria para JSON parsing

## 12. Mantenimiento y Evolución

### 12.1. Actualización de Indicadores

Revisar periódicamente:
- Nuevos indicadores disponibles en ESIOS
- Cambios en esquemas de API
- Actualizaciones de rangos temporales

### 12.2. Escalabilidad

El sistema está diseñado para:
- Añadir nuevos años automáticamente
- Incorporar nuevos indicadores fácilmente
- Escalar horizontalmente con Spark

---

*Última actualización: [Fecha]*
*Versión del documento: 1.0*