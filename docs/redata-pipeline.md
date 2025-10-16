# Pipeline REData - Red Eléctrica de España

## Descripción General

Este documento describe el pipeline completo de ingesta, transformación y modelado de datos desde la API REData de Red Eléctrica de España (REE). El pipeline sigue la arquitectura Medallion con tres capas: Bronze (ingesta), Silver (limpieza) y Gold (modelo dimensional).

---

## 🔗 Fuente de Datos

- **API**: https://apidatos.ree.es
- **Tipo**: REST API pública (sin autenticación)
- **Formato**: JSON
- **Documentación oficial**: [Red Eléctrica API](https://www.ree.es/es/apidatos)

### Endpoints Utilizados

| Categoría      | Endpoint                                    | Descripción                                              |
| -------------- | ------------------------------------------- | -------------------------------------------------------- |
| **balance**    | `balance-electrico`                         | Balance del sistema eléctrico con métricas anidadas      |
| **demanda**    | `evolucion`                                 | Evolución temporal de la demanda eléctrica               |
| **generacion** | `estructura-generacion`                     | Generación por tecnología (nuclear, eólica, solar, etc.) |
| **generacion** | `estructura-generacion-emisiones-asociadas` | Generación con clasificación de emisiones CO₂            |
| **generacion** | `estructura-renovables`                     | Desglose detallado de tecnologías renovables             |
| **generacion** | `evolucion-renovable-no-renovable`          | Comparativa renovable vs no renovable                    |

### Parámetros de Consulta

- **time_trunc**: `month` (agregación mensual)
- **geo_trunc**: `electric_system`
- **geo_limit**: `ccaa` (comunidades autónomas)
- **geo_ids**: 20 comunidades + sistemas insulares + territorios
- **start_date** / **end_date**: Rango temporal (máx. 24 meses por petición)

---

## 📦 Capa Bronze - Ingesta

### Notebook: `nb_redata_01_ingest.ipynb`

#### Objetivo

Extraer datos crudos desde la API REData y almacenarlos sin transformaciones en formato JSON/Delta.

#### Configuración Externa

El pipeline utiliza un archivo de configuración centralizado:

```json
{
  "ccaa_ids": {
    "Península": 8741,
    "Andalucía": 4,
    "Aragón": 5,
    ...
  },
  "api_config": {
    "balance": ["balance-electrico"],
    "demanda": ["evolucion"],
    "generacion": [
      "estructura-generacion",
      "estructura-generacion-emisiones-asociadas",
      "estructura-renovables",
      "evolucion-renovable-no-renovable"
    ]
  }
}
```

**Ubicación**: `Files/config/redata_config.json`

#### Clase Principal: `REDataAPIExplorer`

```python
class REDataAPIExplorer:
    def __init__(self, base_lakehouse_path, start_date, end_date):
        self.base_url = "https://apidatos.ree.es"
        self.ccaa_ids = config["ccaa_ids"]
        self.api_config = config["api_config"]
        self.start_date = datetime.fromisoformat(start_date)
        self.end_date = datetime.fromisoformat(end_date)
```

#### Funcionalidades Clave

##### 1. Construcción de URLs

```python
def build_api_url(self, lang, category, widget, time_trunc, geo_id, geo_name):
    """Construye URL con parámetros - SIEMPRE usando geo_limit=ccaa"""
    params = {
        "start_date": start_str,
        "end_date": end_str,
        "time_trunc": "month",
        "geo_trunc": "electric_system",
        "geo_limit": "ccaa",
        "geo_ids": str(geo_id)
    }
```

##### 2. Almacenamiento con Metadata Enriquecida

```python
def save_to_bronze(self, data, category, widget, region, timestamp, ...):
    enriched_data = {
        "request_metadata": {
            "geo_id": geo_id,
            "geo_name": geo_name,
            "category": category,
            "widget": widget,
            "time_trunc": time_trunc,
            "ingestion_timestamp": timestamp,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat()
        },
        "api_response": data
    }
```

##### 3. Sistema de Logging

- **Success Log**: `Files/bronze/REDATA/logs/success.log`
- **Error Log**: `Files/bronze/REDATA/logs/error.log`

Cada petición registra:

- Timestamp UTC con microsegundos
- URL completa de la petición
- Estado (OK/FAIL)

#### Estructura de Salida

```
Files/bronze/REDATA/data/
├── balance/
│   └── balance-electrico/
│       └── month/
│           └── brz-andalucia-balance-balance-electrico-month-2025-10-15t12-30-45-123456z.json
├── demanda/
│   └── evolucion/
│       └── month/
│           └── brz-peninsula-demanda-evolucion-month-2025-10-15t12-31-02-654321z.json
└── generacion/
    ├── estructura-generacion/
    ├── estructura-generacion-emisiones-asociadas/
    ├── estructura-renovables/
    └── evolucion-renovable-no-renovable/
```

#### Decisiones de Diseño

**✅ SOLO datos mensuales**

- Se descartaron datos diarios por volumen excesivo y granularidad innecesaria para el alcance actual
- Facilita el análisis de tendencias a medio/largo plazo
- Reduce la complejidad del modelo dimensional

**✅ SOLO geo_limit=ccaa**

- Se excluyen otros niveles geográficos (península, sistema eléctrico nacional)
- Evita duplicación de datos a diferentes niveles de agregación
- Mantiene coherencia en el modelo relacional

**✅ Limitación de 24 meses por petición**

- Para cubrir enero 2023 - junio 2025 (30 meses) se requieren 2 llamadas por endpoint
- Se implementó gestión iterativa de rangos temporales

#### Ejemplo de Ejecución

```python
explorer = REDataAPIExplorer(
    base_lakehouse_path="Files/bronze/REDATA",
    start_date="2023-01-01T00:00:00Z",
    end_date="2025-06-30T23:59:59Z"
)

results = explorer.explore_all(max_combinations=None)
summary = explorer.analyze_results(results)

# Output esperado:
# 🚀 Ejecutando 120 combinaciones (SOLO MENSUALES + CCAA)
# 📊 Resumen: {'total': 120, 'ok': 118, 'with_data': 116, 'failures': 2}
```

---

## 🧹 Capa Silver - Limpieza y Normalización

### Notebook: `nb_redata_02_json_to_delta.ipynb` (JSON → Delta)

#### Objetivo

Transformar los archivos JSON crudos de Bronze en tablas Delta estructuradas, aplicando el primer nivel de limpieza.

#### Funciones Clave

##### 1. Detección de Archivos Nuevos

```python
def get_new_json_files(category, widget, time_trunc):
    """
    Obtiene lista de archivos JSON aún no procesados.
    ✅ SOLO procesa 'month' - ignora 'day'
    """
    if time_trunc != "month":
        return []

    # Verificar en tabla de log si ya fue procesado
    if not check_json_log_exists(...):
        json_paths.append(full_path)
```

##### 2. Extracción de Metadata

```python
def extract_metadata(df_raw, category, widget, time_trunc):
    """Extrae metadatos del nodo api_response.data"""
    sample = df_raw.select(
        col("api_response.data.type").alias("data_type"),
        col("api_response.data.id").alias("data_id"),
        col("api_response.data.attributes.title").alias("data_title"),
        col("api_response.data.attributes.last-update").alias("data_last_update")
    ).first()
```

Estos metadatos se guardan en una tabla separada `brz_redata_metadata` para consulta rápida.

##### 3. Normalización de Fechas (CRÍTICO)

**⚠️ PROBLEMA IDENTIFICADO**: Las fechas venían con offset UTC (`+02:00` en verano, `+01:00` en invierno) y al parsearse se aplicaban conversiones erróneas que desplazaban las fechas hasta 2 horas.

**✅ SOLUCIÓN IMPLEMENTADA**:

```python
def parse_datetime_local(datetime_str):
    """
    Convierte datetime preservando la fecha local sin convertir a UTC

    Entrada:  "2024-10-01T00:00:00.000+02:00"
    Salida:   "2024-10-01 00:00:00" (mantiene fecha/hora local)
    """
    return regexp_replace(
        datetime_str,
        r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}).*$",
        "$1"
    )

# Uso en transformación:
to_timestamp(parse_datetime_local(col("val.datetime"))).alias("datetime")
```

##### 4. Procesamiento según Estructura

La API REData tiene dos tipos de estructura JSON:

**A. Balance Eléctrico** (con `content` anidado)

```python
def process_balance_electrico(df_raw, category, widget, time_trunc):
    # Nivel 1: Explotar series
    df_included = df_meta.select("*", explode("included").alias("series"))

    # Nivel 2: Explotar content (métricas anidadas)
    df_step1 = df_included.select(..., col("series.attributes.content").alias("content"))
    df_step2 = df_step1.select("*", explode("content").alias("metric"))

    # Nivel 3: Explotar values
    df_step3 = df_step2.select(..., col("metric.attributes.values").alias("values"))
    df_final = df_step3.select("*", explode("values").alias("val"))
```

**B. Widgets Estándar** (sin `content`)

```python
def process_standard_widget(df_raw, category, widget, time_trunc):
    # Nivel 1: Explotar series
    df_included = df_meta.select("*", explode("included").alias("series"))

    # Nivel 2: Explotar values directamente
    df_step1 = df_included.select(..., col("series.attributes.values").alias("values"))
    df_final = df_step1.select("*", explode("values").alias("val"))
```

#### Tablas Generadas

| Tabla Bronze                                                            | Descripción                             | Registros |
| ----------------------------------------------------------------------- | --------------------------------------- | --------- |
| `brz_redata_balance_balance_electrico_month`                            | Balance eléctrico con métricas anidadas | ~8,460    |
| `brz_redata_demanda_evolucion_month`                                    | Evolución de la demanda                 | ~640      |
| `brz_redata_generacion_estructura_generacion_month`                     | Generación por tecnología               | ~5,312    |
| `brz_redata_generacion_estructura_generacion_emisiones_asociadas_month` | Generación con emisiones                | ~4,672    |
| `brz_redata_generacion_estructura_renovables_month`                     | Desglose renovables                     | ~3,365    |
| `brz_redata_generacion_evolucion_renovable_no_renovable_month`          | Renovable vs no renovable               | ~1,263    |
| `brz_redata_metadata`                                                   | Metadatos de endpoints                  | ~6        |

#### Sistema de Auditoría

Tabla de log de procesamiento:

```python
# Estructura de brz_json_log
{
    "api_name": "generacion/estructura-generacion/month",
    "source_file": "Files/bronze/.../brz-andalucia-generacion-...-month-2025-10-15...json",
    "target_table": "brz_redata_generacion_estructura_generacion_month",
    "ingestion_date": "2025-10-15T12:45:30"
}
```

Esto evita reprocesar archivos ya transformados.

---

### Notebook: `nb_redata_04_brz_to_slv.ipynb` (Bronze → Silver)

#### Objetivo

Aplicar transformaciones avanzadas de calidad de datos, incluyendo deduplicación, normalización de IDs y optimización de esquema.

#### Configuración de Claves de Negocio

El notebook carga una configuración que define las **claves de unicidad** (business keys) para cada tipo de tabla:

```json
{
  "business_keys": {
    "balance_balance_electrico": {
      "keys": ["geo_id", "datetime", "series_type", "metric_type"],
      "description": "Balance eléctrico con métricas anidadas"
    },
    "demanda_evolucion": {
      "keys": ["geo_id", "datetime", "series_type"],
      "description": "Evolución de la demanda eléctrica"
    },
    "generacion_estructura_generacion": {
      "keys": ["geo_id", "datetime", "series_type"],
      "description": "Estructura de generación por tecnología"
    }
  }
}
```

#### Transformaciones Aplicadas

##### 1. Deduplicación por Claves de Negocio

**⚠️ PROBLEMA**: Múltiples registros para la misma combinación (geo_id, datetime, series_type) debido a reingestas con diferentes timestamps de captura.

**✅ SOLUCIÓN**:

```python
def deduplicate_by_business_keys(df, business_keys: list):
    """
    Deduplica usando SOLO claves de negocio.
    Mantiene el registro con ingestion_timestamp más reciente.
    """
    window = Window.partitionBy(business_keys).orderBy(desc("ingestion_timestamp"))

    df_deduped = df.withColumn("_row_num", row_number().over(window)) \
                   .filter(col("_row_num") == 1) \
                   .drop("_row_num")

    return df_deduped
```

**Resultado**: Se eliminaron 720 registros duplicados (3.0% del total de 23,712).

##### 2. Normalización de IDs

Algunos IDs actúan como placeholders redundantes:

```python
def normalize_ids(df):
    """Normaliza IDs: NULL si son placeholders"""
    if 'metric_id' in df.columns and 'metric_group_id' in df.columns:
        df = df.withColumn("metric_id",
            when(col("metric_id") == col("metric_group_id"), None)
            .otherwise(col("metric_id")))

    return df
```

##### 3. Eliminación de Columnas

**Columnas eliminadas automáticamente**:

1. **Decorativas** (sin valor analítico):

   - `metric_icon`, `metric_color`
   - `series_icon`, `series_color`

2. **Columnas con >95% nulos** (umbral configurable)

3. **Redundantes** (detectadas mediante análisis de cardinalidad):
   - Si `series_type` tiene 1:1 con `series_title`, se elimina `series_title`
   - Si `metric_type` tiene 1:1 con `metric_title`, se elimina `metric_title`

```python
def get_columns_to_drop(df):
    to_drop = []

    # 1. Decorativas
    to_drop.extend(["metric_icon", "metric_color", "series_icon", "series_color"])

    # 2. Alto % nulos
    null_pct = calculate_null_percentage(df)
    to_drop.extend([c for c, p in null_pct.items() if p > 0.95])

    # 3. Redundantes (conservar _type)
    for keep, *check in [('series_type', 'series_title', 'series_id')]:
        # Análisis de cardinalidad...

    return list(set(to_drop))
```

##### 4. Adición de Columnas de Particionamiento

```python
def add_partition_columns(df):
    """Agrega year/month desde datetime"""
    return df.withColumn("year", year(col("datetime"))) \
             .withColumn("month", month(col("datetime")))
```

##### 5. Particionamiento Estratégico

```python
def should_partition(table_name, record_count):
    """Decide estrategia de particionamiento"""
    if '_month' in table_name and record_count > 500:
        return True, ["year"]
    return False, []
```

**Resultado**: Todas las tablas Silver se particionan por `year` para optimización de queries.

#### Validación de Calidad

Tras cada transformación, se validan:

- ✅ Ausencia de duplicados según claves de negocio
- ✅ Valores no nulos en campos críticos (`datetime`, `value`)
- ✅ Integridad referencial de claves foráneas

```python
# Verificación post-escritura
df_verify = spark.table(f"{LAKEHOUSE_SILVER}.{silver_table}")
verify_validation = validate_business_keys(df_verify, business_keys, silver_table)

if verify_validation["duplicate_groups"] > 0:
    print(f"⚠️ ADVERTENCIA: Aún hay duplicados")
else:
    print(f"✅ Verificación: Sin duplicados en Silver")
```

#### Tablas Silver Generadas

| Tabla Silver                                                            | Registros | Columnas | Duplicados Eliminados |
| ----------------------------------------------------------------------- | --------- | -------- | --------------------- |
| `slv_redata_balance_balance_electrico_month`                            | 8,202     | 17       | 258                   |
| `slv_redata_demanda_evolucion_month`                                    | 620       | 11       | 20                    |
| `slv_redata_generacion_estructura_generacion_month`                     | 5,152     | 13       | 160                   |
| `slv_redata_generacion_estructura_generacion_emisiones_asociadas_month` | 4,532     | 13       | 140                   |
| `slv_redata_generacion_estructura_renovables_month`                     | 3,262     | 14       | 103                   |
| `slv_redata_generacion_evolucion_renovable_no_renovable_month`          | 1,224     | 13       | 39                    |

**Reducción total**: 720 registros duplicados (3.0%)

---

## ⭐ Capa Gold - Modelo Dimensional

### Notebook: `nb_redata_06_create_dimensions.ipynb`

#### Objetivo

Crear dimensiones desnormalizadas que representan las entidades clave del dominio.

#### Dimensiones Creadas

##### 1. `dim_date` - Dimensión Temporal

```python
dim_date = df_dates.select(
    col("datetime").alias("date"),
    year("datetime").alias("year"),
    month("datetime").alias("month"),
    dayofmonth("datetime").alias("day"),
    quarter("datetime").alias("quarter"),
    (floor((month("datetime") - 1) / 6) + 1).cast("integer").alias("semester"),
    dayofweek("datetime").alias("day_of_week_num"),
    date_format("datetime", "EEEE").alias("day_of_week_name"),
    date_format("datetime", "MMMM").alias("month_name"),
    when(dayofweek("datetime").isin([1, 7]), True).otherwise(False).alias("is_weekend")
).distinct()

# Añadir clave surrogada
window = Window.orderBy("date")
dim_date = dim_date.withColumn("date_key", row_number().over(window))
```

**Atributos**:

- `date_key` (PK): Clave surrogada autoincremental
- `date`: Fecha completa (formato YYYY-MM-DD)
- `year`, `month`, `day`: Componentes temporales
- `quarter`, `semester`: Agregaciones fiscales
- `day_of_week_num`, `day_of_week_name`: Día de la semana
- `month_name`: Nombre del mes
- `is_weekend`: Flag booleano

**Registros**: 34 meses (enero 2023 - junio 2025)

##### 2. `dim_geography` - Dimensión Geográfica

```python
df_geo = spark.table(f"{LAKEHOUSE_SILVER}.slv_redata_generacion_estructura_generacion_month") \
    .select("geo_id", "geo_name") \
    .distinct()

window = Window.orderBy("geo_id")
dim_geography = df_geo.withColumn("geography_key", row_number().over(window))
```

**Atributos**:

- `geography_key` (PK): Clave surrogada
- `geo_id`: ID oficial de REE
- `geo_name`: Nombre de la comunidad autónoma

**Registros**: 20 comunidades autónomas

**Ejemplo de datos**:

```
+-------------+------+----------------------+
|geography_key|geo_id|geo_name              |
+-------------+------+----------------------+
|1            |4     |Andalucía             |
|2            |5     |Aragón                |
|3            |6     |Cantabria             |
|4            |7     |Castilla-La Mancha    |
|5            |8     |Castilla y León       |
|6            |9     |Cataluña              |
|7            |10    |País Vasco            |
|8            |11    |Principado de Asturias|
|9            |13    |Comunidad de Madrid   |
|10           |14    |Comunidad de Navarra  |
+-------------+------+----------------------+
```

##### 3. `dim_technology` - Dimensión Tecnológica

**Fuentes de datos**:

1. `slv_redata_generacion_estructura_generacion_month` → categorías
2. `slv_redata_generacion_estructura_generacion_emisiones_asociadas_month` → flag de emisiones CO₂

```python
# Paso 1: Obtener categorías
df_base = spark.table("slv_redata_generacion_estructura_generacion_month") \
    .select("series_type", "series_attribute_type") \
    .distinct()

# Paso 2: Obtener emisiones
df_emissions = spark.table("slv_redata_generacion_estructura_generacion_emisiones_asociadas_month") \
    .select("series_type", col("series_attribute_type").alias("emissions_attr")) \
    .distinct()

# Paso 3: JOIN para combinar
dim_technology = df_base.join(df_emissions, "series_type", "left")

# Paso 4: Crear flag de emisiones
dim_technology = dim_technology.withColumn(
    "has_co2_emissions",
    when(col("emissions_attr") == "Con emisiones de CO2 eq.", True)
    .when(col("emissions_attr") == "Sin emisiones de CO2 eq.", False)
    .otherwise(None)
)
```

**Atributos**:

- `technology_key` (PK): Clave surrogada
- `series_type`: Nombre de la tecnología (ej: "Eólica", "Nuclear", "Solar fotovoltaica")
- `category`: Clasificación (`Renovable`, `No-Renovable`, `Generación total`)
- `has_co2_emissions`: Flag booleano (TRUE/FALSE/NULL)

**Registros**: 18 tecnologías

**Distribución por categoría**:

```
+----------------+-----+
|        category|count|
+----------------+-----+
|Generación total|    1|
|    No-Renovable|    9|
|       Renovable|    8|
+----------------+-----+
```

**Tecnologías CON emisiones** (has_co2_emissions = TRUE):

- Carbón
- Ciclo combinado
- Cogeneración
- Fuel + Gas
- Motores diésel
- Residuos no renovables
- Turbina de gas
- Turbina de vapor

**Tecnologías SIN emisiones** (has_co2_emissions = FALSE):

- Eólica
- Hidroeólica
- Hidráulica
- Nuclear
- Otras renovables
- Residuos renovables
- Solar fotovoltaica
- Solar térmica

---

### Notebook: `nb_redata_07_create_facts.ipynb`

#### Objetivo

Crear tablas de hechos que contienen las métricas cuantitativas con referencias a las dimensiones.

#### Tabla de Hechos: `fact_generation_month`

```python
# Leer datos de generación desde Silver
df_gen_month = spark.table("slv_redata_generacion_estructura_generacion_month") \
    .select("datetime", "geo_id", "series_type", "value", "percentage")

# Join con dimensiones
fact_gen_month = df_gen_month \
    .join(dim_date, df_gen_month.datetime == dim_date.date, "left") \
    .join(dim_geography, df_gen_month.geo_id == dim_geography.geo_id, "left") \
    .join(dim_technology, df_gen_month.series_type == dim_technology.series_type, "left") \
    .select(
        col("date_key"),
        col("geography_key"),
        col("technology_key"),
        col("value").alias("generation_mwh"),
        col("percentage").alias("generation_percentage"),
        year("datetime").alias("year")
    )
```

**Atributos**:

- `date_key` (FK → `dim_date`)
- `geography_key` (FK → `dim_geography`)
- `technology_key` (FK → `dim_technology`)
- `generation_mwh`: Generación en megavatios-hora
- `generation_percentage`: Porcentaje sobre el total
- `year`: Columna de particionamiento

**Registros**: 5,559

**Particionamiento**: `year` (optimización de queries)

**Validación de Integridad Referencial**:

```python
nulls_date = fact_gen_month.filter(col("date_key").isNull()).count()
nulls_geo = fact_gen_month.filter(col("geography_key").isNull()).count()
nulls_tech = fact_gen_month.filter(col("technology_key").isNull()).count()

# Resultado esperado: 0 nulls en todas las claves foráneas
```

---

## 📊 Modelo Dimensional Final (Star Schema)

```
                    ┌──────────────┐
                    │   dim_date   │
                    ├──────────────┤
                    │ date_key (PK)│
                    │ date         │
                    │ year         │
                    │ month        │
                    │ quarter      │
                    │ is_weekend   │
                    │ ...          │
                    └──────┬───────┘
                           │
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
┌───────▼────────┐  ┌──────▼──────────────┐  ┌▼─────────────────┐
│ dim_geography  │  │ fact_generation_    │  │ dim_technology   │
├────────────────┤  │       month         │  ├──────────────────┤
│geography_key(PK)│◄─┤─────────────────────┤─►│technology_key(PK)│
│ geo_id         │  │ date_key (FK)       │  │ series_type      │
│ geo_name       │  │ geography_key (FK)  │  │ category         │
└────────────────┘  │ technology_key (FK) │  │ has_co2_emissions│
                    │ generation_mwh      │  └──────────────────┘
                    │ generation_percentage│
                    │ year (partition)    │
                    └─────────────────────┘
```

**Cardinalidades**:

- `dim_date`: 34 registros (34 meses)
- `dim_geography`: 20 registros (20 CCAA)
- `dim_technology`: 18 registros (18 tecnologías)
- `fact_generation_month`: 5,559 registros

**Granularidad**: Mensual por comunidad autónoma y tecnología

---

## 🔍 Análisis Exploratorio de Datos (EDA)

Tras cada capa de transformación, se ejecutaron análisis exploratorios automatizados:

### Post-Bronze

- Verificación de completitud de respuestas de la API
- Análisis de estructura y anidamiento de JSONs
- Cobertura temporal y geográfica
- Identificación de campos disponibles

### Post-Silver

- Distribución estadística de variables numéricas
- Detección de outliers y valores anómalos
- Verificación de unicidad según claves de negocio
- Análisis de series temporales (detección de gaps)
- Matrices de correlación entre variables

### Post-Gold

- Validación de integridad referencial (0 nulls en FKs)
- Comprobación de cardinalidades esperadas
- Coherencia de agregaciones (sumas por dimensiones = totales)

---

## 📈 Métricas de Calidad del Pipeline

| Métrica                               | Valor                          |
| ------------------------------------- | ------------------------------ |
| **Registros originales (Bronze)**     | 23,712                         |
| **Registros finales (Silver)**        | 22,992                         |
| **Duplicados eliminados**             | 720 (3.0%)                     |
| **Tablas Bronze**                     | 6 + 1 metadata                 |
| **Tablas Silver**                     | 6 + 1 metadata                 |
| **Dimensiones Gold**                  | 3                              |
| **Tablas de Hechos Gold**             | 1                              |
| **Cobertura temporal**                | 34 meses (ene 2023 - jun 2025) |
| **Cobertura geográfica**              | 20 comunidades autónomas       |
| **Tecnologías catalogadas**           | 18                             |
| **Validación integridad referencial** | 100% (0 nulls en FKs)          |
| **Tablas particionadas**              | 100% de Silver y Gold          |

---

## 🚀 Ejecución del Pipeline Completo

### Orden de Ejecución

1. **Ingesta Bronze**:

   ```python
   # Ejecutar: nb_redata_01_ingest.ipynb
   # Output: ~120 archivos JSON en Files/bronze/REDATA/data/
   ```

2. **Transformación Bronze → Delta**:

   ```python
   # Ejecutar: nb_redata_02_json_to_delta.ipynb
   # Output: 7 tablas Delta en lh_bronze
   ```

3. **Limpieza Silver**:

   ```python
   # Ejecutar: nb_redata_04_brz_to_slv.ipynb
   # Output: 7 tablas optimizadas en lh_silver
   ```

4. **Creación de Dimensiones Gold**:

   ```python
   # Ejecutar: nb_redata_06_create_dimensions.ipynb
   # Output: 3 tablas de dimensiones en lh_golden
   ```

5. **Creación de Hechos Gold**:
   ```python
   # Ejecutar: nb_redata_07_create_facts.ipynb
   # Output: 1 tabla de hechos en lh_golden
   ```

### Tiempos de Ejecución Estimados

| Notebook                         | Duración Aproximada |
| -------------------------------- | ------------------- |
| `nb_redata_01_ingest`            | 10-15 min           |
| `nb_redata_02_json_to_delta`     | 5-8 min             |
| `nb_redata_04_brz_to_slv`        | 3-5 min             |
| `nb_redata_06_create_dimensions` | 1-2 min             |
| `nb_redata_07_create_facts`      | 1-2 min             |
| **TOTAL**                        | **20-32 min**       |

---

## 🛠️ Troubleshooting

### Problema: Duplicados persistentes tras Silver

**Síntoma**: La verificación post-escritura detecta duplicados.

**Causa**: Claves de negocio mal definidas para ese endpoint.

**Solución**: Revisar `redata_config.json` y ajustar las `business_keys` para esa tabla.

### Problema: Errores 408 (Timeout) en ingesta

**Síntoma**: Algunos endpoints devuelven timeout.

**Causa**: El rango temporal solicitado es demasiado amplio.

**Solución**: Reducir el rango en la llamada a `REDataAPIExplorer` (usar ventanas de 12 meses en lugar de 24).

### Problema: Fechas desplazadas 1-2 horas

**Síntoma**: Los datos de generación no coinciden con las horas esperadas.

**Causa**: Conversión errónea de zona horaria.

**Solución**: Verificar que se está usando `parse_datetime_local()` en `nb_redata_02_json_to_delta.ipynb`.

### Problema: Nulls en claves foráneas de Gold

**Síntoma**: `fact_generation_month` tiene nulls en `date_key`, `geography_key` o `technology_key`.

**Causa**: Datos en Silver que no tienen correspondencia en las dimensiones.

**Solución**:

1. Verificar que las dimensiones se crearon correctamente
2. Ejecutar queries de diagnóstico:
   ```sql
   -- Encontrar series_type sin correspondencia
   SELECT DISTINCT series_type
   FROM lh_silver.slv_redata_generacion_estructura_generacion_month
   WHERE series_type NOT IN (SELECT series_type FROM lh_golden.dim_technology)
   ```

---

## 📚 Referencias Adicionales

- [Documentación oficial API REData](https://www.ree.es/es/apidatos)
- [Arquitectura Medallion](./arquitectura-medallion.md)
- [Modelo Dimensional](./modelo-dimensional.md)
- [Problemas Técnicos y Soluciones](./problemas-tecnicos.md)

---

**Última actualización**: Octubre 2025  
**Mantenido por**: Equipo Enerlytics - Factoría F5
