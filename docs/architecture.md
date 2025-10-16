# Arquitectura Medallion en Enerlytics

## Introducción

La **Arquitectura Medallion** (también conocida como arquitectura de medallas o en capas) es un patrón de diseño ampliamente adoptado en entornos **Lakehouse** modernos. Organiza los datos en tres capas progresivas de refinamiento — **Bronze, Silver y Gold** — donde cada capa añade valor y calidad a los datos del sistema.

Este documento describe cómo Enerlytics implementa esta arquitectura para transformar datos crudos del sistema eléctrico español en información lista para análisis de negocio.

---

## 🎯 Principios Fundamentales

### 1. Separación de Responsabilidades

Cada capa tiene un propósito específico y bien definido:

- **Bronze**: Preservar la verdad inmutable
- **Silver**: Garantizar calidad y confiabilidad
- **Gold**: Optimizar para consumo de negocio

### 2. Incrementalidad

Los datos fluyen de forma unidireccional: Bronze → Silver → Gold. Cada capa puede actualizarse independientemente sin afectar a las anteriores.

### 3. Trazabilidad

Es posible rastrear cualquier dato en Gold hasta su origen en Bronze, garantizando reproducibilidad y auditoría completa.

### 4. Idempotencia

Las transformaciones pueden ejecutarse múltiples veces produciendo el mismo resultado, facilitando la corrección de errores.

### 5. Escalabilidad

La arquitectura soporta volúmenes crecientes de datos mediante particionamiento estratégico y formatos optimizados (Delta Lake).

---

## 📊 Las Tres Capas

```
┌─────────────────────────────────────────────────────────┐
│                     DATA SOURCES                        │
│        REData API  │  ESIOS API  │  Open-Meteo API      │
└────────────────────────┬────────────────────────────────┘
                         │
                         │ Ingesta sin transformaciones
                         ▼
┌─────────────────────────────────────────────────────────┐
│                    BRONZE LAYER                         │
│                   (Raw Data Lake)                       │
│                                                         │
│  • Datos crudos en JSON/Delta                          │
│  • Estructura original de la API preservada            │
│  • Metadata de captura enriquecida                     │
│  • Sistema de logging de peticiones                    │
│  • Fuente de verdad inmutable                          │
│                                                         │
│  Files/bronze/REDATA/data/                             │
│    ├── balance/balance-electrico/month/                │
│    ├── demanda/evolucion/month/                        │
│    └── generacion/.../month/                           │
└────────────────────────┬────────────────────────────────┘
                         │
                         │ Limpieza y normalización
                         ▼
┌─────────────────────────────────────────────────────────┐
│                    SILVER LAYER                         │
│                  (Curated Data Lake)                    │
│                                                         │
│  • Deduplicación mediante claves de negocio            │
│  • Corrección de zonas horarias                        │
│  • Normalización de IDs y nomenclatura                 │
│  • Eliminación de columnas decorativas                 │
│  • Validación de integridad de datos                   │
│  • Análisis exploratorio (EDA)                         │
│  • Tablas Delta particionadas                          │
│                                                         │
│  lh_silver.slv_redata_*_month                          │
└────────────────────────┬────────────────────────────────┘
                         │
                         │ Modelado dimensional
                         ▼
┌─────────────────────────────────────────────────────────┐
│                     GOLD LAYER                          │
│               (Business-Ready Analytics)                │
│                                                         │
│  • Modelo dimensional (star schema)                    │
│  • Dimensiones desnormalizadas                         │
│  • Tablas de hechos agregadas                          │
│  • Integridad referencial garantizada                  │
│  • Optimizado para queries de BI                       │
│  • Listo para Power BI                                 │
│                                                         │
│  lh_golden:                                            │
│    ├── dim_date                                        │
│    ├── dim_geography                                   │
│    ├── dim_technology                                  │
│    └── fact_generation_month                           │
└─────────────────────────────────────────────────────────┘
```

---

## 🥉 Capa Bronze - Raw Data Lake

### Propósito

Actuar como **registro histórico inmutable** de todos los datos obtenidos de fuentes externas, preservando su estructura original.

### Características

#### ✅ Inmutabilidad

- Los archivos Bronze **nunca** se modifican una vez escritos
- Si hay errores de ingesta, se corrigen reingestionando con nuevo timestamp
- Permite auditoría completa y reproducibilidad

#### ✅ Estructura Original

```json
{
  "request_metadata": {
    "geo_id": 4,
    "geo_name": "Andalucía",
    "category": "generacion",
    "widget": "estructura-generacion",
    "time_trunc": "month",
    "ingestion_timestamp": "2025-10-15T12:30:45.123456Z",
    "start_date": "2023-01-01T00:00:00",
    "end_date": "2025-06-30T23:59:59"
  },
  "api_response": {
    "included": [
      {
        "type": "Nuclear",
        "attributes": {
          "title": "Nuclear",
          "values": [
            { "datetime": "2023-01-01T00:00:00.000+01:00", "value": 12345.67 }
          ]
        }
      }
    ]
  }
}
```

Se preserva:

- La respuesta JSON completa de la API
- Metadata de contexto (qué se pidió, cuándo, con qué parámetros)
- Timestamps de captura para trazabilidad

#### ✅ Sistema de Logging

Cada petición a la API se registra en:

**Success Log** (`logs/success.log`):

```
2025-10-15T12:30:45.123456Z OK https://apidatos.ree.es/es/datos/generacion/...
2025-10-15T12:30:46.789012Z OK https://apidatos.ree.es/es/datos/demanda/...
```

**Error Log** (`logs/error.log`):

```
2025-10-15T12:31:00.555555Z FAIL https://apidatos.ree.es/es/datos/mercados/...
```

Esto permite:

- Diagnosticar problemas de conectividad
- Identificar endpoints problemáticos
- Reintentar peticiones fallidas de forma selectiva

### Organización de Archivos

```
Files/bronze/REDATA/
├── data/
│   ├── balance/
│   │   └── balance-electrico/
│   │       └── month/
│   │           ├── brz-andalucia-balance-balance-electrico-month-2025-10-15t12-30-45-123456z.json
│   │           ├── brz-aragon-balance-balance-electrico-month-2025-10-15t12-30-46-234567z.json
│   │           └── ...
│   ├── demanda/
│   │   └── evolucion/
│   │       └── month/
│   └── generacion/
│       ├── estructura-generacion/
│       │   └── month/
│       ├── estructura-generacion-emisiones-asociadas/
│       ├── estructura-renovables/
│       └── evolucion-renovable-no-renovable/
└── logs/
    ├── success.log
    └── error.log
```

**Nomenclatura de archivos**:

- `brz-{region}-{category}-{widget}-{time_trunc}-{timestamp}.json`
- El timestamp incluye microsegundos para evitar colisiones
- Caracteres especiales normalizados (slugify)

### Decisiones de Diseño en Bronze

#### 1. JSON vs. Delta

**Decisión**: Usar JSON para archivos individuales, luego consolidar en Delta.

**Razones**:

- JSON preserva estructura anidada original
- Facilita inspección manual y debugging
- Delta se usa después para queries eficientes

#### 2. Un Archivo por Petición

**Decisión**: Cada llamada a la API genera un archivo independiente.

**Razones**:

- Granularidad fina para reingesta selectiva
- Trazabilidad completa por petición
- Evita dependencias entre ingestas

#### 3. Metadata Enriquecida

**Decisión**: Envolver respuesta de API en objeto con metadata.

**Razones**:

- Conocer contexto de cada dato (qué se pidió, cuándo)
- Facilita diagnóstico de problemas
- Permite filtrado eficiente en Silver

### Escenarios de Uso

#### Reingestión Selectiva

Si se detecta un problema en los datos de octubre 2024:

```python
explorer = REDataAPIExplorer(
    base_lakehouse_path="Files/bronze/REDATA",
    start_date="2024-10-01T00:00:00Z",
    end_date="2024-10-31T23:59:59Z"
)
results = explorer.explore_all()
```

Los archivos antiguos se mantienen (inmutabilidad) y los nuevos se añaden con timestamp más reciente. Silver decidirá cuál conservar.

#### Auditoría de Fuente

¿Qué nos devolvió exactamente la API para Andalucía en marzo 2024?

```bash
cat Files/bronze/REDATA/data/generacion/.../brz-andalucia-generacion-...-2024-03-01...json
```

---

## 🥈 Capa Silver - Curated Data Lake

### Propósito

Transformar datos crudos en **datasets confiables y consistentes**, aplicando reglas de negocio, validaciones de calidad y normalizaciones.

### Características

#### ✅ Deduplicación Inteligente

**Problema**: Reingestas generan múltiples registros para la misma combinación (geo_id, datetime, series_type).

**Solución**: Deduplicación basada en **claves de negocio** (business keys).

```python
# Definición de claves únicas por tipo de tabla
business_keys = {
    "demanda_evolucion": ["geo_id", "datetime", "series_type"],
    "balance_balance_electrico": ["geo_id", "datetime", "series_type", "metric_type"]
}

# Deduplicación preservando registro más reciente
window = Window.partitionBy(business_keys).orderBy(desc("ingestion_timestamp"))
df_deduped = df.withColumn("_row_num", row_number().over(window)) \
               .filter(col("_row_num") == 1) \
               .drop("_row_num")
```

**Resultado**: Eliminación de 720 registros duplicados (3.0% del total).

#### ✅ Normalización Temporal

**Problema**: Fechas con offset UTC (`+02:00`, `+01:00`) se parseaban incorrectamente, desplazando datos hasta 2 horas.

**Impacto**: Un dato de "01-01-2024 00:00" se convertía en "31-12-2023 22:00", causando descuadres en agregaciones mensuales.

**Solución**: Función personalizada que preserva la hora local:

```python
def parse_datetime_local(datetime_str):
    """
    Entrada:  "2024-10-01T00:00:00.000+02:00"
    Salida:   "2024-10-01 00:00:00"

    Extrae solo YYYY-MM-DD HH:mm:ss, ignorando offset
    """
    return regexp_replace(
        datetime_str,
        r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}).*$",
        "$1"
    )

# Uso:
to_timestamp(parse_datetime_local(col("val.datetime"))).alias("datetime")
```

**Consecuencia**: Reingesta completa del histórico para garantizar coherencia temporal en todo el dataset.

#### ✅ Optimización de Esquema

**Columnas eliminadas automáticamente**:

1. **Decorativas** (sin valor analítico):

   - `metric_icon`, `metric_color`, `series_icon`, `series_color`
   - No aportan información cuantitativa ni categórica útil

2. **Alto % de nulos** (>95%):

   - Columnas con casi todos los valores NULL
   - Excepción: `ccaa_name` se conserva aunque tenga nulos (valor semántico)

3. **Redundantes** (detectadas por análisis de cardinalidad):
   - Si `series_type` ↔ `series_title` tienen relación 1:1, eliminar `series_title`
   - Conservar siempre el campo `_type` (más conciso), eliminar `_title` y `_id`

**Ejemplo**:

```python
# Análisis de cardinalidad
sample = df.select("series_type", "series_title", "series_id").limit(100)

distinct_type_title = sample.select("series_type", "series_title").distinct().count()
distinct_type_only = sample.select("series_type").distinct().count()

if distinct_type_title <= distinct_type_only * 1.1:  # Tolerancia del 10%
    # series_title es redundante, eliminar
    to_drop.append("series_title")
```

#### ✅ Normalización de Identificadores

Algunos IDs actúan como placeholders sin valor informativo:

```python
# Ejemplo: metric_id == metric_group_id → el ID no aporta distinción
df = df.withColumn("metric_id",
    when(col("metric_id") == col("metric_group_id"), None)
    .otherwise(col("metric_id")))
```

#### ✅ Particionamiento Estratégico

```python
# Añadir columnas de partición
df = df.withColumn("year", year(col("datetime"))) \
       .withColumn("month", month(col("datetime")))

# Escribir con particionamiento si el volumen lo justifica
if record_count > 500:
    df.write.partitionBy("year").saveAsTable(table_name)
```

**Beneficios**:

- Queries filtradas por año se ejecutan más rápido
- Partición física en storage para optimización I/O
- Facilita eliminación selectiva de datos antiguos

### Análisis Exploratorio de Datos (EDA)

Tras cada transformación Silver, se ejecutan automáticamente:

```python
# 1. Distribuciones estadísticas
df.select("value", "percentage").describe().show()

# 2. Detección de outliers
mean_val = df.select(mean("value")).first()[0]
std_val = df.select(stddev("value")).first()[0]
outliers = df.filter((col("value") > mean_val + 3*std_val) |
                     (col("value") < mean_val - 3*std_val))

# 3. Análisis de series temporales
df.groupBy(year("datetime"), month("datetime")) \
  .agg(count("*"), sum("value")) \
  .orderBy("year", "month") \
  .show()

# 4. Verificación de claves de negocio
duplicates = df.groupBy(business_keys).agg(count("*").alias("dup_count")) \
               .filter(col("dup_count") > 1)
```

### Tablas Silver Generadas

| Tabla                                                                   | Registros | Columnas | Reducción |
| ----------------------------------------------------------------------- | --------- | -------- | --------- |
| `slv_redata_balance_balance_electrico_month`                            | 8,202     | 17       | -258      |
| `slv_redata_demanda_evolucion_month`                                    | 620       | 11       | -20       |
| `slv_redata_generacion_estructura_generacion_month`                     | 5,152     | 13       | -160      |
| `slv_redata_generacion_estructura_generacion_emisiones_asociadas_month` | 4,532     | 13       | -140      |
| `slv_redata_generacion_estructura_renovables_month`                     | 3,262     | 14       | -103      |
| `slv_redata_generacion_evolucion_renovable_no_renovable_month`          | 1,224     | 13       | -39       |

**Mejoras cualitativas**:

- ✅ Sin duplicados según claves de negocio
- ✅ Fechas corregidas y consistentes
- ✅ Esquema optimizado (eliminadas 5-8 columnas por tabla)
- ✅ Valores nulos validados
- ✅ Particionamiento por año implementado

### Validación de Calidad

```python
def validate_business_keys(df, business_keys, table_name):
    """Valida ausencia de duplicados y calcula métricas"""
    duplicates = df.groupBy(business_keys).agg(count("*").alias("dup_count"))
    dup_groups = duplicates.filter(col("dup_count") > 1).count()

    return {
        "valid": dup_groups == 0,
        "duplicate_groups": dup_groups,
        "total_records": df.count()
    }

# Ejecutar tras escritura
df_verify = spark.table(f"lh_silver.{silver_table}")
validation = validate_business_keys(df_verify, business_keys, silver_table)

if not validation["valid"]:
    raise Exception(f"❌ Validación fallida: {validation['duplicate_groups']} duplicados")
```

---

## 🥇 Capa Gold - Business-Ready Analytics

### Propósito

Crear un **modelo dimensional optimizado** (star schema) que responde directamente a preguntas de negocio, con performance optimizado para herramientas de BI.

### Características

#### ✅ Modelo Estrella (Star Schema)

```
       dim_date
          │
          │
   ┌──────┼──────┐
   │      │      │
dim_geo   │   dim_tech
   │      │      │
   └──────┼──────┘
          │
    fact_generation
```

**Ventajas del Star Schema**:

- Queries simples y eficientes (pocos JOINs)
- Fácil de entender para usuarios de negocio
- Optimizado para agregaciones
- Compatible con herramientas OLAP (Power BI, Tableau)

#### ✅ Dimensiones Desnormalizadas

##### dim_date (Dimensión Temporal)

```sql
SELECT * FROM lh_golden.dim_date LIMIT 3;

+----------+------------+------+-------+-----+---------+----------+
| date_key | date       | year | month | day | quarter | semester |
+----------+------------+------+-------+-----+---------+----------+
| 1        | 2023-01-01 | 2023 | 1     | 1   | 1       | 1        |
| 2        | 2023-02-01 | 2023 | 2     | 1   | 1       | 1        |
| 3        | 2023-03-01 | 2023 | 3     | 1   | 1       | 1        |
+----------+------------+------+-------+-----+---------+----------+
```

**Atributos adicionales**:

- `day_of_week_num`, `day_of_week_name`
- `month_name`
- `is_weekend` (booleano)

**Uso en queries**:

```sql
-- Generación total en fines de semana de 2024
SELECT SUM(generation_mwh)
FROM fact_generation_month f
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.year = 2024 AND d.is_weekend = TRUE;
```

##### dim_geography (Dimensión Geográfica)

```sql
SELECT * FROM lh_golden.dim_geography WHERE geography_key <= 5;

+---------------+--------+--------------------+
| geography_key | geo_id | geo_name           |
+---------------+--------+--------------------+
| 1             | 4      | Andalucía          |
| 2             | 5      | Aragón             |
| 3             | 6      | Cantabria          |
| 4             | 7      | Castilla-La Mancha |
| 5             | 8      | Castilla y León    |
+---------------+--------+--------------------+
```

**Uso en queries**:

```sql
-- Top 5 comunidades por generación renovable
SELECT g.geo_name, SUM(f.generation_mwh) as total_mwh
FROM fact_generation_month f
JOIN dim_geography g ON f.geography_key = g.geography_key
JOIN dim_technology t ON f.technology_key = t.technology_key
WHERE t.category = 'Renovable'
GROUP BY g.geo_name
ORDER BY total_mwh DESC
LIMIT 5;
```

##### dim_technology (Dimensión Tecnológica)

```sql
SELECT * FROM lh_golden.dim_technology WHERE has_co2_emissions = TRUE LIMIT 5;

+----------------+------------------+-------------+-------------------+
| technology_key | series_type      | category    | has_co2_emissions |
+----------------+------------------+-------------+-------------------+
| 1              | Carbón           | No-Renovable| true              |
| 2              | Ciclo combinado  | No-Renovable| true              |
| 3              | Cogeneración     | No-Renovable| true              |
| 4              | Fuel + Gas       | No-Renovable| true              |
| 5              | Motores diésel   | No-Renovable| true              |
+----------------+------------------+-------------+-------------------+
```

**Enriquecimiento**:

- Combina datos de dos tablas Silver:
  1. `estructura-generacion` → categorías (Renovable/No-Renovable)
  2. `estructura-generacion-emisiones-asociadas` → flag de emisiones CO₂

**Uso en queries**:

```sql
-- Porcentaje de generación con emisiones CO₂
SELECT
    CASE WHEN has_co2_emissions THEN 'Con emisiones' ELSE 'Sin emisiones' END as tipo,
    SUM(generation_mwh) as total_mwh,
    ROUND(100.0 * SUM(generation_mwh) / SUM(SUM(generation_mwh)) OVER (), 2) as percentage
FROM fact_generation_month f
JOIN dim_technology t ON f.technology_key = t.technology_key
GROUP BY has_co2_emissions;
```

#### ✅ Tabla de Hechos

##### fact_generation_month

```sql
SELECT * FROM lh_golden.fact_generation_month LIMIT 5;

+----------+---------------+----------------+----------------+----------------------+------+
| date_key | geography_key | technology_key | generation_mwh | generation_percentage| year |
+----------+---------------+----------------+----------------+----------------------+------+
| 1        | 1             | 5              | 12345.67       | 8.5                  | 2023 |
| 1        | 1             | 7              | 23456.78       | 16.2                 | 2023 |
| 1        | 2             | 5              | 11223.45       | 9.1                  | 2023 |
| 2        | 1             | 5              | 12567.89       | 8.7                  | 2023 |
| 2        | 1             | 7              | 22890.12       | 15.9                 | 2023 |
+----------+---------------+----------------+----------------+----------------------+------+
```

**Granularidad**: Mensual por comunidad autónoma y tecnología

**Métricas**:

- `generation_mwh`: Generación absoluta en megavatios-hora
- `generation_percentage`: Porcentaje sobre el total de esa comunidad en ese mes

**Particionamiento**: Por `year` para optimización de queries temporales

#### ✅ Integridad Referencial

```python
# Validación automática tras creación
nulls_date = fact_gen.filter(col("date_key").isNull()).count()
nulls_geo = fact_gen.filter(col("geography_key").isNull()).count()
nulls_tech = fact_gen.filter(col("technology_key").isNull()).count()

assert nulls_date == 0, "❌ Hay nulls en date_key"
assert nulls_geo == 0, "❌ Hay nulls en geography_key"
assert nulls_tech == 0, "❌ Hay nulls en technology_key"

print("✅ Integridad referencial verificada: 0 nulls en FKs")
```

**Resultado en Enerlytics**: 100% de integridad (0 nulls en claves foráneas)

### Queries de Negocio Típicas

#### 1. Evolución de Renovables vs. No Renovables

```sql
SELECT
    d.year,
    d.month,
    t.category,
    SUM(f.generation_mwh) as total_mwh
FROM fact_generation_month f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_technology t ON f.technology_key = t.technology_key
WHERE t.category IN ('Renovable', 'No-Renovable')
GROUP BY d.year, d.month, t.category
ORDER BY d.year, d.month, t.category;
```

#### 2. Mix Energético por Comunidad Autónoma

```sql
SELECT
    g.geo_name,
    t.series_type,
    SUM(f.generation_mwh) as total_mwh,
    ROUND(100.0 * SUM(f.generation_mwh) / SUM(SUM(f.generation_mwh)) OVER (PARTITION BY g.geo_name), 2) as pct
FROM fact_generation_month f
JOIN dim_geography g ON f.geography_key = g.geography_key
JOIN dim_technology t ON f.technology_key = t.technology_key
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.year = 2024
GROUP BY g.geo_name, t.series_type
ORDER BY g.geo_name, total_mwh DESC;
```

#### 3. Generación con Emisiones CO₂ por Trimestre

```sql
SELECT
    d.year,
    d.quarter,
    SUM(CASE WHEN t.has_co2_emissions THEN f.generation_mwh ELSE 0 END) as emisiones_mwh,
    SUM(CASE WHEN NOT t.has_co2_emissions THEN f.generation_mwh ELSE 0 END) as sin_emisiones_mwh
FROM fact_generation_month f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_technology t ON f.technology_key = t.technology_key
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter;
```

### Optimizaciones para Power BI

#### 1. Relaciones Automáticas

Power BI detecta automáticamente las relaciones entre tablas basándose en nombres de columnas:

- `fact_generation_month[date_key]` → `dim_date[date_key]`
- `fact_generation_month[geography_key]` → `dim_geography[geography_key]`
- `fact_generation_month[technology_key]` → `dim_technology[technology_key]`

#### 2. Jerarquías Temporales

En Power BI, crear jerarquía en `dim_date`:

```
Fecha
  └── Año
      └── Trimestre
          └── Mes
```

#### 3. Medidas DAX Precalculadas

```dax
// Generación Total
Total Generación = SUM(fact_generation_month[generation_mwh])

// Porcentaje Renovable
% Renovable =
DIVIDE(
    CALCULATE(
        SUM(fact_generation_month[generation_mwh]),
        dim_technology[category] = "Renovable"
    ),
    SUM(fact_generation_month[generation_mwh]),
    0
)

// Generación Mes Anterior
Generación Mes Anterior =
CALCULATE(
    SUM(fact_generation_month[generation_mwh]),
    DATEADD(dim_date[date], -1, MONTH)
)

// Variación Intermensual
Variación MoM =
DIVIDE(
    [Total Generación] - [Generación Mes Anterior],
    [Generación Mes Anterior],
    0
)
```

---

## 🔄 Flujo de Datos Completo

### Ejemplo: Tracking de un Registro

Seguimos un registro de generación eólica en Andalucía para marzo de 2024:

#### Bronze

```json
{
  "request_metadata": {
    "geo_id": 4,
    "geo_name": "Andalucía",
    "ingestion_timestamp": "2025-10-15T12:30:45.123456Z"
  },
  "api_response": {
    "included": [
      {
        "type": "Eólica",
        "attributes": {
          "values": [
            {
              "datetime": "2024-03-01T00:00:00.000+01:00",
              "value": 1234567.89,
              "percentage": 22.5
            }
          ]
        }
      }
    ]
  }
}
```

#### Silver (tras transformaciones)

```sql
SELECT * FROM lh_silver.slv_redata_generacion_estructura_generacion_month
WHERE geo_id = 4 AND datetime = '2024-03-01' AND series_type = 'Eólica';

+--------+------------+--------------+----------+------------+------+-------+
| geo_id | geo_name   | series_type  | datetime | value      | year | month |
+--------+------------+--------------+----------+------------+------+-------+
| 4      | Andalucía  | Eólica       | 2024-03-01| 1234567.89| 2024 | 3     |
+--------+------------+--------------+----------+------------+------+-------+
```

#### Gold (modelo dimensional)

```sql
SELECT
    d.date,
    g.geo_name,
    t.series_type,
    t.category,
    t.has_co2_emissions,
    f.generation_mwh
FROM lh_golden.fact_generation_month f
JOIN lh_golden.dim_date d ON f.date_key = d.date_key
JOIN lh_golden.dim_geography g ON f.geography_key = g.geography_key
JOIN lh_golden.dim_technology t ON f.technology_key = t.technology_key
WHERE d.date = '2024-03-01' AND g.geo_id = 4 AND t.series_type = 'Eólica';

+------------+-----------+-------------+-----------+-------------------+----------------+
| date       | geo_name  | series_type | category  | has_co2_emissions | generation_mwh |
+------------+-----------+-------------+-----------+-------------------+----------------+
| 2024-03-01 | Andalucía | Eólica      | Renovable | false             | 1234567.89     |
+------------+-----------+-------------+-----------+-------------------+----------------+
```

---

## 📊 Beneficios de la Arquitectura Medallion en Enerlytics

### 1. Trazabilidad Completa

Cualquier valor en Gold puede rastrearse hasta el JSON original en Bronze:

```python
# Encontrar origen de un valor específico
bronze_file = "Files/bronze/REDATA/data/generacion/.../brz-andalucia-...-2024-03-01...json"
# Contiene el JSON completo de la respuesta de la API
```

### 2. Recuperación ante Errores

Si se detecta un error en Silver:

- No afecta a Bronze (inmutable)
- Se corrige la transformación
- Se re-ejecuta Bronze → Silver sin reingestión

Si se detecta un error en Gold:

- No afecta a Bronze ni Silver
- Se corrige el modelado dimensional
- Se re-ejecuta Silver → Gold rápidamente

### 3. Flexibilidad en Modelado

El mismo Silver puede alimentar múltiples modelos Gold:

- Gold para análisis mensual (actual)
- Gold para análisis diario (futuro)
- Gold para análisis horario (futuro con ESIOS)

### 4. Optimización Incremental

Cada capa puede optimizarse independientemente:

- Bronze: Optimización de ingesta (paralelización)
- Silver: Optimización de transformaciones (caching, broadcast joins)
- Gold: Optimización de queries (índices, particionamiento avanzado)

### 5. Separación de Responsabilidades

- **Data Engineers**: Gestionan Bronze y Silver
- **Data Analysts**: Gestionan Gold y dashboards
- **Data Scientists**: Pueden trabajar en cualquier capa según necesidad

---

## 🛠️ Herramientas Específicas en Microsoft Fabric

### Delta Lake

Formato de almacenamiento usado en todas las capas:

- **Transacciones ACID**: Garantiza consistencia
- **Time Travel**: Acceso a versiones históricas de tablas
- **Merge/Update/Delete**: Operaciones avanzadas sobre datos

```python
# Ejemplo de time travel
df_v1 = spark.read.format("delta").option("versionAsOf", 1).table("slv_redata_generacion_...")
df_v2 = spark.read.format("delta").option("versionAsOf", 2).table("slv_redata_generacion_...")
```

### Lakehouse (OneLake)

- Almacenamiento unificado para todas las capas
- Acceso vía Spark, SQL, Power BI
- Integración nativa con Azure Data Lake Storage Gen2

### PySpark en Notebooks

- Transformaciones distribuidas para grandes volúmenes
- Integración con Pandas para operaciones locales
- Ejecución interactiva para prototipado rápido

---

## 📚 Referencias

- [Delta Lake Documentation](https://docs.delta.io/)
- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Microsoft Fabric Lakehouse](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-overview)

---

**Última actualización**: Octubre 2025  
**Mantenido por**: Equipo Enerlytics - Factoría F5
