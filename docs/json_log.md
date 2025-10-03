# JSON Log Manager (PySpark + Delta)

This module provides utility functions to manage JSON ingestion logs in a **medallion architecture** (Bronze/Silver/Gold) using **PySpark** and **Delta tables**.  

It is designed to:
- Track source files ingested into Delta tables.  
- Prevent duplicate ingestion by checking if a log already exists.  
- Write ingestion metadata into a centralized JSON log table.  

---

## 📂 Features

- **Open Spark Session** with Fabric Lakehouse context.  
- **Check if log exists** (`check_json_log_exists`) to avoid re-ingesting the same file/API.  
- **Save ingestion log** (`save_json_log`) with schema enforcement.  
- **Write Delta tables** safely, with support for `append` and `overwrite`.  

---

## ⚙️ Functions

### `spark_open_session(lakehouse_name: str) -> SparkSession`
Opens a Spark session configured with a target Fabric Lakehouse.  

### `spark_write_table(df, table_name: str, write_mode: str = "append")`
Writes a DataFrame to a Delta table.  
- Supports `append` (default) and `overwrite`.  
- Uses `overwriteSchema` only if overwrite mode is enabled.  

### `check_json_log_exists(lakehouse_name: str, medallion_short: str, log_table: str, source_file: str, api_name: str = None) -> bool`
Checks if a given `source_file` (optionally filtered by `api_name`) already exists in the JSON log table.  

### `save_json_log(api_name: str, source_file: str, target_table: str, ingestion_date: datetime, write_mode: str = "append", lakehouse_name: str = "lh_bronze", medallion_short: str = "brz")`
Creates a new log entry for a file/API ingestion:  
- Prevents duplicates.  
- Writes a structured row into the log table `{medallion_short}_json_log`.  

---

## 🏗️ Schema

The JSON log table follows this schema:

| Column           | Type      | Description                           |
|------------------|-----------|---------------------------------------|
| `api_name`       | String    | API name or source identifier         |
| `source_file`    | String    | ABFS path of ingested file.           |
| `target_table`   | String    | Destination table for ingested data   |
| `ingestion_date` | Timestamp | Timestamp of ingestion event          |

---

## 🚀 Usage Example

```python
import datetime
from json_log_manager import check_json_log_exists, save_json_log

lakehouse = "lh_bronze"
medallion_short = "brz"
log_table = f"{medallion_short}_json_log"
source_file = "file=abfss://ecf[...].json", # Use Azure  ABFS path
api_name = "REDATA"
target_table="brz_example_data"

# 1. Check if log already exists
exists = check_json_log_exists(
    lakehouse_name=lakehouse,
    medallion_short=medallion_short,
    log_table=log_table,
    source_file=source_file,
    api_name=api_name
)

if not exists:
    # 2. Save new log entry
    save_json_log(
        api_name=api_name,
        source_file=source_file,
        target_table=target_table,
        ingestion_date=datetime.datetime.now(),
        lakehouse_name=lakehouse,
        medallion_short=medallion_short
    )
```