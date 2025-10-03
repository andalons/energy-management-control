"""
OMIE Change Detection and Delta Table Management
===============================================

This module provides utilities for:
1. Change detection and file tracking
2. Delta table creation and schema management
3. Data deduplication and validation
4. File checksum and metadata tracking

Used by both daily and monthly pipelines for consistent data management.
"""

import os
import json
import hashlib
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
import pandas as pd
import requests
from urllib.parse import urljoin

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit, current_timestamp, hash as spark_hash
    from pyspark.sql.types import *
    from delta import DeltaTable
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


class OMIEChangeDetector:
    """Handles change detection and file tracking for OMIE data"""
    
    def __init__(self, config: Dict, spark_session: Optional[SparkSession] = None):
        self.config = config
        self.spark = spark_session
        self.metadata_dir = Path(config['lakehouse_root']) / 'bronze' / 'OMIE' / 'delta_tables' / 'metadata'
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        
        # File tracking cache
        self._processed_files_cache = None
        self._last_update_time = None
    
    def calculate_file_hash(self, content: bytes) -> str:
        """Calculate SHA256 hash of file content"""
        return hashlib.sha256(content).hexdigest()
    
    def extract_file_metadata(self, filename: str, url: str, content: bytes) -> Dict:
        """Extract comprehensive metadata from file"""
        metadata = {
            'filename': filename,
            'url': url,
            'content_hash': self.calculate_file_hash(content),
            'file_size': len(content),
            'discovered_at': datetime.now().isoformat()
        }
        
        # Extract date from filename
        date_match = re.search(r'(\d{8})', filename)
        if date_match:
            date_str = date_match.group(1)
            metadata['extraction_date'] = date_str
            metadata['extraction_year'] = int(date_str[:4])
            metadata['extraction_month'] = int(date_str[4:6])
        else:
            metadata['extraction_date'] = None
            metadata['extraction_year'] = None
            metadata['extraction_month'] = None
        
        # File type detection
        file_ext = Path(filename).suffix.lower()
        metadata['file_type'] = file_ext
        metadata['estimated_format'] = self._detect_content_format(content, file_ext)
        
        return metadata
    
    def _detect_content_format(self, content: bytes, file_ext: str) -> str:
        """Detect the actual content format of the file"""
        try:
            # Try to decode first 1000 bytes to analyze content
            sample = content[:1000].decode('utf-8', errors='ignore').lower()
            
            if file_ext == '.csv' or ',' in sample:
                return 'csv'
            elif file_ext in ['.xls', '.xlsx']:
                return 'excel'
            elif '\t' in sample:
                return 'tsv'
            elif sample.strip().startswith('{') or sample.strip().startswith('['):
                return 'json'
            elif '<' in sample:
                return 'xml_or_html'
            else:
                return 'text'
        except:
            return 'binary'
    
    def get_processed_files_registry(self) -> Dict[str, Dict]:
        """Get registry of all previously processed files with their metadata"""
        if self._processed_files_cache and self._last_update_time:
            # Cache valid for 5 minutes
            if (datetime.now() - self._last_update_time).total_seconds() < 300:
                return self._processed_files_cache
        
        registry = {}
        
        try:
            processing_log_path = self.metadata_dir / 'processing_log'
            
            if processing_log_path.exists():
                if self.spark and SPARK_AVAILABLE:
                    # Read from Delta table
                    df = self.spark.read.format("delta").load(str(processing_log_path))
                    
                    # Get latest entry for each file
                    latest_entries = df.groupBy("file_name") \
                        .agg({"processed_at": "max"}) \
                        .withColumnRenamed("max(processed_at)", "latest_processed_at")
                    
                    # Join back to get full records
                    latest_files = df.join(
                        latest_entries,
                        (df.file_name == latest_entries.file_name) &
                        (df.processed_at == latest_entries.latest_processed_at)
                    ).select(df["*"]).collect()
                    
                    for row in latest_files:
                        registry[row.file_name] = {
                            'file_url': row.file_url,
                            'file_size': row.file_size,
                            'file_checksum': row.file_checksum,
                            'extraction_date': row.extraction_date,
                            'processed_at': row.processed_at.isoformat() if row.processed_at else None,
                            'processing_status': row.processing_status,
                            'row_count': row.row_count,
                            'pipeline_run_id': row.pipeline_run_id
                        }
                else:
                    # Pandas fallback
                    parquet_files = list(processing_log_path.glob("*.parquet"))
                    
                    if parquet_files:
                        # Read all parquet files and combine
                        dfs = []
                        for file in parquet_files:
                            try:
                                df = pd.read_parquet(file)
                                dfs.append(df)
                            except Exception as e:
                                print(f"Warning: Could not read {file}: {e}")
                        
                        if dfs:
                            combined_df = pd.concat(dfs, ignore_index=True)
                            
                            # Get latest entry for each file
                            latest_entries = combined_df.loc[
                                combined_df.groupby('file_name')['processed_at'].idxmax()
                            ]
                            
                            for _, row in latest_entries.iterrows():
                                registry[row['file_name']] = {
                                    'file_url': row['file_url'],
                                    'file_size': row['file_size'],
                                    'file_checksum': row['file_checksum'],
                                    'extraction_date': row['extraction_date'],
                                    'processed_at': row['processed_at'],
                                    'processing_status': row['processing_status'],
                                    'row_count': row.get('row_count', 0),
                                    'pipeline_run_id': row.get('pipeline_run_id', '')
                                }
        
        except Exception as e:
            print(f"Warning: Could not load processed files registry: {e}")
        
        # Update cache
        self._processed_files_cache = registry
        self._last_update_time = datetime.now()
        
        return registry
    
    def detect_file_changes(self, available_files: List[Dict]) -> Dict[str, List[Dict]]:
        """Categorize files as new, modified, or unchanged"""
        processed_registry = self.get_processed_files_registry()
        
        categorized = {
            'new_files': [],
            'modified_files': [],
            'unchanged_files': [],
            'potentially_removed_files': []
        }
        
        # Check each available file
        for file_info in available_files:
            filename = file_info['name']
            file_url = file_info['url']
            
            if filename in processed_registry:
                # File was processed before
                previous_info = processed_registry[filename]
                
                # Check if URL changed (could indicate content change)
                if previous_info['file_url'] != file_url:
                    file_info['change_reason'] = 'url_changed'
                    file_info['previous_url'] = previous_info['file_url']
                    categorized['modified_files'].append(file_info)
                
                # Check if we need to verify checksum (for modified files detection)
                # This would require downloading the file, so we'll mark for verification
                elif previous_info['processing_status'] != 'processed':
                    file_info['change_reason'] = 'previous_processing_failed'
                    categorized['modified_files'].append(file_info)
                
                else:
                    # Assume unchanged for now (could verify with HEAD request)
                    file_info['previous_info'] = previous_info
                    categorized['unchanged_files'].append(file_info)
            
            else:
                # New file
                categorized['new_files'].append(file_info)
        
        # Find potentially removed files (in registry but not in available files)
        available_filenames = {f['name'] for f in available_files}
        for filename, registry_info in processed_registry.items():
            if filename not in available_filenames:
                if registry_info['processing_status'] == 'processed':
                    # Only consider successfully processed files as potentially removed
                    categorized['potentially_removed_files'].append({
                        'name': filename,
                        'last_seen': registry_info['processed_at'],
                        'registry_info': registry_info
                    })
        
        return categorized
    
    def verify_file_content_change(self, file_info: Dict, previous_checksum: str) -> bool:
        """Verify if file content has actually changed by downloading and comparing checksum"""
        try:
            # Download file content
            response = requests.get(file_info['url'], timeout=30)
            response.raise_for_status()
            
            # Calculate current checksum
            current_checksum = self.calculate_file_hash(response.content)
            
            # Compare with previous
            return current_checksum != previous_checksum
            
        except Exception as e:
            print(f"Warning: Could not verify content change for {file_info['name']}: {e}")
            # Assume changed to be safe
            return True
    
    def get_files_requiring_processing(self, available_files: List[Dict], 
                                     force_reprocess_days: int = 7) -> List[Dict]:
        """Get list of files that need to be processed (new or changed)"""
        changes = self.detect_file_changes(available_files)
        
        files_to_process = []
        
        # Always process new files
        files_to_process.extend(changes['new_files'])
        
        # Process modified files
        for file_info in changes['modified_files']:
            if file_info.get('change_reason') == 'url_changed':
                # URL changed - likely content changed
                files_to_process.append(file_info)
            elif file_info.get('change_reason') == 'previous_processing_failed':
                # Previous processing failed - retry
                files_to_process.append(file_info)
        
        # Optionally reprocess recent files for validation
        if force_reprocess_days > 0:
            cutoff_date = (datetime.now() - timedelta(days=force_reprocess_days)).strftime('%Y%m%d')
            
            for file_info in changes['unchanged_files']:
                if file_info.get('date', '') >= cutoff_date:
                    # Recent file - consider for reprocessing
                    previous_info = file_info.get('previous_info', {})
                    
                    # Only reprocess if it's been a while or there were issues
                    last_processed = previous_info.get('processed_at', '')
                    if last_processed:
                        try:
                            last_processed_dt = datetime.fromisoformat(last_processed.replace('Z', '+00:00'))
                            days_since_processed = (datetime.now() - last_processed_dt).days
                            
                            if days_since_processed >= force_reprocess_days:
                                file_info['change_reason'] = 'force_reprocess_old'
                                files_to_process.append(file_info)
                        except:
                            # Could not parse date - reprocess to be safe
                            file_info['change_reason'] = 'force_reprocess_unknown_date'
                            files_to_process.append(file_info)
        
        return files_to_process
    
    def update_processing_registry(self, file_metadata: Dict, processing_result: Dict, 
                                 pipeline_run_id: str) -> bool:
        """Update the processing registry with new file processing results"""
        try:
            processing_log_path = self.metadata_dir / 'processing_log'
            
            # Prepare record
            log_record = {
                'file_name': file_metadata['filename'],
                'file_url': file_metadata['url'],
                'file_size': file_metadata['file_size'],
                'file_checksum': file_metadata['content_hash'],
                'extraction_date': file_metadata.get('extraction_date'),
                'extraction_year': file_metadata.get('extraction_year'),
                'processed_at': datetime.now(),
                'processing_status': processing_result.get('status', 'processed'),
                'delta_version': processing_result.get('delta_version'),
                'row_count': processing_result.get('row_count', 0),
                'pipeline_run_id': pipeline_run_id,
                'file_type': file_metadata.get('file_type'),
                'estimated_format': file_metadata.get('estimated_format'),
                'error_message': processing_result.get('error_message')
            }
            
            if self.spark and SPARK_AVAILABLE:
                # Create Spark DataFrame and append to Delta table
                log_df = self.spark.createDataFrame([log_record])
                
                log_df.write \
                    .format("delta") \
                    .mode("append") \
                    .save(str(processing_log_path))
            
            else:
                # Pandas fallback
                log_df = pd.DataFrame([log_record])
                log_file = processing_log_path / f"log_{pipeline_run_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
                log_df.to_parquet(log_file, index=False)
            
            # Invalidate cache
            self._processed_files_cache = None
            self._last_update_time = None
            
            return True
            
        except Exception as e:
            print(f"Error updating processing registry: {e}")
            return False


class OMIEDeltaTableManager:
    """Manages Delta table creation, schema evolution, and optimization"""
    
    def __init__(self, config: Dict, spark_session: Optional[SparkSession] = None):
        self.config = config
        self.spark = spark_session
        self.tables_dir = Path(config['lakehouse_root']) / 'bronze' / 'OMIE' / 'delta_tables'
        self.tables_dir.mkdir(parents=True, exist_ok=True)
    
    def get_omie_data_schema(self) -> StructType:
        """Define the standard schema for OMIE data"""
        return StructType([
            # Original data columns (will be dynamically extended)
            StructField("Fecha", StringType(), True),
            StructField("Hora", StringType(), True),
            StructField("Precio", DoubleType(), True),
            StructField("Demanda", DoubleType(), True),
            
            # Metadata columns (always present)
            StructField("_source_file", StringType(), False),
            StructField("_source_url", StringType(), False),
            StructField("_extraction_year", IntegerType(), True),
            StructField("_extraction_date", StringType(), True),
            StructField("_file_checksum", StringType(), True),
            StructField("_ingested_at", TimestampType(), False),
            StructField("_pipeline_run_id", StringType(), True),
            
            # Partition columns
            StructField("partition_year", IntegerType(), False),
            StructField("partition_month", IntegerType(), False)
        ])
    
    def get_processing_log_schema(self) -> StructType:
        """Define schema for processing log table"""
        return StructType([
            StructField("file_name", StringType(), False),
            StructField("file_url", StringType(), False),
            StructField("file_size", LongType(), True),
            StructField("file_checksum", StringType(), True),
            StructField("extraction_date", StringType(), True),
            StructField("extraction_year", IntegerType(), True),
            StructField("processed_at", TimestampType(), False),
            StructField("processing_status", StringType(), False),
            StructField("delta_version", LongType(), True),
            StructField("row_count", LongType(), True),
            StructField("pipeline_run_id", StringType(), True),
            StructField("file_type", StringType(), True),
            StructField("estimated_format", StringType(), True),
            StructField("error_message", StringType(), True)
        ])
    
    def get_last_processed_schema(self) -> StructType:
        """Define schema for last processed tracking table"""
        return StructType([
            StructField("data_source", StringType(), False),
            StructField("last_processed_date", StringType(), False),
            StructField("last_processed_file", StringType(), True),
            StructField("last_update_timestamp", TimestampType(), False),
            StructField("files_count", IntegerType(), True),
            StructField("total_rows", LongType(), True)
        ])
    
    def create_daily_prices_table(self) -> bool:
        """Create the main daily prices Delta table"""
        if not self.spark or not SPARK_AVAILABLE:
            print("Warning: Cannot create Delta table without Spark. Using directory structure.")
            table_path = self.tables_dir / 'daily_prices'
            table_path.mkdir(parents=True, exist_ok=True)
            return True
        
        try:
            table_path = self.tables_dir / 'daily_prices'
            
            # Check if table already exists
            if table_path.exists() and list(table_path.glob("*.parquet")):
                print(f"Daily prices table already exists: {table_path}")
                return True
            
            # Create empty DataFrame with schema
            schema = self.get_omie_data_schema()
            empty_df = self.spark.createDataFrame([], schema)
            
            # Write initial empty table
            empty_df.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("partition_year", "partition_month") \
                .option("delta.autoOptimize.optimizeWrite", "true") \
                .option("delta.autoOptimize.autoCompact", "true") \
                .save(str(table_path))
            
            print(f"✅ Daily prices Delta table created: {table_path}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to create daily prices table: {e}")
            return False
    
    def create_metadata_tables(self) -> bool:
        """Create all metadata tracking tables"""
        if not self.spark or not SPARK_AVAILABLE:
            print("Warning: Cannot create Delta metadata tables without Spark.")
            # Create directories for pandas fallback
            for table_name in ['processing_log', 'last_processed']:
                table_path = self.tables_dir / 'metadata' / table_name
                table_path.mkdir(parents=True, exist_ok=True)
            return True
        
        success = True
        
        # Create processing log table
        try:
            log_path = self.tables_dir / 'metadata' / 'processing_log'
            
            if not (log_path.exists() and list(log_path.glob("*.parquet"))):
                schema = self.get_processing_log_schema()
                empty_df = self.spark.createDataFrame([], schema)
                
                empty_df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .save(str(log_path))
                
                print(f"✅ Processing log table created: {log_path}")
            else:
                print(f"Processing log table already exists: {log_path}")
                
        except Exception as e:
            print(f"❌ Failed to create processing log table: {e}")
            success = False
        
        # Create last processed table
        try:
            last_processed_path = self.tables_dir / 'metadata' / 'last_processed'
            
            if not (last_processed_path.exists() and list(last_processed_path.glob("*.parquet"))):
                schema = self.get_last_processed_schema()
                
                # Initialize with default values
                initial_data = [(
                    "OMIE_daily_prices",
                    "20240101",
                    None,
                    datetime.now(),
                    0,
                    0
                )]
                
                df = self.spark.createDataFrame(initial_data, schema)
                
                df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .save(str(last_processed_path))
                
                print(f"✅ Last processed table created: {last_processed_path}")
            else:
                print(f"Last processed table already exists: {last_processed_path}")
                
        except Exception as e:
            print(f"❌ Failed to create last processed table: {e}")
            success = False
        
        return success
    
    def setup_all_tables(self) -> bool:
        """Set up all required Delta tables for OMIE data management"""
        print("🏗️  Setting up OMIE Delta tables...")
        
        success = True
        
        # Create main data table
        if not self.create_daily_prices_table():
            success = False
        
        # Create metadata tables
        if not self.create_metadata_tables():
            success = False
        
        if success:
            print("✅ All OMIE Delta tables set up successfully")
        else:
            print("❌ Some table creation failed")
        
        return success
    
    def optimize_table(self, table_name: str) -> bool:
        """Optimize a specific Delta table"""
        if not self.spark or not SPARK_AVAILABLE:
            print(f"Warning: Cannot optimize {table_name} without Spark")
            return True
        
        try:
            table_path = self.tables_dir / table_name
            if table_name.startswith('metadata/'):
                table_path = self.tables_dir / table_name
            
            if not table_path.exists():
                print(f"Table {table_name} does not exist: {table_path}")
                return False
            
            # Run OPTIMIZE command
            self.spark.sql(f"OPTIMIZE delta.`{table_path}`")
            print(f"✅ Optimized table: {table_name}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to optimize table {table_name}: {e}")
            return False
    
    def vacuum_table(self, table_name: str, retention_hours: int = 168) -> bool:
        """Vacuum a specific Delta table to remove old files"""
        if not self.spark or not SPARK_AVAILABLE:
            print(f"Warning: Cannot vacuum {table_name} without Spark")
            return True
        
        try:
            table_path = self.tables_dir / table_name
            if table_name.startswith('metadata/'):
                table_path = self.tables_dir / table_name
            
            if not table_path.exists():
                print(f"Table {table_name} does not exist: {table_path}")
                return False
            
            # Run VACUUM command
            self.spark.sql(f"VACUUM delta.`{table_path}` RETAIN {retention_hours} HOURS")
            print(f"✅ Vacuumed table: {table_name} (retained {retention_hours} hours)")
            return True
            
        except Exception as e:
            print(f"❌ Failed to vacuum table {table_name}: {e}")
            return False


def create_pipeline_configuration(lakehouse_root: str) -> Dict:
    """Create default pipeline configuration"""
    config = {
        'lakehouse_root': lakehouse_root,
        'delta_tables_dir': f"{lakehouse_root}/bronze/OMIE/delta_tables",
        'daily_prices_table': f"{lakehouse_root}/bronze/OMIE/delta_tables/daily_prices",
        'metadata_dir': f"{lakehouse_root}/bronze/OMIE/delta_tables/metadata",
        'staging_dir': f"{lakehouse_root}/bronze/OMIE/delta_tables/staging",
        'processing_log_table': f"{lakehouse_root}/bronze/OMIE/delta_tables/metadata/processing_log",
        'last_processed_table': f"{lakehouse_root}/bronze/OMIE/delta_tables/metadata/last_processed",
        'omie_base_url': 'https://www.omie.es',
        'target_years': [2023, 2024, 2025],
        'file_patterns': ['marginalpdbc'],
        'change_detection': {
            'enable_checksum_verification': True,
            'force_reprocess_days': 7,
            'max_unchanged_days': 30
        },
        'optimization': {
            'auto_optimize_enabled': True,
            'auto_compact_enabled': True,
            'optimize_write_enabled': True,
            'vacuum_retention_hours': 168
        },
        'spark_config': {
            'app_name': 'OMIE_Pipeline',
            'delta_enabled': True,
            'adaptive_query_enabled': True,
            'coalesce_partitions_enabled': True
        }
    }
    
    return config


def main():
    """Main function for setting up change detection and Delta tables"""
    
    # Determine lakehouse root
    if os.path.exists('/lakehouse/default/Files'):
        lakehouse_root = '/lakehouse/default/Files'
    else:
        lakehouse_root = 'notebooks/bronze/OMIE'
    
    print(f"🏗️  Setting up OMIE change detection and Delta tables")
    print(f"   Lakehouse root: {lakehouse_root}")
    
    # Create configuration
    config = create_pipeline_configuration(lakehouse_root)
    
    # Save configuration
    metadata_dir = Path(lakehouse_root) / 'bronze' / 'OMIE' / 'delta_tables' / 'metadata'
    metadata_dir.mkdir(parents=True, exist_ok=True)
    
    config_path = metadata_dir / 'pipeline_config.json'
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    
    print(f"✅ Configuration saved: {config_path}")
    
    # Initialize Spark if available
    spark = None
    if SPARK_AVAILABLE:
        try:
            from delta import configure_spark_with_delta_pip
            
            builder = SparkSession.builder \
                .appName("OMIE_Setup") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
            print("✅ Spark session created")
        except Exception as e:
            print(f"⚠️  Could not create Spark session: {e}")
    
    # Set up Delta tables
    table_manager = OMIEDeltaTableManager(config, spark)
    success = table_manager.setup_all_tables()
    
    # Test change detection
    change_detector = OMIEChangeDetector(config, spark)
    print("✅ Change detection system initialized")
    
    if spark:
        spark.stop()
    
    if success:
        print("\n🎉 OMIE change detection and Delta tables setup completed successfully!")
        print("📋 Next steps:")
        print("   1. Run the Delta migration notebook to convert existing data")
        print("   2. Test the daily pipeline with: python scripts/automation/omie_daily_pipeline.py")
        print("   3. Set up Fabric Data Factory pipelines for automation")
    else:
        print("\n❌ Setup completed with some errors. Check the logs above.")


if __name__ == "__main__":
    main()