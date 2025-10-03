"""
OMIE Daily Update Pipeline
==========================

This pipeline runs daily to:
1. Check for new OMIE files since last update
2. Download only new/changed files
3. Process and append to Delta table
4. Update metadata tracking
5. Handle errors and notifications

Designed for:
- Microsoft Fabric Data Factory execution
- Incremental processing (only new data)
- Error handling and recovery
- Change detection and deduplication
"""

import os
import sys
import json
import re
import hashlib
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, unquote

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from delta import configure_spark_with_delta_pip
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("⚠️  PySpark/Delta not available. Running in pandas mode.")


class OMIEDailyPipeline:
    """Daily pipeline for OMIE data incremental updates"""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize pipeline with configuration"""
        self.config = self._load_config(config_path)
        self.spark = self._init_spark() if SPARK_AVAILABLE else None
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'omie-daily-pipeline/1.0 (+https://github.com)'
        })
        
        # Pipeline run metadata
        self.run_id = f"daily_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.run_start = datetime.now()
        self.processed_files = []
        self.errors = []
        
        print(f"🚀 OMIE Daily Pipeline initialized")
        print(f"   Run ID: {self.run_id}")
        print(f"   Spark available: {self.spark is not None}")
        print(f"   Config: {self.config.get('lakehouse_root', 'default')}")
    
    def _load_config(self, config_path: Optional[str] = None) -> Dict:
        """Load pipeline configuration"""
        if config_path is None:
            # Try to find config in metadata directory
            if os.path.exists('/lakehouse/default/Files'):
                base_dir = Path('/lakehouse/default/Files/bronze/OMIE/delta_tables/metadata')
            else:
                base_dir = Path('notebooks/bronze/OMIE/delta_tables/metadata')
            config_path = base_dir / 'pipeline_config.json'
        
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return json.load(f)
        
        # Default configuration
        return {
            'lakehouse_root': '/lakehouse/default/Files' if os.path.exists('/lakehouse/default/Files') 
                             else 'notebooks/bronze/OMIE',
            'omie_base_url': 'https://www.omie.es',
            'target_years': [2023, 2024, 2025],
            'file_patterns': ['marginalpdbc'],
            'max_files_per_run': 50,
            'timeout_seconds': 300
        }
    
    def _init_spark(self) -> Optional[SparkSession]:
        """Initialize Spark session with Delta support"""
        try:
            builder = SparkSession.builder \
                .appName(f"OMIE_Daily_Pipeline_{self.run_id}") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
            
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
            spark.sparkContext.setLogLevel("WARN")
            return spark
        except Exception as e:
            print(f"❌ Failed to initialize Spark: {e}")
            return None
    
    def get_last_processed_info(self) -> Tuple[str, Optional[str]]:
        """Get information about last processed file/date"""
        metadata_dir = Path(self.config['lakehouse_root']) / 'bronze' / 'OMIE' / 'delta_tables' / 'metadata'
        last_processed_path = metadata_dir / 'last_processed'
        
        if not last_processed_path.exists():
            print("ℹ️  No previous processing history found. Starting fresh.")
            return "20240101", None
        
        try:
            if self.spark:
                df = self.spark.read.format("delta").load(str(last_processed_path))
                row = df.filter(col("data_source") == "OMIE_daily_prices").collect()
                if row:
                    return row[0].last_processed_date, row[0].last_processed_file
            else:
                # Pandas fallback
                files = list(last_processed_path.glob("*.parquet"))
                if files:
                    df = pd.read_parquet(files[0])
                    omie_rows = df[df['data_source'] == 'OMIE_daily_prices']
                    if not omie_rows.empty:
                        row = omie_rows.iloc[0]
                        return row['last_processed_date'], row['last_processed_file']
        except Exception as e:
            print(f"⚠️  Error reading last processed info: {e}")
        
        return "20240101", None
    
    def fetch_available_files(self) -> List[Dict]:
        """Fetch list of available files from OMIE website"""
        print("🔍 Fetching available files from OMIE...")
        
        target_url = f"{self.config['omie_base_url']}/es/file-access-list?parents=/Mercado%20Diario/1.%20Precios&dir=Precios%20horarios%20del%20mercado%20diario%20en%20Espa%C3%B1a&realdir=marginalpdbc"
        
        try:
            response = self.session.get(target_url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            files = []
            
            # Look for table-based file listings (OMIE structure)
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    links = row.find_all('a')
                    
                    if not links:
                        continue
                    
                    row_text = ' '.join([cell.get_text(strip=True) for cell in cells])
                    
                    # Check if this looks like a file row
                    is_file_row = any([
                        re.search(r'\d{8}', row_text),  # Date pattern YYYYMMDD
                        re.search(r'\d+\s*(kb|mb)', row_text, re.IGNORECASE),  # Size pattern
                        any(kw in row_text.lower() for kw in ['marginal', 'csv', 'descarg'])
                    ])
                    
                    if is_file_row:
                        for link in links:
                            href = link.get('href')
                            if href and href != '#':
                                link_text = link.get_text(strip=True)
                                name = link_text if link_text and len(link_text) < 100 else f"omie_file_{len(files)}"
                                full_url = urljoin(self.config['omie_base_url'], href)
                                
                                # Extract date from filename for filtering
                                date_match = re.search(r'(\d{8})', name)
                                if date_match:
                                    file_date = date_match.group(1)
                                    year = int(file_date[:4])
                                    
                                    if year in self.config['target_years']:
                                        files.append({
                                            'name': name,
                                            'url': full_url,
                                            'date': file_date,
                                            'year': year,
                                            'link_text': link_text,
                                            'discovered_at': datetime.now().isoformat()
                                        })
            
            # Remove duplicates by URL
            seen_urls = set()
            unique_files = []
            for file_info in files:
                url = file_info['url']
                if url not in seen_urls:
                    seen_urls.add(url)
                    unique_files.append(file_info)
            
            print(f"✅ Found {len(unique_files)} unique files")
            return unique_files
            
        except Exception as e:
            print(f"❌ Failed to fetch file list: {e}")
            self.errors.append(f"File listing failed: {e}")
            return []
    
    def filter_new_files(self, available_files: List[Dict], last_processed_date: str) -> List[Dict]:
        """Filter files to only those newer than last processed date"""
        new_files = []
        
        for file_info in available_files:
            file_date = file_info.get('date')
            if file_date and file_date > last_processed_date:
                new_files.append(file_info)
        
        # Sort by date to process chronologically
        new_files.sort(key=lambda x: x.get('date', ''))
        
        # Limit files per run to avoid overwhelming the pipeline
        max_files = self.config.get('max_files_per_run', 50)
        if len(new_files) > max_files:
            print(f"⚠️  Found {len(new_files)} new files, limiting to {max_files} per run")
            new_files = new_files[:max_files]
        
        print(f"🆕 {len(new_files)} new files to process (after {last_processed_date})")
        return new_files
    
    def calculate_file_checksum(self, content: bytes) -> str:
        """Calculate MD5 checksum for file content"""
        return hashlib.md5(content).hexdigest()
    
    def download_and_process_file(self, file_info: Dict) -> Optional[Dict]:
        """Download a single file and convert to Delta format"""
        filename = file_info['name']
        url = file_info['url']
        
        print(f"   📥 Processing: {filename}")
        
        try:
            # Download file content
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            content = response.content
            
            # Calculate checksum for change detection
            checksum = self.calculate_file_checksum(content)
            
            # Determine file format and parse
            file_ext = Path(filename).suffix.lower()
            
            if file_ext == '.csv':
                df = pd.read_csv(pd.io.common.BytesIO(content))
            elif file_ext in ['.xls', '.xlsx']:
                df = pd.read_excel(pd.io.common.BytesIO(content))
            elif file_ext == '.txt':
                try:
                    df = pd.read_csv(pd.io.common.BytesIO(content), sep=None, engine='python')
                except:
                    # Fallback for text files
                    text_content = content.decode('utf-8', errors='ignore')
                    df = pd.DataFrame({'raw_content': [text_content]})
            else:
                # Try CSV parsing as fallback
                try:
                    df = pd.read_csv(pd.io.common.BytesIO(content), sep=None, engine='python')
                except:
                    text_content = content.decode('utf-8', errors='ignore')
                    df = pd.DataFrame({'raw_content': [text_content]})
            
            # Add metadata columns
            df['_source_file'] = filename
            df['_source_url'] = url
            df['_extraction_year'] = file_info['year']
            df['_extraction_date'] = file_info['date']
            df['_file_checksum'] = checksum
            df['_ingested_at'] = datetime.now().isoformat()
            df['_pipeline_run_id'] = self.run_id
            
            # Add partition columns
            df['partition_year'] = file_info['year']
            df['partition_month'] = int(file_info['date'][4:6]) if len(file_info['date']) >= 6 else 1
            
            return {
                'file_info': file_info,
                'dataframe': df,
                'checksum': checksum,
                'row_count': len(df),
                'file_size': len(content)
            }
            
        except Exception as e:
            print(f"   ❌ Failed to process {filename}: {e}")
            self.errors.append(f"File processing failed - {filename}: {e}")
            return None
    
    def append_to_delta_table(self, processed_data: List[Dict]) -> bool:
        """Append new data to Delta table"""
        if not processed_data:
            print("ℹ️  No data to append to Delta table")
            return True
        
        print(f"💾 Appending {len(processed_data)} files to Delta table...")
        
        try:
            # Combine all DataFrames
            all_dfs = [item['dataframe'] for item in processed_data]
            combined_df = pd.concat(all_dfs, ignore_index=True)
            
            if self.spark:
                # Convert to Spark DataFrame
                spark_df = self.spark.createDataFrame(combined_df)
                
                # Write to Delta table
                delta_table_path = Path(self.config['lakehouse_root']) / 'bronze' / 'OMIE' / 'delta_tables' / 'daily_prices'
                
                spark_df.write \
                    .format("delta") \
                    .mode("append") \
                    .partitionBy("partition_year", "partition_month") \
                    .option("mergeSchema", "true") \
                    .save(str(delta_table_path))
                
                print(f"✅ Successfully appended {len(combined_df)} rows to Delta table")
                
            else:
                # Pandas fallback - save as Parquet
                delta_table_path = Path(self.config['lakehouse_root']) / 'bronze' / 'OMIE' / 'delta_tables' / 'daily_prices'
                delta_table_path.mkdir(parents=True, exist_ok=True)
                
                # Save with timestamp to avoid conflicts
                parquet_file = delta_table_path / f"daily_update_{self.run_id}.parquet"
                combined_df.to_parquet(parquet_file, index=False)
                
                print(f"✅ Successfully saved {len(combined_df)} rows to {parquet_file}")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to append to Delta table: {e}")
            self.errors.append(f"Delta table append failed: {e}")
            return False
    
    def update_metadata(self, processed_data: List[Dict]) -> None:
        """Update processing metadata and last processed tracking"""
        if not processed_data:
            return
        
        print("📝 Updating metadata...")
        
        try:
            metadata_dir = Path(self.config['lakehouse_root']) / 'bronze' / 'OMIE' / 'delta_tables' / 'metadata'
            
            # Update processing log
            log_entries = []
            for item in processed_data:
                file_info = item['file_info']
                log_entries.append({
                    'file_name': file_info['name'],
                    'file_url': file_info['url'],
                    'file_size': item['file_size'],
                    'file_checksum': item['checksum'],
                    'extraction_date': file_info['date'],
                    'extraction_year': file_info['year'],
                    'processed_at': datetime.now(),
                    'processing_status': 'processed',
                    'delta_version': None,  # Could be populated with Delta version info
                    'row_count': item['row_count'],
                    'pipeline_run_id': self.run_id
                })
            
            log_df = pd.DataFrame(log_entries)
            
            if self.spark:
                # Append to processing log Delta table
                spark_log_df = self.spark.createDataFrame(log_df)
                processing_log_path = metadata_dir / 'processing_log'
                
                spark_log_df.write \
                    .format("delta") \
                    .mode("append") \
                    .save(str(processing_log_path))
            else:
                # Pandas fallback
                processing_log_path = metadata_dir / 'processing_log'
                processing_log_path.mkdir(parents=True, exist_ok=True)
                log_file = processing_log_path / f"log_{self.run_id}.parquet"
                log_df.to_parquet(log_file, index=False)
            
            # Update last processed info
            latest_file = max(processed_data, key=lambda x: x['file_info']['date'])
            last_processed_data = [{
                'data_source': 'OMIE_daily_prices',
                'last_processed_date': latest_file['file_info']['date'],
                'last_processed_file': latest_file['file_info']['name'],
                'last_update_timestamp': datetime.now(),
                'files_count': len(processed_data),
                'total_rows': sum(item['row_count'] for item in processed_data)
            }]
            
            last_processed_df = pd.DataFrame(last_processed_data)
            
            if self.spark:
                spark_last_df = self.spark.createDataFrame(last_processed_df)
                last_processed_path = metadata_dir / 'last_processed'
                
                spark_last_df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .save(str(last_processed_path))
            else:
                last_processed_path = metadata_dir / 'last_processed'
                last_processed_path.mkdir(parents=True, exist_ok=True)
                last_file = last_processed_path / "last_processed.parquet"
                last_processed_df.to_parquet(last_file, index=False)
            
            print(f"✅ Metadata updated - last processed: {latest_file['file_info']['date']}")
            
        except Exception as e:
            print(f"❌ Failed to update metadata: {e}")
            self.errors.append(f"Metadata update failed: {e}")
    
    def run_daily_pipeline(self) -> Dict:
        """Execute the complete daily pipeline"""
        print("=" * 80)
        print(f"🚀 OMIE DAILY PIPELINE - {self.run_start.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        # Step 1: Get last processed info
        last_processed_date, last_processed_file = self.get_last_processed_info()
        print(f"📅 Last processed: {last_processed_date} ({last_processed_file or 'N/A'})")
        
        # Step 2: Fetch available files
        available_files = self.fetch_available_files()
        if not available_files:
            print("❌ No files available. Pipeline terminated.")
            return self._create_result(success=False, reason="No files available")
        
        # Step 3: Filter for new files
        new_files = self.filter_new_files(available_files, last_processed_date)
        if not new_files:
            print("ℹ️  No new files to process. Pipeline completed.")
            return self._create_result(success=True, reason="No new files")
        
        # Step 4: Process new files
        print(f"\n📥 Processing {len(new_files)} new files...")
        processed_data = []
        
        for i, file_info in enumerate(new_files, 1):
            print(f"[{i}/{len(new_files)}] {file_info['name']} ({file_info['date']})")
            
            result = self.download_and_process_file(file_info)
            if result:
                processed_data.append(result)
                self.processed_files.append(file_info['name'])
            else:
                # Continue processing other files even if one fails
                continue
        
        if not processed_data:
            print("❌ No files were successfully processed.")
            return self._create_result(success=False, reason="All file processing failed")
        
        # Step 5: Append to Delta table
        if not self.append_to_delta_table(processed_data):
            return self._create_result(success=False, reason="Delta table append failed")
        
        # Step 6: Update metadata
        self.update_metadata(processed_data)
        
        # Step 7: Summary
        run_duration = datetime.now() - self.run_start
        
        print(f"\n🎉 DAILY PIPELINE COMPLETED")
        print(f"   Duration: {run_duration}")
        print(f"   Files processed: {len(processed_data)}")
        print(f"   Total rows: {sum(item['row_count'] for item in processed_data):,}")
        print(f"   Errors: {len(self.errors)}")
        
        if self.errors:
            print(f"\n⚠️  Errors encountered:")
            for error in self.errors:
                print(f"   - {error}")
        
        return self._create_result(
            success=True,
            processed_files=len(processed_data),
            total_rows=sum(item['row_count'] for item in processed_data),
            duration=str(run_duration),
            errors=self.errors
        )
    
    def _create_result(self, success: bool, reason: str = None, **kwargs) -> Dict:
        """Create standardized pipeline result"""
        result = {
            'success': success,
            'run_id': self.run_id,
            'timestamp': datetime.now().isoformat(),
            'duration': str(datetime.now() - self.run_start),
            'processed_files': len(self.processed_files),
            'errors': self.errors
        }
        
        if reason:
            result['reason'] = reason
        
        result.update(kwargs)
        return result
    
    def cleanup(self) -> None:
        """Cleanup resources"""
        if self.spark:
            self.spark.stop()
        self.session.close()


def main():
    """Main entry point for the daily pipeline"""
    pipeline = None
    
    try:
        # Initialize pipeline
        pipeline = OMIEDailyPipeline()
        
        # Run the pipeline
        result = pipeline.run_daily_pipeline()
        
        # Print result
        print(f"\n📊 PIPELINE RESULT:")
        print(json.dumps(result, indent=2))
        
        # Exit with appropriate code
        exit_code = 0 if result['success'] else 1
        sys.exit(exit_code)
        
    except Exception as e:
        print(f"💥 PIPELINE FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        if pipeline:
            pipeline.cleanup()


if __name__ == "__main__":
    main()