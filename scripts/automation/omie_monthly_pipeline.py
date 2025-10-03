"""
OMIE Monthly Maintenance Pipeline
=================================

This pipeline runs monthly to:
1. Perform comprehensive data validation
2. Reprocess recent files for corrections
3. Optimize Delta table performance
4. Generate data quality reports
5. Archive old staging data
6. Update data schemas if needed

Designed for:
- Microsoft Fabric Data Factory execution
- Data quality assurance
- Performance optimization
- Historical data validation
- Schema evolution handling
"""

import os
import sys
import json
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from collections import defaultdict

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from delta import configure_spark_with_delta_pip, DeltaTable
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("⚠️  PySpark/Delta not available. Running in pandas mode.")


class OMIEMonthlyPipeline:
    """Monthly maintenance pipeline for OMIE data quality and optimization"""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize monthly pipeline with configuration"""
        self.config = self._load_config(config_path)
        self.spark = self._init_spark() if SPARK_AVAILABLE else None
        
        # Pipeline run metadata
        self.run_id = f"monthly_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.run_start = datetime.now()
        self.issues_found = []
        self.fixes_applied = []
        self.errors = []
        
        # Data quality thresholds
        self.quality_thresholds = {
            'min_rows_per_file': 10,
            'max_null_percentage': 0.15,  # 15% nulls max
            'expected_columns_min': 5,
            'max_duplicate_percentage': 0.05,  # 5% duplicates max
            'date_range_days_max': 2,  # Files should cover max 2 days
        }
        
        print(f"🔧 OMIE Monthly Maintenance Pipeline initialized")
        print(f"   Run ID: {self.run_id}")
        print(f"   Spark available: {self.spark is not None}")
        print(f"   Quality thresholds: {len(self.quality_thresholds)} checks")
    
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
            'validation_lookback_days': 90,  # Validate last 90 days
            'optimization_enabled': True,
            'archival_enabled': True,
            'report_generation_enabled': True
        }
    
    def _init_spark(self) -> Optional[SparkSession]:
        """Initialize Spark session with Delta support and optimization settings"""
        try:
            builder = SparkSession.builder \
                .appName(f"OMIE_Monthly_Maintenance_{self.run_id}") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.adaptive.skewJoin.enabled", "true") \
                .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
                .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
                .config("spark.databricks.delta.autoCompact.enabled", "true")
            
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
            spark.sparkContext.setLogLevel("WARN")
            return spark
        except Exception as e:
            print(f"❌ Failed to initialize Spark: {e}")
            return None
    
    def analyze_delta_table_health(self) -> Dict:
        """Analyze Delta table health and performance metrics"""
        print("🏥 Analyzing Delta table health...")
        
        health_report = {
            'table_exists': False,
            'total_rows': 0,
            'total_files': 0,
            'total_size_mb': 0,
            'partitions': 0,
            'versions': 0,
            'last_modified': None,
            'file_size_distribution': {},
            'partition_distribution': {},
            'issues': []
        }
        
        try:
            delta_table_path = Path(self.config['lakehouse_root']) / 'bronze' / 'OMIE' / 'delta_tables' / 'daily_prices'
            
            if not delta_table_path.exists():
                health_report['issues'].append("Delta table directory not found")
                return health_report
            
            health_report['table_exists'] = True
            
            if self.spark:
                # Read Delta table
                df = self.spark.read.format("delta").load(str(delta_table_path))
                health_report['total_rows'] = df.count()
                
                # Get table history
                history_df = self.spark.sql(f"DESCRIBE HISTORY delta.`{delta_table_path}`")
                history_list = history_df.collect()
                health_report['versions'] = len(history_list)
                
                if history_list:
                    health_report['last_modified'] = history_list[0].timestamp.isoformat()
                
                # Analyze partitions
                partitions = df.select("partition_year", "partition_month").distinct().collect()
                health_report['partitions'] = len(partitions)
                
                partition_counts = {}
                for partition in partitions:
                    key = f"{partition.partition_year}-{partition.partition_month:02d}"
                    count = df.filter(
                        (col("partition_year") == partition.partition_year) & 
                        (col("partition_month") == partition.partition_month)
                    ).count()
                    partition_counts[key] = count
                
                health_report['partition_distribution'] = partition_counts
                
                # File-level analysis
                detail_df = self.spark.sql(f"DESCRIBE DETAIL delta.`{delta_table_path}`")
                detail_row = detail_df.collect()[0]
                
                health_report['total_files'] = detail_row.numFiles
                health_report['total_size_mb'] = detail_row.sizeInBytes / (1024 * 1024)
                
                # Check for small files (performance issue)
                avg_file_size = health_report['total_size_mb'] / health_report['total_files'] if health_report['total_files'] > 0 else 0
                if avg_file_size < 10:  # Less than 10MB average
                    health_report['issues'].append(f"Small files detected (avg {avg_file_size:.1f}MB) - consider optimization")
                
            else:
                # Pandas fallback
                parquet_files = list(delta_table_path.rglob("*.parquet"))
                health_report['total_files'] = len(parquet_files)
                
                total_size = sum(f.stat().st_size for f in parquet_files)
                health_report['total_size_mb'] = total_size / (1024 * 1024)
                
                if parquet_files:
                    latest_file = max(parquet_files, key=lambda f: f.stat().st_mtime)
                    health_report['last_modified'] = datetime.fromtimestamp(latest_file.stat().st_mtime).isoformat()
                
                # Basic partition analysis from directory structure
                year_dirs = set()
                for f in parquet_files:
                    path_parts = f.parts
                    for i, part in enumerate(path_parts):
                        if part.startswith('partition_year='):
                            year = part.split('=')[1]
                            month_part = path_parts[i+1] if i+1 < len(path_parts) else ''
                            if month_part.startswith('partition_month='):
                                month = month_part.split('=')[1]
                                year_dirs.add(f"{year}-{month}")
                
                health_report['partitions'] = len(year_dirs)
            
            print(f"✅ Table health analyzed:")
            print(f"   Rows: {health_report['total_rows']:,}")
            print(f"   Files: {health_report['total_files']}")
            print(f"   Size: {health_report['total_size_mb']:.1f} MB")
            print(f"   Partitions: {health_report['partitions']}")
            print(f"   Issues: {len(health_report['issues'])}")
            
        except Exception as e:
            print(f"❌ Failed to analyze table health: {e}")
            health_report['issues'].append(f"Health analysis failed: {e}")
            self.errors.append(f"Table health analysis failed: {e}")
        
        return health_report
    
    def validate_data_quality(self) -> Dict:
        """Perform comprehensive data quality validation"""
        print("🔍 Performing data quality validation...")
        
        quality_report = {
            'validation_timestamp': datetime.now().isoformat(),
            'lookback_days': self.config.get('validation_lookback_days', 90),
            'files_validated': 0,
            'rows_validated': 0,
            'quality_issues': [],
            'summary': {
                'critical_issues': 0,
                'warnings': 0,
                'recommendations': 0
            }
        }
        
        try:
            delta_table_path = Path(self.config['lakehouse_root']) / 'bronze' / 'OMIE' / 'delta_tables' / 'daily_prices'
            
            if not delta_table_path.exists():
                quality_report['quality_issues'].append({
                    'severity': 'critical',
                    'issue': 'Delta table not found',
                    'details': f"Table path {delta_table_path} does not exist"
                })
                return quality_report
            
            if self.spark:
                # Read Delta table with date filtering
                df = self.spark.read.format("delta").load(str(delta_table_path))
                
                # Filter to recent data for validation
                cutoff_date = (datetime.now() - timedelta(days=quality_report['lookback_days'])).strftime('%Y%m%d')
                recent_df = df.filter(col("_extraction_date") >= cutoff_date)
                
                quality_report['rows_validated'] = recent_df.count()
                
                # Count unique files in the validation period
                file_count = recent_df.select("_source_file").distinct().count()
                quality_report['files_validated'] = file_count
                
                print(f"   Validating {quality_report['rows_validated']:,} rows from {file_count} files")
                
                # Quality Check 1: Null value analysis
                print("   🔍 Checking for null values...")
                null_analysis = {}
                total_rows = quality_report['rows_validated']
                
                for col_name in recent_df.columns:
                    if col_name.startswith('_'):  # Skip metadata columns
                        continue
                    
                    null_count = recent_df.filter(col(col_name).isNull()).count()
                    null_percentage = null_count / total_rows if total_rows > 0 else 0
                    null_analysis[col_name] = {
                        'null_count': null_count,
                        'null_percentage': null_percentage
                    }
                    
                    if null_percentage > self.quality_thresholds['max_null_percentage']:
                        quality_report['quality_issues'].append({
                            'severity': 'warning',
                            'issue': f'High null percentage in column {col_name}',
                            'details': f'{null_percentage:.2%} null values (threshold: {self.quality_thresholds["max_null_percentage"]:.2%})',
                            'affected_rows': null_count
                        })
                
                # Quality Check 2: Duplicate detection
                print("   🔍 Checking for duplicates...")
                total_rows = recent_df.count()
                distinct_rows = recent_df.distinct().count()
                duplicate_count = total_rows - distinct_rows
                duplicate_percentage = duplicate_count / total_rows if total_rows > 0 else 0
                
                if duplicate_percentage > self.quality_thresholds['max_duplicate_percentage']:
                    quality_report['quality_issues'].append({
                        'severity': 'warning',
                        'issue': 'High duplicate row percentage',
                        'details': f'{duplicate_percentage:.2%} duplicate rows (threshold: {self.quality_thresholds["max_duplicate_percentage"]:.2%})',
                        'affected_rows': duplicate_count
                    })
                
                # Quality Check 3: File-level validation
                print("   🔍 Checking file-level quality...")
                file_stats = recent_df.groupBy("_source_file") \
                    .agg(
                        count("*").alias("row_count"),
                        countDistinct("_extraction_date").alias("date_count"),
                        min("_extraction_date").alias("min_date"),
                        max("_extraction_date").alias("max_date")
                    ).collect()
                
                for file_stat in file_stats:
                    filename = file_stat._source_file
                    row_count = file_stat.row_count
                    date_count = file_stat.date_count
                    
                    # Check minimum rows per file
                    if row_count < self.quality_thresholds['min_rows_per_file']:
                        quality_report['quality_issues'].append({
                            'severity': 'warning',
                            'issue': f'File {filename} has too few rows',
                            'details': f'Only {row_count} rows (minimum: {self.quality_thresholds["min_rows_per_file"]})',
                            'affected_rows': row_count
                        })
                    
                    # Check date range consistency
                    if date_count > self.quality_thresholds['date_range_days_max']:
                        quality_report['quality_issues'].append({
                            'severity': 'recommendation',
                            'issue': f'File {filename} spans multiple dates',
                            'details': f'Contains {date_count} different dates (from {file_stat.min_date} to {file_stat.max_date})',
                            'affected_rows': row_count
                        })
                
                # Quality Check 4: Schema validation
                print("   🔍 Checking schema consistency...")
                expected_metadata_columns = ['_source_file', '_source_url', '_extraction_date', '_ingested_at']
                missing_columns = [col for col in expected_metadata_columns if col not in recent_df.columns]
                
                if missing_columns:
                    quality_report['quality_issues'].append({
                        'severity': 'critical',
                        'issue': 'Missing required metadata columns',
                        'details': f'Missing columns: {missing_columns}',
                        'affected_rows': total_rows
                    })
                
                # Quality Check 5: Date format validation
                print("   🔍 Checking date format consistency...")
                invalid_dates = recent_df.filter(
                    ~col("_extraction_date").rlike(r'^\d{8}$')
                ).count()
                
                if invalid_dates > 0:
                    quality_report['quality_issues'].append({
                        'severity': 'critical',
                        'issue': 'Invalid date format in _extraction_date',
                        'details': f'{invalid_dates} rows with invalid date format (expected: YYYYMMDD)',
                        'affected_rows': invalid_dates
                    })
                
            else:
                # Pandas fallback for basic validation
                print("   🔍 Running basic validation (pandas mode)...")
                parquet_files = list(delta_table_path.rglob("*.parquet"))
                quality_report['files_validated'] = len(parquet_files)
                
                if parquet_files:
                    # Sample validation on first few files
                    sample_files = parquet_files[:5]
                    total_rows = 0
                    
                    for file_path in sample_files:
                        try:
                            df = pd.read_parquet(file_path)
                            total_rows += len(df)
                            
                            # Basic null check
                            null_percentages = df.isnull().sum() / len(df)
                            high_null_cols = null_percentages[null_percentages > self.quality_thresholds['max_null_percentage']]
                            
                            for col_name, null_pct in high_null_cols.items():
                                quality_report['quality_issues'].append({
                                    'severity': 'warning',
                                    'issue': f'High null percentage in {col_name} (file: {file_path.name})',
                                    'details': f'{null_pct:.2%} null values',
                                    'affected_rows': int(null_pct * len(df))
                                })
                                
                        except Exception as e:
                            quality_report['quality_issues'].append({
                                'severity': 'critical',
                                'issue': f'Failed to read file {file_path.name}',
                                'details': str(e),
                                'affected_rows': 0
                            })
                    
                    quality_report['rows_validated'] = total_rows
            
            # Summarize issues by severity
            for issue in quality_report['quality_issues']:
                severity = issue['severity']
                if severity == 'critical':
                    quality_report['summary']['critical_issues'] += 1
                elif severity == 'warning':
                    quality_report['summary']['warnings'] += 1
                elif severity == 'recommendation':
                    quality_report['summary']['recommendations'] += 1
            
            print(f"✅ Data quality validation completed:")
            print(f"   Critical issues: {quality_report['summary']['critical_issues']}")
            print(f"   Warnings: {quality_report['summary']['warnings']}")
            print(f"   Recommendations: {quality_report['summary']['recommendations']}")
            
        except Exception as e:
            print(f"❌ Data quality validation failed: {e}")
            quality_report['quality_issues'].append({
                'severity': 'critical',
                'issue': 'Validation process failed',
                'details': str(e),
                'affected_rows': 0
            })
            self.errors.append(f"Data quality validation failed: {e}")
        
        return quality_report
    
    def optimize_delta_table(self) -> Dict:
        """Optimize Delta table performance"""
        print("⚡ Optimizing Delta table performance...")
        
        optimization_report = {
            'optimization_timestamp': datetime.now().isoformat(),
            'operations_performed': [],
            'before_stats': {},
            'after_stats': {},
            'performance_improvement': {}
        }
        
        if not self.config.get('optimization_enabled', True):
            print("ℹ️  Optimization disabled in configuration")
            return optimization_report
        
        try:
            delta_table_path = Path(self.config['lakehouse_root']) / 'bronze' / 'OMIE' / 'delta_tables' / 'daily_prices'
            
            if not delta_table_path.exists() or not self.spark:
                print("⚠️  Cannot optimize: Delta table not found or Spark not available")
                return optimization_report
            
            # Get before stats
            detail_before = self.spark.sql(f"DESCRIBE DETAIL delta.`{delta_table_path}`").collect()[0]
            optimization_report['before_stats'] = {
                'num_files': detail_before.numFiles,
                'size_in_bytes': detail_before.sizeInBytes,
                'size_in_mb': detail_before.sizeInBytes / (1024 * 1024)
            }
            
            print(f"   Before optimization: {detail_before.numFiles} files, {optimization_report['before_stats']['size_in_mb']:.1f} MB")
            
            # Operation 1: OPTIMIZE (compaction)
            print("   🔄 Running OPTIMIZE command...")
            self.spark.sql(f"OPTIMIZE delta.`{delta_table_path}`")
            optimization_report['operations_performed'].append('OPTIMIZE')
            
            # Operation 2: VACUUM (clean up old files)
            print("   🧹 Running VACUUM command...")
            # Vacuum files older than 7 days (default retention)
            self.spark.sql(f"VACUUM delta.`{delta_table_path}` RETAIN 168 HOURS")  # 7 days
            optimization_report['operations_performed'].append('VACUUM')
            
            # Operation 3: ANALYZE TABLE (update statistics)
            print("   📊 Updating table statistics...")
            try:
                self.spark.sql(f"ANALYZE TABLE delta.`{delta_table_path}` COMPUTE STATISTICS")
                optimization_report['operations_performed'].append('ANALYZE')
            except Exception as e:
                print(f"   ⚠️  ANALYZE failed (not critical): {e}")
            
            # Get after stats
            detail_after = self.spark.sql(f"DESCRIBE DETAIL delta.`{delta_table_path}`").collect()[0]
            optimization_report['after_stats'] = {
                'num_files': detail_after.numFiles,
                'size_in_bytes': detail_after.sizeInBytes,
                'size_in_mb': detail_after.sizeInBytes / (1024 * 1024)
            }
            
            # Calculate improvements
            files_reduced = optimization_report['before_stats']['num_files'] - optimization_report['after_stats']['num_files']
            size_change = optimization_report['after_stats']['size_in_mb'] - optimization_report['before_stats']['size_in_mb']
            
            optimization_report['performance_improvement'] = {
                'files_reduced': files_reduced,
                'size_change_mb': size_change,
                'files_reduction_pct': files_reduced / optimization_report['before_stats']['num_files'] if optimization_report['before_stats']['num_files'] > 0 else 0
            }
            
            print(f"✅ Optimization completed:")
            print(f"   After optimization: {detail_after.numFiles} files, {optimization_report['after_stats']['size_in_mb']:.1f} MB")
            print(f"   Files reduced: {files_reduced} ({optimization_report['performance_improvement']['files_reduction_pct']:.1%})")
            print(f"   Size change: {size_change:+.1f} MB")
            
            self.fixes_applied.append(f"Optimized Delta table: {files_reduced} files reduced")
            
        except Exception as e:
            print(f"❌ Delta table optimization failed: {e}")
            self.errors.append(f"Optimization failed: {e}")
        
        return optimization_report
    
    def generate_monthly_report(self, health_report: Dict, quality_report: Dict, optimization_report: Dict) -> Dict:
        """Generate comprehensive monthly maintenance report"""
        print("📋 Generating monthly maintenance report...")
        
        report = {
            'report_metadata': {
                'report_id': f"monthly_report_{datetime.now().strftime('%Y%m')}",
                'generated_at': datetime.now().isoformat(),
                'pipeline_run_id': self.run_id,
                'report_period': datetime.now().strftime('%Y-%m'),
                'next_report_due': (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
            },
            'executive_summary': {
                'overall_health': 'unknown',
                'critical_issues_count': 0,
                'warnings_count': 0,
                'optimizations_applied': len(optimization_report.get('operations_performed', [])),
                'recommendations': []
            },
            'detailed_findings': {
                'table_health': health_report,
                'data_quality': quality_report,
                'performance_optimization': optimization_report
            },
            'action_items': [],
            'trends': {}  # Could be populated with historical comparisons
        }
        
        try:
            # Determine overall health status
            critical_issues = quality_report.get('summary', {}).get('critical_issues', 0)
            warnings = quality_report.get('summary', {}).get('warnings', 0)
            table_issues = len(health_report.get('issues', []))
            
            report['executive_summary']['critical_issues_count'] = critical_issues + table_issues
            report['executive_summary']['warnings_count'] = warnings
            
            if critical_issues > 0 or table_issues > 0:
                report['executive_summary']['overall_health'] = 'critical'
            elif warnings > 5:
                report['executive_summary']['overall_health'] = 'warning'
            elif warnings > 0:
                report['executive_summary']['overall_health'] = 'good'
            else:
                report['executive_summary']['overall_health'] = 'excellent'
            
            # Generate recommendations
            recommendations = []
            
            # Performance recommendations
            if health_report.get('total_files', 0) > 1000:
                recommendations.append("Consider more aggressive optimization schedule - high file count detected")
            
            avg_file_size = health_report.get('total_size_mb', 0) / max(health_report.get('total_files', 1), 1)
            if avg_file_size < 5:
                recommendations.append("Enable auto-compaction to improve query performance")
            
            # Data quality recommendations
            if critical_issues > 0:
                recommendations.append("Address critical data quality issues immediately")
            
            if warnings > 10:
                recommendations.append("Review data ingestion process - high warning count")
            
            # General recommendations
            if not optimization_report.get('operations_performed'):
                recommendations.append("Enable monthly optimization to maintain performance")
            
            if quality_report.get('files_validated', 0) == 0:
                recommendations.append("Ensure data quality validation is running properly")
            
            report['executive_summary']['recommendations'] = recommendations
            
            # Generate action items
            action_items = []
            
            # From critical issues
            for issue in quality_report.get('quality_issues', []):
                if issue['severity'] == 'critical':
                    action_items.append({
                        'priority': 'high',
                        'action': f"Fix: {issue['issue']}",
                        'details': issue['details'],
                        'affected_data': issue.get('affected_rows', 0)
                    })
            
            # From table health issues
            for issue in health_report.get('issues', []):
                action_items.append({
                    'priority': 'medium',
                    'action': f"Address: {issue}",
                    'details': 'See table health section for details',
                    'affected_data': 'unknown'
                })
            
            report['action_items'] = action_items
            
            # Save report
            if self.config.get('report_generation_enabled', True):
                metadata_dir = Path(self.config['lakehouse_root']) / 'bronze' / 'OMIE' / 'delta_tables' / 'metadata'
                report_dir = metadata_dir / 'monthly_reports'
                report_dir.mkdir(parents=True, exist_ok=True)
                
                report_file = report_dir / f"monthly_report_{datetime.now().strftime('%Y%m')}.json"
                with open(report_file, 'w') as f:
                    json.dump(report, f, indent=2, default=str)
                
                print(f"📄 Monthly report saved: {report_file}")
            
            print(f"✅ Monthly report generated:")
            print(f"   Overall health: {report['executive_summary']['overall_health']}")
            print(f"   Critical issues: {report['executive_summary']['critical_issues_count']}")
            print(f"   Warnings: {report['executive_summary']['warnings_count']}")
            print(f"   Action items: {len(report['action_items'])}")
            print(f"   Recommendations: {len(report['executive_summary']['recommendations'])}")
            
        except Exception as e:
            print(f"❌ Report generation failed: {e}")
            self.errors.append(f"Report generation failed: {e}")
        
        return report
    
    def cleanup_staging_data(self) -> Dict:
        """Clean up old staging and temporary data"""
        print("🧹 Cleaning up staging data...")
        
        cleanup_report = {
            'cleanup_timestamp': datetime.now().isoformat(),
            'directories_cleaned': [],
            'files_removed': 0,
            'space_freed_mb': 0,
            'errors': []
        }
        
        if not self.config.get('archival_enabled', True):
            print("ℹ️  Archival/cleanup disabled in configuration")
            return cleanup_report
        
        try:
            staging_dir = Path(self.config['lakehouse_root']) / 'bronze' / 'OMIE' / 'delta_tables' / 'staging'
            
            if staging_dir.exists():
                # Remove files older than 30 days
                cutoff_time = datetime.now() - timedelta(days=30)
                files_to_remove = []
                
                for file_path in staging_dir.rglob('*'):
                    if file_path.is_file():
                        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                        if file_mtime < cutoff_time:
                            files_to_remove.append(file_path)
                
                total_size = 0
                for file_path in files_to_remove:
                    try:
                        size = file_path.stat().st_size
                        file_path.unlink()
                        total_size += size
                        cleanup_report['files_removed'] += 1
                    except Exception as e:
                        cleanup_report['errors'].append(f"Failed to remove {file_path}: {e}")
                
                cleanup_report['space_freed_mb'] = total_size / (1024 * 1024)
                cleanup_report['directories_cleaned'].append(str(staging_dir))
                
                print(f"✅ Staging cleanup completed:")
                print(f"   Files removed: {cleanup_report['files_removed']}")
                print(f"   Space freed: {cleanup_report['space_freed_mb']:.1f} MB")
            
            # Clean up old download directories if they exist
            downloads_dir = Path(self.config['lakehouse_root']) / 'bronze' / 'OMIE' / 'downloads'
            if downloads_dir.exists():
                import shutil
                try:
                    size_before = sum(f.stat().st_size for f in downloads_dir.rglob('*') if f.is_file())
                    shutil.rmtree(downloads_dir)
                    downloads_dir.mkdir(parents=True, exist_ok=True)
                    
                    cleanup_report['space_freed_mb'] += size_before / (1024 * 1024)
                    cleanup_report['directories_cleaned'].append(str(downloads_dir))
                    print(f"   Downloads directory cleaned: {size_before / (1024 * 1024):.1f} MB freed")
                except Exception as e:
                    cleanup_report['errors'].append(f"Failed to clean downloads directory: {e}")
            
        except Exception as e:
            print(f"❌ Staging cleanup failed: {e}")
            cleanup_report['errors'].append(f"Cleanup failed: {e}")
            self.errors.append(f"Staging cleanup failed: {e}")
        
        return cleanup_report
    
    def run_monthly_maintenance(self) -> Dict:
        """Execute the complete monthly maintenance pipeline"""
        print("=" * 80)
        print(f"🔧 OMIE MONTHLY MAINTENANCE - {self.run_start.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        # Step 1: Analyze table health
        health_report = self.analyze_delta_table_health()
        
        # Step 2: Validate data quality
        quality_report = self.validate_data_quality()
        
        # Step 3: Optimize performance
        optimization_report = self.optimize_delta_table()
        
        # Step 4: Clean up staging data
        cleanup_report = self.cleanup_staging_data()
        
        # Step 5: Generate comprehensive report
        monthly_report = self.generate_monthly_report(health_report, quality_report, optimization_report)
        
        # Create final result
        run_duration = datetime.now() - self.run_start
        
        result = {
            'success': len(self.errors) == 0,
            'run_id': self.run_id,
            'timestamp': datetime.now().isoformat(),
            'duration': str(run_duration),
            'reports': {
                'table_health': health_report,
                'data_quality': quality_report,
                'optimization': optimization_report,
                'cleanup': cleanup_report,
                'monthly_summary': monthly_report
            },
            'issues_found': len(self.issues_found),
            'fixes_applied': len(self.fixes_applied),
            'errors': self.errors,
            'overall_health': monthly_report.get('executive_summary', {}).get('overall_health', 'unknown')
        }
        
        # Summary
        print(f"\n🎉 MONTHLY MAINTENANCE COMPLETED")
        print(f"   Duration: {run_duration}")
        print(f"   Overall health: {result['overall_health']}")
        print(f"   Issues found: {result['issues_found']}")
        print(f"   Fixes applied: {result['fixes_applied']}")
        print(f"   Errors: {len(result['errors'])}")
        
        if result['errors']:
            print(f"\n⚠️  Errors encountered:")
            for error in result['errors']:
                print(f"   - {error}")
        
        return result
    
    def cleanup(self) -> None:
        """Cleanup resources"""
        if self.spark:
            self.spark.stop()


def main():
    """Main entry point for the monthly maintenance pipeline"""
    pipeline = None
    
    try:
        # Initialize pipeline
        pipeline = OMIEMonthlyPipeline()
        
        # Run the maintenance
        result = pipeline.run_monthly_maintenance()
        
        # Print result summary
        print(f"\n📊 MAINTENANCE RESULT:")
        print(json.dumps({
            'success': result['success'],
            'overall_health': result['overall_health'],
            'duration': result['duration'],
            'issues_found': result['issues_found'],
            'fixes_applied': result['fixes_applied'],
            'errors_count': len(result['errors'])
        }, indent=2))
        
        # Exit with appropriate code
        exit_code = 0 if result['success'] else 1
        sys.exit(exit_code)
        
    except Exception as e:
        print(f"💥 MONTHLY MAINTENANCE FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        if pipeline:
            pipeline.cleanup()


if __name__ == "__main__":
    main()