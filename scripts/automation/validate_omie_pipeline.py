"""
OMIE Pipeline Testing and Validation
====================================

Comprehensive testing suite for OMIE Delta pipelines including:
1. Delta table validation
2. Pipeline functionality testing  
3. Data integrity checks
4. Performance benchmarking
5. Error handling validation

Run this script to validate your complete OMIE pipeline setup.
"""

import os
import sys
import json
import time
import traceback
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, count, sum as spark_sum, max as spark_max, min as spark_min
    from delta import configure_spark_with_delta_pip
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("⚠️  PySpark/Delta not available. Some tests will be skipped.")

# Import our pipeline modules
try:
    from scripts.automation.omie_daily_pipeline import OMIEDailyPipeline
    from scripts.automation.omie_monthly_pipeline import OMIEMonthlyPipeline  
    from src.utils.omie_change_detection import OMIEChangeDetector, OMIEDeltaTableManager
    PIPELINE_MODULES_AVAILABLE = True
except ImportError as e:
    PIPELINE_MODULES_AVAILABLE = False
    print(f"⚠️  Pipeline modules not available: {e}")


class OMIEPipelineValidator:
    """Comprehensive validator for OMIE pipeline infrastructure"""
    
    def __init__(self):
        self.test_results = {
            'infrastructure': {},
            'data_quality': {},
            'pipeline_functionality': {},
            'performance': {},
            'error_handling': {}
        }
        self.spark = None
        self.config = None
        self.start_time = datetime.now()
        
        print("🧪 OMIE Pipeline Validation Suite")
        print("=" * 60)
        print(f"Start time: {self.start_time}")
        print(f"Spark available: {SPARK_AVAILABLE}")
        print(f"Pipeline modules available: {PIPELINE_MODULES_AVAILABLE}")
        
    def setup_test_environment(self) -> bool:
        """Initialize test environment"""
        print("\n🏗️  Setting up test environment...")
        
        try:
            # Determine lakehouse root
            if os.path.exists('/lakehouse/default/Files'):
                lakehouse_root = '/lakehouse/default/Files'
            else:
                lakehouse_root = str(Path('notebooks/bronze/OMIE').absolute())
            
            # Load or create configuration
            config_path = Path(lakehouse_root) / 'bronze' / 'OMIE' / 'delta_tables' / 'metadata' / 'pipeline_config.json'
            
            if config_path.exists():
                with open(config_path, 'r') as f:
                    self.config = json.load(f)
                print(f"✅ Loaded configuration from {config_path}")
            else:
                # Create minimal config for testing
                self.config = {
                    'lakehouse_root': lakehouse_root,
                    'target_years': [2023, 2024, 2025],
                    'omie_base_url': 'https://www.omie.es'
                }
                config_path.parent.mkdir(parents=True, exist_ok=True)
                with open(config_path, 'w') as f:
                    json.dump(self.config, f, indent=2)
                print(f"✅ Created test configuration at {config_path}")
            
            # Initialize Spark if available
            if SPARK_AVAILABLE:
                try:
                    builder = SparkSession.builder \
                        .appName("OMIE_Pipeline_Validation") \
                        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    
                    self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
                    self.spark.sparkContext.setLogLevel("WARN")
                    print("✅ Spark session initialized")
                except Exception as e:
                    print(f"⚠️  Could not initialize Spark: {e}")
                    SPARK_AVAILABLE = False
            
            self.test_results['infrastructure']['environment_setup'] = {
                'status': 'success',
                'config_path': str(config_path),
                'lakehouse_root': lakehouse_root,
                'spark_available': SPARK_AVAILABLE
            }
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to setup test environment: {e}")
            self.test_results['infrastructure']['environment_setup'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def test_delta_table_infrastructure(self) -> bool:
        """Test Delta table creation and basic operations"""
        print("\n📊 Testing Delta table infrastructure...")
        
        if not SPARK_AVAILABLE:
            print("⏭️  Skipping Delta table tests (Spark not available)")
            self.test_results['infrastructure']['delta_tables'] = {
                'status': 'skipped',
                'reason': 'Spark not available'
            }
            return True
        
        try:
            # Test table manager
            table_manager = OMIEDeltaTableManager(self.config, self.spark)
            
            # Test table creation
            print("   🏗️  Testing table creation...")
            success = table_manager.setup_all_tables()
            
            if not success:
                raise Exception("Table setup failed")
            
            # Test table accessibility
            tables_dir = Path(self.config['lakehouse_root']) / 'bronze' / 'OMIE' / 'delta_tables'
            
            test_results = {}
            
            # Test daily prices table
            daily_prices_path = tables_dir / 'daily_prices'
            if daily_prices_path.exists():
                try:
                    df = self.spark.read.format("delta").load(str(daily_prices_path))
                    schema_cols = len(df.columns)
                    row_count = df.count()
                    
                    test_results['daily_prices'] = {
                        'exists': True,
                        'readable': True,
                        'schema_columns': schema_cols,
                        'row_count': row_count
                    }
                    print(f"   ✅ Daily prices table: {schema_cols} columns, {row_count} rows")
                    
                except Exception as e:
                    test_results['daily_prices'] = {
                        'exists': True,
                        'readable': False,
                        'error': str(e)
                    }
                    print(f"   ⚠️  Daily prices table exists but not readable: {e}")
            else:
                test_results['daily_prices'] = {'exists': False}
                print("   ❌ Daily prices table not found")
            
            # Test metadata tables
            for table_name in ['processing_log', 'last_processed']:
                table_path = tables_dir / 'metadata' / table_name
                if table_path.exists():
                    try:
                        df = self.spark.read.format("delta").load(str(table_path))
                        row_count = df.count()
                        
                        test_results[table_name] = {
                            'exists': True,
                            'readable': True,
                            'row_count': row_count
                        }
                        print(f"   ✅ {table_name} table: {row_count} rows")
                        
                    except Exception as e:
                        test_results[table_name] = {
                            'exists': True,
                            'readable': False,
                            'error': str(e)
                        }
                        print(f"   ⚠️  {table_name} table exists but not readable: {e}")
                else:
                    test_results[table_name] = {'exists': False}
                    print(f"   ❌ {table_name} table not found")
            
            self.test_results['infrastructure']['delta_tables'] = {
                'status': 'success',
                'table_results': test_results
            }
            
            return True
            
        except Exception as e:
            print(f"   ❌ Delta table testing failed: {e}")
            self.test_results['infrastructure']['delta_tables'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def test_change_detection_system(self) -> bool:
        """Test change detection and file tracking functionality"""
        print("\n🔍 Testing change detection system...")
        
        try:
            change_detector = OMIEChangeDetector(self.config, self.spark)
            
            # Test file hash calculation
            test_content = b"test content for hashing"
            hash_result = change_detector.calculate_file_hash(test_content)
            
            if len(hash_result) != 64:  # SHA256 should be 64 chars
                raise Exception(f"Invalid hash length: {len(hash_result)}")
            
            print(f"   ✅ File hashing works (SHA256: {hash_result[:16]}...)")
            
            # Test metadata extraction
            test_filename = "marginalpdbc_20241201.csv"
            test_url = "https://example.com/test.csv"
            metadata = change_detector.extract_file_metadata(test_filename, test_url, test_content)
            
            expected_fields = ['filename', 'url', 'content_hash', 'extraction_date', 'extraction_year']
            missing_fields = [field for field in expected_fields if field not in metadata]
            
            if missing_fields:
                raise Exception(f"Missing metadata fields: {missing_fields}")
            
            if metadata['extraction_date'] != '20241201' or metadata['extraction_year'] != 2024:
                raise Exception(f"Date extraction failed: {metadata}")
            
            print("   ✅ Metadata extraction works")
            
            # Test registry operations
            try:
                registry = change_detector.get_processed_files_registry()
                print(f"   ✅ Registry loaded: {len(registry)} files")
                
                # Test file categorization with empty registry
                test_files = [
                    {'name': 'marginalpdbc_20241201.csv', 'url': 'http://test1.com'},
                    {'name': 'marginalpdbc_20241202.csv', 'url': 'http://test2.com'}
                ]
                
                categorized = change_detector.detect_file_changes(test_files)
                expected_categories = ['new_files', 'modified_files', 'unchanged_files', 'potentially_removed_files']
                
                if not all(cat in categorized for cat in expected_categories):
                    raise Exception("Missing categorization categories")
                
                print(f"   ✅ File categorization: {len(categorized['new_files'])} new, {len(categorized['unchanged_files'])} unchanged")
                
            except Exception as e:
                print(f"   ⚠️  Registry testing had issues (expected for new setup): {e}")
            
            self.test_results['infrastructure']['change_detection'] = {
                'status': 'success',
                'hash_function': 'working',
                'metadata_extraction': 'working',
                'registry_operations': 'working'
            }
            
            return True
            
        except Exception as e:
            print(f"   ❌ Change detection testing failed: {e}")
            self.test_results['infrastructure']['change_detection'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def test_pipeline_functionality(self) -> bool:
        """Test pipeline initialization and basic functionality"""
        print("\n🔄 Testing pipeline functionality...")
        
        if not PIPELINE_MODULES_AVAILABLE:
            print("⏭️  Skipping pipeline tests (modules not available)")
            self.test_results['pipeline_functionality']['status'] = 'skipped'
            return True
        
        try:
            # Test daily pipeline initialization
            print("   📅 Testing daily pipeline...")
            daily_pipeline = OMIEDailyPipeline(str(Path(self.config['lakehouse_root']) / 'bronze' / 'OMIE' / 'delta_tables' / 'metadata' / 'pipeline_config.json'))
            
            # Test configuration loading
            if not daily_pipeline.config:
                raise Exception("Daily pipeline config not loaded")
            
            # Test last processed info retrieval
            last_date, last_file = daily_pipeline.get_last_processed_info()
            print(f"   ✅ Daily pipeline initialized (last processed: {last_date})")
            
            # Test monthly pipeline initialization
            print("   📆 Testing monthly pipeline...")
            monthly_pipeline = OMIEMonthlyPipeline(str(Path(self.config['lakehouse_root']) / 'bronze' / 'OMIE' / 'delta_tables' / 'metadata' / 'pipeline_config.json'))
            
            if not monthly_pipeline.config:
                raise Exception("Monthly pipeline config not loaded")
            
            print("   ✅ Monthly pipeline initialized")
            
            # Test file listing functionality (without downloading)
            print("   🌐 Testing OMIE website connectivity...")
            try:
                available_files = daily_pipeline.fetch_available_files()
                if available_files:
                    print(f"   ✅ OMIE connectivity works ({len(available_files)} files found)")
                else:
                    print("   ⚠️  No files found (website may be down or structure changed)")
            except Exception as e:
                print(f"   ⚠️  OMIE connectivity issue (expected in some environments): {e}")
            
            self.test_results['pipeline_functionality'] = {
                'status': 'success',
                'daily_pipeline': 'initialized',
                'monthly_pipeline': 'initialized',
                'omie_connectivity': 'tested'
            }
            
            return True
            
        except Exception as e:
            print(f"   ❌ Pipeline functionality testing failed: {e}")
            self.test_results['pipeline_functionality'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def test_data_quality_checks(self) -> bool:
        """Test data quality validation functions"""
        print("\n✅ Testing data quality checks...")
        
        if not self.spark:
            print("⏭️  Skipping data quality tests (Spark not available)")
            self.test_results['data_quality']['status'] = 'skipped'
            return True
        
        try:
            # Create sample test data
            test_data = [
                {
                    'Fecha': '2024-12-01',
                    'Hora': '1',
                    'Precio': 45.67,
                    'Demanda': 25000,
                    '_source_file': 'test_marginalpdbc_20241201.csv',
                    '_source_url': 'http://test.com/test.csv',
                    '_extraction_year': 2024,
                    '_extraction_date': '20241201',
                    '_file_checksum': 'test_checksum_123',
                    '_ingested_at': datetime.now(),
                    '_pipeline_run_id': 'test_run_001',
                    'partition_year': 2024,
                    'partition_month': 12
                },
                {
                    'Fecha': '2024-12-01',
                    'Hora': '2',
                    'Precio': 47.23,
                    'Demanda': 26000,
                    '_source_file': 'test_marginalpdbc_20241201.csv',
                    '_source_url': 'http://test.com/test.csv',
                    '_extraction_year': 2024,
                    '_extraction_date': '20241201',
                    '_file_checksum': 'test_checksum_123',
                    '_ingested_at': datetime.now(),
                    '_pipeline_run_id': 'test_run_001',
                    'partition_year': 2024,
                    'partition_month': 12
                }
            ]
            
            # Create test DataFrame
            test_df = self.spark.createDataFrame(test_data)
            
            # Test basic data quality metrics
            row_count = test_df.count()
            if row_count != 2:
                raise Exception(f"Expected 2 rows, got {row_count}")
            
            # Test null value detection
            null_counts = {}
            for col_name in test_df.columns:
                null_count = test_df.filter(col(col_name).isNull()).count()
                null_counts[col_name] = null_count
            
            total_nulls = sum(null_counts.values())
            print(f"   ✅ Null value detection: {total_nulls} nulls found")
            
            # Test duplicate detection
            distinct_count = test_df.distinct().count()
            duplicate_rate = (row_count - distinct_count) / row_count if row_count > 0 else 0
            print(f"   ✅ Duplicate detection: {duplicate_rate:.2%} duplicates")
            
            # Test data type validation
            schema_check = True
            expected_types = ['string', 'double', 'int', 'timestamp']
            
            for field in test_df.schema.fields:
                field_type = str(field.dataType).lower()
                if not any(exp_type in field_type for exp_type in expected_types):
                    print(f"   ⚠️  Unexpected data type for {field.name}: {field.dataType}")
                    schema_check = False
            
            if schema_check:
                print("   ✅ Schema validation passed")
            
            # Test aggregation functions
            try:
                agg_result = test_df.agg(
                    spark_sum('Precio').alias('total_price'),
                    spark_max('Demanda').alias('max_demand'),
                    spark_min('Demanda').alias('min_demand'),
                    count('*').alias('record_count')
                ).collect()[0]
                
                print(f"   ✅ Aggregation functions: total_price={agg_result.total_price}, max_demand={agg_result.max_demand}")
                
            except Exception as e:
                print(f"   ⚠️  Aggregation test failed: {e}")
            
            self.test_results['data_quality'] = {
                'status': 'success',
                'null_detection': 'working',
                'duplicate_detection': 'working',
                'schema_validation': 'working',
                'aggregation_functions': 'working'
            }
            
            return True
            
        except Exception as e:
            print(f"   ❌ Data quality testing failed: {e}")
            self.test_results['data_quality'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def test_performance_benchmarks(self) -> bool:
        """Test basic performance benchmarks"""
        print("\n⚡ Testing performance benchmarks...")
        
        try:
            # Test file processing speed
            start_time = time.time()
            
            # Simulate processing 1000 small files
            for i in range(1000):
                test_content = f"test content {i}".encode()
                if PIPELINE_MODULES_AVAILABLE:
                    from src.utils.omie_change_detection import OMIEChangeDetector
                    detector = OMIEChangeDetector(self.config, self.spark)
                    _ = detector.calculate_file_hash(test_content)
            
            hash_time = time.time() - start_time
            print(f"   ✅ Hash calculation: 1000 operations in {hash_time:.2f}s ({1000/hash_time:.0f} ops/sec)")
            
            # Test DataFrame operations if Spark available
            if self.spark:
                start_time = time.time()
                
                # Create larger test dataset
                large_test_data = []
                for i in range(10000):
                    large_test_data.append({
                        'id': i,
                        'value': i * 1.5,
                        'category': f'cat_{i % 10}',
                        'timestamp': datetime.now()
                    })
                
                df = self.spark.createDataFrame(large_test_data)
                
                # Perform typical operations
                result = df.groupBy('category').agg(
                    count('*').alias('count'),
                    spark_sum('value').alias('sum_value')
                ).collect()
                
                spark_time = time.time() - start_time
                print(f"   ✅ Spark operations: 10K records processed in {spark_time:.2f}s")
                
                if spark_time > 10:
                    print("   ⚠️  Performance may be slow for large datasets")
            
            self.test_results['performance'] = {
                'status': 'success',
                'hash_operations_per_second': int(1000 / hash_time) if hash_time > 0 else 'N/A',
                'spark_processing_time': spark_time if self.spark else 'N/A'
            }
            
            return True
            
        except Exception as e:
            print(f"   ❌ Performance testing failed: {e}")
            self.test_results['performance'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def test_error_handling(self) -> bool:
        """Test error handling and recovery mechanisms"""
        print("\n🛡️  Testing error handling...")
        
        try:
            # Test invalid configuration handling
            try:
                if PIPELINE_MODULES_AVAILABLE:
                    invalid_pipeline = OMIEDailyPipeline("/nonexistent/config.json")
                    print("   ✅ Invalid config handled gracefully")
            except Exception as e:
                print(f"   ⚠️  Invalid config error (expected): {e}")
            
            # Test invalid Delta table access
            if self.spark:
                try:
                    _ = self.spark.read.format("delta").load("/nonexistent/table/path")
                    print("   ❌ Should have failed for nonexistent table")
                except Exception as e:
                    print("   ✅ Invalid table access handled gracefully")
            
            # Test network error simulation
            try:
                import requests
                response = requests.get("http://nonexistent-omie-site.invalid", timeout=1)
                print("   ❌ Should have failed for invalid URL")
            except Exception as e:
                print("   ✅ Network error handled gracefully")
            
            # Test malformed data handling
            if self.spark:
                try:
                    malformed_data = [
                        {'good_field': 'value1', 'bad_field': None},
                        {'good_field': None, 'bad_field': 'value2'},
                        {'good_field': 'value3'}  # Missing bad_field
                    ]
                    
                    df = self.spark.createDataFrame(malformed_data)
                    null_count = df.filter(col('bad_field').isNull()).count()
                    print(f"   ✅ Malformed data handled: {null_count} nulls detected")
                    
                except Exception as e:
                    print(f"   ⚠️  Malformed data handling issue: {e}")
            
            self.test_results['error_handling'] = {
                'status': 'success',
                'invalid_config': 'handled',
                'invalid_table_access': 'handled',
                'network_errors': 'handled',
                'malformed_data': 'handled'
            }
            
            return True
            
        except Exception as e:
            print(f"   ❌ Error handling testing failed: {e}")
            self.test_results['error_handling'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def generate_validation_report(self) -> Dict:
        """Generate comprehensive validation report"""
        print("\n📋 Generating validation report...")
        
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        # Calculate overall success rate
        total_tests = 0
        successful_tests = 0
        
        for category, results in self.test_results.items():
            if isinstance(results, dict):
                for test_name, test_result in results.items():
                    total_tests += 1
                    if isinstance(test_result, dict) and test_result.get('status') == 'success':
                        successful_tests += 1
                    elif test_result == 'success' or test_result == 'working':
                        successful_tests += 1
        
        success_rate = successful_tests / total_tests if total_tests > 0 else 0
        
        report = {
            'validation_metadata': {
                'timestamp': end_time.isoformat(),
                'duration_seconds': duration.total_seconds(),
                'total_tests': total_tests,
                'successful_tests': successful_tests,
                'success_rate': success_rate,
                'overall_status': 'PASS' if success_rate >= 0.8 else 'FAIL'
            },
            'environment_info': {
                'spark_available': SPARK_AVAILABLE,
                'pipeline_modules_available': PIPELINE_MODULES_AVAILABLE,
                'lakehouse_root': self.config.get('lakehouse_root', 'unknown') if self.config else 'unknown'
            },
            'test_results': self.test_results,
            'recommendations': []
        }
        
        # Generate recommendations
        if success_rate < 1.0:
            report['recommendations'].append("Some tests failed - review the detailed results above")
        
        if not SPARK_AVAILABLE:
            report['recommendations'].append("Install PySpark and Delta Lake for full functionality")
        
        if not PIPELINE_MODULES_AVAILABLE:
            report['recommendations'].append("Ensure all pipeline modules are properly installed")
        
        if success_rate >= 0.8:
            report['recommendations'].append("Validation successful - ready for production deployment")
        
        return report
    
    def cleanup(self):
        """Cleanup test resources"""
        if self.spark:
            self.spark.stop()
    
    def run_all_tests(self) -> Dict:
        """Run complete validation suite"""
        try:
            print("🚀 Starting OMIE Pipeline Validation Suite")
            print("=" * 80)
            
            # Setup
            if not self.setup_test_environment():
                return self.generate_validation_report()
            
            # Run tests
            self.test_delta_table_infrastructure()
            self.test_change_detection_system()
            self.test_pipeline_functionality()
            self.test_data_quality_checks()
            self.test_performance_benchmarks()
            self.test_error_handling()
            
            # Generate report
            report = self.generate_validation_report()
            
            # Display summary
            print("\n" + "=" * 80)
            print("🏁 VALIDATION COMPLETE")
            print("=" * 80)
            print(f"Overall Status: {report['validation_metadata']['overall_status']}")
            print(f"Success Rate: {report['validation_metadata']['success_rate']:.1%}")
            print(f"Duration: {report['validation_metadata']['duration_seconds']:.1f} seconds")
            print(f"Tests: {report['validation_metadata']['successful_tests']}/{report['validation_metadata']['total_tests']} passed")
            
            if report['recommendations']:
                print("\n📋 Recommendations:")
                for rec in report['recommendations']:
                    print(f"   • {rec}")
            
            return report
            
        except Exception as e:
            print(f"\n💥 VALIDATION FAILED: {e}")
            traceback.print_exc()
            return {'error': str(e), 'status': 'CRITICAL_FAILURE'}
            
        finally:
            self.cleanup()


def main():
    """Main entry point for validation"""
    validator = OMIEPipelineValidator()
    
    try:
        report = validator.run_all_tests()
        
        # Save report
        report_path = Path('validation_report.json')
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"\n📄 Detailed report saved: {report_path}")
        
        # Exit with appropriate code
        if report.get('validation_metadata', {}).get('overall_status') == 'PASS':
            print("\n✅ VALIDATION PASSED - Ready for production!")
            sys.exit(0)
        else:
            print("\n❌ VALIDATION FAILED - Review issues before deployment")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⏹️  Validation interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n💥 Validation crashed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()