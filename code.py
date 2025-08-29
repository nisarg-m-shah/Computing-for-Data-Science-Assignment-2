import json
import gzip
import hashlib
import datetime
import pandas as pd
from typing import Dict, List, Any, Tuple
from collections import defaultdict
import zlib
import pickle
import math

class AdaptiveDataManager:
    """
    Adaptive Multi-Tier Data Management System
    Implements intelligent data reduction strategies based on content analysis
    """
    
    def __init__(self, compression_threshold: int = 1000, 
                 dedup_window_hours: int = 24,
                 aggregation_window_minutes: int = 60):
        self.compression_threshold = compression_threshold
        self.dedup_window_hours = dedup_window_hours
        self.aggregation_window_minutes = aggregation_window_minutes
        
        # Storage for different data tiers
        self.hot_storage = {}  # Recent, frequently accessed data
        self.warm_storage = {}  # Compressed recent data
        self.cold_storage = {}  # Heavily compressed/aggregated historical data
        
        # Deduplication tracking
        self.content_hashes = defaultdict(list)
        self.temporal_signatures = {}
        
        # Statistics tracking
        self.stats = {
            'original_size': 0,
            'compressed_size': 0,
            'deduplicated_items': 0,
            'aggregated_records': 0
        }
    
    def analyze_data_characteristics(self, data: Any, data_type: str = 'general') -> Dict[str, Any]:
        text = str(data)
        characteristics = {
            'type': type(data).__name__,
            'size': len(text),
            'entropy': self._calculate_entropy(text),
            'repetition_ratio': self._calculate_repetition_ratio(text),
            'timestamp': datetime.datetime.now()
        }

        # Strategy selection
        if characteristics['size'] > self.compression_threshold:
            strategy = 'compress'
        elif characteristics['repetition_ratio'] > 0.5:
            strategy = 'deduplicate'
        elif data_type in ['time_series', 'iot_telemetry', 'user_transactions']:
            strategy = 'aggregate'
        else:
            strategy = 'store_direct'

        characteristics['strategy'] = strategy
        return characteristics

    
    import math

    def _calculate_entropy(self, text: str) -> float:
        """Calculate information entropy of text"""
        if not text:
            return 0.0

        char_counts = defaultdict(int)
        for char in text:
            char_counts[char] += 1

        length = len(text)
        entropy = 0.0
        for count in char_counts.values():
            probability = count / length
            if probability > 0:
                entropy -= probability * math.log2(probability)

        # Normalize entropy to [0, 1]
        max_entropy = math.log2(len(char_counts)) if len(char_counts) > 1 else 1
        return entropy / max_entropy if max_entropy > 0 else 0.0

    
    def _calculate_repetition_ratio(self, text: str) -> float:
        """Calculate ratio of repeated content"""
        if len(text) < 10:
            return 0
        
        words = text.split()
        if len(words) < 2:
            return 0
        
        unique_words = set(words)
        return 1 - (len(unique_words) / len(words))
    
    def compress_data(self, data: Any, algorithm: str = 'zlib') -> bytes:
        """
        Compress data using specified algorithm
        """
        serialized = json.dumps(data) if not isinstance(data, str) else data
        
        if algorithm == 'zlib':
            compressed = zlib.compress(serialized.encode('utf-8'), level=9)
        elif algorithm == 'gzip':
            compressed = gzip.compress(serialized.encode('utf-8'), compresslevel=9)
        else:
            compressed = serialized.encode('utf-8')
        
        return compressed
    
    def decompress_data(self, compressed_data: bytes, algorithm: str = 'zlib') -> Any:
        """
        Decompress data using specified algorithm
        """
        if algorithm == 'zlib':
            decompressed = zlib.decompress(compressed_data).decode('utf-8')
        elif algorithm == 'gzip':
            decompressed = gzip.decompress(compressed_data).decode('utf-8')
        else:
            decompressed = compressed_data.decode('utf-8')
        
        try:
            return json.loads(decompressed)
        except json.JSONDecodeError:
            return decompressed
    
    def calculate_content_hash(self, data: Any, data_type: str = 'general') -> str:
        if data_type == 'server_logs':
            content = f"{data.get('level')}-{data.get('endpoint')}-{data.get('status_code')}"
        elif data_type == 'iot_telemetry':
            content = f"{data.get('device_id')}-{round(data.get('temperature', 0), 1)}"
        elif data_type == 'user_transactions':
            content = f"{data.get('user_id')}-{data.get('merchant')}-{round(data.get('amount', 0))}"
        else:
            content = str(data)
        return hashlib.sha256(content.encode('utf-8')).hexdigest()

    
    def is_duplicate(self, data: Any, similarity_threshold: float = 0.95) -> bool:
        """
        Check if data is duplicate based on content similarity
        """
        current_hash = self.calculate_content_hash(data)
        current_time = datetime.datetime.now()
        
        # Check for exact duplicates
        if current_hash in self.content_hashes:
            recent_entries = [
                entry for entry in self.content_hashes[current_hash]
                if (current_time - entry['timestamp']).total_seconds() < self.dedup_window_hours * 3600
            ]
            if recent_entries:
                self.stats['deduplicated_items'] += 1
                return True
        
        # Store current hash with timestamp
        self.content_hashes[current_hash].append({
            'timestamp': current_time,
            'data_preview': str(data)[:100]
        })
        
        return False
    
    def aggregate_time_series(self, data_points: List[Dict], 
                            time_field: str = 'timestamp',
                            value_fields: List[str] = None) -> Dict:
        """
        Aggregate time-series data into summary statistics
        """
        if not data_points or not value_fields:
            return {}
        
        aggregated = {
            'period_start': min(point[time_field] for point in data_points),
            'period_end': max(point[time_field] for point in data_points),
            'record_count': len(data_points),
            'aggregated_values': {}
        }
        
        for field in value_fields:
            values = [point[field] for point in data_points if field in point and isinstance(point[field], (int, float))]
            if values:
                aggregated['aggregated_values'][field] = {
                    'min': min(values),
                    'max': max(values),
                    'avg': sum(values) / len(values),
                    'sum': sum(values),
                    'count': len(values)
                }
        
        return aggregated
    
    def process_data_batch(self, data_batch: List[Any], data_type: str = 'general') -> Dict[str, Any]:
        """
        Process a batch of data using AMDM strategies
        """
        results = {
            'processed_items': 0,
            'original_size': 0,
            'final_size': 0,
            'compression_ratio': 0,
            'strategies_applied': []
        }
        
        processed_data = []
        
        for item in data_batch:
            characteristics = self.analyze_data_characteristics(item)
            original_size = len(str(item))
            results['original_size'] += original_size
            self.stats['original_size'] += original_size
            
            # Skip duplicates
            if self.is_duplicate(item):
                results['strategies_applied'].append('deduplication')
                continue
            
            # Apply strategy based on characteristics
            if characteristics['strategy'] == 'compress':
                compressed = self.compress_data(item)
                self.warm_storage[f"compressed_{len(self.warm_storage)}"] = {
                    'data': compressed,
                    'algorithm': 'zlib',
                    'timestamp': characteristics['timestamp'],
                    'original_size': original_size
                }
                results['final_size'] += len(compressed)
                results['strategies_applied'].append('compression')
                
            elif characteristics['strategy'] == 'aggregate' and data_type == 'time_series':
                # For time-series data, group similar timestamps for aggregation
                processed_data.append(item)
                results['strategies_applied'].append('aggregation_candidate')
                
            else:
                # Store directly in hot storage
                self.hot_storage[f"direct_{len(self.hot_storage)}"] = {
                    'data': item,
                    'timestamp': characteristics['timestamp'],
                    'size': original_size
                }
                results['final_size'] += original_size
                results['strategies_applied'].append('direct_storage')
            
            results['processed_items'] += 1
        
        # Post-process aggregation candidates
        if processed_data and data_type == 'time_series':
            aggregated = self.aggregate_time_series(
                processed_data, 
                value_fields=['value', 'count', 'metric']
            )
            if aggregated:
                aggregated_size = len(str(aggregated))
                self.cold_storage[f"aggregated_{len(self.cold_storage)}"] = aggregated
                results['final_size'] += aggregated_size
                results['strategies_applied'].append('aggregation')
                self.stats['aggregated_records'] += len(processed_data)
        
        # Calculate compression ratio
        if results['original_size'] > 0:
            results['compression_ratio'] = (results['original_size'] - results['final_size']) / results['original_size']
        
        self.stats['compressed_size'] += results['final_size']
        
        return results
    
    def generate_sample_data(self, data_type: str, count: int = 1000) -> List[Dict]:
        """
        Generate sample data for testing different scenarios
        """
        if data_type == 'server_logs':
            return self._generate_server_logs(count)
        elif data_type == 'iot_telemetry':
            return self._generate_iot_data(count)
        elif data_type == 'user_transactions':
            return self._generate_transaction_data(count)
        else:
            return self._generate_generic_data(count)
    
    def _generate_server_logs(self, count: int) -> List[Dict]:
        """Generate realistic server log entries"""
        import random
        
        log_levels = ['INFO', 'WARNING', 'ERROR', 'DEBUG']
        endpoints = ['/api/users', '/api/orders', '/api/products', '/health', '/metrics']
        status_codes = [200, 201, 400, 404, 500]
        
        logs = []
        base_time = datetime.datetime.now()
        
        for i in range(count):
            # Introduce duplicates and patterns for realistic testing
            if random.random() < 0.3:  # 30% chance of duplicate pattern
                endpoint = '/health'
                status = 200
                level = 'INFO'
            else:
                endpoint = random.choice(endpoints)
                status = random.choice(status_codes)
                level = random.choice(log_levels)
            
            logs.append({
                'timestamp': (base_time + datetime.timedelta(seconds=i*10)).isoformat(),
                'level': level,
                'endpoint': endpoint,
                'status_code': status,
                'response_time': random.uniform(10, 500),
                'user_id': f"user_{random.randint(1, 100)}",
                'message': f"{level}: {endpoint} returned {status}"
            })
        
        return logs
    
    def _generate_iot_data(self, count: int) -> List[Dict]:
        """Generate IoT sensor telemetry data"""
        import random
        
        devices = ['sensor_001', 'sensor_002', 'sensor_003']
        base_time = datetime.datetime.now()
        
        data = []
        for i in range(count):
            device = random.choice(devices)
            # Simulate gradual changes in sensor readings
            base_temp = 20 + (i % 100) * 0.1
            
            data.append({
                'timestamp': (base_time + datetime.timedelta(minutes=i)).isoformat(),
                'device_id': device,
                'temperature': base_temp + random.uniform(-2, 2),
                'humidity': 45 + random.uniform(-10, 10),
                'battery_level': max(0, 100 - (i * 0.01)),
                'signal_strength': random.uniform(-80, -40)
            })
        
        return data
    
    def _generate_transaction_data(self, count: int) -> List[Dict]:
        """Generate financial transaction data"""
        import random
        
        merchants = ['Amazon', 'Starbucks', 'Shell', 'Walmart', 'Target']
        categories = ['retail', 'food', 'fuel', 'entertainment']
        
        transactions = []
        base_time = datetime.datetime.now()
        
        for i in range(count):
            transactions.append({
                'timestamp': (base_time + datetime.timedelta(hours=i*0.1)).isoformat(),
                'transaction_id': f"txn_{i:06d}",
                'user_id': f"user_{random.randint(1, 50)}",
                'merchant': random.choice(merchants),
                'category': random.choice(categories),
                'amount': round(random.uniform(5, 200), 2),
                'currency': 'USD'
            })
        
        return transactions
    
    def _generate_generic_data(self, count: int) -> List[Dict]:
        """Generate generic structured data"""
        import random
        
        data = []
        for i in range(count):
            data.append({
                'id': i,
                'value': random.uniform(0, 100),
                'category': f"cat_{random.randint(1, 5)}",
                'metadata': {
                    'source': 'generator',
                    'version': '1.0',
                    'quality_score': random.uniform(0.8, 1.0)
                }
            })
        
        return data
    
    def demonstrate_amdm(self, data_type: str = 'server_logs', sample_size: int = 1000):
        """
        Demonstrate the AMDM system with sample data
        """
        print(f"=== Adaptive Multi-Tier Data Management Demonstration ===")
        print(f"Data Type: {data_type}")
        print(f"Sample Size: {sample_size} records")
        print()
        
        # Generate sample data
        print("Generating sample data...")
        sample_data = self.generate_sample_data(data_type, sample_size)
        
        # Process data in batches to simulate real-world scenarios
        batch_size = 100
        total_results = {
            'processed_items': 0,
            'original_size': 0,
            'final_size': 0,
            'strategies_applied': []
        }
        
        print(f"Processing data in batches of {batch_size}...")
        
        for i in range(0, len(sample_data), batch_size):
            batch = sample_data[i:i+batch_size]
            batch_results = self.process_data_batch(batch, data_type)
            
            # Accumulate results
            total_results['processed_items'] += batch_results['processed_items']
            total_results['original_size'] += batch_results['original_size']
            total_results['final_size'] += batch_results['final_size']
            total_results['strategies_applied'].extend(batch_results['strategies_applied'])
        
        # Calculate final metrics
        if total_results['original_size'] > 0:
            compression_ratio = (total_results['original_size'] - total_results['final_size']) / total_results['original_size']
        else:
            compression_ratio = 0
        
        # Display results
        print(f"\n=== Processing Results ===")
        print(f"Items Processed: {total_results['processed_items']}")
        print(f"Original Data Size: {total_results['original_size']:,} bytes")
        print(f"Final Data Size: {total_results['final_size']:,} bytes")
        print(f"Space Savings: {compression_ratio:.2%}")
        print(f"Deduplication Saves: {self.stats['deduplicated_items']} items")
        
        # Strategy analysis
        strategy_counts = defaultdict(int)
        for strategy in total_results['strategies_applied']:
            strategy_counts[strategy] += 1
        
        print(f"\n=== Strategies Applied ===")
        for strategy, count in strategy_counts.items():
            print(f"{strategy}: {count} applications")
        
        # Storage tier analysis
        print(f"\n=== Storage Tier Distribution ===")
        print(f"Hot Storage: {len(self.hot_storage)} items")
        print(f"Warm Storage: {len(self.warm_storage)} items (compressed)")
        print(f"Cold Storage: {len(self.cold_storage)} items (aggregated)")
        
        return {
            'compression_ratio': compression_ratio,
            'space_savings_bytes': total_results['original_size'] - total_results['final_size'],
            'strategies_used': dict(strategy_counts),
            'storage_distribution': {
                'hot': len(self.hot_storage),
                'warm': len(self.warm_storage),
                'cold': len(self.cold_storage)
            }
        }
    
    def advanced_compression_comparison(self):
        """
        Compare different compression algorithms on various data types
        """
        print("=== Advanced Compression Algorithm Comparison ===")
        
        data_types = ['server_logs', 'iot_telemetry', 'user_transactions']
        algorithms = ['zlib', 'gzip', 'none']
        
        comparison_results = {}
        
        for data_type in data_types:
            print(f"\nTesting {data_type}:")
            sample_data = self.generate_sample_data(data_type, 100)
            sample_text = json.dumps(sample_data)
            original_size = len(sample_text.encode('utf-8'))
            
            comparison_results[data_type] = {'original_size': original_size}
            
            for algorithm in algorithms:
                if algorithm == 'none':
                    compressed_size = original_size
                else:
                    compressed = self.compress_data(sample_data, algorithm)
                    compressed_size = len(compressed)
                
                ratio = (original_size - compressed_size) / original_size if original_size > 0 else 0
                comparison_results[data_type][algorithm] = {
                    'compressed_size': compressed_size,
                    'compression_ratio': ratio
                }
                
                print(f"  {algorithm}: {compressed_size:,} bytes ({ratio:.2%} reduction)")
        
        return comparison_results

# Demonstration and Testing
def run_comprehensive_analysis():
    """
    Run comprehensive analysis of the AMDM system
    """
    manager = AdaptiveDataManager(
        compression_threshold=100,
        dedup_window_hours=12,
        aggregation_window_minutes=30
    )
    
    print("ADAPTIVE MULTI-TIER DATA MANAGEMENT SYSTEM")
    print("=" * 50)
    
    # Test different data types
    data_types = [
        ('server_logs', 800),
        ('iot_telemetry', 1200),
        ('user_transactions', 600)
    ]
    
    overall_results = {}
    
    for data_type, sample_size in data_types:
        print(f"\n{'='*20} {data_type.upper()} {'='*20}")
        results = manager.demonstrate_amdm(data_type, sample_size)
        overall_results[data_type] = results
    
    # Advanced compression comparison
    print(f"\n{'='*50}")
    compression_comparison = manager.advanced_compression_comparison()
    
    # Summary analysis
    print(f"\n{'='*20} OVERALL ANALYSIS {'='*20}")
    total_original = sum(r['space_savings_bytes'] + manager.stats['compressed_size'] for r in overall_results.values())
    total_saved = sum(r['space_savings_bytes'] for r in overall_results.values())
    
    if total_original > 0:
        overall_compression = total_saved / total_original
        print(f"Total Space Savings: {overall_compression:.2%}")
        print(f"Total Bytes Saved: {total_saved:,}")
    
    print(f"Total Deduplicated Items: {manager.stats['deduplicated_items']}")
    print(f"Total Aggregated Records: {manager.stats['aggregated_records']}")
    
    return overall_results, compression_comparison

# Execute the demonstration
if __name__ == "__main__":
    results, compression_data = run_comprehensive_analysis()