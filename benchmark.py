#!/usr/bin/env python3
import requests as http_requests
import time
import logging
import re
import sys
import json
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [Benchmark] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Strategy port mappings
STRATEGY_PORTS = {
    "direct": 3306,
    "random": 3307,
    "customized": 3308
}

class ClusterBenchmark:
    def __init__(self, gatekeeper_host, num_requests=1000):
        """Initialize benchmark configuration."""
        self.gatekeeper_url = f"http://{gatekeeper_host}:5000"
        self.num_requests = num_requests
        self.timeout = 30
        self.retry_limit = 3
        self.results = {
            'read': {'success': 0, 'fail': 0, 'time': 0, 'latencies': [], 'retries': 0},
            'write': {'success': 0, 'fail': 0, 'time': 0, 'latencies': [], 'retries': 0}
        }
        #self.warmup_requests = 10
        self.current_strategy = "direct"
        self.current_port = STRATEGY_PORTS["direct"]
        logger.info(f"Initialized benchmark for {gatekeeper_host}")

    def generate_read_query(self, i):
        """Generate simple SELECT query."""
        return f"SELECT * FROM actor WHERE actor_id = {(i % 200) + 1}"

    def generate_write_query(self, i):
        """Generate simple INSERT query."""
        timestamp = int(time.time())
        return f"INSERT INTO actor (first_name, last_name) VALUES ('Benchmark{timestamp}{i}', 'Test{timestamp}{i}')"

    def set_strategy(self, strategy):
        """Set routing strategy via Gatekeeper."""
        try:
            if strategy not in STRATEGY_PORTS:
                logger.error(f"Invalid strategy: {strategy}")
                return False

            response = http_requests.get(
                f"{self.gatekeeper_url}/set_strategy/{strategy}",
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('status') == 'success':
                    self.current_strategy = strategy
                    self.current_port = STRATEGY_PORTS[strategy]
                    logger.info(f"Successfully set strategy to: {strategy} (Port: {self.current_port})")
                    return True
                else:
                    logger.error(f"Failed to set strategy: {result}")
                    return False
            else:
                logger.error(f"Failed to set strategy: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error setting strategy: {str(e)}")
            return False

    def send_query(self, query, operation_type, results_dict=None):
        """Send a query with enhanced error handling."""
        results = results_dict if results_dict is not None else self.results[operation_type]
        retries = 0
        while retries < self.retry_limit:
            try:
                start_time = time.time()
                response = http_requests.post(
                    f"{self.gatekeeper_url}/query",
                    json={
                        'query': query,
                        'strategy': self.current_strategy,
                        'port': self.current_port
                    },
                    timeout=self.timeout
                )
                latency = time.time() - start_time

                if response.status_code == 200:
                    result = response.json()
                    if result.get('status') == 'success':
                        results['success'] += 1
                        results['latencies'].append(latency)
                        return
                    else:
                        logger.warning(f"Query failed: {result.get('message')}")
                else:
                    logger.warning(f"HTTP {response.status_code}: {response.text}")

            except http_requests.Timeout:
                logger.error("Request timed out")
            except http_requests.ConnectionError:
                logger.error("Connection failed")
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")

            retries += 1
            results['retries'] += 1
            time.sleep(1)

        results['fail'] += 1
    """"
    def warmup(self):
        '''Perform warmup requests.'''
        print(f"\nPerforming warmup phase (Strategy: {self.current_strategy}, Port: {self.current_port})...")
        warmup_results = {'success': 0, 'fail': 0, 'time': 0, 'latencies': [], 'retries': 0}
        for _ in range(self.warmup_requests):
            self.send_query("SELECT 1", 'read', warmup_results)
            time.sleep(0.1)
    """

    def run_read_benchmark(self):
        """Run read benchmark with progress bar."""
        print(f"\nExecuting READ Operations (Port: {self.current_port}):")
        start_time = time.time()
        with tqdm(total=self.num_requests, desc="Progress", unit="req") as pbar:
            for i in range(self.num_requests):
                query = self.generate_read_query(i)
                self.send_query(query, 'read')
                pbar.update(1)
        self.results['read']['time'] = time.time() - start_time

    def run_write_benchmark(self):
        """Run write benchmark with progress bar."""
        print(f"\nExecuting WRITE Operations (Port: {self.current_port}):")
        start_time = time.time()
        with tqdm(total=self.num_requests, desc="Progress", unit="req") as pbar:
            for i in range(self.num_requests):
                query = self.generate_write_query(i)
                self.send_query(query, 'write')
                pbar.update(1)
        self.results['write']['time'] = time.time() - start_time

    def run_benchmark(self, strategy):
        """Run complete benchmark for a strategy."""
        print(f"\nRunning benchmark for {strategy.upper()} strategy (Port: {STRATEGY_PORTS[strategy]})...")
        print("=" * 50)

        if strategy != "direct":
            if not self.set_strategy(strategy):
                logger.error(f"Failed to set {strategy} strategy")
                return False, None

        # Reset results
        for op_type in self.results:
            self.results[op_type] = {
                'success': 0, 'fail': 0, 'time': 0,
                'latencies': [], 'retries': 0
            }

        # Perform warmup
        #self.warmup()

        # Run benchmarks
        self.run_read_benchmark()
        self.run_write_benchmark()

        self.print_results(strategy)
        return True, self.results.copy()

    def calculate_metrics(self, operation_type):
        """Calculate metrics for operations."""
        results = self.results[operation_type]
        total_requests = results['success'] + results['fail']
        success_rate = (results['success'] / total_requests) * 100 if total_requests > 0 else 0
        avg_latency = sum(results['latencies']) / len(results['latencies']) if results['latencies'] else 0
        error_rate = (results['fail'] / total_requests) * 100 if total_requests > 0 else 0

        return {
            'total_requests': total_requests,
            'successful': results['success'],
            'failed': results['fail'],
            'success_rate': success_rate,
            'error_rate': error_rate,
            'total_time': results['time'],
            'avg_latency': avg_latency,
            'port': self.current_port
        }

    def print_results(self, strategy):
        """Print enhanced benchmark results."""
        print(f"\nResults for {strategy.upper()} strategy (Port: {self.current_port}):")
        print("=" * 50)

        for operation in ['read', 'write']:
            metrics = self.calculate_metrics(operation)
            print(f"\n{operation.upper()} Operations Performance:")
            print(f"  Total Requests    : {metrics['total_requests']:,d}")
            print(f"  Success Rate      : {metrics['success_rate']:.2f}%")
            print(f"  Error Rate        : {metrics['error_rate']:.2f}%")
            print(f"  Execution Time    : {metrics['total_time']:.2f} sec")
            print(f"  Avg Latency       : {metrics['avg_latency']*1000:.2f} ms")
            print(f"  Port Used         : {metrics['port']}")

def get_gatekeeper_hostname():
    """Retrieve Gatekeeper hostname from information.json."""
    try:
        with open('information.json', 'r') as f:
            info = json.load(f)
            gatekeeper_info = info['instances']['services']['gatekeeper']
            return gatekeeper_info['hostname']
    except FileNotFoundError:
        logger.error("information.json not found. Run main.py first.")
        sys.exit(1)
    except KeyError:
        logger.error("Gatekeeper information not found in information.json")
        sys.exit(1)
    except json.JSONDecodeError:
        logger.error("Error parsing information.json. File may be corrupted.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error reading information.json: {str(e)}")
        sys.exit(1)

def save_results_to_json(all_results):
    """Save benchmark results to JSON file."""
    formatted_results = {
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
        "strategies": {}
    }

    def calculate_json_metrics(results):
        total_requests = results['success'] + results['fail']
        success_rate = (results['success'] / total_requests) * 100 if total_requests > 0 else 0
        avg_latency = sum(results['latencies']) / len(results['latencies']) if results['latencies'] else 0
        error_rate = (results['fail'] / total_requests) * 100 if total_requests > 0 else 0
        
        return {
            "total_requests": total_requests,
            "success_rate": round(success_rate, 2),
            "error_rate": round(error_rate, 2),
            "execution_time": round(results['time'], 2),
            "avg_latency_ms": round(avg_latency * 1000, 2),
            "port": STRATEGY_PORTS[strategy]
        }

    for strategy, results in all_results.items():
        formatted_results["strategies"][strategy] = {
            "port": STRATEGY_PORTS[strategy],
            "read": calculate_json_metrics(results['read']),
            "write": calculate_json_metrics(results['write'])
        }

    with open('benchmark_results.json', 'w') as f:
        json.dump(formatted_results, f, indent=4)

    print("\nBenchmark results saved to benchmark_results.json")

def main():
    print("\nStarting MySQL Cluster Benchmark")
    print("=" * 50)

    try:
        gatekeeper_host = get_gatekeeper_hostname()
        print(f"Using Gatekeeper host: {gatekeeper_host}")

        # Verify connectivity
        response = http_requests.get(f"http://{gatekeeper_host}:5000/health")
        if response.status_code != 200:
            logger.error("Gatekeeper health check failed")
            sys.exit(1)
        print("Gatekeeper health check passed")
    except Exception as e:
        logger.error(f"Cannot connect to Gatekeeper: {str(e)}")
        sys.exit(1)

    benchmark = ClusterBenchmark(gatekeeper_host)
    strategies = ['direct', 'random', 'customized']
    all_results = {}

    for strategy in strategies:
        print(f"\nTesting {strategy} strategy...")
        success, results = benchmark.run_benchmark(strategy)
        if success and results:
            all_results[strategy] = results
        else:
            logger.error(f"Benchmark failed for {strategy} strategy")

    save_results_to_json(all_results)
    print("\nBenchmark completed!")

if __name__ == "__main__":
    main()