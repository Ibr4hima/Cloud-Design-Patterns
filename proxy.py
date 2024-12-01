import mysql.connector
import logging
import random
import time
from flask import Flask, request, jsonify

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [ProxyService] %(levelname)s: %(message)s')
logger = logging.getLogger("ProxyService")

app = Flask(__name__)

class ProxyManager:
    def __init__(self, manager_host, worker_hosts, mysql_user, mysql_password):
        """Initialize Proxy Manager with cluster configuration."""
        self.manager_host = manager_host
        self.worker_hosts = worker_hosts.split(',') if isinstance(worker_hosts, str) else worker_hosts
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.current_strategy = "direct"
        # Define ports for different strategies
        self.strategy_ports = {
            "direct": 3306,    # Default MySQL port
            "random": 3307,    # Port for random strategy
            "customized": 3308 # Port for customized strategy
        }
        logger.info(f"Proxy initialized with Manager: {manager_host}, Workers: {worker_hosts}")

    def _get_connection(self, host, strategy=None):
        """Create MySQL connection to specified host with strategy-specific port."""
        try:
            port = self.strategy_ports.get(strategy, 3306) if strategy else 3306
            logger.info(f"Attempting connection to MySQL host: {host}:{port} with user: {self.mysql_user}")
            connection = mysql.connector.connect(
                host=host,
                port=port,
                user=self.mysql_user,
                password=self.mysql_password,
                database='sakila',
                connect_timeout=10
            )
            logger.info(f"Successfully connected to MySQL host: {host}:{port}")
            return connection
        except mysql.connector.Error as e:
            logger.error(f"MySQL Error connecting to {host}:{port}: Code {e.errno}, Message: {e.msg}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error connecting to {host}:{port}: {str(e)}")
            return None

    def route_request(self, query, is_write=False):
        """Route SQL query based on request type and current strategy."""
        try:
            # Handle write operations
            if is_write:
                logger.info("Write operation detected. Routing to Manager node.")
                result = self._execute_query(self.manager_host, query)
                if result["status"] == "success":
                    self._replicate_to_workers(query)
                return result

            # Handle read operations
            target_host = self._select_read_host()
            logger.info(f"Read operation routed to: {target_host} using strategy: {self.current_strategy}")
            result = self._execute_query(target_host, query, self.current_strategy)
            
            # Fallback to manager if worker fails
            if result["status"] == "error" and target_host != self.manager_host:
                logger.warning(f"Worker {target_host} failed, falling back to manager")
                return self._execute_query(self.manager_host, query)
            
            return result
            
        except Exception as e:
            logger.error(f"Error routing query: {str(e)}")
            return {"status": "error", "message": "Internal error in Proxy"}

    def _select_read_host(self):
        """Select appropriate host for read operations based on strategy."""
        if self.current_strategy == "direct":
            # For direct strategy, use first worker
            return self.worker_hosts[0]
        elif self.current_strategy == "random":
            # Randomly select a worker
            return random.choice(self.worker_hosts)
        elif self.current_strategy == "customized":
            # Use fastest worker
            return self._get_fastest_worker()
        else:
            logger.error(f"Unknown strategy: {self.current_strategy}")
            return self.worker_hosts[0]

    def _replicate_to_workers(self, query):
        """Replicate write operations to all worker nodes."""
        for worker in self.worker_hosts:
            try:
                logger.info(f"Replicating write operation to worker: {worker}")
                result = self._execute_query(worker, query)
                if result["status"] != "success":
                    logger.error(f"Failed to replicate to worker {worker}: {result['message']}")
            except Exception as e:
                logger.error(f"Error replicating to worker {worker}: {str(e)}")

    def _execute_query(self, host, query, strategy=None):
        """Execute the query on the specified host using strategy-specific port."""
        conn = None
        cursor = None
        try:
            logger.info(f"Executing query on {host} with strategy {strategy}: {query[:100]}...")
            conn = self._get_connection(host, strategy)
            if not conn:
                return {"status": "error", "message": f"Failed to connect to MySQL on {host}"}

            cursor = conn.cursor()
            cursor.execute(query)
            
            # Convert results to a serializable format
            if cursor.with_rows:
                columns = [column[0] for column in cursor.description]
                results = []
                for row in cursor.fetchall():
                    # Convert each row to a list and handle non-serializable types
                    row_data = []
                    for item in row:
                        if isinstance(item, (set, bytes)):
                            row_data.append(list(item) if isinstance(item, set) else str(item))
                        else:
                            row_data.append(item)
                    results.append(dict(zip(columns, row_data)))
            else:
                results = None

            conn.commit()
            logger.info(f"Query executed successfully on {host}")
            return {"status": "success", "result": results}
                
        except mysql.connector.Error as e:
            logger.error(f"MySQL Error on {host}: {e.msg}")
            return {"status": "error", "message": f"MySQL Error: {e.msg}"}
        except Exception as e:
            logger.error(f"Error executing query on {host}: {str(e)}")
            return {"status": "error", "message": str(e)}
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def _get_fastest_worker(self):
        """Determine the fastest worker node by measuring response time."""
        response_times = {}
        for worker in self.worker_hosts:
            start_time = time.time()
            conn = self._get_connection(worker, "customized")
            if conn:
                response_times[worker] = time.time() - start_time
                conn.close()
            else:
                response_times[worker] = float('inf')

        if not response_times or all(rt == float('inf') for rt in response_times.values()):
            logger.warning("No responsive workers found, using first worker")
            return self.worker_hosts[0]

        return min(response_times, key=response_times.get)

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "ok"})

@app.route('/set_strategy/<strategy>', methods=['GET'])
def set_strategy(strategy):
    """Set routing strategy for the Proxy."""
    if strategy in ["direct", "random", "customized"]:
        proxy.current_strategy = strategy
        logger.info(f"Strategy set to: {strategy} (Port: {proxy.strategy_ports.get(strategy, 3306)})")
        return jsonify({
            "status": "success", 
            "strategy": strategy,
            "port": proxy.strategy_ports.get(strategy, 3306)
        })
    else:
        logger.error(f"Invalid strategy: {strategy}")
        return jsonify({"status": "error", "message": "Invalid strategy"})

@app.route('/query', methods=['POST'])
def query_handler():
    """Handle incoming SQL queries."""
    try:
        data = request.json
        if not data or 'query' not in data:
            return jsonify({"status": "error", "message": "No query provided"})

        query = data['query']
        logger.info(f"Received query: {query}")

        is_write = any(word in query.upper() for word in ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER'])
        result = proxy.route_request(query, is_write)
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error processing query: {str(e)}")
        return jsonify({"status": "error", "message": str(e)})

def main():
    """Main function to initialize and start the Proxy service."""
    import sys
    if len(sys.argv) < 5:
        logger.error("Usage: proxy.py <manager_host> <worker_hosts> <mysql_user> <mysql_password>")
        sys.exit(1)

    global proxy
    manager_host = sys.argv[1]
    worker_hosts = sys.argv[2]
    mysql_user = sys.argv[3]
    mysql_password = sys.argv[4]

    proxy = ProxyManager(manager_host, worker_hosts, mysql_user, mysql_password)
    logger.info("Starting Proxy service...")
    app.run(host='0.0.0.0', port=5000)

if __name__ == '__main__':
    main()