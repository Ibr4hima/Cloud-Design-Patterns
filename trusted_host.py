import logging
import requests
import time
from flask import Flask, request, jsonify
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [Trusted Host] %(levelname)s: %(message)s'
)
logger = logging.getLogger("TrustedHost")

app = Flask(__name__)

# Global variables
START_TIME = time.time()
RATE_LIMIT = 2000  
RATE_WINDOW = 60  
request_counts = defaultdict(list)

# Strategy port mappings
STRATEGY_PORTS = {
    "direct": 3306,
    "random": 3307,
    "customized": 3308
}

class CircuitBreaker:
    def __init__(self):
        self.failures = 0
        self.last_failure_time = 0
        self.state = "closed"  
        self.threshold = 5  # failures before opening
        self.timeout = 60  

    def record_failure(self):
        self.failures += 1
        self.last_failure_time = time.time()
        if self.failures >= self.threshold:
            self.state = "open"
            logger.warning("Circuit breaker opened")

    def record_success(self):
        if self.state != "closed":
            logger.info("Circuit breaker closing")
        self.failures = 0
        self.state = "closed"

    def can_execute(self):
        if self.state == "open":
            if time.time() - self.last_failure_time > self.timeout:
                logger.info("Circuit breaker entering half-open state")
                self.state = "half-open"
                return True
            return False
        return True

class TrustedHostService:
    """Enhanced service handling secure communication with the Proxy."""
    
    def __init__(self, proxy_host, timeout=30, retry_attempts=3):
        self.proxy_host = proxy_host
        self.proxy_url = f'http://{proxy_host}:5000'
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.circuit_breaker = CircuitBreaker()
        self.last_proxy_health_check = 0
        self.health_check_interval = 60  # seconds
        self.current_strategy = "direct"
        logger.info(f"Trusted Host initialized with Proxy: {proxy_host}")

    def _check_rate_limit(self, client_ip):
        """Check if request is within rate limits."""
        current_time = time.time()
        request_counts[client_ip] = [t for t in request_counts[client_ip] 
                                   if current_time - t < RATE_WINDOW]
        request_counts[client_ip].append(current_time)
        return len(request_counts[client_ip]) <= RATE_LIMIT

    def _check_proxy_health(self):
        """Check proxy health status."""
        current_time = time.time()
        if current_time - self.last_proxy_health_check < self.health_check_interval:
            return True

        try:
            response = requests.get(f"{self.proxy_url}/health", timeout=5)
            self.last_proxy_health_check = current_time
            return response.status_code == 200
        except:
            logger.error("Proxy health check failed")
            return False

    def forward_request(self, request_data):
        """Forward validated requests to the Proxy with enhanced error handling."""
        try:
            # Check circuit breaker
            if not self.circuit_breaker.can_execute():
                return {
                    "status": "error",
                    "message": "Service temporarily unavailable"
                }

            # Validate request format
            if not request_data or 'query' not in request_data:
                logger.warning("Invalid request format: Missing 'query'")
                return {
                    "status": "error",
                    "message": "Invalid request format. Must include 'query' field."
                }

            # Extract strategy and port information
            strategy = request_data.get('strategy', self.current_strategy)
            port = STRATEGY_PORTS.get(strategy, STRATEGY_PORTS['direct'])
            
            # Enhance request data with port information
            request_data['port'] = port

            query = request_data.get('query')
            logger.info(f"Processing query with strategy '{strategy}' on port {port}: {query[:100]}...")

            # Check proxy health
            if not self._check_proxy_health():
                return {
                    "status": "error",
                    "message": "Proxy service unavailable"
                }

            # Forward request with retry logic
            proxy_url = f"{self.proxy_url}/query"
            for attempt in range(self.retry_attempts):
                try:
                    response = requests.post(
                        proxy_url,
                        json=request_data,
                        timeout=self.timeout
                    )
                    response.raise_for_status()
                    self.circuit_breaker.record_success()
                    return response.json()
                except requests.exceptions.RequestException as e:
                    logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                    if attempt == self.retry_attempts - 1:
                        self.circuit_breaker.record_failure()
                        raise

            return {
                "status": "error",
                "message": "Failed to forward request after retries"
            }

        except requests.exceptions.Timeout:
            logger.error("Request to Proxy timed out")
            return {
                "status": "error",
                "message": "Request timed out"
            }
        except requests.exceptions.ConnectionError:
            logger.error("Failed to connect to Proxy")
            return {
                "status": "error",
                "message": "Connection to Proxy failed"
            }
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return {
                "status": "error",
                "message": "Internal server error"
            }

    def set_strategy(self, strategy):
        """Set the routing strategy with enhanced error handling."""
        try:
            if strategy not in STRATEGY_PORTS:
                return {
                    "status": "error",
                    "message": f"Invalid strategy. Must be one of: {', '.join(STRATEGY_PORTS.keys())}"
                }

            if not self.circuit_breaker.can_execute():
                return {
                    "status": "error",
                    "message": "Service temporarily unavailable"
                }

            logger.info(f"Setting routing strategy to {strategy} (Port: {STRATEGY_PORTS[strategy]})")
            
            for attempt in range(self.retry_attempts):
                try:
                    response = requests.get(
                        f"{self.proxy_url}/set_strategy/{strategy}",
                        timeout=self.timeout
                    )
                    response.raise_for_status()
                    self.circuit_breaker.record_success()
                    self.current_strategy = strategy
                    return response.json()
                except requests.exceptions.RequestException as e:
                    logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                    if attempt == self.retry_attempts - 1:
                        self.circuit_breaker.record_failure()
                        raise

            return {
                "status": "error",
                "message": "Failed to set strategy after retries"
            }

        except Exception as e:
            logger.error(f"Error setting strategy: {str(e)}")
            return {
                "status": "error",
                "message": f"Failed to set strategy: {str(e)}"
            }

# Global TrustedHostService instance
trusted_host_service = None

@app.route('/health', methods=['GET'])
def health_check():
    """Enhanced health check endpoint."""
    try:
        # Check Proxy health
        proxy_health = "unknown"
        try:
            proxy_response = requests.get(
                f"{trusted_host_service.proxy_url}/health",
                timeout=5
            )
            proxy_health = "healthy" if proxy_response.status_code == 200 else "unhealthy"
        except:
            proxy_health = "unhealthy"

        return jsonify({
            "status": "ok",
            "proxy_status": proxy_health,
            "current_strategy": trusted_host_service.current_strategy,
            "current_port": STRATEGY_PORTS[trusted_host_service.current_strategy],
            "uptime": time.time() - START_TIME,
            "circuit_breaker_state": trusted_host_service.circuit_breaker.state
        })
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Health check failed"
        }), 500

@app.route('/query', methods=['POST'])
def handle_query():
    """Handle incoming queries."""
    try:
        client_ip = request.remote_addr
        if not trusted_host_service._check_rate_limit(client_ip):
            return jsonify({
                "status": "error",
                "message": "Rate limit exceeded"
            })

        response = trusted_host_service.forward_request(request.json)
        return jsonify(response)
    except Exception as e:
        logger.error(f"Error handling query: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Internal server error"
        })

@app.route('/set_strategy/<strategy>', methods=['GET'])
def set_strategy(strategy):
    """Set routing strategy."""
    try:
        response = trusted_host_service.set_strategy(strategy)
        return jsonify(response)
    except Exception as e:
        logger.error(f"Error setting strategy: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Failed to set strategy: {str(e)}"
        })

def main():
    """Initialize and start the Trusted Host service."""
    import sys
    if len(sys.argv) != 2:
        logger.error("Usage: trusted_host.py <proxy_host>")
        sys.exit(1)

    proxy_host = sys.argv[1]

    try:
        global trusted_host_service
        trusted_host_service = TrustedHostService(proxy_host)
        logger.info("Starting Trusted Host service...")
        # Changed port to 5001 for internal API
        app.run(host='0.0.0.0', port=5001)
    except Exception as e:
        logger.error(f"Failed to start Trusted Host service: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()