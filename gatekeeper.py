#!/usr/bin/env python3
import logging
import requests
import re
import time
from flask import Flask, request, jsonify
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [Gatekeeper] %(levelname)s: %(message)s'
)
logger = logging.getLogger("Gatekeeper")

app = Flask(__name__)

# Global variables
START_TIME = time.time()
RATE_LIMIT = 1000  # requests per minute
RATE_WINDOW = 60  # seconds
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
        self.state = "closed"  # closed, open, half-open
        self.threshold = 5  # failures before opening
        self.timeout = 60  # seconds to wait before attempting reset

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

class QueryValidator:
    """Enhanced query validator for SQL security."""
    def __init__(self):
        # List of forbidden SQL keywords
        self.dangerous_keywords = [
            'DROP', 'DELETE', 'TRUNCATE', 'ALTER', 
            'RENAME', 'MODIFY', 'SHUTDOWN', 'GRANT', 
            'REVOKE', 'ROLE', 'BACKUP', 'RESTORE',
            'CREATE TABLE'
        ]
        # List of valid SQL commands
        self.valid_commands = [
            'SELECT', 'INSERT', 'UPDATE', 'CREATE', 
            'SHOW', 'DESCRIBE', 'EXPLAIN', 'USE'
        ]
        # Enhanced SQL injection patterns
        self.injection_patterns = [
            "--", ";", "/*", "*/", "@@", "@", 
            "UNION", "SLEEP", "WAITFOR", "DELAY",
            "IF(", "WHEN"
        ]
        # Common attack patterns
        self.common_attacks = [
            r'\bOR\s+1\s*=\s*1\b',
            r'\bOR\s+[\'"].*[\'"]=[\'"].*[\'"]\b',
            r'(?i)(?:INFORMATION_SCHEMA|SYS)\.',
            r'\bEXEC\b|\bXP_\w+\b'
        ]

    def validate_query(self, query):
        """Validate the SQL query for security."""
        if not query:
            return False, "Empty query"

        query_upper = query.upper().replace("\n", " ").strip()

        # Check query length
        if len(query) > 5000:
            logger.warning("Query exceeds maximum length")
            return False, "Query exceeds maximum length"

        # Check for multiple statements
        if ";" in query[:-1]:  # Allow semicolon at end
            logger.warning("Multiple statements detected")
            return False, "Multiple statements are not allowed"

        # Check for SQL comments
        if '--' in query or '/*' in query:
            logger.warning("SQL comments detected")
            return False, "Comments are not allowed in queries"

        # Check for dangerous keywords
        for keyword in self.dangerous_keywords:
            if re.search(rf'\b{keyword}\b', query_upper):
                logger.warning(f"Dangerous keyword detected: {keyword}")
                return False, f"Query contains forbidden keyword: {keyword}"

        # Check if query starts with valid command
        if not any(query_upper.startswith(cmd) for cmd in self.valid_commands):
            logger.warning("Invalid SQL command")
            return False, "Query must start with a valid SQL command"

        # Check for injection patterns
        for pattern in self.injection_patterns:
            if pattern in query_upper:
                logger.warning(f"SQL injection pattern detected: {pattern}")
                return False, f"Query contains suspicious pattern: {pattern}"

        # Check for common attack patterns
        for pattern in self.common_attacks:
            if re.search(pattern, query_upper):
                logger.warning(f"Attack pattern detected: {pattern}")
                return False, "Query contains suspicious pattern"

        return True, None
    
class GatekeeperService:
    """Enhanced Gatekeeper service with security features."""
    def __init__(self, trusted_host):
        self.trusted_host = trusted_host
        self.trusted_host_url = f'http://{trusted_host}:5001'  # Using port 5001 for internal API
        self.validator = QueryValidator()
        self.circuit_breaker = CircuitBreaker()
        self.current_strategy = "direct"
        logger.info(f"Gatekeeper initialized with Trusted Host: {trusted_host}")

    def _check_rate_limit(self, client_ip):
        """Check if request is within rate limits."""
        current_time = time.time()
        request_counts[client_ip] = [t for t in request_counts[client_ip] 
                                   if current_time - t < RATE_WINDOW]
        request_counts[client_ip].append(current_time)
        return len(request_counts[client_ip]) <= RATE_LIMIT

    def process_request(self, request_data, client_ip):
        """Process and validate incoming requests."""
        try:
            # Circuit breaker check
            if not self.circuit_breaker.can_execute():
                logger.warning("Circuit breaker is open")
                return {
                    "status": "error",
                    "message": "Service temporarily unavailable"
                }

            # Rate limit check
            if not self._check_rate_limit(client_ip):
                logger.warning(f"Rate limit exceeded for IP: {client_ip}")
                return {
                    "status": "error",
                    "message": "Rate limit exceeded"
                }

            # Request validation
            if not request_data or 'query' not in request_data:
                logger.warning("Invalid request format")
                return {
                    "status": "error",
                    "message": "Invalid request format. Must include 'query' field."
                }

            # Size check
            if request.content_length and request.content_length > 1024 * 1024:
                logger.warning("Request too large")
                return {
                    "status": "error",
                    "message": "Request too large"
                }

            query = request_data['query']
            logger.info(f"Processing query from {client_ip}: {query[:100]}...")  # Log first 100 chars

            # Query validation
            is_valid, error_message = self.validator.validate_query(query)
            if not is_valid:
                logger.warning(f"Query validation failed: {error_message}")
                return {
                    "status": "error",
                    "message": error_message
                }

            # Add strategy information to request
            request_data.update({
                'strategy': self.current_strategy,
                'port': STRATEGY_PORTS[self.current_strategy]
            })

            # Forward to Trusted Host
            logger.info(f"Query validated. Forwarding to Trusted Host (Strategy: {self.current_strategy}, Port: {STRATEGY_PORTS[self.current_strategy]})")
            for attempt in range(3):
                try:
                    response = requests.post(
                        f"{self.trusted_host_url}/query",
                        json=request_data,
                        timeout=30
                    )
                    response.raise_for_status()
                    self.circuit_breaker.record_success()
                    logger.info("Query forwarded successfully")
                    return response.json()
                except requests.exceptions.RequestException as e:
                    logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                    if attempt == 2:
                        self.circuit_breaker.record_failure()
                        raise

            return {
                "status": "error",
                "message": "Failed to forward request to Trusted Host after retries"
            }

        except requests.exceptions.Timeout:
            logger.error("Request to Trusted Host timed out")
            return {"status": "error", "message": "Request timed out"}
        except requests.exceptions.ConnectionError:
            logger.error("Failed to connect to Trusted Host")
            return {"status": "error", "message": "Connection to internal service failed"}
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return {"status": "error", "message": "Internal server error"}

    def set_strategy(self, strategy):
        """Set routing strategy on Trusted Host."""
        try:
            if strategy not in STRATEGY_PORTS:
                return {
                    "status": "error",
                    "message": f"Invalid strategy. Must be one of: {', '.join(STRATEGY_PORTS.keys())}"
                }

            logger.info(f"Setting routing strategy to: {strategy} (Port: {STRATEGY_PORTS[strategy]})")
            response = requests.get(
                f"{self.trusted_host_url}/set_strategy/{strategy}",
                timeout=30
            )
            response.raise_for_status()
            self.current_strategy = strategy
            logger.info(f"Strategy set successfully: {strategy}")
            return response.json()
        except Exception as e:
            logger.error(f"Error setting strategy: {str(e)}")
            return {
                "status": "error",
                "message": f"Failed to set strategy: {str(e)}"
            }

# Flask route handlers
@app.route('/health', methods=['GET'])
def health_check():
    """Enhanced health check endpoint."""
    try:
        trusted_host_health = requests.get(
            f"{gatekeeper_service.trusted_host_url}/health",
            timeout=5
        ).json()
        
        return jsonify({
            "status": "ok",
            "trusted_host_status": trusted_host_health.get("status"),
            "current_strategy": gatekeeper_service.current_strategy,
            "current_port": STRATEGY_PORTS[gatekeeper_service.current_strategy],
            "uptime": time.time() - START_TIME,
            "version": "1.0",
            "circuit_breaker_state": gatekeeper_service.circuit_breaker.state
        })
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Health check failed"
        }), 500

@app.route('/set_strategy/<strategy>', methods=['GET'])
def set_strategy(strategy):
    """Set routing strategy endpoint."""
    try:
        response = gatekeeper_service.set_strategy(strategy)
        return jsonify(response)
    except Exception as e:
        logger.error(f"Error setting strategy: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Failed to set strategy: {str(e)}"
        })

@app.route('/query', methods=['POST'])
def handle_query():
    """Handle incoming query requests."""
    try:
        client_ip = request.remote_addr
        response = gatekeeper_service.process_request(request.json, client_ip)
        return jsonify(response)
    except Exception as e:
        logger.error(f"Error handling request: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Internal server error"
        })

def main():
    """Initialize and start the Gatekeeper service."""
    import sys
    if len(sys.argv) != 2:
        logger.error("Usage: gatekeeper.py <trusted_host>")
        sys.exit(1)

    trusted_host = sys.argv[1]

    try:
        global gatekeeper_service
        gatekeeper_service = GatekeeperService(trusted_host)
        logger.info("Starting Gatekeeper service...")
        app.run(host='0.0.0.0', port=5000)
    except Exception as e:
        logger.error(f"Failed to start Gatekeeper service: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()