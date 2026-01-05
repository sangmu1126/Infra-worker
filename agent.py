import os
import json
import time
import threading
import signal
import sys
import socket
import urllib.request
import urllib.error
import boto3
import redis
import structlog
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
from prometheus_client import start_http_server, Counter, Histogram, Gauge

from executor import TaskExecutor
from models import TaskMessage

# --- Setup ---
load_dotenv()

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.PrintLoggerFactory(),
)
logger = structlog.get_logger()

class InfraAgent:
    def __init__(self):
        logger.info("🤖 Infra Worker Agent Starting...")
        
        # Load environment variables
        self.config = {k: v for k, v in os.environ.items()}
        
        # Clients
        self.sqs = boto3.client('sqs', region_name=self.config.get("AWS_REGION", "ap-northeast-2"))
        self.redis_client = redis.Redis(
            host=self.config["REDIS_HOST"],
            port=int(self.config.get("REDIS_PORT", 6379)),
            decode_responses=True
        )
        
        # Execution engine (includes Warm Pool)
        self.executor = TaskExecutor(self.config)
        self.running = True
        self._start_time = time.time()  # For uptime tracking

        # Prometheus Metrics
        self.jobs_processed = Counter('worker_jobs_processed_total', 'Total jobs processed', ['status', 'runtime', 'model'])
        self.job_duration = Histogram('worker_job_duration_seconds', 'Job execution duration in seconds', ['runtime', 'model'])
        self.active_jobs = Gauge('worker_active_jobs', 'Number of jobs currently running')

        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

    def _stop(self, signum, frame):
        logger.info("🛑 Shutdown signal received")
        self.running = False

    def run(self):
        queue_url = self.config["SQS_URL"]
        logger.info("📡 Listening for tasks", queue=queue_url)

        # Start Metrics Server
        try:
            start_http_server(8000)
            logger.info("Prometheus Metrics Server Started", port=8000)
        except Exception as e:
            logger.error("Failed to start metrics server", error=str(e))

        # Start System Status Publisher (Background Thread)
        threading.Thread(target=self._publish_system_status, daemon=True).start()
        
        # Start Health Check Server (port 8001)
        threading.Thread(target=self._start_health_server, daemon=True).start()
        
        # Start Heartbeat Push to Controller (every 10 seconds)
        threading.Thread(target=self._heartbeat_push, daemon=True).start()

        while self.running:
            try:
                # SQS Poll
                resp = self.sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=10, # Batch fetching (Parallelism key)
                    WaitTimeSeconds=20
                )

                if "Messages" not in resp:
                    continue

                for msg in resp["Messages"]:
                    # Dispatch to thread for parallel processing
                    threading.Thread(target=self._process_message, args=(queue_url, msg)).start()

            except Exception as e:
                logger.error("Polling loop error", error=str(e))
                time.sleep(1)
        
        logger.info("👋 Agent stopped cleanly")

    def _process_message(self, queue_url, msg):
        task = None # Initialize task to None for error handling
        try:
            self.active_jobs.inc() # Increment active jobs gauge
            body = json.loads(msg["Body"])
            task = TaskMessage(
                request_id=body["requestId"],
                function_id=body.get("functionId", "unknown"),
                runtime=body.get("runtime", "python"),
                s3_key=body["s3Key"],
                s3_bucket=body.get("s3Bucket"),
                memory_mb=body.get("memoryMb", 128),
                timeout_ms=body.get("timeoutMs", 300000),
                payload=body.get("input", {}),
                model_id=body.get("modelId", "llama3:8b"),
                env_vars=body.get("envVars", {})
            )
            
            logger.info("🚀 Processing Task", id=task.request_id, runtime=task.runtime)

            # Execute Task
            result = None
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    result = self.executor.run(task)
                    break
                except Exception as e:
                    logger.warning("Docker execution failed", attempt=attempt+1, error=str(e))
                    if attempt == max_attempts - 1:
                        raise e
                    time.sleep(1)

            # Publish Result
            result_dict = result.to_dict()
            json_result = json.dumps(result_dict)
            
            # Pub/Sub channel
            channel = f"result:{task.request_id}"
            for attempt in range(max_attempts):
                try:
                    self.redis_client.publish(channel, json_result)
                    
                    # Store key for async retrieval (TTL 1 hour)
                    self.redis_client.setex(f"job:{task.request_id}", 3600, json_result)
                    break
                except Exception as e:
                    logger.warning("Redis publish failed", attempt=attempt+1, error=str(e))
                    if attempt == max_attempts - 1:
                        raise e
                    time.sleep(1)

            # Delete Message
            self.sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
            
            logger.info("✅ Task Completed", id=task.request_id, ms=result.duration_ms)

            # Metrics Update
            status = "success" if result.success else "failure"
            self.jobs_processed.labels(status=status, runtime=task.runtime, model=task.model_id).inc()
            self.job_duration.labels(runtime=task.runtime, model=task.model_id).observe(result.duration_ms / 1000.0)

        except Exception as e:
            logger.error("Task processing failed", error=str(e))
            self.jobs_processed.labels(status="error", runtime=task.runtime if task else "unknown", model=task.model_id if task else "unknown").inc()
            
        finally:
            self.active_jobs.dec()

    def _publish_system_status(self):
        """
        [Background Task] Publish System Status to Redis
        - Frequency: Every 2 seconds
        - Data: Warm Pool sizes, Active Job count, Worker ID
        - Purpose: Real-time dashboard monitoring
        """
        while self.running:
            try:
                status = {
                    "timestamp": time.time(),
                    "worker_id": self.config.get("HOSTNAME", "unknown"),
                    "pools": {
                        "python": len(self.executor.containers.pools["python"]),
                        "nodejs": len(self.executor.containers.pools["nodejs"]),
                        "cpp": len(self.executor.containers.pools["cpp"]),
                        "go": len(self.executor.containers.pools["go"])
                    },
                    "active_jobs": self.active_jobs._value.get()
                }
                
                # Publish to Redis
                self.redis_client.setex("system:status", 10, json.dumps(status))
                
            except Exception as e:
                logger.warning("Failed to publish system status", error=str(e))
            
            time.sleep(2)

    def _heartbeat_push(self):
        """
        [Background Task] Push Heartbeat to Controller
        - Frequency: Every 10 seconds
        - Data: Worker status, pools, active jobs
        - Purpose: Controller knows Worker is alive (NAT-free health check)
        """
        controller_url = self.config.get("CONTROLLER_URL", "")
        if not controller_url:
            logger.warning("CONTROLLER_URL not set, heartbeat push disabled")
            return
            
        heartbeat_endpoint = f"{controller_url}/api/worker/heartbeat"
        worker_id = socket.gethostname()
        
        while self.running:
            try:
                heartbeat_data = {
                    "workerId": worker_id,
                    "timestamp": time.time(),
                    "status": "healthy" if self.running else "stopping",
                    "pools": {
                        "python": len(self.executor.containers.pools["python"]),
                        "nodejs": len(self.executor.containers.pools["nodejs"]),
                        "cpp": len(self.executor.containers.pools["cpp"]),
                        "go": len(self.executor.containers.pools["go"])
                    },
                    "activeJobs": self.active_jobs._value.get(),
                    "uptimeSeconds": int(time.time() - self._start_time)
                }
                
                # Send HTTP POST to Controller
                req = urllib.request.Request(
                    heartbeat_endpoint,
                    data=json.dumps(heartbeat_data).encode('utf-8'),
                    headers={'Content-Type': 'application/json'},
                    method='POST'
                )
                
                with urllib.request.urlopen(req, timeout=5) as response:
                    if response.status == 200:
                        logger.debug("Heartbeat sent successfully")
                
            except urllib.error.URLError as e:
                logger.warning("Heartbeat push failed", error=str(e))
            except Exception as e:
                logger.warning("Heartbeat push error", error=str(e))
            
            time.sleep(10)  # Every 10 seconds

    def _start_health_server(self):
        """Start a simple HTTP health check server on port 8001"""
        agent = self
        
        class HealthHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/health" or self.path == "/":
                    try:
                        health_status = {
                            "status": "healthy" if agent.running else "stopping",
                            "worker_id": socket.gethostname(),
                            "uptime_seconds": int(time.time() - agent._start_time),
                            "pools": {
                                "python": len(agent.executor.containers.pools["python"]),
                                "nodejs": len(agent.executor.containers.pools["nodejs"]),
                                "cpp": len(agent.executor.containers.pools["cpp"]),
                                "go": len(agent.executor.containers.pools["go"])
                            },
                            "active_jobs": agent.active_jobs._value.get()
                        }
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps(health_status).encode())
                    except Exception as e:
                        self.send_response(500)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps({"status": "error", "message": str(e)}).encode())
                else:
                    self.send_response(404)
                    self.end_headers()
            
            def log_message(self, format, *args):
                pass  # Suppress access logs
        
        try:
            server = HTTPServer(("0.0.0.0", 8001), HealthHandler)
            logger.info("🏥 Health Check Server Started", port=8001, endpoint="/health")
            server.serve_forever()
        except Exception as e:
            logger.error("Failed to start health check server", error=str(e))


if __name__ == "__main__":
    agent = InfraAgent()
    agent.run()
