import os
import json
import time
import signal
import sys
import boto3
import redis
import structlog
from dotenv import load_dotenv
from prometheus_client import start_http_server, Counter, Histogram, Gauge

from executor import TaskExecutor, TaskMessage

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
        
        # 환경 변수 로드
        self.config = {k: v for k, v in os.environ.items()}
        
        # Clients
        self.sqs = boto3.client('sqs', region_name=self.config.get("AWS_REGION", "ap-northeast-2"))
        self.redis_client = redis.Redis(
            host=self.config["REDIS_HOST"],
            port=int(self.config.get("REDIS_PORT", 6379)),
            decode_responses=True
        )
        
        # 실행 엔진 (Warm Pool 포함)
        self.executor = TaskExecutor(self.config)
        self.running = True

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

        while self.running:
            try:
                # 1. SQS Long Polling
                resp = self.sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=20
                )

                if "Messages" not in resp:
                    continue

                for msg in resp["Messages"]:
                    self._process_message(queue_url, msg)

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
                model_id=body.get("modelId", "llama3:8b")
            )
            
            logger.info("🚀 Processing Task", id=task.request_id, runtime=task.runtime)

            # 2. 작업 실행 (Warm Pool 사용)
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

            # 3. 결과 Redis 발행 (Pub/Sub + KV 저장)
            result_dict = result.to_dict()
            json_result = json.dumps(result_dict)
            
            # Pub/Sub 채널
            channel = f"result:{task.request_id}"
            for attempt in range(max_attempts):
                try:
                    self.redis_client.publish(channel, json_result)
                    
                    # Async 조회용 키 저장 (TTL 1시간)
                    self.redis_client.setex(f"job:{task.request_id}", 3600, json_result)
                    break
                except Exception as e:
                    logger.warning("Redis publish failed", attempt=attempt+1, error=str(e))
                    if attempt == max_attempts - 1:
                        raise e
                    time.sleep(1)

            # 4. SQS 메시지 삭제 (Successful processing)
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

if __name__ == "__main__":
    agent = InfraAgent()
    agent.run()
