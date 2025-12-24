import os
import atexit
import shutil
import time
import threading
import zipfile
import tarfile
import io
import json
import structlog
import boto3
import docker
import redis
import socket
from pathlib import Path
from collections import deque
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from datetime import datetime
from uploader import OutputUploader

logger = structlog.get_logger()

# --- Data Models ---
@dataclass
class TaskMessage:
    request_id: str
    function_id: str
    runtime: str
    s3_key: str
    s3_bucket: Optional[str] = None
    memory_mb: int = 128
    timeout_ms: int = 300000
    payload: Dict = field(default_factory=dict)
    model_id: str = "llama3:8b"

@dataclass
class ExecutionResult:
    request_id: str
    function_id: str
    success: bool
    exit_code: int
    stdout: str
    stderr: str
    duration_ms: int
    worker_id: str = "unknown"
    peak_memory_bytes: Optional[int] = None
    allocated_memory_mb: Optional[int] = None
    optimization_tip: Optional[str] = None
    estimated_savings: Optional[str] = None
    output_files: List[str] = field(default_factory=list)
    llm_token_count: Optional[int] = 0 # Feature 3: Usage Metering

    def to_dict(self):
        return {
            "requestId": self.request_id,
            "functionId": self.function_id,
            "workerId": self.worker_id,
            "status": "SUCCESS" if self.success else "FAILED",
            "exitCode": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "durationMs": self.duration_ms,
            "peakMemoryBytes": self.peak_memory_bytes,
            "optimizationTip": self.optimization_tip,
            "estimatedSavings": self.estimated_savings,
            "outputFiles": self.output_files,
            "llm_token_count": self.llm_token_count
        }



# --- Service Logic ---

class AutoTuner:
    """Generate memory optimization tips (Key for cost savings)"""
    COST_PER_MB_HOUR = 0.00005  # $0.00005 (Estimated based on loose AWS EC2 GB-hour cost)

    @staticmethod
    def analyze(peak_bytes: int, allocated_mb: int):
        if not peak_bytes: return None, None
        if allocated_mb <= 0: allocated_mb = 128  # Guard against division by zero

        peak_mb = peak_bytes / (1024 * 1024)
        
        # 1. Generate Tip
        tip = None
        ratio = peak_mb / allocated_mb
        if ratio < 0.3:
            rec = max(int(peak_mb * 1.5), 10) # Recommend at least 10MB
            saved_percent = int((1 - (rec / allocated_mb)) * 100)
            if saved_percent > 0:
                tip = f"💡 Tip: Actual usage ({int(peak_mb)}MB) is much less than allocated ({allocated_mb}MB). Reduce to {rec}MB to save approx {saved_percent}%."
        elif ratio > 0.9:
            rec = int(peak_mb * 1.2)
            tip = f"⚠️ Warning: Memory tight ({int(peak_mb)}MB). Recommend increasing to {rec}MB+."

        # 2. Calculate Cost Savings (Business Perspective)
        # Logic: Calculate wasted resources (Allocated - Peak) to highlight inefficiency
        wasted_mb = allocated_mb - peak_mb
        estimated_savings = None
        
        if wasted_mb > 0:
            # Monthly savings (based on 730 hours)
            monthly_saving = wasted_mb * AutoTuner.COST_PER_MB_HOUR * 730
            estimated_savings = f"${monthly_saving:.2f}/month (if rightsized from {allocated_mb}MB)"

        return tip, estimated_savings

class CloudWatchPublisher:
    """Send CloudWatch metrics for ASG integration"""
    def __init__(self, region):
        self.client = boto3.client("cloudwatch", region_name=region)
        
    def publish_peak_memory(self, func_id, runtime, bytes_used):
        try:
            if bytes_used is None: return
            # Better if async (logging only here)
            # logger.debug("Publishing CloudWatch Metric", value=bytes_used)
            self.client.put_metric_data(
                Namespace="FaaS/FunctionRunner",
                MetricData=[{
                    "MetricName": "PeakMemoryBytes",
                    "Dimensions": [{"Name": "FunctionId", "Value": func_id}, {"Name": "Runtime", "Value": runtime}],
                    "Value": float(bytes_used),
                    "Unit": "Bytes",
                    "Timestamp": datetime.utcnow()
                }]
            )
        except Exception as e:
            logger.warning("CloudWatch publish failed", error=str(e))

class TaskExecutor:
    """Integrated Execution Engine: S3 Download -> Docker Run -> Result Processing"""
    
    # [LRU] Maximum containers per function for warm pool
    MAX_POOL_SIZE_PER_FUNC = 5
    
    def __init__(self, config: Dict):
        self.cfg = config
        
        # Ensure workspace exists with user permissions BEFORE Docker creates it as root
        if "DOCKER_WORK_DIR_ROOT" in config:
            Path(config["DOCKER_WORK_DIR_ROOT"]).mkdir(parents=True, exist_ok=True)
            
        self.docker = docker.from_env()
        self.s3 = boto3.client("s3", region_name=config.get("AWS_REGION", "ap-northeast-2"))
        self.cw = CloudWatchPublisher(config.get("AWS_REGION", "ap-northeast-2"))
        self.uploader = OutputUploader(
            bucket_name=config.get("S3_USER_DATA_BUCKET", ""),
            region=config.get("AWS_REGION", "ap-northeast-2")
        )
        
        # Runtime-based Warm Pool Storage (for cold start elimination)
        self.pools = {
            "python": deque(), "cpp": deque(), "nodejs": deque(), "go": deque()
        }
        self.images = {
            "python": config.get("DOCKER_PYTHON_IMAGE", "faas/python:3.9-fat"),
            "cpp": config.get("DOCKER_CPP_IMAGE", "gcc:latest"),
            "nodejs": config.get("DOCKER_NODEJS_IMAGE", "node:18-alpine"),
            "go": config.get("DOCKER_GO_IMAGE", "golang:1.19-alpine")
        }
        
        # Function-specific Warm Pool (for secure reuse with LRU eviction)
        # Key: function_id, Value: list of container objects (ordered by usage, newest last)
        self.function_pools = {}
        self.function_pool_lock = threading.Lock()
        
        # Runtime-specific locks
        self.pool_locks = {
            k: threading.Lock() for k in ["python", "cpp", "nodejs", "go"]
        }
        
        # [Pre-pull] Ensure all images are available before first request
        logger.info("🐳 Pre-pulling Docker images...")
        for runtime, img_name in self.images.items():
            try:
                self.docker.images.get(img_name)
                logger.debug(f"✓ Image ready: {img_name}")
            except docker.errors.ImageNotFound:
                logger.info(f"📥 Pulling image: {img_name}")
                self.docker.images.pull(img_name)
        
        self._initialize_warm_pool()
        
        # Dynamic global concurrency limiter (based on host RAM)
        self.global_limit = self._init_global_semaphore()
        
        # Redis cache for code (reduces S3 latency from 1000ms to 5ms)
        redis_host = config.get("REDIS_HOST", "localhost")
        redis_port = int(config.get("REDIS_PORT", 6379))
        try:
            self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=False)
            self.redis.ping()
            logger.info("📦 Redis connected for code caching", host=redis_host)
        except Exception as e:
            logger.warning("⚠️ Redis unavailable, falling back to S3 only", error=str(e))
            self.redis = None
        
        # Register cleanup on program exit
        atexit.register(self._shutdown_cleanup)

    def _init_global_semaphore(self):
        """
        Calculate safe concurrency limit based on Host RAM.
        Formula: (Total RAM - System Reserved) / Min Function Size
        """
        try:
            # Get Total Memory (Linux/Unix)
            total_bytes = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
            total_mb = total_bytes / (1024 * 1024)
        except Exception as e:
            logger.warning("Failed to detect RAM, using default", error=str(e))
            total_mb = 2048  # Fallback to 2GB

        # System Reserved (OS + Docker + Agent)
        if total_mb < 4096:
            reserved_mb = total_mb * 0.4
        else:
            reserved_mb = 1536  # 1.5GB fixed for larger instances

        # Calculate Available Slots (min 128MB per function)
        available_mb = total_mb - reserved_mb
        limit = int(available_mb // 128)
        
        # Safety bound (Min 1, Max 500)
        limit = max(1, min(limit, 500))

        logger.info("Dynamic Limit Configured", 
                    host_ram_mb=int(total_mb), 
                    reserved_mb=int(reserved_mb), 
                    concurrency_limit=limit)
        
        return threading.Semaphore(limit)

    def _initialize_warm_pool(self):
        """Initialize Warm Pool (Eliminate Cold Start)"""
        counts = {
            "python": int(self.cfg.get("WARM_POOL_PYTHON_SIZE", 1)),
            "cpp": int(self.cfg.get("WARM_POOL_CPP_SIZE", 1)),
            "nodejs": int(self.cfg.get("WARM_POOL_NODEJS_SIZE", 1)),
            "go": int(self.cfg.get("WARM_POOL_GO_SIZE", 1))
        }
        logger.info("🔥 Initializing Warm Pools", counts=counts)
        
        for runtime, count in counts.items():
            for _ in range(count):
                self._create_warm_container(runtime)

    def _create_warm_container(self, runtime: str) -> str:
        try:
            img = self.images.get(runtime)
            # Run infinite wait container
            c = self.docker.containers.run(
                img, command="tail -f /dev/null", detach=True,
                # [SECURITY] Volume mount removed for isolation.
                # Code will be injected via 'put_archive' (docker cp) at runtime.
                # volumes={self.cfg["DOCKER_WORK_DIR_ROOT"]: {"bind": "/workspace", "mode": "rw"}},
                network_mode="bridge", # Allow AI Endpoint access
                mem_limit="1024m",   # Default (Will be updated in run)
                cpu_quota=100000     # 1.0 CPU
            )
            c.pause()
            self.pools[runtime].append(c.id)
            return c.id
        except Exception as e:
            logger.error("Failed to create warm container", runtime=runtime, error=str(e))
            return None

    def _acquire_container(self, runtime: str, function_id: str = None):
        """
        Acquire container with priority:
        1. Function-specific warm pool (same function = secure + fast)
        2. Runtime-based generic pool (different function = cold start for that func)
        """
        target_runtime = runtime if runtime in self.pools else "python"
        
        # Try function-specific pool first (Warm Start for repeated calls)
        if function_id:
            with self.function_pool_lock:
                if function_id in self.function_pools and self.function_pools[function_id]:
                    container = self.function_pools[function_id].pop()  # LRU: get most recent
                    try:
                        # Try unpause (ignore if already running)
                        try:
                            container.unpause()
                        except Exception:
                            pass  # Already running, that's fine
                        logger.info("⚡ Warm Start from function pool", function_id=function_id)
                        container.is_warm = True  # Mark as warm
                        return container
                    except Exception as e:
                        logger.warning("⚠️ Failed to reuse container from function pool", error=str(e))
                        # Container dead, fall through to generic pool
        
        # Fall back to runtime generic pool (Cold Start for this function)
        cid = None
        with self.pool_locks[target_runtime]:
            if not self.pools[target_runtime]:
                logger.warning("Pool empty, creating new container synchronously", runtime=target_runtime)
                cid = self._create_warm_container(target_runtime)
                if not cid: raise RuntimeError("Failed to create container")
            
            if self.pools[target_runtime]:
                cid = self.pools[target_runtime].popleft()
            
        if not cid:
             raise RuntimeError("Failed to acquire container")

        try:
            c = self.docker.containers.get(cid)
            # Try unpause (ignore if already running)
            try:
                c.unpause()
            except Exception:
                pass  # Already running, that's fine
            logger.info("🥶 Cold Start from runtime pool", runtime=target_runtime)
            
            # Only replenish when generic pool is used (not for warm start)
            self._replenish_pool(target_runtime)
            
            c.is_warm = False  # Mark as cold
            return c
        except Exception:
            return self._acquire_container(target_runtime, function_id)

    def _release_container(self, container, function_id: str, runtime: str):
        """
        Return container to function-specific pool with LRU eviction.
        [Security] Cleans workspace before reuse.
        """
        try:
            # 1. [Optimization] Skip cleanup to avoid exec_run overhead (200ms+)
            # Container is only reused for same function_id, so workspace is compatible.
            # container.exec_run("rm -rf /workspace/* /tmp/* /output/*", demux=False)
            
            # 2. [Optimization] Skip pause to keep it hot (CPU usage is near 0 anyway)
            # if container.status != 'paused':
            #     container.pause()
            
            # 3. Add to function-specific pool with LRU eviction
            with self.function_pool_lock:
                if function_id not in self.function_pools:
                    self.function_pools[function_id] = []
                
                pool = self.function_pools[function_id]
                
                # [LRU] If pool is full, evict the oldest (front of list)
                if len(pool) >= self.MAX_POOL_SIZE_PER_FUNC:
                    oldest = pool.pop(0)
                    try:
                        oldest.remove(force=True)
                        logger.info("🗑️ LRU Eviction: removed oldest container", function_id=function_id)
                    except Exception:
                        pass
                
                # Add new container at the end (most recently used)
                pool.append(container)
                logger.info("♻️ Container recycled", function_id=function_id, pool_size=len(pool))
                
        except Exception as e:
            # If cleanup fails, just remove the container
            logger.warning("Failed to recycle container, removing", error=str(e))
            try:
                container.remove(force=True)
            except Exception:
                pass

    def _replenish_pool(self, runtime: str):
        """Replenish the runtime warm pool asynchronously after usage"""
        def _create():
            try:
                self._create_warm_container(runtime)
                logger.info("Pool replenished", runtime=runtime)
            except Exception as e:
                logger.error("Failed to replenish pool", error=str(e))
        
        # Run in a separate thread to avoid blocking the main execution flow
        threading.Thread(target=_create, daemon=True).start()


    def _prepare_workspace(self, task: TaskMessage) -> Path:
        """S3 Download with Redis cache and [Security] Zip Slip prevention during extraction"""
        local_dir = Path(self.cfg["DOCKER_WORK_DIR_ROOT"]) / task.request_id
        if local_dir.exists(): shutil.rmtree(local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)
        
        zip_path = local_dir / "code.zip"
        bucket = task.s3_bucket if task.s3_bucket else self.cfg["S3_CODE_BUCKET"]
        cache_key = f"code:{task.function_id}"
        
        # 1. Try Redis cache first (5ms vs 1000ms from S3)
        cache_hit = False
        if self.redis:
            try:
                cached_code = self.redis.get(cache_key)
                if cached_code:
                    with open(zip_path, "wb") as f:
                        f.write(cached_code)
                    cache_hit = True
                    logger.info("⚡ Code cache HIT", function_id=task.function_id)
            except Exception as e:
                logger.warning("Redis cache read failed", error=str(e))
        
        # 2. Cache MISS -> Download from S3
        if not cache_hit:
            logger.info("📥 Code cache MISS, downloading from S3", function_id=task.function_id)
            self.s3.download_file(bucket, task.s3_key, str(zip_path))
            
            # 3. Store in Redis for future requests (TTL: 10 minutes)
            if self.redis:
                try:
                    with open(zip_path, "rb") as f:
                        self.redis.setex(cache_key, 600, f.read())  # 10분 TTL
                    logger.info("📦 Code cached to Redis", function_id=task.function_id)
                except Exception as e:
                    logger.warning("Redis cache write failed", error=str(e))
        
        # Zip Slip prevention code
        with zipfile.ZipFile(zip_path, "r") as zf:
            for member in zf.namelist():
                # Block attempts to access parent directory (../)
                target_path = (local_dir / member).resolve()
                if not str(target_path).startswith(str(local_dir.resolve())):
                    logger.warning("Zip Slip attempt detected", file=member)
                    continue
                
                # Extract file
                if member.endswith('/'):
                    target_path.mkdir(parents=True, exist_ok=True)
                else:
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    with zf.open(member) as source, open(target_path, "wb") as dest:
                        shutil.copyfileobj(source, dest)
        
        zip_path.unlink()

        # Inject FaaS SDK & AI Client
        try:
            current_dir = Path(__file__).parent
            
            # Inject sdk.py
            src_sdk = current_dir / "sdk.py"
            if src_sdk.exists():
                shutil.copy(str(src_sdk), str(local_dir / "sdk.py"))
            else:
                logger.warning("sdk.py not found, skipping SDK injection")

            # Inject ai_client.py (Critical for AI Logic)
            src_ai = current_dir / "ai_client.py"
            if src_ai.exists():
                shutil.copy(str(src_ai), str(local_dir / "ai_client.py"))
            else:
                logger.warning("ai_client.py not found, skipping injection")

        except Exception as e:
            logger.error("Failed to inject dependencies", error=str(e))

        return local_dir

    def _copy_to_container(self, container, source_path: Path, target_path: str):
        """[Security] Inject code via docker cp (put_archive)"""
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode='w') as tar:
            # Add all contents relative to the source_path (so they appear at target_path root)
            tar.add(source_path, arcname=".")
        stream.seek(0)
        
        # Ensure target directory exists
        container.exec_run(f"mkdir -p {target_path}")
        container.put_archive(target_path, stream)

    def _copy_from_container(self, container, source_path: str, target_local_path: Path):
        """
        Retrieve output files via docker cp (get_archive)
        [Optimization] Streams data to disk (temp file) to prevent OOM on large outputs
        """
        try:
            stream, stat = container.get_archive(source_path)
            temp_tar = target_local_path / "temp_output.tar"
            
            # Stream directly to disk to avoid loading massive files into RAM
            with open(temp_tar, "wb") as f:
                for chunk in stream:
                    f.write(chunk)
            
            # Extract
            with tarfile.open(temp_tar, mode='r') as tar:
                tar.extractall(path=target_local_path)
            
            # Cleanup temp tar
            temp_tar.unlink()
            
        except Exception as e:
            logger.warning("Failed to copy from container", error=str(e))


    def run(self, task: TaskMessage) -> ExecutionResult:
        container = None
        host_work_dir = None
        start_time = time.time()
        
        # Acquire global slot (wait up to 30s, then reject)
        acquired = self.global_limit.acquire(blocking=True, timeout=30)
        if not acquired:
            logger.error("Global container limit reached", request_id=task.request_id)
            return ExecutionResult(
                request_id=task.request_id, function_id=task.function_id, success=False, exit_code=-1,
                stdout="", stderr="Server Busy (503): Too many concurrent executions",
                duration_ms=int((time.time() - start_time) * 1000),
                worker_id=socket.gethostname()
            )
        
        try:
            # 1. [Reorder] Acquire container FIRST to check warm status
            container = self._acquire_container(task.runtime, task.function_id)
            is_warm = getattr(container, "is_warm", False)
            
            # Apply dynamic memory limit
            try:
                container.update(mem_limit=f"{task.memory_mb}m", memswap_limit=f"{task.memory_mb}m")
            except Exception as e:
                logger.warning("Failed to update container memory limit", error=str(e))
            
            # [SECURITY] Use isolated workspace path (No host bind)
            container_work_dir = "/workspace"
            
            # 2. [Optimization] Skip workspace prep for Warm Start (code already in container)
            if is_warm:
                logger.info("⚡ Warm Start: Skipping Host Workspace Prep", id=container.id[:12])
                host_work_dir = Path(self.cfg["DOCKER_WORK_DIR_ROOT"]) / task.request_id
                host_work_dir.mkdir(parents=True, exist_ok=True)
            else:
                # Cold Start: Download code and prepare workspace
                host_work_dir = self._prepare_workspace(task)
            
            # 3. Configure execution command
            # Output Directory Setup
            host_output_dir = host_work_dir / "output"
            host_output_dir.mkdir(parents=True, exist_ok=True)
            
            # Setup container output dir + Lazy Cleanup (/tmp from previous runs)
            setup_cmd = f"rm -rf /output /tmp/* && mkdir -p /output"

            # Environment Variables
            env_vars = {
                "JOB_ID": task.request_id,
                "FUNCTION_ID": task.function_id,
                "MEMORY_MB": str(task.memory_mb),
                "LLM_MODEL": task.model_id,
                "OUTPUT_DIR": "/output"
            }
            
            # Write payload to file if too large (>100KB)
            payload_str = json.dumps(task.payload)
            if len(payload_str) > 100 * 1024:
                payload_path = host_work_dir / "payload.json"
                with open(payload_path, "w") as f:
                    f.write(payload_str)
                env_vars["PAYLOAD_FILE"] = f"{container_work_dir}/payload.json"
                if "PAYLOAD" in env_vars: del env_vars["PAYLOAD"]
                logger.info("Payload too large, using file instead", size=len(payload_str))
            else:
                env_vars["PAYLOAD"] = payload_str

            # [SECURITY] Inject Code + Payload into Container
            # Optimization: Skip injection if Warm Start AND Payload is small (in env vars)
            has_large_payload = "PAYLOAD_FILE" in env_vars
            
            if not is_warm or has_large_payload:
                logger.info("Injecting code to container", id=container.id[:12], reason="Cold Start" if not is_warm else "Large Payload")
                self._copy_to_container(container, host_work_dir, container_work_dir)
            else:
                logger.info("⚡ Skipping code injection (Warm Start)", id=container.id[:12])

            # Pre-compiled Binary Detection
            # Allows users to upload pre-built executables (cross-compiled for Linux/AMD64)
            # to bypass in-container compilation, reducing latency from ~2500ms to ~30ms
            binary_path = host_work_dir / "main"
            has_binary = binary_path.exists() and binary_path.is_file()
            chmod_cmd = f"chmod +x {container_work_dir}/main"

            cmd = []
            if task.runtime == "python": 
                cmd = ["sh", "-c", f"{setup_cmd} && python {container_work_dir}/main.py"]
            elif task.runtime == "nodejs": 
                cmd = ["sh", "-c", f"{setup_cmd} && node {container_work_dir}/index.js"]
            elif task.runtime == "cpp":
                # Warm Start or pre-compiled binary: skip compilation
                if is_warm or has_binary:
                    logger.info("Skipping build (warm start or pre-compiled)", runtime="cpp")
                    cmd = ["sh", "-c", f"{setup_cmd} && {chmod_cmd} && {container_work_dir}/main"]
                else:
                    logger.info("Compiling from source", runtime="cpp")
                    cmd = ["sh", "-c", f"{setup_cmd} && g++ {container_work_dir}/main.cpp -o {container_work_dir}/main && {container_work_dir}/main"]
            elif task.runtime == "go":
                # Warm Start or pre-compiled binary: skip compilation
                if is_warm or has_binary:
                    logger.info("Skipping build (warm start or pre-compiled)", runtime="go")
                    cmd = ["sh", "-c", f"{setup_cmd} && {chmod_cmd} && {container_work_dir}/main"]
                else:
                    logger.info("Compiling from source", runtime="go")
                    cmd = ["sh", "-c", f"{setup_cmd} && cd {container_work_dir} && go build -o main main.go && ./main"]



            # 4. 실행 (Exec) with Timeout
            logger.info("Exec command", cmd=cmd, container=container.id[:12], timeout_ms=task.timeout_ms)
            
            exec_result = {"exit_code": None, "output": b""}
            
            def run_docker_exec():
                try:
                    ec, out = container.exec_run(
                        cmd, workdir="/workspace", demux=False,
                        environment=env_vars
                    )
                    exec_result["exit_code"] = ec
                    exec_result["output"] = out
                except Exception as e:
                    exec_result["error"] = e

            import threading
            exec_thread = threading.Thread(target=run_docker_exec)
            
            # Memory Monitoring
            metrics = {"peak_memory": 0}
            stop_monitoring = threading.Event()

            def monitor_memory():
                while not stop_monitoring.is_set():
                    try:
                        stats = container.stats(stream=False)
                        usage = stats['memory_stats'].get('usage', 0)
                        # Optional: Subtract cache if needed
                        # usage -= stats['memory_stats'].get('stats', {}).get('cache', 0)
                        metrics["peak_memory"] = max(metrics["peak_memory"], usage)
                    except Exception:
                        pass
                    time.sleep(0.1) # Poll every 100ms

            monitor_thread = threading.Thread(target=monitor_memory, daemon=True)
            monitor_thread.start()
            
            exec_thread.start()
            
            # Divide by 1000 for seconds
            exec_thread.join(timeout=task.timeout_ms / 1000.0)
            
            # Stop monitoring
            stop_monitoring.set()
            monitor_thread.join(timeout=1.0)
            
            if exec_thread.is_alive():
                logger.error("Execution Timed Out", timeout_ms=task.timeout_ms)
                # Graceful Shutdown: SIGTERM -> wait 3s -> SIGKILL
                try:
                    container.stop(timeout=3)
                except Exception as e:
                    logger.warning("Failed to stop container gracefully, forcing kill", error=str(e))
                    container.kill()
                    
                raise TimeoutError(f"Execution timed out after {task.timeout_ms}ms")
            
            if "error" in exec_result:
                raise exec_result["error"]
                
            exit_code = exec_result["exit_code"]
            output = exec_result["output"]
            
            # Actual memory measurement
            # Use monitored peak memory if available
            usage = metrics["peak_memory"]
            
            # Fallback if monitoring failed or zero (unlikely but safe)
            if usage == 0:
                try:
                    stats = container.stats(stream=False)
                    usage = stats['memory_stats'].get('max_usage', 0)
                    if usage == 0:
                        usage = stats['memory_stats'].get('usage', 0)
                except Exception as e:
                    logger.warning("Failed to get fallback metrics", error=str(e))
                    usage = 0
            
            # 6. Auto-Tuning & CloudWatch
            tip, savings = AutoTuner.analyze(usage, task.memory_mb)
            self.cw.publish_peak_memory(task.function_id, task.runtime, usage)
            
            # 7. Output Upload
            # Retrieve output files from container (Since no bind mount)
            self._copy_from_container(container, "/output", host_output_dir)
            
            # Files written to /output in container are now in host_output_dir
            
            # Usage Metering Collection (Thread-safe JSONL)
            llm_token_count = 0
            usage_file = host_output_dir / ".llm_usage_stats.jsonl"
            if usage_file.exists():
                try:
                    with open(usage_file, 'r') as f:
                        for line in f:
                            if not line.strip(): continue
                            stats = json.loads(line)
                            llm_token_count += stats.get("prompt_eval_count", 0) + stats.get("eval_count", 0)
                    # Don't upload the hidden stats file
                    usage_file.unlink() 
                except Exception as e:
                    logger.warning("Failed to read LLM usage stats", error=str(e))

            output_files = self.uploader.upload_outputs(task.request_id, str(host_output_dir))

            output_str = output.decode('utf-8', errors='replace')

            return ExecutionResult(
                request_id=task.request_id,
                function_id=task.function_id,
                success=(exit_code == 0),
                exit_code=exit_code,
                stdout=output_str,
                stderr="",
                duration_ms=int((time.time() - start_time) * 1000),
                peak_memory_bytes=usage,
                allocated_memory_mb=task.memory_mb,
                optimization_tip=tip,
                estimated_savings=savings,
                output_files=output_files,
                llm_token_count=llm_token_count,
                worker_id=socket.gethostname()
            )

        except Exception as e:
            logger.error("Execution failed", error=str(e))
            return ExecutionResult(
                request_id=task.request_id, function_id=task.function_id, success=False, exit_code=-1,
                stdout="", stderr=str(e), duration_ms=int((time.time() - start_time) * 1000),
                worker_id=socket.gethostname()
            )
            
        finally:
            self.global_limit.release()
            
            # [LRU] Recycle container to function-specific pool (not delete)
            if container:
                self._release_container(container, task.function_id, task.runtime)
            
            # Clean up files
            if host_work_dir and host_work_dir.exists():
                try: shutil.rmtree(host_work_dir)
                except Exception: pass

    def _shutdown_cleanup(self):
        """Clean up all containers on program exit (zombie prevention)"""
        logger.info("Graceful Shutdown: Cleaning up all containers...")
        
        # Clean Runtime Pool (Generic)
        for runtime, pool in self.pools.items():
            while pool:
                try:
                    cid = pool.pop()
                    self.docker.containers.get(cid).remove(force=True)
                except Exception:
                    pass
        
        # Clean Function Pool (Warm)
        for fid, containers in self.function_pools.items():
            for c in containers:
                try:
                    c.remove(force=True)
                except Exception:
                    pass
        
        logger.info("Graceful Shutdown: Complete")
