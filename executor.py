import os
import shutil
import time
import threading
import zipfile
import json
import structlog
import boto3
import docker
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
    success: bool
    exit_code: int
    stdout: str
    stderr: str
    duration_ms: int
    peak_memory_bytes: Optional[int] = None
    allocated_memory_mb: Optional[int] = None
    optimization_tip: Optional[str] = None
    estimated_savings: Optional[str] = None
    output_files: List[str] = field(default_factory=list)
    llm_token_count: Optional[int] = 0 # Feature 3: Usage Metering

    def to_dict(self):
        return {
            "requestId": self.request_id,
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
        # Assumption: Savings compared to 1024MB VM overhead
        vm_overhead_mb = 1024
        saved_mb = vm_overhead_mb - peak_mb
        estimated_savings = None
        
        if saved_mb > 0:
            # Monthly savings (based on 730 hours)
            monthly_saving = saved_mb * AutoTuner.COST_PER_MB_HOUR * 730
            estimated_savings = f"${monthly_saving:.2f}/month (vs 1GB VM)"

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
                Namespace="NanoGrid/FunctionRunner",
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
    
    def __init__(self, config: Dict):
        self.cfg = config
        self.docker = docker.from_env()
        self.s3 = boto3.client("s3", region_name=config.get("AWS_REGION", "ap-northeast-2"))
        self.cw = CloudWatchPublisher(config.get("AWS_REGION", "ap-northeast-2"))
        self.uploader = OutputUploader(
            bucket_name=config.get("S3_USER_DATA_BUCKET", ""),
            region=config.get("AWS_REGION", "ap-northeast-2")
        )
        
        # Warm Pool Storage
        self.pools = {
            "python": deque(), "cpp": deque(), "nodejs": deque(), "go": deque()
        }
        self.images = {
            "python": config.get("DOCKER_PYTHON_IMAGE", "nanogrid/python:3.9-fat"),
            "cpp": config.get("DOCKER_CPP_IMAGE", "gcc:latest"),
            "nodejs": config.get("DOCKER_NODEJS_IMAGE", "node:18-alpine"),
            "go": config.get("DOCKER_GO_IMAGE", "golang:1.19-alpine")
        }
        
        # Runtime-specific locks
        self.pool_locks = {
            k: threading.Lock() for k in ["python", "cpp", "nodejs", "go"]
        }
        
        self._initialize_warm_pool()

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
                # Mount host path (for code execution) - Read-only or limiting paths is recommended
                # Mounting entire work root for convenience here
                volumes={self.cfg["DOCKER_WORK_DIR_ROOT"]: {"bind": "/workspace", "mode": "rw"}},
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

    def _acquire_container(self, runtime: str):
        """Acquire container from Warm Pool (Unpause)"""
        target_runtime = runtime if runtime in self.pools else "python"
        
        # Create immediately if pool is empty (Synchronous)
        # Use lock to ensure atomic check-and-act
        cid = None
        with self.pool_locks[target_runtime]:
            if not self.pools[target_runtime]:
                logger.warning("Pool empty, creating new container synchronously", runtime=target_runtime)
                # Create container INSIDE lock to prevent multiple threads creating simultaneously for same depletion
                cid = self._create_warm_container(target_runtime)
                if not cid: raise RuntimeError("Failed to create container")
            
            # Now pop (guaranteed to have one unless creation failed)
            if self.pools[target_runtime]:
                cid = self.pools[target_runtime].popleft()
            
        if not cid:
             raise RuntimeError("Failed to acquire container")

        try:
            c = self.docker.containers.get(cid)
            if c.status == 'paused':
                c.unpause()
            return c
        except Exception:
            # Retry recursively on failure (e.g., dead container)
            return self._acquire_container(target_runtime)

    def _replenish_pool(self, runtime: str):
        """Replenish the warm pool asynchronously after usage"""
        def _create():
            try:
                self._create_warm_container(runtime)
                logger.info("Pool replenished", runtime=runtime)
            except Exception as e:
                logger.error("Failed to replenish pool", error=str(e))
        
        # Run in a separate thread to avoid blocking the main execution flow
        threading.Thread(target=_create, daemon=True).start()

    def _prepare_workspace(self, task: TaskMessage) -> Path:
        """S3 Download and [Security] Zip Slip prevention during extraction"""
        local_dir = Path(self.cfg["DOCKER_WORK_DIR_ROOT"]) / task.request_id
        if local_dir.exists(): shutil.rmtree(local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)
        
        zip_path = local_dir / "code.zip"
        bucket = task.s3_bucket if task.s3_bucket else self.cfg["S3_CODE_BUCKET"]
        self.s3.download_file(bucket, task.s3_key, str(zip_path))
        
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

        # Inject ai_client.py
        try:
            current_dir = Path(__file__).parent
            src_client = current_dir / "ai_client.py"
            shutil.copy(str(src_client), str(local_dir / "ai_client.py"))
        except Exception as e:
            logger.error("Failed to inject ai_client.py", error=str(e))

        return local_dir

    def run(self, task: TaskMessage) -> ExecutionResult:
        container = None
        host_work_dir = None
        start_time = time.time()
        
        try:
            # 1. Prepare workspace
            host_work_dir = self._prepare_workspace(task)
            # Container internal path (use subpath as /workspace is bound)
            container_work_dir = f"/workspace/{task.request_id}"

            # 2. Acquire container (Warm Start)
            container = self._acquire_container(task.runtime)
            
            # Trigger pool replenishment immediately
            self._replenish_pool(task.runtime)
            
            # Apply dynamic memory limit (Just before execution)
            try:
                container.update(mem_limit=f"{task.memory_mb}m", memswap_limit=f"{task.memory_mb}m")
            except Exception as e:
                logger.warning("Failed to update container memory limit", error=str(e))
            
            # 3. Configure execution command
            # Output Directory Setup
            host_output_dir = host_work_dir / "output"
            host_output_dir.mkdir(parents=True, exist_ok=True)
            
            # Symlink /workspace/{req_id}/output -> /output inside container
            # This allows user code to write to /output transparently
            # Note: /output exists from Dockerfile, so we must remove it to create the symlink at /output
            setup_cmd = f"rm -rf /output && ln -s {container_work_dir}/output /output"

            # Environment Variables
            env_vars = {
                "PAYLOAD": json.dumps(task.payload),
                "AI_ENDPOINT": self.cfg.get("AI_ENDPOINT", "http://10.0.20.100:11434"),
                "JOB_ID": task.request_id,
                "LLM_MODEL": task.model_id,
                "OUTPUT_DIR": "/output" # Explicit output directory env var
            }

            cmd = []
            if task.runtime == "python": 
                cmd = ["sh", "-c", f"{setup_cmd} && python {container_work_dir}/main.py"]
            elif task.runtime == "cpp":  
                # C++: Compile then execute
                cmd = ["sh", "-c", f"{setup_cmd} && g++ {container_work_dir}/main.cpp -o {container_work_dir}/out && {container_work_dir}/out"]
            elif task.runtime == "nodejs": 
                cmd = ["sh", "-c", f"{setup_cmd} && node {container_work_dir}/index.js"]
            elif task.runtime == "go":
                # Go: Build then execute
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
                    except:
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
                # Force kill container (Removed in finally block)
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
            # Files written to /output in container are saved to host_output_dir
            
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
                llm_token_count=llm_token_count
            )

        except Exception as e:
            logger.error("Execution failed", error=str(e))
            return ExecutionResult(
                request_id=task.request_id, success=False, exit_code=-1,
                stdout="", stderr=str(e), duration_ms=int((time.time() - start_time) * 1000)
            )
            
        finally:
            # Discard polluted container (do not reuse)
            if container:
                try:
                    container.remove(force=True)
                except: pass
            
            # Clean up files
            if host_work_dir and host_work_dir.exists():
                try: shutil.rmtree(host_work_dir)
                except: pass
