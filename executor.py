import os
import time
import json
import threading
import structlog
import socket
from pathlib import Path
from typing import List, Optional, Dict

import config
from container_manager import ContainerManager
from storage_adapter import StorageAdapter
from metrics_collector import MetricsCollector
from uploader import OutputUploader

logger = structlog.get_logger()

# --- Data Models ---
from models import TaskMessage, ExecutionResult

class TaskExecutor:
    """
    Orchestrates the Function-as-a-Service execution flow:
    1. Acquire Container
    2. Prepare Workspace (if cold start)
    3. Inject Code & Payload
    4. Execute
    5. Collect Metrics & Upload Results
    """
    
    def __init__(self, 
                 config_dict: Dict = None, 
                 container_manager: ContainerManager = None,
                 storage_adapter: StorageAdapter = None,
                 metrics_collector: MetricsCollector = None,
                 uploader: OutputUploader = None):
        
        self.cfg = config_dict or {}
        
        # Dependency Injection
        self.containers = container_manager or ContainerManager()
        self.storage = storage_adapter or StorageAdapter()
        self.metrics = metrics_collector or MetricsCollector(
            region=self.cfg.get("AWS_REGION", config.AWS_REGION)
        )
        self.uploader = uploader or OutputUploader(
            bucket_name=self.cfg.get("S3_USER_DATA_BUCKET", config.S3_USER_DATA_BUCKET),
            region=self.cfg.get("AWS_REGION", config.AWS_REGION)
        )

    def run(self, task: TaskMessage) -> ExecutionResult:
        start_time = time.time()
        container = None
        host_work_dir = None
        
        acquired = self.metrics.global_limit.acquire(blocking=True, timeout=30)
        if not acquired:
            logger.error("Global container limit reached", request_id=task.request_id)
            return self._create_busy_response(task, start_time)

        try:
            # Acquire Container
            try:
                container = self.containers.acquire_container(task.runtime, task.function_id)
            except Exception as e:
                logger.error("Failed to acquire container", error=str(e))
                raise e

            is_warm = getattr(container, "is_warm", False)
            
            # Resource Limits
            current_mem = getattr(container, "_mem_limit_mb", None)
            if not is_warm or current_mem != task.memory_mb:
                self.containers.update_resources(container, task.memory_mb)
                container._mem_limit_mb = task.memory_mb

            
            # Workspace Preparation
            if is_warm:
                logger.info("⚡ Warm Start: Skipping Host Workspace Prep", id=container.id[:12])
                host_work_dir = Path(config.DOCKER_WORK_DIR_ROOT) / task.request_id
                host_work_dir.mkdir(parents=True, exist_ok=True)
            else:
                host_work_dir = self.storage.prepare_workspace(
                    task.request_id, task.function_id, task.s3_key, task.s3_bucket
                )
                self.storage.inject_dependencies(host_work_dir)

            # Command & Payload Setup
            host_output_dir = host_work_dir / "output"
            host_output_dir.mkdir(parents=True, exist_ok=True)
            
            payload_str = json.dumps(task.payload)
            use_payload_file = len(payload_str) > 100 * 1024
            
            if use_payload_file:
                with open(host_work_dir / "payload.json", "w") as f:
                    f.write(payload_str)
            
            # Inject into Container
            # Logic: If Cold Start OR Payload file needed, we copy.
            if not is_warm or use_payload_file:
                self.containers.copy_to_container(container, host_work_dir, "/workspace")
            
            cmd, env_vars = self._build_command(task, use_payload_file)
            
            # Execute with Timeout
            start_io = self.containers.get_io_bytes(container.id)
            self.containers.reset_cgroup_peak(container.id)
            
            exit_code, output_bytes = self._execute_in_container(container, cmd, env_vars, task.timeout_ms, host_output_dir)
            
            # Metrics & Cleanup
            end_io = self.containers.get_io_bytes(container.id)
            peak_memory = self.containers.get_cgroup_memory_peak(container.id)
            
            output_str = output_bytes.decode('utf-8', errors='replace')
            duration_ms = int((time.time() - start_time) * 1000)
            
            # Analysis
            tip, savings = self.metrics.analyze_execution(peak_memory, task.memory_mb, end_io - start_io)
            
            # Retrieve Output Files
            self.containers.copy_from_container(container, "/output", host_output_dir)
            
            # Extract LLM Usage
            llm_tokens = self._read_llm_usage(host_output_dir)

            # Background Upload & Reporting
            self._trigger_background_reporting(
                task, peak_memory, host_output_dir, host_work_dir
            )

            return ExecutionResult(
                request_id=task.request_id,
                function_id=task.function_id,
                success=(exit_code == 0),
                exit_code=exit_code,
                stdout=output_str,
                stderr="",
                duration_ms=duration_ms,
                worker_id=socket.gethostname(),
                peak_memory_bytes=peak_memory,
                allocated_memory_mb=task.memory_mb,
                optimization_tip=tip,
                estimated_savings=savings,
                output_files=[f.name for f in host_output_dir.glob("*")],
                llm_token_count=llm_tokens
            )

        except Exception as e:
            logger.error("Execution Flow Failed", error=str(e))
            return ExecutionResult(
                request_id=task.request_id,
                function_id=task.function_id,
                success=False,
                exit_code=-1,
                stdout="",
                stderr=str(e),
                duration_ms=int((time.time() - start_time) * 1000)
            )
        finally:
            self.metrics.global_limit.release()
            # Always return container to pool regardless of success/failure
            if container:
                self.containers.release_container(container, task.function_id)

    def _create_busy_response(self, task, start_time):
        return ExecutionResult(
            request_id=task.request_id,
            function_id=task.function_id,
            success=False,
            exit_code=-1,
            stdout="",
            stderr="Server Busy (503): Too many concurrent executions",
            duration_ms=int((time.time() - start_time) * 1000),
            worker_id=socket.gethostname()
        )

    def _build_command(self, task: TaskMessage, use_payload_file: bool):
        env_vars = {
            "JOB_ID": task.request_id,
            "FUNCTION_ID": task.function_id,
            "MEMORY_MB": str(task.memory_mb),
            "LLM_MODEL": task.model_id,
            "OUTPUT_DIR": "/output"
        }
        
        # Merge user-defined environment variables
        if task.env_vars:
            for key, value in task.env_vars.items():
                # Don't allow overwriting system variables
                if key not in env_vars:
                    env_vars[key] = str(value)
        
        if use_payload_file:
            env_vars["PAYLOAD_FILE"] = "/workspace/payload.json"
        else:
            env_vars["PAYLOAD"] = json.dumps(task.payload)

        setup_cmd = "rm -rf /output /tmp/* && mkdir -p /output"
        
        # Simple command builder (can be moved to a Factory if complex)
        cmd_str = ""
        if task.runtime == "python":
            cmd_str = f"{setup_cmd} && python /workspace/main.py"
        elif task.runtime == "nodejs":
            cmd_str = f"{setup_cmd} && node /workspace/index.js"
        elif task.runtime == "cpp":
            # C++ Execution
            cmd_str = f"{setup_cmd} && if [ -f /workspace/main ]; then chmod +x /workspace/main && /workspace/main; else g++ /workspace/main.cpp -o /workspace/main && /workspace/main; fi"
        elif task.runtime == "go":
            cmd_str = f"{setup_cmd} && cd /workspace && if [ -f main ]; then chmod +x main && ./main; else go build -o main main.go && ./main; fi"
            
        return ["sh", "-c", cmd_str], env_vars

    def _execute_in_container(self, container, cmd, env, timeout_ms, output_dir: Path):
        result = {"exit_code": -1, "output": b""}
        log_file = output_dir / "stdout.log"
        
        # Inject exit code capture into the command
        final_cmd = cmd
        if cmd[0] == "sh" and cmd[1] == "-c":
             final_cmd = ["sh", "-c", f"{{ {cmd[2]} ; }} ; echo $? > /workspace/exit_code.txt"]

        def _run_streaming():
             try:
                 # stream=True returns (exit_code, generator) in newer docker-py
                 # We need to extract the generator
                 _, stream = container.exec_run(final_cmd, workdir="/workspace", environment=env, stream=True)
                 
                 with open(log_file, "wb") as f:
                     for chunk in stream:
                         if chunk: # Only write if chunk is not empty
                             f.write(chunk)
             except Exception as e:
                 logger.error("Stream error", error=str(e))
                 with open(log_file, "ab") as f:
                     f.write(f"\n[System Error] {str(e)}".encode())

        t = threading.Thread(target=_run_streaming)
        t.start()
        t.join(timeout=timeout_ms / 1000.0)
        
        if t.is_alive():
            try: container.stop(timeout=1)
            except: pass
            with open(log_file, "ab") as f:
                f.write(b"\n...[TIMEOUT]...")
            raise TimeoutError(f"Execution timed out after {timeout_ms}ms")
            
        # Read Exit Code
        try:
            ec_out = container.exec_run("cat /workspace/exit_code.txt", workdir="/workspace")
            result["exit_code"] = int(ec_out.output.decode().strip())
        except:
            result["exit_code"] = -1 # content not found or error
            
        # Read Head of Log for DynamoDB (Preview)
        try:
            with open(log_file, "rb") as f:
                result["output"] = f.read(config.MAX_OUTPUT_SIZE)
                if os.path.getsize(log_file) > config.MAX_OUTPUT_SIZE:
                     result["output"] += b"\n...[TRUNCATED: Full logs in S3]..."
        except:
            result["output"] = b""

        return result["exit_code"], result["output"]

    def _read_llm_usage(self, output_dir: Path) -> int:
        usage_file = output_dir / ".llm_usage_stats.jsonl"
        count = 0
        if usage_file.exists():
            try:
                with open(usage_file, 'r') as f:
                    for line in f:
                        if line.strip():
                            stats = json.loads(line)
                            count += stats.get("prompt_eval_count", 0) + stats.get("eval_count", 0)
                usage_file.unlink()
            except: pass
        return count

    def _trigger_background_reporting(self, task, peak_mem, host_out_dir, work_dir):
        def _bg():
            try:
                self.metrics.cw.publish_peak_memory(task.function_id, task.runtime, peak_mem)
                self.uploader.upload_outputs(task.request_id, str(host_out_dir))
            except: pass
            finally:
                if work_dir.exists():
                    try: shutil.rmtree(work_dir)
                    except: pass
        
        threading.Thread(target=_bg, daemon=True).start()

