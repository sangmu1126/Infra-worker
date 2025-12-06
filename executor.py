import os
import shutil
import time
import shutil
import time
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
    s3_key: str
    s3_bucket: Optional[str] = None
    memory_mb: int = 128
    timeout_ms: int = 300000
    payload: Dict = field(default_factory=dict)

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

    def to_dict(self):
        return {
            "requestId": self.request_id,
            "status": "SUCCESS" if self.success else "FAILED",
            "exitCode": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "durationMs": self.duration_ms,
            "peakMemoryBytes": self.peak_memory_bytes,
            "allocatedMemoryMb": self.allocated_memory_mb,
            "optimizationTip": self.optimization_tip,
            "estimatedSavings": self.estimated_savings,
            "outputFiles": self.output_files
        }

# --- Service Logic ---

class AutoTuner:
    """메모리 최적화 팁 생성 (비용 절감 핵심)"""
    COST_PER_MB_HOUR = 0.00005  # $0.00005 (임의의 AWS EC2 GB-hour 비용 기반 추정)

    @staticmethod
    def analyze(peak_bytes: int, allocated_mb: int):
        if not peak_bytes: return None, None
        if allocated_mb <= 0: allocated_mb = 128  # 0 나누기 방어

        peak_mb = peak_bytes / (1024 * 1024)
        
        # 1. 팁 생성
        tip = None
        ratio = peak_mb / allocated_mb
        if ratio < 0.3:
            rec = max(int(peak_mb * 1.5), 10) # 최소 10MB 권장
            saved_percent = int((1 - (rec / allocated_mb)) * 100)
            if saved_percent > 0:
                tip = f"💡 Tip: 실제 사용량({int(peak_mb)}MB)이 할당량({allocated_mb}MB)보다 훨씬 적습니다. {rec}MB로 줄여 비용을 약 {saved_percent}% 절감하세요."
        elif ratio > 0.9:
            rec = int(peak_mb * 1.2)
            tip = f"⚠️ Warning: 메모리가 부족합니다({int(peak_mb)}MB). {rec}MB 이상으로 늘리는 것을 권장합니다."

        # 2. 비용 절감액 계산 (비즈니스 관점)
        # 가정: 기존 VM 오버헤드 1024MB 대비 절감
        vm_overhead_mb = 1024
        saved_mb = vm_overhead_mb - peak_mb
        estimated_savings = None
        
        if saved_mb > 0:
            # 월간 절감액 (730시간 기준)
            monthly_saving = saved_mb * AutoTuner.COST_PER_MB_HOUR * 730
            estimated_savings = f"${monthly_saving:.2f}/month (vs 1GB VM)"

        return tip, estimated_savings

class CloudWatchPublisher:
    """ASG 연동을 위한 CloudWatch 메트릭 전송"""
    def __init__(self, region):
        self.client = boto3.client("cloudwatch", region_name=region)
        
    def publish_peak_memory(self, func_id, runtime, bytes_used):
        try:
            if bytes_used is None: return
            # 비동기로 처리하면 더 좋음 (여기선 로깅만)
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
    """통합 실행 엔진: S3 다운로드 -> Docker 실행 -> 결과 처리"""
    
    def __init__(self, config: Dict):
        self.cfg = config
        self.docker = docker.from_env()
        self.s3 = boto3.client("s3", region_name=config.get("AWS_REGION", "ap-northeast-2"))
        self.cw = CloudWatchPublisher(config.get("AWS_REGION", "ap-northeast-2"))
        self.uploader = OutputUploader(
            bucket_name=config.get("S3_USER_DATA_BUCKET", ""),
            region=config.get("AWS_REGION", "ap-northeast-2")
        )
        
        # Warm Pool 저장소
        self.pools = {
            "python": deque(), "cpp": deque(), "nodejs": deque(), "go": deque()
        }
        self.images = {
            "python": config.get("DOCKER_PYTHON_IMAGE", "nanogrid/python:3.9-fat"),
            "cpp": config.get("DOCKER_CPP_IMAGE", "gcc:latest"),
            "nodejs": config.get("DOCKER_NODEJS_IMAGE", "node:18-alpine"),
            "go": config.get("DOCKER_GO_IMAGE", "golang:1.19-alpine")
        }
        
        self._initialize_warm_pool()

    def _initialize_warm_pool(self):
        """Warm Pool 초기화 (Cold Start 제거)"""
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
            # 무한 대기 컨테이너 실행
            c = self.docker.containers.run(
                img, command="tail -f /dev/null", detach=True,
                # 호스트 경로 마운트 (코드 실행용) - 읽기 전용으로 마운트하거나 필요한 경로만 마운트 권장
                # 여기서는 편의상 전체 작업 루트를 마운트
                volumes={self.cfg["DOCKER_WORK_DIR_ROOT"]: {"bind": "/workspace", "mode": "rw"}},
                network_mode="bridge", # AI Endpoint 접근 허용
                mem_limit=f"{task.memory_mb}m",    # 컨테이너 하드 리밋 (Dynamic)
                cpu_quota=50000      # 0.5 CPU
            )
            c.pause()
            self.pools[runtime].append(c.id)
            return c.id
        except Exception as e:
            logger.error("Failed to create warm container", runtime=runtime, error=str(e))
            return None

    def _acquire_container(self, runtime: str):
        """Warm Pool에서 컨테이너 획득 (Unpause)"""
        target_runtime = runtime if runtime in self.pools else "python"
        
        # Pool이 비어있으면 즉시 생성 (동기)
        if not self.pools[target_runtime]:
            logger.warning("Pool empty, creating new container synchronously", runtime=target_runtime)
            cid = self._create_warm_container(target_runtime)
            # 방금 만든건 append 되었으므로 다시 pop 해야 함
            if not cid: raise RuntimeError("Failed to create container")
            
        try:
            cid = self.pools[target_runtime].popleft()
            c = self.docker.containers.get(cid)
            if c.status == 'paused':
                c.unpause()
            return c
        except Exception:
            # 실패 시(이미 죽은 컨테이너 등) 재귀적으로 다시 시도
            return self._acquire_container(target_runtime)

    def _prepare_workspace(self, task: TaskMessage) -> Path:
        """S3 다운로드 및 [보안] Zip Slip 방지 압축 해제"""
        local_dir = Path(self.cfg["DOCKER_WORK_DIR_ROOT"]) / task.request_id
        if local_dir.exists(): shutil.rmtree(local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)
        
        zip_path = local_dir / "code.zip"
        bucket = task.s3_bucket if task.s3_bucket else self.cfg["S3_CODE_BUCKET"]
        self.s3.download_file(bucket, task.s3_key, str(zip_path))
        
        # ✅ [FIX] Zip Slip 방지 코드 적용
        with zipfile.ZipFile(zip_path, "r") as zf:
            for member in zf.namelist():
                # 상위 디렉터리(../) 접근 시도 차단
                target_path = (local_dir / member).resolve()
                if not str(target_path).startswith(str(local_dir.resolve())):
                    logger.warning("Zip Slip attempt detected", file=member)
                    continue
                
                # 파일 추출
                if member.endswith('/'):
                    target_path.mkdir(parents=True, exist_ok=True)
                else:
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    with zf.open(member) as source, open(target_path, "wb") as dest:
                        shutil.copyfileobj(source, dest)
        
        zip_path.unlink()
        return local_dir

    def run(self, task: TaskMessage) -> ExecutionResult:
        container = None
        host_work_dir = None
        start_time = time.time()
        
        try:
            # 1. 작업 공간 준비
            host_work_dir = self._prepare_workspace(task)
            # 컨테이너 내부 경로 (/workspace 가 바인드 되어있으므로 그 하위 경로 사용)
            container_work_dir = f"/workspace/{task.request_id}"

            # 2. 컨테이너 획득 (Warm Start)
            container = self._acquire_container(task.runtime)
            
            # 3. 실행 커맨드 구성
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
                "JOB_ID": task.request_id
            }

            cmd = []
            if task.runtime == "python": 
                cmd = ["sh", "-c", f"{setup_cmd} && python {container_work_dir}/main.py"]
            elif task.runtime == "cpp":  
                # C++은 컴파일 후 실행
                cmd = ["sh", "-c", f"{setup_cmd} && g++ {container_work_dir}/main.cpp -o {container_work_dir}/out && {container_work_dir}/out"]
            elif task.runtime == "nodejs": 
                cmd = ["sh", "-c", f"{setup_cmd} && node {container_work_dir}/index.js"]
            elif task.runtime == "go":
                # Go는 빌드 후 실행
                cmd = ["sh", "-c", f"{setup_cmd} && cd {container_work_dir} && go build -o main main.go && ./main"]

            # 4. 실행 (Exec)
            logger.info("Exec command", cmd=cmd, container=container.id[:12])
            exit_code, output = container.exec_run(
                cmd, workdir="/workspace", demux=False,
                environment=env_vars
            )
            
            # ✅ [FIX] 하드코딩 제거 & 실제 메모리 측정
            try:
                stats = container.stats(stream=False)
                # Max usage is more accurate for peak memory during execution
                usage = stats['memory_stats'].get('max_usage', 0)
                # Fallback to usage if max_usage is 0 or missing (rare in normal docker)
                if usage == 0:
                    usage = stats['memory_stats'].get('usage', 0)
            except Exception as e:
                logger.warning("Failed to get metrics", error=str(e))
                usage = 0
            
            # 6. Auto-Tuning & CloudWatch
            tip, savings = AutoTuner.analyze(usage, task.memory_mb)
            self.cw.publish_peak_memory(task.function_id, task.runtime, usage)
            
            # 7. Output Upload
            # 컨테이너 내에서 /output에 쓴 파일들은 host_output_dir에 저장됨
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
                output_files=output_files
            )

        except Exception as e:
            logger.error("Execution failed", error=str(e))
            return ExecutionResult(
                request_id=task.request_id, success=False, exit_code=-1,
                stdout="", stderr=str(e), duration_ms=int((time.time() - start_time) * 1000)
            )
            
        finally:
            # ✅ [FIX] 오염된 컨테이너는 재사용하지 않고 폐기
            if container:
                try:
                    # Dirty Container Removal
                    container.remove(force=True)
                except: pass
            
            # 파일 삭제
            if host_work_dir and host_work_dir.exists():
                try: shutil.rmtree(host_work_dir)
                except: pass