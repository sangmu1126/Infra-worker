from dataclasses import dataclass, field
from typing import List, Optional, Dict

@dataclass
class TaskMessage:
    """Represents a task received from SQS."""
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
    """Represents the result of a function execution."""
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
    llm_token_count: Optional[int] = 0

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
