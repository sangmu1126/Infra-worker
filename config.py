import os

# --- AWS Configuration ---
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
S3_CODE_BUCKET = os.getenv("S3_CODE_BUCKET", "faas-user-code")
S3_USER_DATA_BUCKET = os.getenv("S3_USER_DATA_BUCKET", "faas-user-data")

# --- Redis Configuration ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# --- Costs & Limits ---
# Cost per MB-hour (Estimated based on loose AWS EC2 GB-hour cost)
COST_PER_MB_HOUR = 0.00005 

# Max containers per function for warm pool (LRU)
MAX_POOL_SIZE_PER_FUNC = 5

# --- Paths ---
# OS-specific Cgroup paths (Amazon Linux 2023 / Cgroup v2)
CGROUP_PATH_IO_STAT = "/sys/fs/cgroup/system.slice/docker-{container_id}.scope/io.stat"
CGROUP_PATH_MEMORY_PEAK = "/sys/fs/cgroup/system.slice/docker-{container_id}.scope/memory.peak"

# Docker Work Directory (Host)
DOCKER_WORK_DIR_ROOT = os.getenv("DOCKER_WORK_DIR_ROOT", "/tmp/faas/workspace")

# --- Docker Images ---
DOCKER_IMAGES = {
    "python": os.getenv("DOCKER_PYTHON_IMAGE", "faas/python:3.9-fat"),
    "cpp": os.getenv("DOCKER_CPP_IMAGE", "gcc:latest"),
    "nodejs": os.getenv("DOCKER_NODEJS_IMAGE", "node:18-alpine"),
    "go": os.getenv("DOCKER_GO_IMAGE", "golang:1.19-alpine")
}

# --- Warm Pool Sizes ---
WARM_POOL_SIZES = {
    "python": int(os.getenv("WARM_POOL_PYTHON_SIZE", 1)),
    "cpp": int(os.getenv("WARM_POOL_CPP_SIZE", 1)),
    "nodejs": int(os.getenv("WARM_POOL_NODEJS_SIZE", 1)),
    "go": int(os.getenv("WARM_POOL_GO_SIZE", 1))
}

# --- Execution Limits ---
# Max size of stdout/stderr to capture (bytes)
MAX_OUTPUT_SIZE = int(os.getenv("MAX_OUTPUT_SIZE", 1024 * 1024)) # 1MB
