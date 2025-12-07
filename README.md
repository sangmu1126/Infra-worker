# Infra-worker (NanoAgent)

<div align="center">

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?style=for-the-badge&logo=python)
![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED?style=for-the-badge&logo=docker)
![AWS](https://img.shields.io/badge/AWS-SQS%20%7C%20S3-FF9900?style=for-the-badge&logo=amazonaws)
![Redis](https://img.shields.io/badge/Redis-Pub%2FSub-DC382D?style=for-the-badge&logo=redis)
![Prometheus](https://img.shields.io/badge/Prometheus-Monitoring-E6522C?style=for-the-badge&logo=prometheus)

**High-Performance Serverless Task Executor for NanoGrid**

</div>

---

## 📖 Introduction

`Infra-worker`(NanoAgent)는 서버리스 아키텍처의 핵심 실행 단위로, **AWS SQS**로부터 작업을 수신하고 **Docker** 컨테이너 내에서 안전하고 격리된 환경으로 코드를 실행하는 고성능 워커입니다.

이 프로젝트는 단순한 작업 실행을 넘어, **Cold Start 방지**, **메모리 최적화(AutoTuner)**, **보안(Zip Slip 방지)**, **실시간 모니터링(Prometheus)** 등 엔터프라이즈급 기능을 제공합니다.

---

## 🏗️ System Architecture

```mermaid
flowchart TD
    subgraph AWS Cloud
        SQS[AWS SQS Queue] -->|1. Long Polling| Agent[NanoAgent (Worker)]
        S3_Code[S3 Bucket (Code)] -->|2. Download Code| Agent
    end

    subgraph Worker Node
        Agent -->|3. Acquire Container| WarmPool[Warm Pool]
        WarmPool -->|4. Execute| Container[Docker Container]
        
        subgraph Docker Environment
            Container -->|5. Run Code| Runtime[Runtime (Python/Node/C++/Go)]
        end
        
        Runtime -->|6. Logs & Output| Agent
        Agent -->|7. Analyze Memory| AutoTuner[AutoTuner]
    end

    subgraph Data & Monitoring
        Agent -->|8. Pub/Sub Result| Redis[Redis]
        Agent -->|9. Upload Output| S3_Out[S3 Bucket (Output)]
        Agent -->|10. Metrics| Prometheus[Prometheus]
        Agent -->|11. CloudWatch| CW[CloudWatch]
    end
```

---

## 🚀 Key Features

### ⚡ Performance & Efficiency
| Feature | Description | Benefit |
| :--- | :--- | :--- |
| **SQS Long Polling** | SQS 메시지를 실시간으로 수신하여 즉각적인 작업 처리 | 대기 시간 최소화 |
| **Warm Pool** | 미리 실행된 컨테이너 풀(Python, Node, C++, Go) 유지 | **Zero Cold Start** |
| **AutoTuner** | 실제 메모리 사용량을 분석하여 최적의 할당량 제안 | **비용 절감 (최대 40%)** |
| **Async Result** | Redis Pub/Sub을 통한 실시간 결과 전송 및 TTL 캐싱 | 높은 응답성 |

### 🔒 Security & Stability
- **Zip Slip Protection**: 압축 해제 시 상위 디렉토리 접근 공격(`../../`) 자동 차단.
- **Docker Isolation**: 모든 작업은 격리된 컨테이너 내부에서 실행되어 호스트 시스템 보호.
- **Resource Limits**: CPU/Memory Quota 설정을 통한 'Noisy Neighbor' 문제 방지.
- **Time Limits**: 작업별 타임아웃 강제 적용으로 무한 루프 방지.

### 📊 Observability
- **Prometheus Exporter**: 포트 `8000`에서 표준형 메트릭 제공.
- **CloudWatch Integration**: 작업별 피크 메모리 사용량을 AWS CloudWatch로 전송.
- **Execution Metadata**: 실행 시간, Peak Memory, Exit Code, stdout/stderr 수집.

---

## 🛠️ Installation

### Prerequisites
*   Python 3.9+
*   Docker & Docker Compose
*   Redis Server
*   AWS Credentials (configured via `~/.aws/credentials` or Environment Variables)

### Setup Steps

```bash
# 1. Clone Repository
git clone https://github.com/Softbank-Final/Infra-worker.git
cd Infra-worker

# 2. Install Dependencies
pip install -r requirements.txt

# 3. Setup Environment Variables
cp .env.example .env
# Edit .env file with your configurations

# 4. Run Agent
python agent.py
```

---

## ⚙️ Configuration

| Variable | Description | Default | Required |
| :--- | :--- | :--- | :---: |
| `AWS_REGION` | AWS 리전 | `ap-northeast-2` | No |
| `SQS_QUEUE_URL` | 작업을 수신할 SQS URL | - | **Yes** |
| `REDIS_HOST` | Redis 서버 호스트 | `localhost` | **Yes** |
| `REDIS_PORT` | Redis 서버 포트 | `6379` | No |
| `S3_CODE_BUCKET` | 코드가 저장된 S3 버킷 | - | **Yes** |
| `S3_USER_DATA_BUCKET`| 결과를 업로드할 S3 버킷 | - | **Yes** |
| `WARM_POOL_PYTHON_SIZE` | Python 웜 풀 크기 | `1` | No |
| `DOCKER_WORK_DIR_ROOT` | 호스트 작업 디렉토리 | `/tmp/tasks` | No |

---

## 📈 Monitoring Metrics (Prometheus)

The Agent exposes metrics at `http://localhost:8000/metrics`.

| Metric Name | Type | Description | Labels |
| :--- | :--- | :--- | :--- |
| `worker_jobs_processed_total` | Counter | 처리된 총 작업 수 | `status`, `runtime`, `model` |
| `worker_job_duration_seconds` | Histogram | 작업 실행 시간 분포 | `runtime`, `model` |
| `worker_active_jobs` | Gauge | 현재 실행 중인 작업 수 | - |

---

## 📂 Project Structure

```bash
Infra-worker/
├── agent.py                 # Main Entrypoint: SQS Polling & Orchestration
├── executor.py              # TaskExecutor: Docker Management & Logic
├── uploader.py              # OutputUploader: S3 File Uploads
├── ai_client.py             # Injectable Client for User Code
├── requirements.txt         # Python Dependencies
├── nanogrid-worker.service  # Systemd Service File
└── docker/                  # Dockerfiles for Runtimes
    ├── python/
    ├── nodejs/
    ├── cpp/
    └── go/
```

---

## 🧩 Usage Example

### Job Payload (JSON)
SQS 메시지에 포함될 작업 정의입니다.

```json
{
  "requestId": "job-12345",
  "functionId": "func-abc",
  "runtime": "python",
  "s3Key": "users/user1/code.zip",
  "s3Bucket": "my-code-bucket",
  "memoryMb": 256,
  "timeoutMs": 10000,
  "input": {
    "key1": "value1"
  }
}
```

### Redis Result (Channel: `result:job-12345`)
```json
{
  "requestId": "job-12345",
  "status": "SUCCESS",
  "exitCode": 0,
  "stdout": "Hello World",
  "durationMs": 1250,
  "peakMemoryBytes": 15728640,
  "optimizationTip": "💡 Tip: 실제 사용량(15MB)이 할당량(256MB)보다 적습니다...",
  "outputFiles": [
    "https://s3.ap-northeast-2.amazonaws.com/my-bucket/outputs/job-12345/result.png"
  ]
}
```

---

<div align="center">
  <sub>Built with ❤️ by Softbank-Final Team</sub>
</div>
