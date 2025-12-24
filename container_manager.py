import docker
import threading
import structlog
import time
import os
import io
import tarfile
from collections import deque
from pathlib import Path
from typing import Dict, Optional, List

import config

logger = structlog.get_logger()

class ContainerManager:
    """
    Manages Docker container lifecycle, including creation, execution, and warm pools.
    """
    def __init__(self, docker_client=None):
        self.docker = docker_client or docker.from_env()
        
        # Runtime-based Warm Pool (Generic)
        self.pools = {
            "python": deque(), "cpp": deque(), "nodejs": deque(), "go": deque()
        }
        
        # Function-specific Warm Pool (Secure & Fast)
        self.function_pools = {} # Key: function_id, Value: List[Container]
        
        # Locks
        self.function_pool_lock = threading.Lock()
        self.pool_locks = {
            k: threading.Lock() for k in ["python", "cpp", "nodejs", "go"]
        }
        
        # Pre-pull images
        self._ensure_images()
        
        # Initialize pools
        self._initialize_warm_pool()

    def _ensure_images(self):
        logger.info("🐳 Pre-pulling Docker images...")
        for runtime, img_name in config.DOCKER_IMAGES.items():
            try:
                self.docker.images.get(img_name)
                logger.debug(f"✓ Image ready: {img_name}")
            except docker.errors.ImageNotFound:
                logger.info(f"📥 Pulling image: {img_name}")
                self.docker.images.pull(img_name)

    def _initialize_warm_pool(self):
        logger.info("🔥 Initializing Warm Pools", counts=config.WARM_POOL_SIZES)
        for runtime, count in config.WARM_POOL_SIZES.items():
            for _ in range(count):
                self._create_warm_container(runtime)

    def _create_warm_container(self, runtime: str) -> str:
        try:
            img = config.DOCKER_IMAGES.get(runtime)
            # Run infinite wait container
            c = self.docker.containers.run(
                img, command="tail -f /dev/null", detach=True,
                network_mode="bridge",
                mem_limit="1024m",
                cpu_quota=100000 
            )
            c.pause()
            self.pools[runtime].append(c.id)
            return c.id
        except Exception as e:
            logger.error("Failed to create warm container", runtime=runtime, error=str(e))
            return None

    def acquire_container(self, runtime: str, function_id: str = None):
        """
        Acquire container with priority:
        1. Function-specific warm pool
        2. Generic runtime pool
        """
        target_runtime = runtime if runtime in self.pools else "python"
        
        # 1. Warm Pool Check
        if function_id:
            with self.function_pool_lock:
                if function_id in self.function_pools and self.function_pools[function_id]:
                    container = self.function_pools[function_id].pop()
                    # 🛑 [Optimized] Warm Pool items are recycling/running, no need to unpause
                    # (Skipping API call saves ~50ms)
                    logger.info("⚡ Warm Start from function pool", function_id=function_id)
                    container.is_warm = True
                    return container
        
        # 2. Generic Pool Check
        cid = None
        with self.pool_locks[target_runtime]:
            if not self.pools[target_runtime]:
                logger.warning("Pool empty, creating new container synchronously", runtime=target_runtime)
                cid = self._create_warm_container(target_runtime)
                if not cid: raise RuntimeError("Failed to create container")
            
            if self.pools[target_runtime]:
                cid = self.pools[target_runtime].popleft()
                
        if not cid: raise RuntimeError("Failed to acquire container")

        try:
            c = self.docker.containers.get(cid)
            try:
                c.unpause()
            except Exception: pass
            logger.info("🥶 Cold Start from runtime pool", runtime=target_runtime)
            
            # Asynchronously replenish generic pool
            self._replenish_pool(target_runtime)
            
            c.is_warm = False
            return c
        except Exception:
            return self.acquire_container(runtime, function_id)

    def release_container(self, container, function_id: str):
        """Return container to function-specific pool."""
        try:
            with self.function_pool_lock:
                if function_id not in self.function_pools:
                    self.function_pools[function_id] = []
                
                pool = self.function_pools[function_id]
                
                if len(pool) >= config.MAX_POOL_SIZE_PER_FUNC:
                    oldest = pool.pop(0)
                    try:
                        oldest.remove(force=True)
                    except Exception: pass
                
                pool.append(container)
                logger.info("♻️ Container recycled", function_id=function_id, pool_size=len(pool))
        except Exception as e:
            logger.warning("Failed to recycle container", error=str(e))
            try:
                container.remove(force=True)
            except Exception: pass

    def _replenish_pool(self, runtime: str):
        def _create():
            try:
                self._create_warm_container(runtime)
            except Exception as e:
                logger.error("Failed to replenish pool", error=str(e))
        threading.Thread(target=_create, daemon=True).start()

    def update_resources(self, container, memory_mb: int):
        # 🛑 [Optimized] Skip if unchanged (Caching)
        current_limit = getattr(container, "last_memory_mb", None)
        if current_limit == memory_mb:
            return  # Save ~50ms

        try:
            container.update(mem_limit=f"{memory_mb}m", memswap_limit=f"{memory_mb}m")
            container.last_memory_mb = memory_mb
        except Exception as e:
            logger.warning("Failed to update container resources", error=str(e))

    def reset_cgroup_peak(self, container_id: str):
        try:
            path = config.CGROUP_PATH_MEMORY_PEAK.format(container_id=container_id)
            if os.path.exists(path):
                with open(path, "w") as f:
                    f.write("reset")
        except Exception: pass

    def get_cgroup_memory_peak(self, container_id: str) -> int:
        try:
            path = config.CGROUP_PATH_MEMORY_PEAK.format(container_id=container_id)
            if os.path.exists(path):
                with open(path, "r") as f:
                    return int(f.read().strip())
        except Exception: pass
        return 0

    def get_io_bytes(self, container_id: str) -> int:
        try:
            path = config.CGROUP_PATH_IO_STAT.format(container_id=container_id)
            total = 0
            if os.path.exists(path):
                with open(path, "r") as f:
                    for line in f:
                        parts = line.split()
                        for p in parts:
                            if p.startswith("r=") or p.startswith("w="):
                                total += int(p.split("=")[1])
            return total
        except Exception:
            return 0

    def copy_to_container(self, container, source_path: Path, target_path: str):
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode='w') as tar:
            tar.add(source_path, arcname=".")
        stream.seek(0)
        container.exec_run(f"mkdir -p {target_path}")
        container.put_archive(target_path, stream)

    def copy_from_container(self, container, source_path: str, target_local_path: Path):
        try:
            stream, stat = container.get_archive(source_path)
            temp_tar = target_local_path / "temp_output.tar"
            with open(temp_tar, "wb") as f:
                for chunk in stream:
                    f.write(chunk)
            with tarfile.open(temp_tar, mode='r') as tar:
                tar.extractall(path=target_local_path)
            temp_tar.unlink()
        except Exception as e:
            logger.warning("Failed to copy from container", error=str(e))
