import sys
from unittest.mock import MagicMock

# Mock dependencies before import
sys.modules["structlog"] = MagicMock()
sys.modules["docker"] = MagicMock()
sys.modules["boto3"] = MagicMock()
sys.modules["redis"] = MagicMock() 

import unittest
from pathlib import Path
import os
import tempfile
from unittest.mock import patch

# Add parent directory to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import local modules (which now utilize mocks for external libs)
# We need to make sure config is not broken if we mock it? 
# actually config.py doesn't depend on external heavily except imports.
# But container_manager imports docker.
# So mocking sys.modules["docker"] handles container_manager's import.

from executor import TaskExecutor, logger
from models import TaskMessage
from container_manager import ContainerManager
from storage_adapter import StorageAdapter
from metrics_collector import MetricsCollector
from uploader import OutputUploader
import config
import shutil

class TestTaskExecutor(unittest.TestCase):
    def setUp(self):
        self.mock_containers = MagicMock(spec=ContainerManager)
        self.mock_storage = MagicMock(spec=StorageAdapter)
        self.mock_metrics = MagicMock(spec=MetricsCollector)
        self.mock_uploader = MagicMock(spec=OutputUploader)
        
        # Setup Global Semaphore Mock
        self.mock_max_sema = MagicMock()
        self.mock_max_sema.acquire.return_value = True
        self.mock_metrics.global_limit = self.mock_max_sema

        self.executor = TaskExecutor(
            config_dict={},
            container_manager=self.mock_containers,
            storage_adapter=self.mock_storage,
            metrics_collector=self.mock_metrics,
            uploader=self.mock_uploader
        )
        
        # Set return values for stats to avoid TypeError in math ops
        self.mock_containers.get_io_bytes.return_value = 0
        self.mock_containers.get_cgroup_memory_peak.return_value = 1024 * 1024 * 50 # 50MB
        
        # Set return value for metrics analysis
        self.mock_metrics.analyze_execution.return_value = (None, None)
        
        # Init temp dir
        self.test_dir = tempfile.TemporaryDirectory()
        
        # Patch config.DOCKER_WORK_DIR_ROOT
        self.config_patcher = patch('config.DOCKER_WORK_DIR_ROOT', self.test_dir.name)
        self.config_patcher.start()

    def tearDown(self):
        self.config_patcher.stop()
        try:
             self.test_dir.cleanup()
        except: pass

    def test_run_success_cold_start(self):
        # Setup Task
        task = TaskMessage(
            request_id="req-1", function_id="func-1", runtime="python", s3_key="key"
        )
        
        # Setup Container Mock
        mock_container = MagicMock()
        mock_container.id = "container-1"
        mock_container.is_warm = False
        self.mock_containers.acquire_container.return_value = mock_container
        
        # Setup Storage Mock to return a real temporary path (subdir of test_dir)
        # We need a NEW unique dir for cold start return
        cold_start_dir = Path(self.test_dir.name) / "cold_req"
        cold_start_dir.mkdir()
        self.mock_storage.prepare_workspace.return_value = cold_start_dir
        
        # Setup Execution Mock
        # _execute_in_container is internal, but we can verify dependencies
        # Since we refactored, TaskExecutor orchestrates.
        
        # We need to patch the internal methods if we want to test run logic specifically
        # Or blindly trust mocks. 
        # But wait, TaskExecutor calls self.containers.copy_from_container, etc.
        
        # Let's mock the internal helper _execute_in_container to avoid threading issues in unit test
        # effectively just testing the orchestration flow
        with patch.object(self.executor, '_execute_in_container', return_value=(0, b"Success")):
            with patch.object(self.executor, '_read_llm_usage', return_value=10):
                result = self.executor.run(task)
        
        # Assertions
        if not result.success:
            print(f"LOGGER ERROR CALLS: {logger.error.call_args_list}")
        self.assertTrue(result.success, msg=f"Execution failed: {result.stderr} | {result.stdout}")
        self.assertEqual(result.stdout, "Success")
        
        # Verify Flow
        self.mock_metrics.global_limit.acquire.assert_called()
        self.mock_containers.acquire_container.assert_called_with("python", "func-1")
        self.mock_storage.prepare_workspace.assert_called() # Cold start
        self.mock_containers.copy_to_container.assert_called() # Inject code
        
        # Cleanup
        self.mock_containers.release_container.assert_called_with(mock_container, "func-1")

    def test_run_success_warm_start(self):
        # Setup Task
        task = TaskMessage(
            request_id="req-2", function_id="func-1", runtime="python", s3_key="key"
        )
        
        # Setup Container Mock (Warm)
        mock_container = MagicMock()
        mock_container.id = "container-1"
        mock_container.is_warm = True
        self.mock_containers.acquire_container.return_value = mock_container
        
        with patch.object(self.executor, '_execute_in_container', return_value=(0, b"Warm Success")):
            with patch.object(self.executor, '_read_llm_usage', return_value=0):
                 result = self.executor.run(task)

        # Assertions
        if not result.success:
            print(f"LOGGER ERROR CALLS: {logger.error.call_args_list}")
        self.assertTrue(result.success, msg=f"Execution failed: {result.stderr} | {result.stdout}")
        
        # Verify Flow
        self.mock_storage.prepare_workspace.assert_not_called() # Warm start skips download
        # copy_to_container might be called if output dir setup fails? 
        # In current logic: if not is_warm or use_payload_file: copy_to_container
        # Here payload is small/empty, so no copy_to_container
        self.mock_containers.copy_to_container.assert_not_called()

if __name__ == '__main__':
    unittest.main()
