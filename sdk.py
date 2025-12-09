import os
import json
import sys

class FaaSSDK:
    def __init__(self):
        self._input_data = None

    def get_context(self):
        """
        Returns execution context including Request ID and Model ID.
        """
        return {
            "request_id": os.environ.get("JOB_ID"),
            "function_id": os.environ.get("FUNCTION_ID"),
            "model_id": os.environ.get("LLM_MODEL"),
            "memory_mb": os.environ.get("MEMORY_MB")
        }

    def get_input(self):
        """
        Parses input payload from environment variable or file.
        Automatically handles large payloads passed via file.
        """
        if self._input_data is not None:
            return self._input_data

        try:
            if "PAYLOAD_FILE" in os.environ:
                with open(os.environ["PAYLOAD_FILE"], "r") as f:
                    self._input_data = json.load(f)
            elif "PAYLOAD" in os.environ:
                self._input_data = json.loads(os.environ["PAYLOAD"])
            else:
                self._input_data = {}
        except Exception as e:
            print(f"Error parsing input: {e}", file=sys.stderr)
            self._input_data = {}
        
        return self._input_data

    def return_output(self, data):
        """
        Standardizes output return (JSON over stdout).
        """
        if isinstance(data, dict) or isinstance(data, list):
            print(json.dumps(data))
        else:
            print(str(data))

# Singleton instance
_sdk = FaaSSDK()

# Exposed functions
def get_context():
    return _sdk.get_context()

def get_input():
    return _sdk.get_input()

def return_output(data):
    return _sdk.return_output(data)
