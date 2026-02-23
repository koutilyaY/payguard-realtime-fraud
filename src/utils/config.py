import os
import yaml
from typing import Any, Dict

DEFAULT_CONFIG_PATH = "config.yaml"

def load_config(path: str = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    cfg_path = os.getenv("PAYGUARD_CONFIG", path)
    with open(cfg_path, "r") as f:
        return yaml.safe_load(f)

