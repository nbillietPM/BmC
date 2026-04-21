import yaml
import pandas as pd
import logging
from typing import Dict, Any, Optional, List

def load_yaml_config(yaml_path: str) -> Dict[str, Any]:
    """
    Reads a YAML configuration file and parses it into a Python dictionary.
    """
    try:
        with open(yaml_path, 'r') as file:
            config = yaml.safe_load(file)
            return config
    except FileNotFoundError:
        raise FileNotFoundError(f"Could not find the configuration file at: {yaml_path}")
    except yaml.YAMLError as exc:
        raise ValueError(f"Failed to parse YAML file. Check your formatting: {exc}")