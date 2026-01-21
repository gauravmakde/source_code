import yaml
from typing import Dict


def load_config(config_path: str) -> Dict:
    """
    Load a configuration file in YAML format.

    This function reads a YAML file from the provided path and returns its content as a Python dictionary.

    Args:
        config_path (str): The path to the YAML configuration file.

    Returns:
        Dict: The content of the YAML file as a Python dictionary.
    """

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config
