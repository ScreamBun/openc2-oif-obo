import os
from pathlib import Path

import toml


def get_config_data():
    path = os.path.dirname(__file__)
    root_path = Path(path).parents[0]
    config_file_path = os.path.join(root_path, 'config.toml')
    config_data = toml.load(config_file_path)
    # print(toml.dumps(config_data))
    return config_data
