from dataclasses import dataclass, field

@dataclass
class Config:
    warnings: bool = True
    connections: dict = field(default_factory=dict)
    default_client: str = 'websocket'

def get_config_from_file() -> dict:
    # check config file for connection details. It should be called 'pysurrealdb.json'
    import json
    import os
    from pathlib import Path
    # first check if an env variable is set for the config file
    config_file = os.getenv('PYSURREALDB_CONFIG')
    # otherwise use the working directory to find the config file
    if not config_file:
        config_file = os.getcwd() + '/pysurrealdb.json'
    config_file = Path(config_file)
    
    if not config_file.exists():
        return {}

    with open(config_file, 'r') as f:
        config = json.load(f)

    # Ensure we only have supported keys. Check Config class for supported keys.
    supported_keys = [k for k in Config.__dataclass_fields__.keys()]
    unsupported_keys = [k for k in config.keys() if k not in supported_keys]
    if unsupported_keys:
        print('SurrealDB: Invalid keys in config file.', unsupported_keys)
        # remove the unsupported keys
        for k in unsupported_keys:
            config.pop(k)

    return config

config = Config(**get_config_from_file())