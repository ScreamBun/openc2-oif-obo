import os

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional
from .datafiles import TomlDataFile


@dataclass
class MQTTConfig:
    client_id: str
    host: str
    port: int
    log_level: str
    username: Optional[str] = None
    password: Optional[str] = None
    key: Optional[str] = None
    cert: Optional[str] = None


@dataclass
class HTTPSConfig:
    host: str
    port: int
    log_level: str
    paths: Optional[List[str]] = field(default_factory=list)
    key: Optional[str] = None
    cert: Optional[str] = None


@dataclass
class WebSocketsConfig:
    host: str
    port: int
    log_level: str
    key: Optional[str] = None
    cert: Optional[str] = None


@dataclass
class Device:
    profiles: List[str]
    platform: str
    connection: str
    host: str
    port: int
    client_id: Optional[str]
    username: Optional[str]
    password: Optional[str]


@dataclass
class Config(TomlDataFile):
    # Device Config
    devices: Dict[str, Device]
    # Transport Config
    https: Optional[HTTPSConfig] = None
    mqtt: Optional[MQTTConfig] = None
    websockets: Optional[WebSocketsConfig] = None


def get_config_data() -> Config:
    root_path = Path(__file__).parent.parent.resolve()
    config_file_path = os.path.join(root_path, 'config.toml')
    return Config.load(config_file_path)
