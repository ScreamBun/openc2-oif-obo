import os

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, NoReturn, Optional, Union
from serde import serde
from serde.toml import from_toml, to_toml


@serde
@dataclass
class MQTTConfig:
    host: str = "localhost"
    port: int = 1883
    username: Optional[str] = None
    password: Optional[str] = None
    key: Optional[str] = None
    cert: Optional[str] = None


@serde
@dataclass
class HTTPSConfig:
    port: int = 8080
    paths: Optional[List[str]] = field(default_factory=list)
    key: Optional[str] = None
    cert: Optional[str] = None


@serde
@dataclass
class KevinConfig:
    name: str
    var: str


@serde
@dataclass
class Config:
    https: Optional[HTTPSConfig] = HTTPSConfig()
    mqtt: Optional[MQTTConfig] = MQTTConfig()
    # Dev options
    kevin: Optional[KevinConfig] = None

    @classmethod
    def load(cls, path: Union[bytes, str, os.PathLike]) -> "Config":
        with open(path, "r") as f:
            return from_toml(cls, f.read())

    def dump(self, path: Union[bytes, str, os.PathLike]) -> NoReturn:
        with open(path, "w") as f:
            f.write(to_toml(self))


def get_config_data() -> Config:
    root_path = Path(__file__).parent.parent.resolve()
    config_file_path = os.path.join(root_path, 'config.toml')
    return Config.load(config_file_path)
