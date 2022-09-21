import os

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, NoReturn, Optional, Union
from serde import serde
from serde.toml import from_toml, to_toml


@serde
@dataclass
class MQTTConfig:
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    key: Optional[str] = None
    cert: Optional[str] = None


@serde
@dataclass
class HTTPSConfig:
    port: int
    paths: Optional[List[str]] = field(default_factory=list)
    key: Optional[str] = None
    cert: Optional[str] = None


@serde
@dataclass
class WebSocketsConfig:
    port: int
    key: Optional[str] = None
    cert: Optional[str] = None


@serde
@dataclass
class Config:
    # Transport Config
    https: Optional[HTTPSConfig] = None
    mqtt: Optional[MQTTConfig] = None
    websockets: Optional[WebSocketsConfig] = None
    # Config Path
    __path__: Union[bytes, str, os.PathLike] = field(repr=False, default=None, metadata={'serde_skip': True})

    @classmethod
    def load(cls, path: Union[bytes, str, os.PathLike]) -> "Config":
        with open(path, "r") as f:
            cfg = from_toml(cls, f.read())
        cfg.__path__ = path
        return cfg

    def dump(self, path: Optional[Union[bytes, str, os.PathLike]] = None) -> NoReturn:
        path = path or self.__path__
        if path is None:
            raise IOError("Config path is not specified")
        with open(path, "w") as f:
            f.write(to_toml(self))


def get_config_data() -> Config:
    root_path = Path(__file__).parent.parent.resolve()
    config_file_path = os.path.join(root_path, 'config.toml')
    return Config.load(config_file_path)
