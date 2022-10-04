import json
import os
import sys
import yaml

from dataclasses import asdict, dataclass
from functools import partial
from typing import NoReturn, Optional, Union
from dacite import from_dict

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

if sys.version_info < (3, 11):
    import toml
else:
    import tomllib as toml


@dataclass
class FileDataclass:
    # File Config
    _access = json.load, partial(json.dump, indent=2)
    _path = "config.json"

    @classmethod
    def load(cls, path: Union[bytes, str, os.PathLike]) -> "FileDataclass":
        load = cls._access[0]
        with open(path, "r", encoding="utf-8") as f:
            cfg = from_dict(data_class=cls, data=load(f))
        cfg.__path__ = path
        return cfg

    def dump(self, path: Optional[Union[bytes, str, os.PathLike]] = None) -> NoReturn:
        path = path or self._path
        if path is None:
            raise IOError("Config path is not specified")
        dump = self._access[1]
        with open(path, "w", encoding="utf-8") as f:
            dump(asdict(self), f)


@dataclass
class TomlDataFile(FileDataclass):
    # Config Path
    _access = toml.load, toml.dump
    _path = "config.toml"


@dataclass
class YamlDataFile(FileDataclass):
    # Config Path
    _access = partial(yaml.load, Loader=Loader), partial(yaml.dump, Dumper=Dumper)
    _path = "config.yaml"
