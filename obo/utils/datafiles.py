import json
import os
import sys
import yaml

from dataclasses import asdict, dataclass, field
from functools import partial
from typing import Callable, NoReturn, Optional, Tuple, Union
from dacite import from_dict

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

if sys.version_info < (3, 11):
    import toml
else:
    import tomllib as toml


class FileDataclassMeta(type):
    def __new__(cls, name, bases, attrs):
        newattrs = {**attrs}
        if "__access__" in newattrs:
            newattrs.setdefault("__annotations__", {}).update(__access__=Tuple[Callable, Callable])
            newattrs["__access__"] = field(init=False, repr=False, default=newattrs["__access__"])
        return type.__new__(cls, name, bases, newattrs)


@dataclass
class FileDataclass(metaclass=FileDataclassMeta):
    # File Config
    __path__: Union[bytes, str, os.PathLike] = field(init=False, repr=False, default=None)
    __access__ = json.load, partial(json.dump, indent=2)

    @classmethod
    def load(cls, path: Union[bytes, str, os.PathLike]) -> "FileDataclass":
        load = cls.__access__[0]
        with open(path, "r", encoding="utf-8") as f:
            cfg = from_dict(data_class=cls, data=load(f))
        cfg.__path__ = path
        return cfg

    def dump(self, path: Optional[Union[bytes, str, os.PathLike]] = None) -> NoReturn:
        path = path or self.__path__
        if path is None:
            raise IOError("Config path is not specified")
        dump = self.__access__[1]
        with open(path, "w", encoding="utf-8") as f:
            dump(asdict(self), f, indent=2)


@dataclass
class TomlDataFile(FileDataclass):
    # Config Path
    __access__ = toml.load, toml.dump


@dataclass
class YamlDataFile(FileDataclass):
    # Config Path
    __access__ = partial(yaml.load, Loader=Loader), partial(yaml.dump, Dumper=Dumper)
