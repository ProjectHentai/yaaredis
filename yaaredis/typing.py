# -*- coding: utf-8 -*-
import typing
from typing import Union

Number = Union[int, float]
ByteOrStr = Union[str, bytes]

if typing.TYPE_CHECKING:
    from .client import Redis, RedisCluster
