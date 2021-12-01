# -*- coding: utf-8 -*-
import typing
from typing import Union, TypeVar

Redis = TypeVar("Redis")
RedisCluster = TypeVar("RedisCluster")
BasePipeline = TypeVar("BasePipeline")
if typing.TYPE_CHECKING:
    from .client import Redis, RedisCluster

Number = Union[int, float]
ByteOrStr = Union[str, bytes]