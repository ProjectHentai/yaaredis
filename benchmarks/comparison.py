#!/usr/bin/python
# -*- coding: utf-8 -*-
import time
import asyncio
import asyncio_redis
import aioredis
import redis
import yaaredis


HOST = '127.0.0.1'
NUM = 10000


async def test_yaaredis(i):
    start = time.time()
    client = yaaredis.StrictRedis(host=HOST)
    res = None
    for i in range(i):
        res = await client.keys('*')
    print(time.time() - start)
    return res


async def test_asyncio_redis(i):
    connection = await asyncio_redis.Connection.create(host=HOST, port=6379)
    start = time.time()
    res = None
    for i in range(i):
        res = await connection.keys('*')
    print(time.time() - start)
    connection.close()
    return res


def test_conn(i):
    start = time.time()
    client = redis.StrictRedis(host=HOST)
    res = None
    for i in range(i):
        res = client.keys('*')
    print(time.time() - start)
    return res


async def test_aioredis(i, loop):
    start = time.time()
    redis = await aioredis.create_redis((HOST, 6379), loop=loop)
    val = None
    for i in range(i):
        val = await redis.keys('*')
    print(time.time() - start)
    redis.close()
    await redis.wait_closed()
    return val


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    print('yaaredis')
    print(loop.run_until_complete(test_yaaredis(NUM)))
    print('asyncio_redis')
    print(loop.run_until_complete(test_asyncio_redis(NUM)))
    print('redis-py')
    print(test_conn(NUM))
    print('aioredis')
    print(loop.run_until_complete(test_aioredis(NUM, loop)))

