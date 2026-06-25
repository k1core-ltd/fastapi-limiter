from collections.abc import Callable
from math import ceil
from typing import TYPE_CHECKING

from fastapi import HTTPException
from starlette.requests import Request
from starlette.responses import Response
from starlette.status import HTTP_429_TOO_MANY_REQUESTS
from starlette.websockets import WebSocket

if TYPE_CHECKING:
    from redis import Redis
    from redis.asyncio import Redis as AsyncRedis

DEFAULT_LUA_SCRIPT = """
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local expire_time = ARGV[2]
local current = tonumber(redis.call('get', key) or "0")
if current > 0 then
 if current + 1 > limit then
 return redis.call("PTTL",key)
 else
        redis.call("INCR", key)
 return 0
 end
else
    redis.call("SET", key, 1,"px",expire_time)
 return 0
end
"""


async def default_identifier(request: Request | WebSocket) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    ip = forwarded.split(",")[0] if forwarded else request.client.host if request.client else "unknown"
    return ip + ":" + request.scope["path"]


async def http_default_callback(
    request: Request, response: Response, pexpire: int
) -> None:  # noqa: ARG001
    """
    default callback when too many requests
    :param request:
    :param pexpire: The remaining milliseconds
    :param response:
    :return:
    """
    expire = ceil(pexpire / 1000)
    raise HTTPException(
        HTTP_429_TOO_MANY_REQUESTS,
        "Too Many Requests",
        headers={"Retry-After": str(expire)},
    )


async def ws_default_callback(ws: WebSocket, pexpire: int) -> None:  # noqa: ARG001
    """
    default callback when too many requests
    :param ws:
    :param pexpire: The remaining milliseconds
    :return:
    """
    expire = ceil(pexpire / 1000)
    raise HTTPException(
        HTTP_429_TOO_MANY_REQUESTS,
        "Too Many Requests",
        headers={"Retry-After": str(expire)},
    )


class FastAPILimiter:
    redis: "AsyncRedis | Redis"
    prefix: str | None = None
    lua_sha: str
    identifier: Callable | None = None
    http_callback: Callable | None = None
    ws_callback: Callable | None = None
    lua_script: str

    @classmethod
    async def init(  # noqa: PLR0913
        cls,
        redis,  # noqa: ANN001
        prefix: str = "fastapi-limiter",
        identifier: Callable = default_identifier,
        http_callback: Callable = http_default_callback,
        ws_callback: Callable = ws_default_callback,
        lua_script: str = DEFAULT_LUA_SCRIPT,
    ) -> None:
        cls.redis = redis

        assert hasattr(cls.redis, "evalsha"), (
            "Redis client must support evalsha command"
        )
        assert hasattr(cls.redis, "script_load"), (
            "Redis client must support script_load command"
        )

        cls.prefix = prefix
        cls.identifier = identifier
        cls.http_callback = http_callback
        cls.ws_callback = ws_callback
        cls.lua_sha = await redis.script_load(lua_script)

    @classmethod
    async def close(cls) -> None:
        if hasattr(cls.redis, "aclose"):
            return await cls.redis.aclose()  # ty: ignore
        return cls.redis.close()
