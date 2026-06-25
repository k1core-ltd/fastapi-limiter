from collections.abc import Callable
from inspect import isawaitable
from typing import Annotated

import redis as pyredis
from fastapi import FastAPI
from fastapi.requests import Request
from fastapi.responses import Response
from fastapi.routing import APIRouter, APIRoute, _IncludedRouter, APIWebSocketRoute
from pydantic import Field
from starlette.routing import Match, Route
from starlette.websockets import WebSocket

from fastapi_limiter import FastAPILimiter


def _flatten_routes(app: "FastAPI | APIRouter") -> list[APIRoute | Route | APIWebSocketRoute]:
    routes: list[APIRoute | Route | APIWebSocketRoute] = []
    for route in app.routes:
        if isinstance(route, (APIRoute, Route, APIWebSocketRoute)):
            routes.append(route)
        elif hasattr(route, "original_router"):
            assert isinstance(route, _IncludedRouter)
            routes.extend(_flatten_routes(route.original_router))
        else:
            raise Exception(f"Unknown route type: {type(route)}")
    return routes


class RateLimiterBase:
    def __init__(  # noqa: PLR0913
        self,
        times: Annotated[int, Field(ge=0)] = 1,
        milliseconds: Annotated[int, Field(ge=-1)] = 0,
        seconds: Annotated[int, Field(ge=-1)] = 0,
        minutes: Annotated[int, Field(ge=-1)] = 0,
        hours: Annotated[int, Field(ge=-1)] = 0,
        identifier: Callable | None = None,
        callback: Callable | None = None,
    ) -> None:
        self.times = times
        self.milliseconds = (
            milliseconds + 1000 * seconds + 60000 * minutes + 3600000 * hours
        )
        self.identifier = identifier
        self.callback = callback

    async def _check(self, key: bytes | str | memoryview | float) -> str:
        redis = FastAPILimiter.redis
        return await redis.evalsha(
            FastAPILimiter.lua_sha,
            1,
            key,
            str(self.times),
            str(self.milliseconds),
        )


class RateLimiter(RateLimiterBase):
    async def __call__(
        self,
        request: Request,
        response: Response,
    ) -> None:
        if not FastAPILimiter.redis:
            raise Exception(
                "You must call FastAPILimiter.init in startup event of fastapi!"
            )

        route_index = 0
        dep_index = 0

        assert isinstance(request.app, FastAPI)

        for i, route in enumerate(_flatten_routes(request.app)):
            match, _ = route.matches(scope=request.scope)
            if match == Match.FULL:
                route_index = i

                if not hasattr(route, "dependencies"):
                    continue

                assert isinstance(route, (APIRoute, APIWebSocketRoute))

                for j, dependency in enumerate(route.dependencies):
                    if self is dependency.dependency:
                        dep_index = j
                        break

        # moved here because constructor run before app startup
        identifier = self.identifier or FastAPILimiter.identifier

        if not identifier:
            raise Exception(
                "You must provide an identifier function for RateLimiter (either in the constructor or in FastAPILimiter.init)"
            )

        callback = self.callback or FastAPILimiter.http_callback

        if not callback:
            raise Exception(
                "You must provide a callback function for RateLimiter (either in the constructor or in FastAPILimiter.init)"
            )

        rate_key = await identifier(request)
        key = f"{FastAPILimiter.prefix}:{rate_key}:{route_index}:{dep_index}"
        try:
            pexpire = await self._check(key)
        except pyredis.exceptions.NoScriptError:
            result = FastAPILimiter.redis.script_load(
                FastAPILimiter.lua_script
            )

            if isawaitable(result):
                result = await result
                assert isinstance(result, str), "Lua script SHA must be a string"

            FastAPILimiter.lua_sha = result

            pexpire = await self._check(key)

        if pexpire != 0:
            return await callback(request, response, pexpire)

        return None


class WebSocketRateLimiter(RateLimiterBase):
    async def __call__(
        self,
        ws: WebSocket,
        context_key: str = "",
    ) -> None:
        if not FastAPILimiter.redis:
            raise Exception(
                "You must call FastAPILimiter.init in startup event of fastapi!"
            )

        identifier = self.identifier or FastAPILimiter.identifier

        if not identifier:
            raise Exception(
                "You must provide an identifier function for WebSocketRateLimiter (either in the constructor or in FastAPILimiter.init)"
            )

        rate_key = await identifier(ws)
        key = f"{FastAPILimiter.prefix}:ws:{rate_key}:{context_key}"
        pexpire = await self._check(key)
        callback = self.callback or FastAPILimiter.ws_callback

        if not callback:
            raise Exception(
                "You must provide a callback function for WebSocketRateLimiter (either in the constructor or in FastAPILimiter.init)"
            )

        if pexpire != 0:
            return await callback(ws, pexpire)

        return None
