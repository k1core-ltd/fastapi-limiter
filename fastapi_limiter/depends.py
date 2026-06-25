from collections.abc import Callable
from typing import Annotated

import redis as pyredis
from fastapi import FastAPI
from fastapi.requests import Request
from fastapi.responses import Response
from fastapi.routing import APIRoute, _IncludedRouter
from pydantic import Field
from starlette.routing import Match, Route
from starlette.websockets import WebSocket

from fastapi_limiter import FastAPILimiter


def _flatten_routes(app: "FastAPI") -> list[APIRoute | Route]:
    routes: list[APIRoute | Route] = []
    for route in app.routes:
        if isinstance(route, (APIRoute, Route)):
            routes.append(route)
        elif hasattr(route, "original_router"):
            route: _IncludedRouter
            routes.extend(_flatten_routes(route.original_router))
        else:
            raise Exception(f"Unknown route type: {type(route)}")
    return routes


class RateLimiter:
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

                assert isinstance(route, APIRoute)

                for j, dependency in enumerate(route.dependencies):
                    if self is dependency.dependency:
                        dep_index = j
                        break

        # moved here because constructor run before app startup
        identifier = self.identifier or FastAPILimiter.identifier
        callback = self.callback or FastAPILimiter.http_callback
        rate_key = await identifier(request)
        key = f"{FastAPILimiter.prefix}:{rate_key}:{route_index}:{dep_index}"
        try:
            pexpire = await self._check(key)
        except pyredis.exceptions.NoScriptError:
            FastAPILimiter.lua_sha = await FastAPILimiter.redis.script_load(
                FastAPILimiter.lua_script
            )
            pexpire = await self._check(key)

        if pexpire != 0:
            return await callback(request, response, pexpire)

        return None


class WebSocketRateLimiter(RateLimiter):
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
        rate_key = await identifier(ws)
        key = f"{FastAPILimiter.prefix}:ws:{rate_key}:{context_key}"
        pexpire = await self._check(key)
        callback = self.callback or FastAPILimiter.ws_callback

        if pexpire != 0:
            return await callback(ws, pexpire)

        return None
