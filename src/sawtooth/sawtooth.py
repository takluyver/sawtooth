from typing import Any, Generic, TypeVar, Optional, Set
from contextlib import asynccontextmanager
from uuid import uuid4
import asyncio
from collections import deque
from dataclasses import dataclass
import math

ResourceType = TypeVar("ResourceType")


class SawtoothBackpressure(Exception):
    pass


@dataclass
class SawtoothConfig:
    # The maximum value we can increase concurrency to
    max_concurrency: int = 1000
    # The minimum value we can reduce concurrency to
    min_concurrency: int = 1
    # The amount to increase concurrency by on a successful response
    step_size: int = 1
    # Reduce concurrency to concurrency * backoff_factor upon receiving
    # backpressure
    backoff_factor: float = 0.95
    # Starting concurrency. Defaults to (max_concurrency - min_concurrency) / 2
    starting_concurrency: Optional[int] = None


class Sawtooth(Generic[ResourceType]):
    config: SawtoothConfig
    _resource: ResourceType
    _flying: Set[str]
    _ignored: Set[str]
    _sshthresh: int
    _concurrency: int
    _waiters: deque

    def __init__(
        self, resource: Any, config: SawtoothConfig = SawtoothConfig()
    ):
        self._resource = resource
        self.config = config
        starting_concurrency = (
            config.starting_concurrency
            if config.starting_concurrency is not None
            else math.floor(
                (config.max_concurrency - config.min_concurrency) / 2
            )
        )
        if (
            starting_concurrency > config.max_concurrency
            or starting_concurrency < config.min_concurrency
        ):
            raise ValueError(
                "Starting concurrency needs to be between minimum and maximum concurrency"
            )

        if config.min_concurrency <= 0:
            raise ValueError("Minimum concurrency needs to be greater than 0")

        if config.min_concurrency >= config.max_concurrency:
            raise ValueError(
                "Minimum concurrency needs greater than maximum concurrency"
            )

        if (
            self.config.backoff_factor >= 1.0
            or self.config.backoff_factor <= 0.0
        ):
            raise ValueError(
                "Backoff must have a value between 0 and 1 (exclusive)"
            )

        self._flying = set()
        self._ignored = set()
        self._waiters = deque()

        self._concurrency = starting_concurrency
        self._sshthresh = config.max_concurrency

    def _release_waiters(self):
        """Release as many waiting requests as we can"""
        available_concurrency = max(0, self._concurrency - len(self._flying))
        num_to_release = min(len(self._waiters), available_concurrency)
        for _ in range(num_to_release):
            # First in, first out
            request_id, fut = self._waiters.popleft()
            self._flying.add(request_id)
            fut.set_result(True)

    def _maybe_increase_concurrency(self):
        """Increase our concurrency if we haven't hit the max"""
        step_size = self.config.step_size
        num_flying = len(self._flying)
        new_concurrency = math.ceil(
            max(
                self._concurrency,
                min(
                    num_flying + step_size + 1,
                    self._concurrency + step_size,
                ),
            )
            if num_flying < self._sshthresh
            else max(
                self._concurrency,
                min(
                    num_flying + step_size + 1,
                    self._concurrency + step_size / self._concurrency,
                ),
            )
        )
        self._concurrency = math.ceil(
            min(new_concurrency, self.config.max_concurrency)
        )

    def _maybe_reduce_concurrency(self):
        """Reduce our concurrency if we haven't hit the min"""
        self._sshthresh = math.floor(
            max(
                self.config.min_concurrency,
                self._concurrency * self.config.backoff_factor,
            )
        )
        self._concurrency = self._sshthresh
        self._ignored = set([*self._flying])

    async def acquire(self, request_id: str):
        """
        Acquire the resource. We will be added to waiters if we are currently at
        max concurrency. The request ID should be unique across calls to acquire.
        The same request_id must be passed to *release* when done with the resource.

        :param request_id: request id
        """
        if len(self._flying) >= self._concurrency:
            fut = asyncio.get_running_loop().create_future()
            self._waiters.append((request_id, fut))
            await fut
        else:
            self._flying.add(request_id)
        return self._resource

    def release(self, request_id: str, backpressure: bool = False):
        """
        Release the resource previously acquired with request_id. If `backpressure`
        is `False` we will attempt to increase the concurrency, otherwise we will
        attempt to reduce it.

        :param request_id: request id
        :param backpressure: backpressure
        """
        if request_id in self._flying:
            self._flying.remove(request_id)
            if backpressure:
                if request_id not in self._ignored:
                    self._maybe_reduce_concurrency()
            else:
                self._maybe_increase_concurrency()
        if request_id in self._ignored:
            self._ignored.remove(request_id)
        self._release_waiters()

    @asynccontextmanager
    async def resource(self):
        """
        Acquire the resource in a new context.

        >>> async with sawtooth.resource() as resource:
        >>>   #TODO: do something with resource
        >>>   if received_backpressure:
        >>>     raise SawtoothBackpressure()
        >>>

        The resource will be automatically released when exiting the context. A
        `SawtoothBackpressure` error should be raised to indicate backpressure.
        """

        request_id = str(uuid4())
        backpressure = False
        try:
            yield await self.acquire(request_id)
        except SawtoothBackpressure:
            backpressure = True
        finally:
            self.release(request_id, backpressure=backpressure)
