import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sawtooth import Sawtooth, SawtoothBackpressure, SawtoothConfig


def test_init_invalid_starting_concurrency():
    expected_msg = "Starting concurrency needs to be between minimum and maximum concurrency"
    with pytest.raises(ValueError) as e:
        config = SawtoothConfig(
            starting_concurrency=-1,
        )
        Sawtooth({}, config=config)

    assert str(e.value) == expected_msg


def test_init_invalid_backoff():
    expected_msg = "Backoff must have a value between 0 and 1 (exclusive)"
    with pytest.raises(ValueError) as e:
        config = SawtoothConfig(backoff_factor=1)
        Sawtooth({}, config=config)

    assert str(e.value) == expected_msg

    with pytest.raises(ValueError) as e:
        config = SawtoothConfig(backoff_factor=0)
        Sawtooth({}, config=config)

    assert str(e.value) == expected_msg


def test_init_invalid_min_concurrency():
    expected_msg = "Minimum concurrency needs to be greater than 0"
    with pytest.raises(ValueError) as e:
        config = SawtoothConfig(min_concurrency=0)
        Sawtooth({}, config=config)

    assert str(e.value) == expected_msg


def test_init_invalid_min_max_concurrency():
    expected_msg = "Minimum concurrency needs greater than maximum concurrency"
    with pytest.raises(ValueError) as e:
        config = SawtoothConfig(
            starting_concurrency=1, min_concurrency=1, max_concurrency=1
        )
        Sawtooth({}, config=config)

    assert str(e.value) == expected_msg


async def test_concurrency_limit():
    config = SawtoothConfig(
        starting_concurrency=1, max_concurrency=100, backoff_factor=0.9
    )
    sawtooth = Sawtooth({}, config=config)
    await sawtooth.acquire("1")
    second_acquire = asyncio.create_task(sawtooth.acquire("2"))
    await asyncio.wait(
        [second_acquire, asyncio.create_task(asyncio.sleep(0))],
        return_when=asyncio.FIRST_COMPLETED,
    )
    assert not second_acquire.done()
    sawtooth.release("1")
    await asyncio.wait(
        [second_acquire, asyncio.create_task(asyncio.sleep(0))],
        return_when=asyncio.FIRST_COMPLETED,
    )
    assert second_acquire.done()


async def test_sawtooth():
    config = SawtoothConfig(
        starting_concurrency=1, max_concurrency=100, backoff_factor=0.9
    )

    sawtooth = Sawtooth("Hello", config=config)
    concurrency = 0
    peak_concurrency = 0

    running = True
    supported_concurrency = 25
    request_total = 30000
    count = 0

    from random import randint

    async def do_nothing_with_resource():
        nonlocal count
        if count % 100 == 0:
            print(f"Processing {count}")

        async with sawtooth.resource() as resource:
            nonlocal concurrency
            nonlocal peak_concurrency
            nonlocal supported_concurrency
            count += 1
            if count == request_total // 3:
                supported_concurrency *= 2
            elif count == (request_total / 3) * 2:
                supported_concurrency /= 3
            concurrency += 1
            peak_concurrency = max(concurrency, peak_concurrency)
            await asyncio.sleep(0.01 * randint(1, 10))
            concurrency -= 1
            if concurrency + 1 >= supported_concurrency:
                print("Backpressure")
                raise SawtoothBackpressure()

    samples = []

    async def capture_concurrency():
        from datetime import datetime

        sample = {
            "ts": str(datetime.now()),
            "Concurrency": sawtooth._concurrency,
            "Peak concurrency": peak_concurrency,
            "Max concurrency": config.max_concurrency,
            "Min concurrency": config.min_concurrency,
            "Supported concurrency": supported_concurrency,
            "sshthresh": sawtooth._sshthresh,
        }
        samples.append(sample)
        if running:
            await asyncio.sleep(0.1)
            asyncio.create_task(capture_concurrency())

    await capture_concurrency()
    await asyncio.gather(
        *[do_nothing_with_resource() for _ in range(request_total)]
    )
    running = False
    print(f"Peak concurrency {peak_concurrency}")

    with open("samples.csv", "w") as f:
        import csv

        writer = csv.DictWriter(f, fieldnames=samples[0].keys())
        writer.writeheader()
        writer.writerows(samples)
