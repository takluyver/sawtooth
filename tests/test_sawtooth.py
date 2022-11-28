import asyncio
from os import environ

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
