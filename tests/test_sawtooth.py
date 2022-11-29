import asyncio
import pytest

from sawtooth import Sawtooth, SawtoothConfig


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


async def test_increase_concurrency():
    config = SawtoothConfig(
        starting_concurrency=1, max_concurrency=100, step_size=1
    )
    sawtooth = Sawtooth({}, config=config)
    # This should increase concurrency
    async with sawtooth.resource():
        pass
    assert sawtooth._concurrency == 2


async def test_decrease_concurrency():
    config = SawtoothConfig(
        starting_concurrency=6, max_concurrency=10, backoff_factor=0.5
    )
    sawtooth = Sawtooth({}, config=config)
    # This should increase concurrency
    await sawtooth.acquire("req1")
    # Should cause us to decrease concurrency
    sawtooth.release("req1", backpressure=True)
    assert sawtooth._concurrency == 3


async def test_backpressure_after_decrease_concurrency():
    config = SawtoothConfig(
        starting_concurrency=6, max_concurrency=10, backoff_factor=0.5
    )
    sawtooth = Sawtooth({}, config=config)
    # This should increase concurrency
    await sawtooth.acquire("req1")
    await sawtooth.acquire("req2")
    # Should cause us to decrease concurrency
    sawtooth.release("req1", backpressure=True)
    # Should not decrease concurrency as we were flying during last decrease
    sawtooth.release("req2", backpressure=True)
    assert sawtooth._concurrency == 3


async def test_max_concurrency():
    config = SawtoothConfig(
        starting_concurrency=1, max_concurrency=10, step_size=1
    )
    sawtooth = Sawtooth({}, config=config)
    # This should increase concurrency
    request_ids = [f"req{i}" for i in range(20)]
    coros = {asyncio.create_task(sawtooth.acquire(r)): r for r in request_ids}
    while len(coros) > 0:
        done, _ = await asyncio.wait(coros, return_when=asyncio.FIRST_COMPLETED)
        for f in done:
            sawtooth.release(coros[f])
            del coros[f]

    assert sawtooth._concurrency == 10


async def test_min_concurrency():
    config = SawtoothConfig(
        starting_concurrency=5, max_concurrency=10, step_size=1
    )
    sawtooth = Sawtooth({}, config=config)
    # This should increase concurrency
    request_ids = [f"req{i}" for i in range(20)]
    coros = {asyncio.create_task(sawtooth.acquire(r)): r for r in request_ids}
    while len(coros) > 0:
        done, _ = await asyncio.wait(coros, return_when=asyncio.FIRST_COMPLETED)
        for f in done:
            # Release with backpressure will decrease concurrency
            sawtooth.release(coros[f], backpressure=True)
            del coros[f]

    assert sawtooth._concurrency == 1
