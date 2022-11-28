import asyncio
from sawtooth import Sawtooth, SawtoothBackpressure, SawtoothConfig

async def main():
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
        async with sawtooth.resource():
            nonlocal count
            if count % 100 == 0:
                print(f"Processing {count}")
            count += 1
            nonlocal concurrency
            nonlocal peak_concurrency
            nonlocal supported_concurrency
            if count == request_total // 3:
                supported_concurrency *= 2
            elif count == (request_total / 3) * 2:
                supported_concurrency /= 3
            concurrency += 1
            peak_concurrency = max(concurrency, peak_concurrency)
            await asyncio.sleep(0.01 * randint(1, 10))
            concurrency -= 1
            if concurrency + 1 >= supported_concurrency:
                raise SawtoothBackpressure()

    samples = []
    concurrency_sum = 0

    async def capture_concurrency():
        from datetime import datetime
        nonlocal concurrency_sum

        concurrency_sum += sawtooth._concurrency

        sample = {
            "ts": str(datetime.now()),
            "Concurrency": sawtooth._concurrency,
            "Peak concurrency": peak_concurrency,
            "Max concurrency": config.max_concurrency,
            "Avg concurrency": (concurrency_sum) / (len(samples) + 1),
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

if __name__ == '__main__':
    asyncio.run(main())
