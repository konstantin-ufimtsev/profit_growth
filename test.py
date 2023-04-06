import asyncpg
import asyncio

QUERY = """INSERT INTO companies_2021 VALUES (%1, %2, %3, %4, %5)"""


async def make_request(db_pool):
    await db_pool.fetch(QUERY, ,,,)


async def main():
    chunk = 200
    tasks = []
    pended = 0

    db_pool = await asyncpg.create_pool("companies://::1:5432/companies")

    for x in range(10_000):
        tasks.append(asyncio.create_task(make_request(db_pool)))
        pended += 1
        if len(tasks) == chunk or pended == 10_000:
            await asyncio.gather(*tasks)
            tasks = []
            print(pended)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
