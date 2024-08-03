import asyncio
import os
import time
import uuid
from typing import Union

import asyncpg
from dotenv import load_dotenv
from fastapi import FastAPI

# Load environment variables
load_dotenv()

app = FastAPI()


@app.get("/hello")
def read_root():
    return {"Hello": "World"}


async def get_user_count(pool):
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM users")


@app.post("/insert-million")
async def insert_million_records():
    start_time = time.time()

    # Get database URL from environment variable
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        return {"error": "DATABASE_URL not set in environment"}

    # Create a connection pool
    pool = await asyncpg.create_pool(db_url)

    # Get initial count
    initial_count = await get_user_count(pool)

    async def insert_batch(start, end):
        async with pool.acquire() as conn:
            await conn.executemany(
                "INSERT INTO users(id, email) VALUES($1, $2)",
                [(str(uuid.uuid4()), f"user{i}@test.com") for i in range(start, end)],
            )

    # Number of records to insert in each batch
    batch_size = 10000
    tasks = []

    for i in range(0, 1000000, batch_size):
        task = asyncio.create_task(insert_batch(i, min(i + batch_size, 1000000)))
        tasks.append(task)

    # Wait for all tasks to complete
    await asyncio.gather(*tasks)

    # Get final count
    final_count = await get_user_count(pool)

    # Close the connection pool
    await pool.close()

    duration = time.time() - start_time
    records_inserted = final_count - initial_count

    return {
        "message": "Insertion complete",
        "initial_count": initial_count,
        "final_count": final_count,
        "records_inserted": records_inserted,
        "time_taken": f"{duration:.2f} seconds",
    }
