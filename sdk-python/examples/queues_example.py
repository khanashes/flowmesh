"""
FlowMesh Queues Example

Demonstrates queue operations: enqueueing jobs, reserving/receiving, and worker patterns.
"""

import asyncio
from datetime import timedelta
from flowmesh import FlowMesh


async def main():
    # Create client
    async with FlowMesh(endpoint="localhost:50051") as client:
        # Example 1: Enqueue jobs
        print("Enqueueing jobs...")
        for i in range(5):
            payload = f'{{"job_id": {i}, "task": "process_order"}}'.encode()
            job_id, seq = await client.queue.enqueue(
                "default",
                "test",
                "orders",
                payload,
                headers={"priority": "high"},
            )
            print(f"Enqueued job: ID={job_id}, Seq={seq}")

        # Example 2: Get queue statistics
        print("\nGetting queue statistics...")
        stats = await client.queue.get_stats("default", "test", "orders")
        print(f"Queue Stats:")
        print(f"  Total Jobs: {stats.total_jobs}")
        print(f"  Pending Jobs: {stats.pending_jobs}")
        print(f"  In-Flight Jobs: {stats.in_flight_jobs}")
        print(f"  Completed Jobs: {stats.completed_jobs}")

        # Example 3: Reserve a single job
        print("\nReserving a job...")
        job = await client.queue.reserve(
            "default",
            "test",
            "orders",
            visibility_timeout=timedelta(seconds=30),
        )
        if job:
            print(f"Reserved job: ID={job.id}, Payload={job.payload}")
            # Process job...
            await asyncio.sleep(0.1)
            # Acknowledge job
            await client.queue.ack("default", "test", "orders", job.id)
            print("Job acknowledged")
        else:
            print("No jobs available")

        # Example 4: Receive multiple jobs (batch receive)
        print("\nReceiving multiple jobs...")
        jobs = await client.queue.receive(
            "default",
            "test",
            "orders",
            max_jobs=3,
            visibility_timeout=timedelta(seconds=30),
        )
        print(f"Received {len(jobs)} jobs")
        for job in jobs:
            print(f"  Job ID: {job.id}, Payload: {job.payload}")
            # Process job...
            await client.queue.ack("default", "test", "orders", job.id)

        # Example 5: Worker pattern (simple worker loop)
        print("\nRunning worker pattern...")

        async def worker():
            while True:
                job = await client.queue.reserve(
                    "default",
                    "test",
                    "orders",
                    visibility_timeout=timedelta(seconds=30),
                    long_poll_timeout=timedelta(seconds=5),
                )
                if not job:
                    continue

                # Process job
                print(f"Processing job: {job.id}")
                await asyncio.sleep(0.1)

                # Acknowledge job
                await client.queue.ack("default", "test", "orders", job.id)

        # Run worker for a short time
        try:
            await asyncio.wait_for(worker(), timeout=2.0)
        except asyncio.TimeoutError:
            pass

        print("\nQueue examples completed!")


if __name__ == "__main__":
    asyncio.run(main())

