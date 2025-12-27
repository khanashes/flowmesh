"""
FlowMesh Streams Example

Demonstrates stream operations: writing events, reading messages, and subscribing.
"""

import asyncio
from flowmesh import FlowMesh, Event


async def main():
    # Create client
    async with FlowMesh(endpoint="localhost:50051") as client:
        # Example 1: Write events to a stream
        print("Writing events to stream...")
        events = [
            Event(
                payload=b'{"order_id": "123", "amount": 99.99}',
                headers={"event_type": "order.created"},
            ),
            Event(
                payload=b'{"order_id": "124", "amount": 149.99}',
                headers={"event_type": "order.created"},
            ),
        ]

        offsets = await client.stream.write("default", "test", "orders", events)
        print(f"Events written at offsets: {offsets}")

        # Example 2: Read messages from a stream
        print("\nReading messages from stream...")
        messages = await client.stream.read(
            "default", "test", "orders", partition=0, offset=0, max_messages=10
        )
        print(f"Read {len(messages)} messages")
        for msg in messages:
            print(f"  Offset: {msg.offset}, Payload: {msg.payload}")

        # Example 3: Subscribe to a stream with consumer group
        print("\nSubscribing to stream with consumer group...")

        async def consume_messages():
            async for msg in client.stream.subscribe(
                "default",
                "test",
                "orders",
                "billing-service",
                partition=0,
                start_offset=-2,  # Start from latest
            ):
                print(f"Received message: Offset={msg.offset}, Payload={msg.payload}")
                # Commit offset after processing
                await client.stream.commit_offset(
                    "default", "test", "orders", "billing-service", 0, msg.offset
                )

        # Run consumer for a short time
        try:
            await asyncio.wait_for(consume_messages(), timeout=2.0)
        except asyncio.TimeoutError:
            pass

        # Example 4: Get consumer group state
        print("\nGetting consumer group state...")
        state = await client.stream.get_consumer_group_state(
            "default", "test", "orders", "billing-service", 0
        )
        print(f"Consumer Group State:")
        print(f"  Committed Offset: {state.committed_offset}")
        print(f"  Latest Offset: {state.latest_offset}")
        print(f"  Lag: {state.lag}")

        print("\nStream examples completed!")


if __name__ == "__main__":
    asyncio.run(main())

