"""
FlowMesh KV Store Example

Demonstrates KV store operations: setting, getting, listing, and deleting keys.
"""

import asyncio
from datetime import timedelta
from flowmesh import FlowMesh


async def main():
    # Create client
    async with FlowMesh(endpoint="localhost:50051") as client:
        # Example 1: Set a value without TTL
        print("Setting a value...")
        await client.kv.set(
            "default",
            "test",
            "cache",
            "user:123",
            b'{"name": "John Doe", "email": "john@example.com"}',
        )
        print("Value set successfully")

        # Example 2: Get a value
        print("\nGetting a value...")
        value = await client.kv.get("default", "test", "cache", "user:123")
        print(f"Retrieved value: {value}")

        # Example 3: Set a value with TTL
        print("\nSetting a value with TTL...")
        await client.kv.set(
            "default",
            "test",
            "cache",
            "session:abc123",
            b'{"user_id": "123", "session_token": "xyz"}',
            ttl=timedelta(hours=1),
        )
        print("Value set with 1 hour TTL")

        # Example 4: Check if key exists
        print("\nChecking if key exists...")
        exists = await client.kv.exists("default", "test", "cache", "user:123")
        print(f"Key 'user:123' exists: {exists}")

        # Example 5: List keys with prefix
        print("\nListing keys with prefix...")
        # Set a few more keys
        await client.kv.set(
            "default", "test", "cache", "user:456", b'{"name": "Jane Doe"}'
        )
        await client.kv.set(
            "default", "test", "cache", "user:789", b'{"name": "Bob Smith"}'
        )
        await client.kv.set(
            "default", "test", "cache", "order:123", b'{"order_id": "123"}'
        )

        keys = await client.kv.list_keys("default", "test", "cache", prefix="user:")
        print(f"Keys with prefix 'user:': {keys}")

        # List all keys
        all_keys = await client.kv.list_keys("default", "test", "cache", prefix="")
        print(f"All keys: {all_keys}")

        # Example 6: Delete a key
        print("\nDeleting a key...")
        await client.kv.delete("default", "test", "cache", "user:456")
        print("Key deleted")

        # Verify deletion
        exists = await client.kv.exists("default", "test", "cache", "user:456")
        print(f"Key 'user:456' exists after deletion: {exists}")

        print("\nKV store examples completed!")


if __name__ == "__main__":
    asyncio.run(main())

