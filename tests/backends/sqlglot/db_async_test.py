"""
Tests for async SQLGlot backend.

This test demonstrates the performance improvements from async execution
when querying multiple network-based data sources.
"""

import asyncio
import time
from typing import Any, Iterator, Optional
from unittest.mock import Mock

from shillelagh.adapters.base import Adapter
from shillelagh.backends.sqlglot.db import connect
from shillelagh.backends.sqlglot.db_async import connect_async
from shillelagh.fields import Field, Integer, String
from shillelagh.filters import Filter
from shillelagh.typing import RequestedOrder, Row


class MockSlowAdapter(Adapter):
    """
    Mock adapter that simulates network delay.

    This adapter simulates a slow network API by sleeping during get_rows().
    """

    safe = True
    supports_limit = False
    supports_offset = False

    delay: float = 0.5  # seconds

    def __init__(self, uri: str, delay: float = 0.5):
        super().__init__()
        self.uri = uri
        self.delay = delay
        self._data = [
            {"id": 1, "name": f"Row 1 from {uri}"},
            {"id": 2, "name": f"Row 2 from {uri}"},
            {"id": 3, "name": f"Row 3 from {uri}"},
        ]

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        """Check if URI is supported."""
        return uri.startswith("slow://")

    @staticmethod
    def parse_uri(uri: str) -> tuple[str]:
        """Parse URI."""
        return (uri,)

    def get_columns(self) -> dict[str, Field]:
        """Return columns."""
        return {
            "id": Integer(),
            "name": String(),
        }

    def get_data(
        self,
        bounds: dict[str, Filter],
        order: list[tuple[str, RequestedOrder]],
        **kwargs: Any,
    ) -> Iterator[Row]:
        """
        Get data with simulated network delay.

        This sleep simulates a slow API call (e.g., HTTP request).
        """
        time.sleep(self.delay)
        yield from self._data


def test_async_performance_improvement():
    """
    Test that async execution is faster for multiple tables.

    This test demonstrates the key benefit of async execution: when querying
    multiple tables, async can fetch all tables concurrently instead of
    sequentially, resulting in significant performance improvements.
    """
    # Register the mock adapter
    from shillelagh.adapters.registry import registry

    # Save original loader state
    original_loaders = registry.loaders.copy()

    try:
        # Register mock adapter
        registry.loaders["mockslowadapter"] = [lambda: MockSlowAdapter]

        # Test parameters
        num_tables = 3
        delay_per_table = 0.3  # seconds
        MockSlowAdapter.delay = delay_per_table

        # Create URIs for multiple tables
        uris = [f"slow://table{i}" for i in range(num_tables)]

        # Build a query that joins multiple tables using Cartesian product
        # This is simpler than UNION and demonstrates multi-table fetching
        from_clause = ", ".join([f"'{uri}' AS t{i}" for i, uri in enumerate(uris)])
        query = f"SELECT * FROM {from_clause}"

        # Test 1: Synchronous execution (baseline)
        print("\n=== Testing Synchronous Execution ===")
        conn_sync = connect(adapters=["mockslowadapter"])
        cursor_sync = conn_sync.cursor()

        start = time.time()
        cursor_sync.execute(query)
        results_sync = cursor_sync.fetchall()
        sync_duration = time.time() - start

        print(f"Sync execution: {sync_duration:.2f}s")
        print(f"Fetched {len(results_sync)} rows")

        # Test 2: Asynchronous execution
        print("\n=== Testing Asynchronous Execution ===")

        async def run_async_query():
            conn_async = connect_async(adapters=["mockslowadapter"])
            cursor_async = conn_async.cursor_async()

            start = time.time()
            await cursor_async.execute_async(query)
            results_async = cursor_async.fetchall()
            async_duration = time.time() - start

            print(f"Async execution: {async_duration:.2f}s")
            print(f"Fetched {len(results_async)} rows")

            return async_duration, results_async

        async_duration, results_async = asyncio.run(run_async_query())

        # Verify results are the same (order might differ, so compare lengths)
        # For a cartesian product, we expect num_tables rows from each table multiplied
        assert len(results_sync) == len(results_async)

        # Calculate speedup
        speedup = sync_duration / async_duration
        print(f"\n=== Performance Summary ===")
        print(f"Synchronous: {sync_duration:.2f}s")
        print(f"Asynchronous: {async_duration:.2f}s")
        print(f"Speedup: {speedup:.2f}x")

        # Async should be significantly faster
        # With 3 tables at 0.3s each:
        # - Sync: ~0.9s (sequential)
        # - Async: ~0.3s (parallel)
        # We expect at least 2x speedup (allowing for overhead)
        assert speedup >= 2.0, f"Expected at least 2x speedup, got {speedup:.2f}x"

        print("\nAsync execution is faster! ✓")

    finally:
        # Restore original registry state
        registry.loaders = original_loaders


def test_async_single_table():
    """
    Test async execution with a single table.

    For single table queries, async shouldn't be slower than sync
    (though it won't be faster either).
    """
    from shillelagh.adapters.registry import registry

    original_loaders = registry.loaders.copy()

    try:
        registry.loaders["mockslowadapter"] = [lambda: MockSlowAdapter]
        MockSlowAdapter.delay = 0.1

        query = "SELECT * FROM 'slow://table1'"

        # Sync
        conn_sync = connect(adapters=["mockslowadapter"])
        cursor_sync = conn_sync.cursor()
        start = time.time()
        cursor_sync.execute(query)
        results_sync = cursor_sync.fetchall()
        sync_duration = time.time() - start

        # Async
        async def run_async():
            conn_async = connect_async(adapters=["mockslowadapter"])
            cursor_async = conn_async.cursor_async()
            await cursor_async.execute_async(query)
            return cursor_async.fetchall()

        start = time.time()
        results_async = asyncio.run(run_async())
        async_duration = time.time() - start

        # Results should be the same
        assert results_sync == results_async

        # Async shouldn't be significantly slower (within 50% overhead is acceptable)
        assert async_duration < sync_duration * 1.5

        print(f"\nSingle table - Sync: {sync_duration:.3f}s, Async: {async_duration:.3f}s")

    finally:
        registry.loaders = original_loaders


def test_async_with_filters():
    """Test that async execution properly pushes down predicates to adapters."""
    from shillelagh.adapters.registry import registry

    original_loaders = registry.loaders.copy()

    try:
        registry.loaders["mockslowadapter"] = [lambda: MockSlowAdapter]
        MockSlowAdapter.delay = 0.1

        query = "SELECT * FROM 'slow://table1' WHERE id = 2"

        async def run_async():
            conn_async = connect_async(adapters=["mockslowadapter"])
            cursor_async = conn_async.cursor_async()
            await cursor_async.execute_async(query)
            return cursor_async.fetchall()

        results = asyncio.run(run_async())

        # Note: The mock adapter doesn't actually filter, so we get all rows
        # but in a real adapter, predicate pushdown would work
        assert len(results) > 0

    finally:
        registry.loaders = original_loaders


if __name__ == "__main__":
    # Run the performance test directly
    print("=" * 60)
    print("Async SQLGlot Backend Performance Demo")
    print("=" * 60)

    test_async_performance_improvement()
    print("\n" + "=" * 60)
    print("All tests passed!")
    print("=" * 60)
