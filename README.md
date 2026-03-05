# Raft Key-Value Store

A distributed key-value store implementation using the Raft consensus protocol.

## Setup Instructions

1. Install uv using https://docs.astral.sh/uv/getting-started/installation/
2. Setup the virtual environment and install dependencies using `uv sync`
3. Activate the virtual environment using `source .venv/bin/activate`
4. Change working directory `cd src/kvstore`
5. Generate protocol buffer files:
   ```bash
   python -m grpc_tools.protoc \
   --proto_path=. \
   --python_out=. \
   --grpc_python_out=. \
   --pyi_out=. \
   raft.proto
   ```

## Running Tests

```bash
source .venv/bin/activate
cd src/kvstore
python a1_tests.py  # Infrastructure tests
python a2_tests.py  # Key-value store tests
python a3_tests.py  # Leader election tests
python a4_tests.py  # Log replication tests
python a5_tests.py  # Fault-tolerance tests
python a6_tests.py  # State persistence tests
python -m unittest cache_tests -v  # Cache eviction policy tests
```

## Cache Eviction Policies

The project includes a pluggable cache layer (`src/kvstore/cache.py`) with the following eviction policies:

| Policy | Description |
|--------|-------------|
| `random` | Evicts a random key (O(1) via swap-with-last) |
| `fifo` | Evicts the oldest inserted key (no reordering on access) |
| `lru` | Evicts the least recently used key |
| `lfu` | Evicts the least frequently used key (LRU tiebreak) |
| `slru` | Segmented LRU with probation and protected segments |

Usage:
```python
from cache import create_cache

cache = create_cache("lru", capacity=100)
cache.put("key", "value")
cache.get("key")        # returns "value"
cache.invalidate("key") # returns True
print(cache.stats)      # CacheStats(hits=1, misses=0, ...)
```
