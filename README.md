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
python a5_tests.py  # Fault-tolerance
python a5_tests.py  # State persistence
```
