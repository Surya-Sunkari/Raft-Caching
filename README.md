# Assignment 1: Infrastructure and Frontend Service

## Setup Instructions

1. Install uv using https://docs.astral.sh/uv/getting-started/installation/
1. Setup the virtual environment and install dependencies using `uv sync`
1. Activate the virtual environment using `source .venv/bin/activate`
1. Change working directory `cd src/kvstore`
1. Generate protocol buffer files `python -m grpc_tools.protoc --python_out=. --grpc_python_out=. raft.proto`
1. Run tests using `python a1_tests.py`

## Quick Start (after Initial Setup)

```bash
cd [get to your assignment directory]
source venv/bin/activate
cd src/kvstore
python a1_tests.py
```
