# Assignment 1: Infrastructure and Frontend Service

## Setup Instructions

### 1. Install Python 3.12

```bash
brew install python@3.12
```

### 2. Create Virtual Environment

```bash
/opt/homebrew/bin/python3.12 -m venv venv
```

### 3. Activate Virtual Environment

```bash
source venv/bin/activate
```

### 4. Install Dependencies

```bash
pip install -r requirements.txt
```

### 5. Generate Protocol Buffer Files

```bash
python -m grpc_tools.protoc --python_out=. --grpc_python_out=. raft.proto
```

### 6. Run Tests

```bash
python a1_tests.py
```

## Environment Specifications (Gradescope)

- Ubuntu 22.04.3 LTS
- Python 3.12.11
- grpcio==1.59.0
- grpcio-tools==1.59.0
- protobuf==4.24.4
- configparser==7.2.0

**Note:** Python 3.13 has breaking API changes with gRPC. Use Python 3.12.

## Quick Start (After Initial Setup)

```bash
cd "/Users/omagr/Documents/Personal/distributed/assignment 1"
source venv/bin/activate
python a1_tests.py
```

## Deactivate Virtual Environment

When you're done working on the project:

```bash
deactivate
```

This returns you to your system's default Python.
