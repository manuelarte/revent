
Examples: Python gRPC client
===========================

This folder contains a minimal Python gRPC client for the `Control` service defined in `proto/control.proto`.

Quickstart (fish shell):

```fish
# 1) install dependencies
python -m pip install --upgrade pip
python -m pip install grpcio grpcio-tools protobuf

# 2) generate Python protobuf/grpc code (creates control_pb2.py and control_pb2_grpc.py here)
python generate_protos.py

# 3) run the client (connects to localhost:10000)
python main.py
```

The client opens the bidirectional streaming `Open` RPC, sends a `RegisterClient` request with your hostname, and keeps sending periodic heartbeats while printing server responses.
