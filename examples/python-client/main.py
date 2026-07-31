#!/usr/bin/env python3
"""Simple gRPC client that connects to localhost:10000 and prints server responses.

Usage:
  python generate_protos.py   # once, to generate control_pb2.py and control_pb2_grpc.py
  python main.py
"""
from __future__ import annotations
import json
import socket
import uuid
from queue import Empty, Queue

import grpc
from google.protobuf.json_format import MessageToDict

from revent.v1 import control_pb2, control_pb2_grpc, event_messages_pb2, query_messages_pb2

# Generated protobuf modules expose message symbols dynamically, which some IDE
# inspections fail to resolve statically.
ClientToServerMessage = getattr(control_pb2, "ClientToServerMessage")
RegisterClient = getattr(control_pb2, "RegisterClient")
RegisterSourceEvent = getattr(event_messages_pb2, "RegisterSourceEvent")
QueryRequest = getattr(query_messages_pb2, "QueryRequest")
QueryResponse = getattr(query_messages_pb2, "QueryResponse")
Heartbeat = getattr(control_pb2, "Heartbeat")


def open_requests(hostname: str, response_queue: Queue):
    """Send initial registration and keep the input stream open."""
    yield ClientToServerMessage(
            register_client=RegisterClient(
            client_id=hostname,
            query_handlers=["org.github.manuelarte.users.GetById", "org.github.manuelarte.users.GetAll"]
        )
    )

    # Send a source event so we can receive SourceEventRegistered and SourceEvent.
    yield ClientToServerMessage(
        register_source_event=RegisterSourceEvent(
            event_id=str(uuid.uuid4()),
            aggregate_id="user-123",
            aggregate_type="org.github.manuelarte.users.User",
            payload=json.dumps({"action": "created", "name": "Manuel"}).encode("utf-8"),
            metadata={"source": "python-client", "content-type": "application/json"},
        )
    )

    # Client stream messages must use ClientToServerMessage.
    yield ClientToServerMessage(
        query_request=QueryRequest(
            request_id="06c55848-382d-42d9-8ee8-bc04e1e116a4",
            query_id="org.github.manuelarte.users.GetAll",
        )
    )

    while True:
        try:
            # Check if there's a response to send (wait up to 60 seconds)
            open_request = response_queue.get(timeout=60)
            yield open_request
        except Empty:
            # Timeout occurred, send heartbeat
            yield ClientToServerMessage(
                heartbeat=Heartbeat()
            )


def main() -> None:
    target = "localhost:10000"
    print(f"Connecting to {target}...")
    channel = grpc.insecure_channel(target)
    stub = control_pb2_grpc.ControlStub(channel)

    hostname = socket.gethostname()
    response_queue = Queue()

    try:
        # Open is bidirectional streaming: send an iterator and read responses.
        responses = stub.OpenSession(open_requests(hostname, response_queue))
        print("Waiting for server responses (Ctrl-C to stop)...")
        for resp in responses:
            kind = resp.WhichOneof("payload")
            if kind == "client_registered":
                print(f"Client registered: {resp.client_registered.client_id}")
            elif kind == "client_registration_error":
                cre = resp.client_registration_error
                print(f"Client registration error - client_id: {cre.client_id}, reason: {cre.reason}")
            elif kind == "query_requested":
                qr = resp.query_requested
                print(f"Query requested - request_id: {qr.request_id}, query_id: {qr.query_id}, parameters: {dict(qr.parameters)}")
                result_json = json.dumps({"name": "John"})
                query_response = QueryResponse(
                    request_id=qr.request_id,
                    result=result_json.encode('utf-8')
                )
                response_queue.put(
                    ClientToServerMessage(query_response=query_response)
                )
            elif kind == "query_responded":
                qr = resp.query_responded
                print(f"Query responded - request_id: {qr.request_id}, result: {qr.result.decode('utf-8')}")
            elif kind == "query_requested_error":
                qre = resp.query_requested_error
                print(f"Query requested error - request_id: {qre.request_id}, query_id: {qre.query_id}, reason: {qre.reason}")
            elif kind == "source_event_registered":
                source_event_registered = resp.source_event_registered
                print(
                    f"SourceEventRegistered - event_id: {source_event_registered.event_id}, "
                    f"monotonic_clock: {source_event_registered.monotonic_clock}"
                )
            elif kind == "source_event":
                se = resp.source_event
                payload_dict = MessageToDict(se.payload, preserving_proto_field_name=True)
                print(
                    f"SourceEvent - id: {se.id}, aggregate_type: {se.aggregate_type}, "
                    f"aggregate_id: {se.aggregate_id}, monotonic_clock: {se.monotonic_clock}, "
                    f"payload: {payload_dict}"
                )
            elif kind != "heartbeat":
                print("Unknown response:", resp)

    except KeyboardInterrupt:
        print("Interrupted by user")
    except grpc.RpcError as e:
        print("gRPC error:", e)


if __name__ == "__main__":
    main()
