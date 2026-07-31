#!/usr/bin/env python3
"""Generate Python gRPC code from the project's .proto files.

This script will write generated _pb2.py and _pb2_grpc.py files into the
examples/python-client directory so the client can import them directly.
"""
from __future__ import annotations
import pathlib
import grpc_tools
from grpc_tools import protoc


def main() -> int:
    this_dir = pathlib.Path(__file__).resolve().parent
    repo_root = this_dir.parent.parent
    proto_root = repo_root / "proto"
    third_party_googleapis = proto_root / "third_party" / "googleapis"
    grpc_tools_include = pathlib.Path(grpc_tools.__file__).resolve().parent / "_proto"
    proto_files = [
        proto_root / "revent" / "v1" / "query_messages.proto",
        proto_root / "revent" / "v1" / "event_messages.proto",
        proto_root / "revent" / "v1" / "control.proto",
        ]

    # Output into this directory so imports are simple
    out_dir = this_dir

    args = [
        "protoc",
        f"-I{proto_root}",
        f"-I{third_party_googleapis}",
        f"-I{grpc_tools_include}",
        f"--python_out={out_dir}",
        f"--grpc_python_out={out_dir}",
        *map(str, proto_files),
    ]

    print("Running protoc with:", " ".join(args))
    res = protoc.main(args)
    if res != 0:
        print("protoc failed with code", res)
    else:
        print("Generated Python gRPC files in", out_dir)
    return res


if __name__ == "__main__":
    raise SystemExit(main())
