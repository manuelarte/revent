use std::process::Command;

#[derive(Debug)]
#[allow(dead_code)]
enum BuildError {
    IOError(std::io::Error),
    CantConvertStringToUtf8(std::string::FromUtf8Error),
}

fn main() -> Result<(), BuildError> {
    const APP_PROTOS: &[&str] = &[
        "proto/revent/v1/control.proto",
        "proto/revent/v1/query_messages.proto",
        "proto/revent/v1/event_messages.proto",
    ];
    const PROTO_INCLUDES: &[&str] = &[
        "proto",
        // google/api annotations are vendored here.
        "proto/third_party/googleapis",
    ];

    for proto in APP_PROTOS {
        println!("cargo:rerun-if-changed={proto}");
    }
    println!("cargo:rerun-if-changed=proto/third_party/googleapis/google/api");

    // google/protobuf/* (Timestamp, Struct, etc.) are well-known types provided by protoc.
    tonic_prost_build::configure()
        .compile_protos(&[APP_PROTOS[0]], PROTO_INCLUDES)
        .map_err(BuildError::IOError)?;
    rustc_tools_util::setup_version_info!();
    // Get the current branch name
    let output = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .map_err(BuildError::IOError)?;
    let git_branch =
        String::from_utf8(output.stdout).map_err(BuildError::CantConvertStringToUtf8)?;
    println!("cargo:rustc-env=GIT_BRANCH={git_branch}");
    Ok(())
}
