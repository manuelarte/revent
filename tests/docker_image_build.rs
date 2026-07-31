use std::process::Command;

#[test]
fn docker_image_can_be_built() {
    if docker_is_unavailable() {
        eprintln!("Skipping docker image build test because Docker is not available.");
        return;
    }

    let image_tag = format!("revent:test-{}", std::process::id());

    let output = Command::new("docker")
        .arg("build")
        .arg("--file")
        .arg("Dockerfile")
        .arg("--tag")
        .arg(&image_tag)
        .arg(".")
        .output()
        .expect("Failed to run `docker build`");

    assert!(
        output.status.success(),
        "docker build failed (status: {:?})\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let _ = Command::new("docker")
        .arg("image")
        .arg("rm")
        .arg("--force")
        .arg(&image_tag)
        .output();
}

fn docker_is_unavailable() -> bool {
    let output = Command::new("docker").arg("info").output();
    match output {
        Ok(out) => !out.status.success(),
        Err(_) => true,
    }
}
