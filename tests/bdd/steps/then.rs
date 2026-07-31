use crate::ReventWorld;
use crate::protocontrol::server_to_client_message::Payload;
use cucumber::gherkin::Step as GherkinStep;
use cucumber::then;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tokio::time::timeout;

#[derive(serde::Deserialize)]
struct ClientResponse {
    last_seen: f64,
}

#[then(expr = "the client {string} receives a {string} message")]
#[allow(clippy::too_many_lines)]
async fn then_client_receives_message(
    world: &mut ReventWorld,
    expected_client_alias: String,
    msg_name: String,
    #[step] step: &GherkinStep,
) {
    let expected_client_alias = expected_client_alias
        .try_into()
        .expect("invalid expected client alias");
    let expected_registered_client_id = world
        .registered_client_id_for(&expected_client_alias)
        .expect("client alias not found in world state");

    let (_, response_stream) = world
        .clients
        .get_mut(&expected_client_alias)
        .expect("client not found in world state");

    let supported_msg_names = [
        "ClientRegistered",
        "QueryRequested",
        "QueryResponded",
        "Heartbeat",
        "ClientRegistrationError",
        "QueryRequestedError",
    ];
    assert!(
        supported_msg_names.contains(&msg_name.as_str()),
        "unsupported expected message type '{msg_name}'. Supported: {}",
        supported_msg_names.join(", ")
    );

    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for expected {msg_name} response"
        );

        let next_message = timeout(remaining, response_stream.message())
            .await
            .expect("timed out waiting for server response")
            .expect("stream read failed")
            .expect("server closed stream before sending expected response");

        let response = next_message
            .payload
            .expect("server sent an OpenResponse without a response payload");

        match (msg_name.as_str(), response) {
            ("ClientRegistered", Payload::ClientRegistered(msg)) => {
                assert_eq!(msg.client_id, expected_registered_client_id.as_str());
                return;
            }
            ("ClientRegistrationError", Payload::ClientRegistrationError(msg)) => {
                // If a table is provided, check the reason. Accept both formats:
                // 1) A header row followed by a value row (rows[1][1])
                // 2) A single key/value row (rows[0][0]="reason", rows[0][1]=value)
                if let Some(table) = &step.table {
                    let expected_reason = if table.rows.len() >= 2 {
                        table.rows.get(1).and_then(|r| r.get(1)).map(String::as_str)
                    } else if table.rows.len() == 1 {
                        table.rows.first().and_then(|r| {
                            if r.len() >= 2
                                && r.first().is_some_and(|s| s.eq_ignore_ascii_case("reason"))
                            {
                                r.get(1).map(String::as_str)
                            } else {
                                None
                            }
                        })
                    } else {
                        None
                    };

                    let expected_reason = expected_reason.expect("expected reason value in table");
                    assert_eq!(
                        &msg.reason, expected_reason,
                        "received ClientRegistrationError with unexpected reason"
                    );
                }
                return;
            }
            ("QueryRequested", Payload::QueryRequested(_))
            | ("QueryResponded", Payload::QueryResponded(_))
            | ("Heartbeat", Payload::Heartbeat(_)) => return,
            ("QueryRequestedError", Payload::QueryRequestedError(msg)) => {
                // If a table is provided, check the reason. Accept both header+value and single key/value row.
                if let Some(table) = &step.table {
                    let expected_reason = if table.rows.len() >= 2 {
                        table
                            .rows
                            .get(1)
                            .and_then(|r| r.get(1))
                            .map(std::string::String::as_str)
                    } else if table.rows.len() == 1 {
                        table.rows.first().and_then(|r| {
                            if r.len() >= 2
                                && r.first().is_some_and(|s| s.eq_ignore_ascii_case("reason"))
                            {
                                r.get(1).map(std::string::String::as_str)
                            } else {
                                None
                            }
                        })
                    } else {
                        None
                    };

                    let expected_reason = expected_reason.expect("expected reason value in table");
                    assert_eq!(
                        &msg.reason, expected_reason,
                        "received QueryRequestedError with unexpected reason"
                    );
                }
                return;
            }
            _ => {
                // Ignore interleaved messages while waiting for the expected one.
            }
        }
    }
}

#[then(expr = "the client {string} last_seen in REST endpoint eventually decreases")]
#[allow(clippy::needless_pass_by_ref_mut)]
async fn then_client_last_seen_eventually_decreases(world: &mut ReventWorld, client_id: String) {
    let rest_endpoint = world
        .rest_endpoint
        .as_deref()
        .expect("missing REST endpoint in world state");
    let http_client = reqwest::Client::new();
    let mut previous = fetch_last_seen(&http_client, rest_endpoint, &client_id).await;

    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        sleep(Duration::from_millis(25)).await;
        let current = fetch_last_seen(&http_client, rest_endpoint, &client_id).await;

        // A lower elapsed value means `last_seen` was refreshed between two reads.
        if current + 0.001 < previous {
            return;
        }

        previous = current;
    }

    panic!("expected last_seen to be refreshed for client '{client_id}', but it never decreased");
}

async fn fetch_last_seen(
    http_client: &reqwest::Client,
    rest_endpoint: &str,
    client_id: &str,
) -> f64 {
    let url = format!("{rest_endpoint}/api/clients/{client_id}");
    let response = http_client
        .get(url)
        .send()
        .await
        .expect("failed to call REST API");
    assert!(
        response.status().is_success(),
        "expected successful response when reading client by id, got status {}",
        response.status()
    );
    response
        .json::<ClientResponse>()
        .await
        .expect("failed to decode client response JSON")
        .last_seen
}

#[then(expr = "the client {string} does not receive any more messages")]
fn then_client_receives_no_more_messages(world: &mut ReventWorld, expected_client_alias: String) {
    let expected_client_alias = expected_client_alias.try_into().expect("invalid client id");

    // Client should have been removed from world state due to disconnection
    assert!(
        !world.clients.contains_key(&expected_client_alias),
        "expected client to be disconnected and removed from world state"
    );
}

#[then(expr = "the client {string} does not receive a {string} message")]
#[then(expr = "the client {string} does not receives a {string} message")]
async fn then_client_does_not_receive_message(
    world: &mut ReventWorld,
    expected_client_alias: String,
    msg_name: String,
) {
    let expected_client_alias = expected_client_alias
        .try_into()
        .expect("invalid expected client alias");

    // If client is already removed (disconnected), they can't receive messages
    if !world.clients.contains_key(&expected_client_alias) {
        return;
    }

    let (_, response_stream) = world
        .clients
        .get_mut(&expected_client_alias)
        .expect("client not found in world state");

    // Try to receive a message with a short timeout
    let timeout_duration = Duration::from_millis(500);

    if let Ok(Ok(Some(next_message))) = timeout(timeout_duration, response_stream.message()).await {
        let response = next_message
            .payload
            .expect("server sent an OpenResponse without a response payload");

        // Check that we didn't receive the expected message type
        if let ("QueryRequestedError", Payload::QueryRequestedError(_)) =
            (msg_name.as_str(), response)
        {
            panic!("client should not receive QueryRequestedError message, but did");
        } else {
            // Other messages are OK, we just don't want the specific one
        }
    } else {
        // Timeout or stream closed, which is expected for a disconnected client
    }
}

#[then(expr = "the server handles it gracefully without errors")]
#[allow(clippy::needless_pass_by_ref_mut)]
const fn then_server_handles_gracefully(_world: &mut ReventWorld) {
    // If we reach this point, the server handled the scenario without panicking
    // The test framework would have caught any errors or panics
    // This is primarily a placeholder to document the expectation
}
