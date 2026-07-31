use crate::ReventWorld;
use crate::protocontrol::client_to_server_message::Payload;
use crate::protocontrol::control_client::ControlClient;
use crate::protocontrol::{ClientToServerMessage, QueryRequest, QueryResponse, RegisterClient};
use cucumber::gherkin::Step as GherkinStep;
use cucumber::when;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

#[when(
    regex = r#"^a client registers with client id \"([^\"]+)\"(?: and the following query handlers:)?$"#
)]
async fn when_client_registers(
    world: &mut ReventWorld,
    client_id: String,
    #[step] step: &GherkinStep,
) {
    let endpoint = world
        .endpoint
        .as_deref()
        .expect("missing endpoint in world state");
    let mut client = connect_with_retry(endpoint).await;

    let (tx, rx): (
        Sender<ClientToServerMessage>,
        Receiver<ClientToServerMessage>,
    ) = tokio::sync::mpsc::channel(4);

    let response = client
        .open_session(ReceiverStream::new(rx))
        .await
        .expect("open stream call failed")
        .into_inner();

    world.add_client(
        client_id.clone().try_into().expect("invalid client id"),
        client_id.clone().try_into().expect("invalid client id"),
        tx.clone(),
        response,
    );

    let query_handlers = step
        .table
        .as_ref()
        .map(|table| {
            table
                .rows
                .iter()
                .filter_map(|row| row.first())
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .collect()
        })
        .unwrap_or_default();

    tx.send(ClientToServerMessage {
        payload: Some(Payload::RegisterClient(RegisterClient {
            client_id,
            query_handlers,
        })),
    })
    .await
    .expect("failed to send register request");
    drop(tx);
}

#[when(
    regex = r#"^connection \"([^\"]+)\" registers with client id \"([^\"]+)\"(?: and the following query handlers:)?$"#
)]
async fn when_connection_registers(
    world: &mut ReventWorld,
    alias: String,
    register_client_id: String,
    #[step] step: &GherkinStep,
) {
    let endpoint = world
        .endpoint
        .as_deref()
        .expect("missing endpoint in world state");
    let mut client = connect_with_retry(endpoint).await;

    let (tx, rx): (
        Sender<ClientToServerMessage>,
        Receiver<ClientToServerMessage>,
    ) = tokio::sync::mpsc::channel(4);

    let response = client
        .open_session(ReceiverStream::new(rx))
        .await
        .expect("open stream call failed")
        .into_inner();

    world.add_client(
        alias.clone().try_into().expect("invalid alias"),
        register_client_id
            .clone()
            .try_into()
            .expect("invalid register client id"),
        tx.clone(),
        response,
    );

    let query_handlers = step
        .table
        .as_ref()
        .map(|table| {
            table
                .rows
                .iter()
                .filter_map(|row| row.first())
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .collect()
        })
        .unwrap_or_default();

    tx.send(ClientToServerMessage {
        payload: Some(Payload::RegisterClient(RegisterClient {
            client_id: register_client_id,
            query_handlers,
        })),
    })
    .await
    .expect("failed to send register request");
    drop(tx);
}

#[when(expr = "the client {string} sends an {string} query")]
async fn when_client_sends_query(
    world: &mut ReventWorld,
    requester_client_id: String,
    query_id: String,
) {
    let tx = world
        .clients
        .get(
            &requester_client_id
                .clone()
                .try_into()
                .expect("invalid requester client id"),
        )
        .map(|(tx, _)| tx.clone())
        .expect("source client not found in world state");

    let request_id = Uuid::new_v4().to_string();
    world.set_pending_query_request_id(
        requester_client_id
            .try_into()
            .expect("invalid requester client id"),
        request_id.clone(),
    );

    tx.send(ClientToServerMessage {
        payload: Some(Payload::QueryRequest(QueryRequest {
            request_id,
            query_id,
            parameters: HashMap::new(),
        })),
    })
    .await
    .expect("failed to send query request");
}

#[when(expr = "the client {string} sends the same {string} query again")]
#[allow(clippy::needless_pass_by_ref_mut)]
async fn when_client_sends_same_query_again(
    world: &mut ReventWorld,
    requester_client_id: String,
    query_id: String,
) {
    let requester_client_id = requester_client_id
        .try_into()
        .expect("invalid requester client id");
    let tx = world
        .clients
        .get(&requester_client_id)
        .map(|(tx, _)| tx.clone())
        .expect("source client not found in world state");

    let request_id = world
        .pending_query_request_ids
        .get(&requester_client_id)
        .cloned()
        .expect("no pending query request id found for source client");

    tx.send(ClientToServerMessage {
        payload: Some(Payload::QueryRequest(QueryRequest {
            request_id,
            query_id,
            parameters: HashMap::new(),
        })),
    })
    .await
    .expect("failed to send duplicated query request");
}

#[when(expr = "the client {string} sends an {string} query with the same request id as {string}")]
async fn when_client_sends_query_with_same_request_id_as_other_client(
    world: &mut ReventWorld,
    requester_client_id: String,
    query_id: String,
    duplicated_from_client_id: String,
) {
    let requester_client_id = requester_client_id
        .try_into()
        .expect("invalid requester client id");
    let duplicated_from_client_id = duplicated_from_client_id
        .try_into()
        .expect("invalid duplicated-from client id");

    let tx = world
        .clients
        .get(&requester_client_id)
        .map(|(tx, _)| tx.clone())
        .expect("source client not found in world state");

    let request_id = world
        .pending_query_request_ids
        .get(&duplicated_from_client_id)
        .cloned()
        .expect("no pending query request id found for duplicated-from client");

    tx.send(ClientToServerMessage {
        payload: Some(Payload::QueryRequest(QueryRequest {
            request_id,
            query_id,
            parameters: HashMap::new(),
        })),
    })
    .await
    .expect("failed to send duplicated query request from another client");
}

#[when(expr = "the client {string} responds to the latest query from {string}")]
async fn when_client_responds_to_query(
    world: &mut ReventWorld,
    responder_client_id: String,
    requester_client_id: String,
) {
    let responder_client_id = responder_client_id
        .try_into()
        .expect("invalid responder client id");
    let requester_client_id = requester_client_id
        .try_into()
        .expect("invalid requester client id");
    let request_id = world
        .take_pending_query_request_id(&requester_client_id)
        .expect("no pending query request id found for source client");

    let tx = world
        .clients
        .get(&responder_client_id)
        .map(|(tx, _)| tx.clone())
        .expect("responder client not found in world state");

    tx.send(ClientToServerMessage {
        payload: Some(Payload::QueryResponse(QueryResponse {
            request_id,
            result: b"{}".to_vec(),
        })),
    })
    .await
    .expect("failed to send query response");
}

#[when(expr = "the client {string} disconnects immediately")]
async fn when_client_disconnects_immediately(world: &mut ReventWorld, client_id: String) {
    let client_id = client_id.try_into().expect("invalid client id");
    // Remove the client from the world state, which closes the connection
    world.clients.remove(&client_id);
    // Give the server a moment to process the disconnection
    sleep(Duration::from_millis(100)).await;
}

async fn connect_with_retry(endpoint: &str) -> ControlClient<tonic::transport::Channel> {
    for _ in 0..30 {
        if let Ok(client) = ControlClient::connect(endpoint.to_string()).await {
            return client;
        }
        sleep(Duration::from_millis(100)).await;
    }

    panic!("failed to connect to gRPC server at {endpoint}");
}
