#[allow(clippy::all, clippy::pedantic, clippy::nursery)]
pub mod protocontrol {
    tonic::include_proto!("revent.v1");
}

use crate::protocontrol::ClientToServerMessage;
use cucumber::World;
use protocontrol::ServerToClientMessage;
use revent::domain::ClientId;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tonic::Streaming;

mod steps;

#[derive(Debug, Default, World)]
struct ReventWorld {
    endpoint: Option<String>,
    rest_endpoint: Option<String>,
    server_task: Option<JoinHandle<()>>,

    clients: HashMap<
        ClientId,
        (
            Sender<ClientToServerMessage>,
            Streaming<ServerToClientMessage>,
        ),
    >,
    registered_client_ids: HashMap<ClientId, ClientId>,
    pending_query_request_ids: HashMap<ClientId, String>,
}

impl ReventWorld {
    fn add_client(
        &mut self,
        alias: ClientId,
        registered_client_id: ClientId,
        tx: Sender<ClientToServerMessage>,
        response: Streaming<ServerToClientMessage>,
    ) {
        self.clients.insert(alias.clone(), (tx, response));
        self.registered_client_ids
            .insert(alias, registered_client_id);
    }

    fn registered_client_id_for(&self, alias: &ClientId) -> Option<ClientId> {
        self.registered_client_ids.get(alias).cloned()
    }

    fn set_pending_query_request_id(&mut self, requester_client_id: ClientId, request_id: String) {
        self.pending_query_request_ids
            .insert(requester_client_id, request_id);
    }

    fn take_pending_query_request_id(&mut self, requester_client_id: &ClientId) -> Option<String> {
        self.pending_query_request_ids.remove(requester_client_id)
    }
}

#[tokio::main]
async fn main() {
    ReventWorld::cucumber()
        .after(|_feature, _rule, _scenario, _ev, world| {
            Box::pin(async move {
                if let Some(world) = world
                    && let Some(server_task) = world.server_task.take()
                {
                    server_task.abort();
                    let _ = server_task.await;
                }
            })
        })
        .run("./tests/bdd/features/")
        .await;
}
