#[allow(clippy::all, clippy::pedantic, clippy::nursery)]
pub mod protocontrol {
    tonic::include_proto!("revent.v1");
}
use crate::api::grpc::control::protocontrol::control_server::Control;
use crate::api::grpc::control::protocontrol::{ClientToServerMessage, ServerToClientMessage};
use crate::domain::ClientId;
use crate::domain::Repository;
use crate::domain::messages::client::ClientMessage;
use crate::domain::state::State;
use futures_core::Stream;
use std::pin::Pin;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn};

#[derive(Clone)]
pub struct ControlService<R: Repository> {
    state: State<R>,
    shutdown: watch::Receiver<bool>,
}

impl<R: Repository> ControlService<R> {
    #[must_use]
    pub(crate) const fn new(state: State<R>, shutdown: watch::Receiver<bool>) -> Self {
        Self { state, shutdown }
    }
}

#[tonic::async_trait]
impl<R: Repository + 'static> Control for ControlService<R> {
    type OpenSessionStream =
        Pin<Box<dyn Stream<Item = Result<ServerToClientMessage, Status>> + Send + 'static>>;

    async fn open_session(
        &self,
        request: Request<Streaming<ClientToServerMessage>>,
    ) -> Result<Response<Self::OpenSessionStream>, Status> {
        let mut stream = request.into_inner();
        // Create channel for this client
        let (tx, rx) = mpsc::channel(128);
        let state = self.state.clone();
        let mut shutdown = self.shutdown.clone();

        // Spawn task to handle incoming messages
        tokio::spawn(async move {
            let mut client_id_option: Option<ClientId> = None;
            loop {
                if *shutdown.borrow() {
                    info!("Shutdown requested, closing client stream handler");
                    break;
                }

                let msg = match tokio::select! {
                    changed = shutdown.changed() => {
                        if matches!(changed, Ok(())) { info!("Shutdown requested, closing client stream handler") } else { info!("Shutdown channel closed, stopping client stream handler") }
                        break;
                    }
                    message = stream.message() => message,
                } {
                    Ok(Some(msg)) => msg,
                    Ok(None) => break,
                    Err(status) => {
                        warn!("Open stream receive error: {}", status);
                        break;
                    }
                };

                match msg.payload {
                    Some(msg) => {
                        match ClientMessage::from(msg, client_id_option.clone(), tx.clone()) {
                            Ok(client_message) => {
                                state.handle_message(&client_message).await;
                                if let ClientMessage::RegisterClient(rcm) = client_message.clone() {
                                    client_id_option = Some(rcm.client_id().clone());
                                }
                            }
                            Err(err) => {
                                // TODO(manuelarte): handle errors, inform the client.
                                warn!(err=%err, "Client message validation error");
                                break;
                            }
                        }
                    }
                    None => {
                        warn!("Received an OpenRequest with no request field");
                    }
                }
            }

            // Cleanup on disconnect
            info!(
                client_id = client_id_option
                    .clone()
                    .map_or_else(String::new, |id| id.to_string()),
                "Disconnecting client"
            );

            let Some(client_id) = client_id_option else {
                warn!("Client ID not set, skipping disconnect handling");
                return;
            };
            state
                .handle_message(&ClientMessage::DisconnectClient { client_id })
                .await;
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::OpenSessionStream
        ))
    }
}
