use std::net::SocketAddr;

use actix_web::web::Bytes;
use anyhow::Result;
use async_trait::async_trait;
use log::debug;
use log::error;
#[cfg(unix)]
use pingora::server::ListenFds;
use pingora::server::ShutdownWatch;
use pingora::services::Service;
use tokio::sync::broadcast::Sender;
use tokio::time::interval;
use tokio::time::Duration;
use tokio::time::MissedTickBehavior;

use crate::balancer::status_update::StatusUpdate;
use crate::llamacpp::llamacpp_client::LlamacppClient;

pub struct MonitoringService {
    external_llamacpp_addr: SocketAddr,
    llamacpp_client: LlamacppClient,
    monitoring_interval: Duration,
    name: Option<String>,
    status_update_tx: Sender<Bytes>,
}

impl MonitoringService {
    pub fn new(
        external_llamacpp_addr: SocketAddr,
        llamacpp_client: LlamacppClient,
        monitoring_interval: Duration,
        name: Option<String>,
        status_update_tx: Sender<Bytes>,
    ) -> Result<Self> {
        Ok(MonitoringService {
            external_llamacpp_addr,
            llamacpp_client,
            monitoring_interval,
            name,
            status_update_tx,
        })
    }

    async fn fetch_status(&self) -> StatusUpdate {
        let slots_response = self.llamacpp_client.get_available_slots().await;
        let slots_processing = slots_response
            .slots
            .iter()
            .filter(|slot| slot.is_processing)
            .count();

        StatusUpdate {
            agent_name: self.name.to_owned(),
            error: slots_response.error,
            external_llamacpp_addr: self.external_llamacpp_addr,
            is_authorized: slots_response.is_authorized,
            is_connect_error: slots_response.is_connect_error,
            is_decode_error: slots_response.is_decode_error,
            is_deserialize_error: slots_response.is_deserialize_error,
            is_request_error: slots_response.is_request_error,
            is_slots_endpoint_enabled: slots_response.is_slot_endpoint_enabled,
            is_unexpected_response_status: slots_response.is_unexpected_response_status,
            slots_idle: slots_response.slots.len() - slots_processing,
            slots_processing,
        }
    }

    async fn report_status(&self, status: StatusUpdate) -> Result<usize> {
        let status = Bytes::from(serde_json::to_vec(&status)?);

        Ok(self.status_update_tx.send(status)?)
    }
}

#[async_trait]
impl Service for MonitoringService {
    async fn start_service(
        &mut self,
        #[cfg(unix)] _fds: Option<ListenFds>,
        mut shutdown: ShutdownWatch,
        _listeners_per_fd: usize,
    ) {
        let mut ticker = interval(self.monitoring_interval);

        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    debug!("Shutting down monitoring service");
                    return;
                },
                _ = ticker.tick() => {
                    let status = self.fetch_status().await;

                    if let Err(err) = self.report_status(status).await {
                        error!("Failed to report status: {err}");
                    }
                }
            }
        }
    }

    fn name(&self) -> &str {
        "monitoring"
    }

    fn threads(&self) -> Option<usize> {
        Some(1)
    }
}
