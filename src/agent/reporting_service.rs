use std::net::SocketAddr;

use actix_web::web::Bytes;
use anyhow::Result;
use async_trait::async_trait;
use log::debug;
use log::error;
use log::info;
#[cfg(unix)]
use pingora::server::ListenFds;
use pingora::server::ShutdownWatch;
use pingora::services::Service;
use tokio::sync::broadcast::Sender;
use tokio::time::interval;
use tokio::time::Duration;
use tokio::time::MissedTickBehavior;
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

pub struct ReportingService {
    stats_endpoint_url: String,
    status_update_tx: Sender<Bytes>,
}

impl ReportingService {
    pub fn new(management_addr: SocketAddr, status_update_tx: Sender<Bytes>) -> Result<Self> {
        let agent_id = Uuid::new_v4();

        Ok(ReportingService {
            stats_endpoint_url: format!(
                "http://{management_addr}/api/v1/agent_status_update/{agent_id}"
            ),
            status_update_tx,
        })
    }

    async fn keep_connection_alive(&self) -> Result<()> {
        let mut client = reqwest::Client::new();
        let status_update_rx = self.status_update_tx.subscribe();
        let stream = BroadcastStream::new(status_update_rx);
        let reqwest_body = reqwest::Body::wrap_stream(stream);

        info!("Establishing connection with management server");

        let mut attempt = 0;
        loop {
            match client
                .post(self.stats_endpoint_url.to_owned())
                .body(reqwest_body.clone())
                .send()
                .await
            {
                Ok(response) => {
                    if response.status().is_success() {
                        info!("Successfully sent status update");
                        return Ok(());
                    } else {
                        error!("Received non-success status: {}", response.status());
                    }
                }
                Err(err) => {
                    error!("Failed to send status update: {}", err);
                }
            }

            // Exponential backoff
            attempt += 1;
            let delay = Duration::from_secs(2_u64.pow(attempt.min(5)));
            info!("Retrying in {} seconds...", delay.as_secs());
            tokio::time::sleep(delay).await;
        }
    }
}

#[async_trait]
impl Service for ReportingService {
    async fn start_service(
        &mut self,
        #[cfg(unix)] _fds: Option<ListenFds>,
        mut shutdown: ShutdownWatch,
        _listeners_per_fd: usize,
    ) {
        let mut ticker = interval(Duration::from_secs(1));

        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    debug!("Shutting down reporting service");
                    return;
                },
                _ = ticker.tick() => {
                    if let Err(err) = self.keep_connection_alive().await {
                        error!("Failed to keep the connection alive: {err}");
                    }
                }
            }
        }
    }

    fn name(&self) -> &str {
        "reporting"
    }

    fn threads(&self) -> Option<usize> {
        None
    }
}
