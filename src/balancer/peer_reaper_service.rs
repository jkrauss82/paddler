use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use log::debug;
use log::error;
use log::info;
#[cfg(unix)]
use pingora::server::ListenFds;
use pingora::server::ShutdownWatch;
use pingora::services::Service;
use tokio::time::interval;
use tokio::time::MissedTickBehavior;

use crate::balancer::upstream_peer_pool::UpstreamPeerPool;

/// How long a peer may go without a status update before it is reaped.
///
/// Three times the default agent monitoring interval (10s), so a peer is only
/// considered dead after missing several heartbeats.
const STALE_PEER_MAX_AGE: Duration = Duration::from_secs(30);

/// How often to scan the pool for stale peers.
const REAP_INTERVAL: Duration = Duration::from_secs(5);

pub struct PeerReaperService {
    upstream_peer_pool: Arc<UpstreamPeerPool>,
}

impl PeerReaperService {
    pub fn new(upstream_peer_pool: Arc<UpstreamPeerPool>) -> Self {
        PeerReaperService {
            upstream_peer_pool,
        }
    }
}

#[async_trait]
impl Service for PeerReaperService {
    async fn start_service(
        &mut self,
        #[cfg(unix)] _fds: Option<ListenFds>,
        mut shutdown: ShutdownWatch,
        _listeners_per_fd: usize,
    ) {
        let mut ticker = interval(REAP_INTERVAL);

        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    debug!("Shutting down peer reaper service");
                    return;
                },
                _ = ticker.tick() => {
                    match self.upstream_peer_pool.reap_stale_peers(STALE_PEER_MAX_AGE) {
                        Ok(removed) if !removed.is_empty() => {
                            info!("Reaped stale peers: {removed:?}");
                        },
                        Ok(_) => {},
                        Err(err) => {
                            error!("Failed to reap stale peers: {err}");
                        }
                    }
                }
            }
        }
    }

    fn name(&self) -> &str {
        "peer_reaper"
    }

    fn threads(&self) -> Option<usize> {
        Some(1)
    }
}
