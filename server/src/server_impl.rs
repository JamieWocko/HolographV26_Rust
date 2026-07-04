use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Result;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use crate::core::state::AppState;
use crate::managers::user_manager;
use crate::virtuals::users::virtual_user::run_game_session;

pub async fn run(state: Arc<AppState>) -> Result<()> {
    let game_port = state.runtime_config.read().await.game_port;
    let bind_addr = format!("0.0.0.0:{}", game_port);
    let listener = TcpListener::bind(&bind_addr).await?;
    info!(bind_addr, "game socket listener started");

    let monitor_state = state.clone();
    tokio::spawn(async move {
        if let Err(err) = monitor_loop(monitor_state).await {
            error!(error = %err, "server monitor stopped");
        }
    });

    user_manager::spawn_ping_checker(state.clone());

    loop {
        let (socket, remote_addr) = listener.accept().await?;
        let Some(connection_id) = state.allocate_connection_id().await else {
            warn!("max connections reached, refusing client");
            continue;
        };

        let session_state = state.clone();
        let remote_ip = remote_addr.ip().to_string();
        info!(connection_id, remote_ip, "accepted connection");

        tokio::spawn(async move {
            if let Err(err) =
                run_game_session(session_state, connection_id, socket, remote_ip).await
            {
                warn!(connection_id, error = %err, "session ended with error");
            }
        });
    }
}

async fn monitor_loop(state: Arc<AppState>) -> Result<()> {
    loop {
        let online_count = state.online_count().await as i64;
        let peak_online = state.peak_online_users.load(Ordering::Relaxed) as i64;
        let active_rooms = state.active_rooms.load(Ordering::Relaxed);
        let peak_rooms = state.peak_rooms.load(Ordering::Relaxed);
        let accepted = state.accepted_connections.load(Ordering::Relaxed) as i64;
        let memory_kb = current_memory_usage_kb();

        state
            .db
            .run_query(&format!(
                "UPDATE system SET onlinecount = '{}', onlinecount_peak = '{}', \
                 activerooms = '{}', activerooms_peak = '{}', connections_accepted = '{}'",
                online_count, peak_online, active_rooms, peak_rooms, accepted
            ))
            .await?;

        update_process_title(online_count, active_rooms, memory_kb);

        tokio::time::sleep(Duration::from_secs(6)).await;
    }
}

fn update_process_title(online_count: i64, active_rooms: i64, memory_kb: u64) {
    let title = format!(
        "Holograph Emulator 26 | online users: {} | loaded rooms {} | RAM usage: {}KB",
        online_count, active_rooms, memory_kb
    );

    #[cfg(windows)]
    {
        let _ = std::process::Command::new("cmd")
            .args(["/C", "title", &title])
            .status();
    }

    #[cfg(not(windows))]
    {
        let _ = title;
    }
}

fn current_memory_usage_kb() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if let Some(value) = line.strip_prefix("VmRSS:") {
                    return value
                        .split_whitespace()
                        .next()
                        .and_then(|entry| entry.parse::<u64>().ok())
                        .unwrap_or(0);
                }
            }
        }
    }

    0
}
