use std::sync::Arc;

use anyhow::Result;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

use crate::core::state::AppState;
use crate::managers::{catalogue_manager, user_manager};
use crate::session_impl::{
    mus_kick_user_from_room, mus_refresh_appearance, mus_refresh_badges, mus_refresh_club,
    mus_refresh_hand, mus_refresh_valueables,
};

pub async fn run(state: Arc<AppState>) -> Result<()> {
    let runtime = state.runtime_config.read().await;
    let Some(port) = runtime.mus_port else {
        return Ok(());
    };
    let allowed_host = runtime
        .mus_host
        .clone()
        .unwrap_or_else(|| "127.0.0.1".to_string());
    drop(runtime);

    let bind_host = "0.0.0.0";
    let listener = TcpListener::bind((bind_host, port)).await?;
    info!(bind_host, allowed_host = %allowed_host, port, "mus socket server listening");

    loop {
        let (mut socket, remote_addr) = listener.accept().await?;
        let state = state.clone();
        let allowed_host = allowed_host.clone();

        tokio::spawn(async move {
            let remote_ip = remote_addr.ip().to_string();
            if remote_ip != allowed_host {
                warn!(remote_ip = %remote_ip, allowed_host = %allowed_host, "rejected mus connection");
                return;
            }

            let mut buffer = [0_u8; 10001];
            let Ok(bytes_received) = socket.read(&mut buffer).await else {
                return;
            };
            if bytes_received < 4 {
                return;
            }

            let data = String::from_utf8_lossy(&buffer[..bytes_received]).to_string();
            debug!(payload = %data, "received mus packet");

            let header = &data[..4];
            let parts = data[4..]
                .split('\u{2}')
                .map(str::to_string)
                .collect::<Vec<_>>();

            if let Err(err) = handle_command(&state, header, &parts).await {
                warn!(header = %header, error = %err, "failed to process mus packet");
            }
        });
    }
}

async fn handle_command(state: &Arc<AppState>, header: &str, parts: &[String]) -> Result<()> {
    match header {
        "HKTM" => {
            let user_id = parse_user_id(parts)?;
            let message = parts.get(1).cloned().unwrap_or_default();
            if let Some(user) = user_manager::get_user(state, user_id).await {
                let _ = user.sender.send(format!("BK{}", message));
            }
        }
        "HKMW" => {
            let user_id = parse_user_id(parts)?;
            let message = parts.get(1).cloned().unwrap_or_default();
            if let Some(user) = user_manager::get_user(state, user_id).await {
                let _ = user.sender.send(format!("B!{}\u{2}", message));
            }
        }
        "HKUK" => {
            let user_id = parse_user_id(parts)?;
            let message = parts.get(1).cloned().unwrap_or_default();
            mus_kick_user_from_room(state, user_id, &message).await?;
        }
        "HKAR" => {
            let rank = parts
                .first()
                .and_then(|value| value.parse::<u8>().ok())
                .unwrap_or(0);
            let include_higher = parts.get(1).map(|value| value == "1").unwrap_or(false);
            let message = parts.get(2).cloned().unwrap_or_default();
            user_manager::send_to_rank(state, rank, include_higher, &format!("BK{}", message))
                .await;
        }
        "HKSB" => {
            let user_id = parse_user_id(parts)?;
            let message = parts.get(1).cloned().unwrap_or_default();
            if let Some(user) = user_manager::get_user(state, user_id).await {
                let _ = user.sender.send(format!("@c{}", message));
            }
            user_manager::disconnect_user_after_delay(state, user_id, 1_000);
        }
        "HKRC" => {
            catalogue_manager::init(state).await?;
        }
        "UPRA" => {
            mus_refresh_appearance(state, parse_user_id(parts)?).await?;
        }
        "UPRC" => {
            mus_refresh_valueables(state, parse_user_id(parts)?, true, false).await?;
        }
        "UPRT" => {
            mus_refresh_valueables(state, parse_user_id(parts)?, false, true).await?;
        }
        "UPRS" => {
            let user_id = parse_user_id(parts)?;
            mus_refresh_club(state, user_id).await?;
            mus_refresh_badges(state, user_id).await?;
        }
        "UPRH" => {
            mus_refresh_hand(state, parse_user_id(parts)?).await;
        }
        _ => {
            debug!(header = %header, "ignored mus packet");
        }
    }

    Ok(())
}

fn parse_user_id(parts: &[String]) -> Result<i64> {
    Ok(parts
        .first()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or_default())
}
