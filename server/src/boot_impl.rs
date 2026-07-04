use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use tracing::{error, info};

use crate::core::config::AppConfig;
use crate::core::state::AppState;
use crate::db::db::Database;
use crate::managers::{
    catalogue_manager, event_manager, rank_manager, recycler_manager, string_manager,
};
use crate::socket_servers::{game_socket_server, mus_socket_server};

pub async fn run() -> Result<()> {
    let workdir = std::env::current_dir()?;
    let config = AppConfig::load(&workdir)?;
    let started_at = Instant::now();

    info!("starting Holograph Rust");
    info!(config_path = %AppConfig::default_config_path(&workdir).display(), "loaded configuration");

    let db = Database::connect(&config.database).await?;
    reset_dynamics(&db).await?;

    let stats = db.diagnostics().await?;
    info!(
        users = stats.users,
        rooms = stats.rooms,
        furniture = stats.furniture,
        "database statistics loaded"
    );

    let state = Arc::new(AppState::new(config, db));
    initialize_runtime_config(&state).await?;
    rank_manager::init(&state).await?;
    catalogue_manager::init(&state).await?;
    recycler_manager::init(&state).await?;
    event_manager::init(state.clone());

    info!(
        startup_ms = started_at.elapsed().as_millis(),
        "startup complete"
    );

    let mus_state = state.clone();
    tokio::spawn(async move {
        if let Err(err) = mus_socket_server::run(mus_state).await {
            error!(error = %err, "mus socket server stopped");
        }
    });

    game_socket_server::run(state).await
}

async fn reset_dynamics(db: &Database) -> Result<()> {
    db.run_query(
        "UPDATE system SET onlinecount = '0', onlinecount_peak = '0', connections_accepted = '0', activerooms = '0'",
    )
    .await?;
    info!("client connection statistics reset");
    db.run_query("UPDATE users SET ticket_sso = NULL").await?;
    info!("login tickets nulled");
    db.run_query("UPDATE rooms SET visitors_now = '0'").await?;
    info!("room inside counts reset");
    Ok(())
}

async fn initialize_runtime_config(state: &AppState) -> Result<()> {
    let game_port = state
        .db
        .run_read_unsafe_i64(
            "SELECT sval FROM system_config WHERE skey = 'server_game_port' LIMIT 1",
        )
        .await as u16;
    let game_max_connections = state
        .db
        .run_read_unsafe_i64(
            "SELECT sval FROM system_config WHERE skey = 'server_game_maxconnections' LIMIT 1",
        )
        .await as usize;
    let mus_port = state
        .db
        .run_read_unsafe_i64(
            "SELECT sval FROM system_config WHERE skey = 'server_mus_port' LIMIT 1",
        )
        .await as u16;
    let mus_host = state
        .db
        .run_read_unsafe_string(
            "SELECT sval FROM system_config WHERE skey = 'server_mus_host' LIMIT 1",
        )
        .await;
    let rooms_loadadvertisement_img = state
        .db
        .run_read_unsafe_string(
            "SELECT sval FROM system_config WHERE skey = 'rooms_loadadvertisement_img' LIMIT 1",
        )
        .await;
    let mut rooms_loadadvertisement_uri = String::new();
    if !rooms_loadadvertisement_img.is_empty() {
        rooms_loadadvertisement_uri = state
            .db
            .run_read_unsafe_string(
                "SELECT sval FROM system_config WHERE skey = 'rooms_loadadvertisement_uri' LIMIT 1",
            )
            .await;
        if string_manager::get_string_part(&rooms_loadadvertisement_uri, 0, 7) != "http://" {
            rooms_loadadvertisement_uri = "http://wwww.holographemulator.com".to_string();
        }
    }
    let trading_enabled = state
        .db
        .run_read_unsafe_string(
            "SELECT sval FROM system_config WHERE skey = 'trading_enable' LIMIT 1",
        )
        .await;
    let chatanims_enabled = state
        .db
        .run_read_unsafe_string(
            "SELECT sval FROM system_config WHERE skey = 'chatanims_enable' LIMIT 1",
        )
        .await;
    let game_countdown_seconds = state
        .db
        .run_read_unsafe_i64(
            "SELECT sval FROM system_config WHERE skey = 'game_countdown_seconds' LIMIT 1",
        )
        .await;
    let game_score_window_restart_game_seconds = state
        .db
        .run_read_unsafe_i64(
            "SELECT sval FROM system_config WHERE skey = 'game_scorewindow_restartgame_seconds' LIMIT 1",
        )
        .await;
    let game_battle_ball_game_length_seconds = state
        .db
        .run_read_unsafe_i64(
            "SELECT sval FROM system_config WHERE skey = 'game_bb_gamelength_seconds' LIMIT 1",
        )
        .await;
    let lang = state
        .db
        .run_read_unsafe_string("SELECT sval FROM system_config WHERE skey = 'lang' LIMIT 1")
        .await;

    {
        let mut runtime = state.runtime_config.write().await;
        if game_port > 0 {
            runtime.game_port = game_port;
        }
        if game_max_connections > 0 {
            runtime.game_max_connections = game_max_connections;
        }
        if mus_port > 0 {
            runtime.mus_port = Some(mus_port);
        }
        if !mus_host.is_empty() {
            runtime.mus_host = Some(mus_host);
        }
        if !lang.is_empty() {
            runtime.lang = lang;
        }
        runtime.rooms_loadadvertisement_img = rooms_loadadvertisement_img;
        runtime.rooms_loadadvertisement_uri = rooms_loadadvertisement_uri;
        if game_countdown_seconds > 0 {
            runtime.game_countdown_seconds = game_countdown_seconds as i32;
        }
        if game_score_window_restart_game_seconds > 0 {
            runtime.game_score_window_restart_game_seconds =
                game_score_window_restart_game_seconds as i32;
        }
        if game_battle_ball_game_length_seconds > 0 {
            runtime.game_battle_ball_game_length_seconds = game_battle_ball_game_length_seconds;
        }
        runtime.enable_trading = trading_enabled != "0";
        runtime.enable_chat_anims = chatanims_enabled == "1";
    }

    let lang = state.runtime_config.read().await.lang.clone();
    string_manager::init(state, &lang).await?;
    if state.runtime_config.read().await.enable_chat_anims {
        info!("chat animations enabled");
    } else {
        info!("chat animations disabled");
    }
    if state.runtime_config.read().await.enable_trading {
        info!("trading enabled");
    } else {
        info!("trading disabled");
    }
    info!(
        countdown_seconds = state.runtime_config.read().await.game_countdown_seconds,
        score_window_restart_seconds = state
            .runtime_config
            .read()
            .await
            .game_score_window_restart_game_seconds,
        battle_ball_game_length_seconds = state
            .runtime_config
            .read()
            .await
            .game_battle_ball_game_length_seconds,
        "game timing configuration loaded"
    );
    let welcome_enabled = string_manager::get_table_entry(state, "welcomemessage_enable").await?
        == "1"
        && string_manager::get_string(state, "welcomemessage_text").await? != "welcomemessage_text";
    state.runtime_config.write().await.enable_welcome_message = welcome_enabled;
    if welcome_enabled {
        info!("welcome message enabled");
    } else if string_manager::get_table_entry(state, "welcomemessage_enable").await? == "1" {
        info!("welcome message was preferred as enabled, but has been left blank. ignored");
    } else {
        info!("welcome message disabled");
    }
    string_manager::init_filter(state).await?;
    Ok(())
}
