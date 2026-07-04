use anyhow::Result;
use tracing::info;

use crate::core::state::{AppState, GameRank};

pub async fn init(state: &AppState) -> Result<()> {
    info!("initializing user rank fuserights");
    let mut user_ranks = std::collections::HashMap::new();
    for rank_id in 1..=7u8 {
        let rights = state
            .db
            .run_read_column_string(&format!(
                "SELECT fuseright FROM system_fuserights WHERE minrank <= {}",
                rank_id
            ))
            .await?;
        user_ranks.insert(rank_id, rights);
    }
    info!("fuserights for 7 ranks loaded");

    info!("initializing game ranks");
    let game_ranks_bb = load_game_ranks(state, "bb").await?;
    let game_ranks_ss = load_game_ranks(state, "ss").await?;

    let mut cache = state.rank_cache.write().await;
    cache.user_ranks = user_ranks;
    cache.game_ranks_bb = game_ranks_bb;
    cache.game_ranks_ss = game_ranks_ss;
    info!(
        battle_ball_ranks = cache.game_ranks_bb.len(),
        snow_storm_ranks = cache.game_ranks_ss.len(),
        "game ranks loaded"
    );
    Ok(())
}

pub async fn fuse_rights(state: &AppState, rank: u8) -> Result<String> {
    let cache = state.rank_cache.read().await;
    let rows = cache.user_ranks.get(&rank).cloned().unwrap_or_default();

    let mut payload = String::new();
    for right in rows {
        payload.push_str(&right);
        payload.push('\u{2}');
    }

    Ok(payload)
}

pub async fn contains_right(state: &AppState, rank: u8, right: &str) -> bool {
    let cache = state.rank_cache.read().await;
    cache
        .user_ranks
        .get(&rank)
        .map(|rights| rights.iter().any(|entry| entry == right))
        .unwrap_or(false)
}

pub async fn get_game_rank_title(state: &AppState, is_battle_ball: bool, score: i64) -> String {
    let cache = state.rank_cache.read().await;
    let ranks = if is_battle_ball {
        &cache.game_ranks_bb
    } else {
        &cache.game_ranks_ss
    };

    for rank in ranks {
        if score >= rank.min_points && (rank.max_points == 0 || score <= rank.max_points) {
            return rank.title.clone();
        }
    }

    "holo.cast.gamerank.null".to_string()
}

pub async fn get_game_rank(state: &AppState, is_battle_ball: bool, title: &str) -> GameRank {
    let cache = state.rank_cache.read().await;
    let ranks = if is_battle_ball {
        &cache.game_ranks_bb
    } else {
        &cache.game_ranks_ss
    };

    ranks
        .iter()
        .find(|rank| rank.title == title)
        .cloned()
        .unwrap_or(GameRank {
            title: "holo.cast.gamerank.null".to_string(),
            min_points: 0,
            max_points: 0,
        })
}

async fn load_game_ranks(state: &AppState, game_type: &str) -> Result<Vec<GameRank>> {
    let titles = state
        .db
        .run_read_column_string(&format!(
            "SELECT title FROM games_ranks WHERE type = '{}' ORDER BY id ASC",
            game_type
        ))
        .await?;
    let mins = state
        .db
        .run_read_column_i64(&format!(
            "SELECT minpoints FROM games_ranks WHERE type = '{}' ORDER BY id ASC",
            game_type
        ))
        .await?;
    let maxs = state
        .db
        .run_read_column_i64(&format!(
            "SELECT maxpoints FROM games_ranks WHERE type = '{}' ORDER BY id ASC",
            game_type
        ))
        .await?;

    let mut ranks = Vec::with_capacity(titles.len());
    for (index, title) in titles.into_iter().enumerate() {
        ranks.push(GameRank {
            title,
            min_points: *mins.get(index).unwrap_or(&0),
            max_points: *maxs.get(index).unwrap_or(&0),
        });
    }

    Ok(ranks)
}
