use std::collections::HashMap;

use anyhow::Result;
use tracing::info;

use crate::core::state::{AppState, RecyclerCache};
use crate::encoding::jeax_encoding::encode_vl64;
use crate::managers::{catalogue_manager, string_manager};

pub async fn init(state: &AppState) -> Result<()> {
    let enabled = string_manager::get_table_entry(state, "recycler_enable").await? == "1";
    if !enabled {
        state.recycler_cache.write().await.setup_string = "H".to_string();
        info!("recycler disabled");
        return Ok(());
    }

    info!("initializing recycler");

    let costs = state
        .db
        .run_read_column_i64("SELECT rclr_cost FROM system_recycler")
        .await?;
    let rewards = state
        .db
        .run_read_column_i64("SELECT rclr_reward FROM system_recycler")
        .await?;
    let session_length = string_manager::get_table_entry(state, "recycler_session_length")
        .await?
        .parse::<i64>()
        .unwrap_or(120);
    let session_expire_length =
        string_manager::get_table_entry(state, "recycler_session_expirelength")
            .await?
            .parse::<i64>()
            .unwrap_or(240);
    let item_min_ownership_length = string_manager::get_table_entry(state, "recycler_minownertime")
        .await?
        .parse::<i64>()
        .unwrap_or(0);

    let mut session_rewards = HashMap::new();
    let mut setup = format!(
        "I{}{}{}{}",
        encode_vl64(item_min_ownership_length as i32),
        encode_vl64(session_length as i32),
        encode_vl64(session_expire_length as i32),
        encode_vl64(costs.len() as i32)
    );

    for (index, cost) in costs.iter().enumerate() {
        let Some(reward_id) = rewards.get(index).copied() else {
            continue;
        };
        let template = catalogue_manager::get_template(state, reward_id).await;
        if template.sprite.is_empty() {
            continue;
        }

        session_rewards.insert(*cost, reward_id);
        setup.push_str(&format!(
            "{}H{}\u{2}H{}{}\u{2}",
            encode_vl64(*cost as i32),
            template.sprite,
            encode_vl64(template.length as i32),
            encode_vl64(template.width as i32)
        ));
    }

    *state.recycler_cache.write().await = RecyclerCache {
        enabled: true,
        session_length,
        session_expire_length,
        item_min_ownership_length,
        session_rewards,
        setup_string: setup,
    };

    info!("recycler enabled");

    Ok(())
}

pub async fn reward_exists(state: &AppState, item_count: i64) -> bool {
    state
        .recycler_cache
        .read()
        .await
        .session_rewards
        .contains_key(&item_count)
}

pub async fn setup_string(state: &AppState) -> String {
    state.recycler_cache.read().await.setup_string.clone()
}

pub async fn create_session(state: &AppState, user_id: i64, item_count: i64) -> Result<()> {
    let reward_template_id = match state
        .recycler_cache
        .read()
        .await
        .session_rewards
        .get(&item_count)
        .copied()
    {
        Some(id) => id,
        None => return Ok(()),
    };

    state
        .db
        .run_query(&format!(
            "INSERT INTO users_recycler(userid,session_started,session_reward) VALUES ('{}',NOW(),'{}')",
            user_id, reward_template_id
        ))
        .await
}

pub async fn drop_session(state: &AppState, user_id: i64, drop_items: bool) -> Result<()> {
    state
        .db
        .run_query(&format!(
            "DELETE FROM users_recycler WHERE userid = '{}' LIMIT 1",
            user_id
        ))
        .await?;

    let query = if drop_items {
        format!(
            "DELETE FROM furniture WHERE ownerid = '{}' AND roomid = '-2'",
            user_id
        )
    } else {
        format!(
            "UPDATE furniture SET roomid = '0' WHERE ownerid = '{}' AND roomid = '-2'",
            user_id
        )
    };
    state.db.run_query(&query).await
}

pub async fn reward_session(state: &AppState, user_id: i64) -> Result<()> {
    let reward_template_id = session_reward_id(state, user_id).await;
    if reward_template_id == 0 {
        return Ok(());
    }

    state
        .db
        .run_query(&format!(
            "INSERT INTO furniture(tid,ownerid) VALUES ('{}','{}')",
            reward_template_id, user_id
        ))
        .await?;
    catalogue_manager::handle_purchase(state, reward_template_id, user_id, 0, "", 0).await
}

pub async fn passed_minutes(state: &AppState, user_id: i64) -> i64 {
    state.db
        .run_read_unsafe_i64(&format!(
            "SELECT TIMESTAMPDIFF(MINUTE, session_started, NOW()) FROM users_recycler WHERE userid = '{}' LIMIT 1",
            user_id
        ))
        .await
}

pub async fn session_string(state: &AppState, user_id: i64) -> String {
    let cache = state.recycler_cache.read().await.clone();
    if !cache.enabled {
        return "H".to_string();
    }
    if !session_exists(state, user_id).await {
        return "H".to_string();
    }

    let minutes_passed = passed_minutes(state, user_id).await;
    if minutes_passed < cache.session_length {
        let sprite =
            catalogue_manager::get_template(state, session_reward_id(state, user_id).await)
                .await
                .sprite;
        return format!(
            "IH{}\u{2}{}",
            sprite,
            encode_vl64((cache.session_length - minutes_passed) as i32)
        );
    }
    if minutes_passed > cache.session_expire_length {
        return "K".to_string();
    }
    if minutes_passed > cache.session_length {
        let sprite =
            catalogue_manager::get_template(state, session_reward_id(state, user_id).await)
                .await
                .sprite;
        return format!("JH{}\u{2}", sprite);
    }

    "H".to_string()
}

pub async fn session_exists(state: &AppState, user_id: i64) -> bool {
    state
        .db
        .check_exists(&format!(
            "SELECT userid FROM users_recycler WHERE userid = '{}' LIMIT 1",
            user_id
        ))
        .await
}

pub async fn session_ready(state: &AppState, user_id: i64) -> bool {
    if !session_exists(state, user_id).await {
        return false;
    }

    let minutes_passed = passed_minutes(state, user_id).await;
    let cache = state.recycler_cache.read().await;
    minutes_passed > cache.session_length && minutes_passed < cache.session_expire_length
}

pub async fn item_min_ownership_length(state: &AppState) -> i64 {
    state.recycler_cache.read().await.item_min_ownership_length
}

async fn session_reward_id(state: &AppState, user_id: i64) -> i64 {
    state
        .db
        .run_read_unsafe_i64(&format!(
            "SELECT session_reward FROM users_recycler WHERE userid = '{}' LIMIT 1",
            user_id
        ))
        .await
}
