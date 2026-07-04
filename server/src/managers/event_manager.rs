use std::sync::Arc;

use anyhow::Result;
use chrono::Local;
use tokio::time::{Duration, sleep};
use tracing::info;

use crate::core::state::AppState;
use crate::db::db::Database;
use crate::encoding::jeax_encoding::encode_vl64;
use crate::managers::user_manager;

pub fn init(state: Arc<AppState>) {
    tokio::spawn(async move {
        let interval_seconds = state
            .db
            .run_read_unsafe_i64(
                "SELECT sval FROM system_config WHERE skey = 'events_deadevents_removeinterval' LIMIT 1",
            )
            .await
            .max(1);
        info!(
            interval_seconds,
            "event manager dead-event collector started"
        );

        loop {
            drop_dead_events(&state).await;
            sleep(Duration::from_secs(interval_seconds as u64)).await;
        }
    });
}

pub async fn category_amount(state: &AppState) -> i64 {
    state
        .db
        .run_read_unsafe_i64(
            "SELECT sval FROM system_config WHERE skey = 'events_categorycount' LIMIT 1",
        )
        .await
}

pub async fn category_ok(state: &AppState, category_id: i64) -> bool {
    let count = category_amount(state).await;
    category_id > 0 && category_id <= count
}

pub async fn create_event(
    state: &AppState,
    category_id: i64,
    user_id: i64,
    room_id: i64,
    name: &str,
    description: &str,
) -> Result<()> {
    if !category_ok(state, category_id).await {
        return Ok(());
    }
    if state
        .db
        .check_exists(&format!(
            "SELECT roomid FROM events WHERE roomid = '{}' LIMIT 1",
            room_id
        ))
        .await
    {
        return Ok(());
    }

    state
        .db
        .run_query(&format!(
            "INSERT INTO events (name,description,userid,roomid,category,date) VALUES ('{}','{}','{}','{}','{}','{}')",
            Database::stripslash(name),
            Database::stripslash(description),
            user_id,
            room_id,
            category_id,
            Local::now().format("%H:%M")
        ))
        .await
}

pub async fn remove_event(state: &AppState, room_id: i64) -> Result<()> {
    state
        .db
        .run_query(&format!("DELETE FROM events WHERE roomid = '{}'", room_id))
        .await
}

pub async fn edit_event(
    state: &AppState,
    category_id: i64,
    room_id: i64,
    name: &str,
    description: &str,
) -> Result<()> {
    if !category_ok(state, category_id).await {
        return Ok(());
    }

    state
        .db
        .run_query(&format!(
            "UPDATE events SET name = '{}',description = '{}',category = '{}',date = '{}' WHERE roomid = '{}' LIMIT 1",
            Database::stripslash(name),
            Database::stripslash(description),
            category_id,
            Local::now().format("%H:%M"),
            room_id
        ))
        .await
}

pub async fn get_events(state: &AppState, category_id: i64) -> String {
    if !category_ok(state, category_id).await {
        return "H".to_string();
    }

    let rows = match state
        .db
        .run_read_table(&format!(
            "SELECT roomid,userid,name,description,date FROM events WHERE category = '{}' ORDER BY roomid ASC",
            category_id
        ))
        .await
    {
        Ok(rows) => rows,
        Err(_) => return "H".to_string(),
    };

    let mut count = 0i32;
    let mut body = String::new();
    for row in rows {
        if row.len() < 5 {
            continue;
        }

        let room_id = row[0].parse::<i64>().unwrap_or(0);
        let user_id = row[1].parse::<i64>().unwrap_or(0);
        if !user_manager::contains_user_by_id(state, user_id).await {
            continue;
        }

        let username = match user_manager::get_user(state, user_id).await {
            Some(user) => user.username,
            None => continue,
        };

        body.push_str(&format!(
            "{}\u{2}{}\u{2}{}\u{2}{}\u{2}{}\u{2}",
            room_id, username, row[2], row[3], row[4]
        ));
        count += 1;
    }

    format!("{}{}", encode_vl64(count), body)
}

pub async fn get_event(state: &AppState, room_id: i64) -> String {
    let row = match state
        .db
        .run_read_row(&format!(
            "SELECT userid,roomid,category,name,description,date FROM events WHERE roomid = '{}' LIMIT 1",
            room_id
        ))
        .await
    {
        Ok(row) => row,
        Err(_) => return "-1".to_string(),
    };
    if row.len() < 6 {
        return "-1".to_string();
    }

    let user_id = row[0].parse::<i64>().unwrap_or(0);
    if !user_manager::contains_user_by_id(state, user_id).await {
        return "-1".to_string();
    }

    let username = match user_manager::get_user(state, user_id).await {
        Some(user) => user.username,
        None => return "-1".to_string(),
    };

    format!(
        "{}\u{2}{}\u{2}{}\u{2}{}{}\u{2}{}\u{2}{}\u{2}",
        user_id,
        username,
        row[1],
        encode_vl64(row[2].parse::<i32>().unwrap_or(0)),
        row[3],
        row[4],
        row[5]
    )
}

pub async fn user_hosts_any_event(state: &AppState, user_id: i64) -> bool {
    state
        .db
        .check_exists(&format!(
            "SELECT userid FROM events WHERE userid = '{}' LIMIT 1",
            user_id
        ))
        .await
}

pub async fn user_hosts_event_in_room(state: &AppState, user_id: i64, room_id: i64) -> bool {
    state
        .db
        .check_exists(&format!(
            "SELECT userid FROM events WHERE userid = '{}' AND roomid = '{}' LIMIT 1",
            user_id, room_id
        ))
        .await
}

async fn drop_dead_events(state: &AppState) {
    let rows = match state
        .db
        .run_read_table("SELECT roomid,userid FROM events ORDER BY roomid ASC")
        .await
    {
        Ok(rows) => rows,
        Err(_) => return,
    };

    for row in rows {
        if row.len() < 2 {
            continue;
        }
        let room_id = row[0].parse::<i64>().unwrap_or(0);
        let user_id = row[1].parse::<i64>().unwrap_or(0);

        let remove = match user_manager::get_user(state, user_id).await {
            Some(user) => user.room_id != room_id,
            None => true,
        };
        if remove {
            let _ = state
                .db
                .run_query(&format!(
                    "DELETE FROM events WHERE roomid = '{}' LIMIT 1",
                    room_id
                ))
                .await;
        }
    }
}
