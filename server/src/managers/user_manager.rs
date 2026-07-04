use std::sync::Arc;

use chrono::{Duration, Local};
use tokio::time::{Duration as TokioDuration, sleep};
use tracing::info;

use crate::core::state::{AppState, OnlineUser};
use crate::db::db::Database;

pub async fn add_user(state: &AppState, user_id: i64, user: OnlineUser) {
    if let Some(existing) = get_user(state, user_id).await {
        let _ = existing.disconnect.send(true);
    }
    let username = user.username.clone();
    state.add_online_user(user_id, user).await;
    info!(user_id, username = %username, "user added to user manager");
}

pub async fn remove_user(state: &AppState, user_id: i64) {
    let username = get_user(state, user_id).await.map(|user| user.username);
    state.remove_online_user(user_id).await;
    if let Some(username) = username {
        info!(user_id, username = %username, "user removed from user manager");
    }
}

pub async fn remove_user_if_connection(state: &AppState, user_id: i64, connection_id: usize) {
    let mut users = state.online_users.write().await;
    if users
        .get(&user_id)
        .map(|user| user.connection_id == connection_id)
        .unwrap_or(false)
    {
        if let Some(user) = users.remove(&user_id) {
            info!(
                user_id,
                username = %user.username,
                connection_id,
                "user removed from user manager"
            );
        }
    }
}

pub async fn contains_user_by_id(state: &AppState, user_id: i64) -> bool {
    state.online_users.read().await.contains_key(&user_id)
}

pub async fn get_user(state: &AppState, user_id: i64) -> Option<OnlineUser> {
    state.online_users.read().await.get(&user_id).cloned()
}

pub async fn send_to_all(state: &AppState, payload: &str) {
    let users = state.online_users.read().await;
    for user in users.values() {
        let _ = user.sender.send(payload.to_string());
    }
}

pub async fn disconnect_user(state: &AppState, user_id: i64) {
    if let Some(user) = get_user(state, user_id).await {
        let _ = user.disconnect.send(true);
    }
}

pub fn disconnect_user_after_delay(state: &AppState, user_id: i64, delay_ms: u64) {
    let state = state.clone();
    tokio::spawn(async move {
        sleep(TokioDuration::from_millis(delay_ms)).await;
        disconnect_user(&state, user_id).await;
    });
}

pub fn spawn_ping_checker(state: Arc<AppState>) {
    tokio::spawn(async move {
        loop {
            sleep(TokioDuration::from_secs(60)).await;

            let users = state
                .online_users
                .read()
                .await
                .values()
                .cloned()
                .collect::<Vec<_>>();
            for user in users {
                if user
                    .ping_ok
                    .swap(false, std::sync::atomic::Ordering::Relaxed)
                {
                    let _ = user.sender.send("@r".to_string());
                } else {
                    info!(user_id = user.user_id, username = %user.username, "user timed out");
                    let _ = user.disconnect.send(true);
                }
            }
        }
    });
}

pub async fn send_to_rank(state: &AppState, rank: u8, include_higher: bool, payload: &str) {
    let users = state.online_users.read().await;
    for user in users.values() {
        if user.rank < rank || (!include_higher && user.rank > rank) {
            continue;
        }

        let _ = user.sender.send(payload.to_string());
    }
}

pub async fn get_user_id(state: &AppState, username: &str) -> i64 {
    let query = format!(
        "SELECT id FROM users WHERE name = '{}' LIMIT 1",
        Database::stripslash(username)
    );
    state.db.run_read_unsafe_i64(&query).await
}

pub async fn get_user_name(state: &AppState, user_id: i64) -> String {
    let query = format!("SELECT name FROM users WHERE id = '{}' LIMIT 1", user_id);
    state.db.run_read_unsafe_string(&query).await
}

pub async fn add_chat_message(state: &AppState, username: &str, room_id: i64, message: &str) {
    let _ = state
        .db
        .run_query(&format!(
            "INSERT INTO system_chatlog (username,roomid,mtime,message) VALUES ('{}','{}',CURRENT_TIMESTAMP,'{}')",
            Database::stripslash(username),
            room_id,
            Database::stripslash(message)
        ))
        .await;
}

pub async fn get_user_friend_ids(state: &AppState, user_id: i64) -> Vec<i64> {
    let mut friend_ids = state
        .db
        .run_read_column_i64(&format!(
            "SELECT friendid FROM messenger_friendships WHERE userid = '{}' ORDER BY friendid ASC",
            user_id
        ))
        .await
        .unwrap_or_default();
    let reverse_ids = state
        .db
        .run_read_column_i64(&format!(
            "SELECT userid FROM messenger_friendships WHERE friendid = '{}' ORDER BY userid ASC",
            user_id
        ))
        .await
        .unwrap_or_default();

    for friend_id in reverse_ids {
        if !friend_ids.contains(&friend_id) {
            friend_ids.push(friend_id);
        }
    }

    friend_ids
}

pub async fn get_ban_reason_for_user(state: &AppState, user_id: i64) -> String {
    let exists = state
        .db
        .check_exists(&format!(
            "SELECT userid FROM users_bans WHERE userid = '{}' LIMIT 1",
            user_id
        ))
        .await;
    if !exists {
        return String::new();
    }

    let ban_expires = state
        .db
        .run_read_unsafe_string(&format!(
            "SELECT date_expire FROM users_bans WHERE userid = '{}' LIMIT 1",
            user_id
        ))
        .await;
    if is_pending_ban(&ban_expires) {
        return state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT descr FROM users_bans WHERE userid = '{}' LIMIT 1",
                user_id
            ))
            .await;
    }

    let _ = state
        .db
        .run_query(&format!(
            "DELETE FROM users_bans WHERE userid = '{}' LIMIT 1",
            user_id
        ))
        .await;
    String::new()
}

pub async fn get_ban_reason_for_ip(state: &AppState, ip: &str) -> String {
    let escaped_ip = Database::stripslash(ip);
    let exists = state
        .db
        .check_exists(&format!(
            "SELECT ipaddress FROM users_bans WHERE ipaddress = '{}' LIMIT 1",
            escaped_ip
        ))
        .await;
    if !exists {
        return String::new();
    }

    let ban_expires = state
        .db
        .run_read_unsafe_string(&format!(
            "SELECT date_expire FROM users_bans WHERE ipaddress = '{}' LIMIT 1",
            escaped_ip
        ))
        .await;
    if is_pending_ban(&ban_expires) {
        return state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT descr FROM users_bans WHERE ipaddress = '{}' LIMIT 1",
                escaped_ip
            ))
            .await;
    }

    let _ = state
        .db
        .run_query(&format!(
            "DELETE FROM users_bans WHERE ipaddress = '{}' LIMIT 1",
            escaped_ip
        ))
        .await;
    String::new()
}

pub async fn set_ban_user(state: &AppState, user_id: i64, hours: i64, reason: &str) {
    let expires = Local::now() + Duration::hours(hours.max(0));
    let _ = state
        .db
        .run_query(&format!(
            "DELETE FROM users_bans WHERE userid = '{}' LIMIT 1",
            user_id
        ))
        .await;
    let _ = state
        .db
        .run_query(&format!(
            "INSERT INTO users_bans (userid,date_expire,descr) VALUES ('{}','{}','{}')",
            user_id,
            expires.format("%Y-%m-%d %H:%M:%S"),
            Database::stripslash(reason)
        ))
        .await;

    if let Some(user) = get_user(state, user_id).await {
        let _ = user.sender.send(format!("@c{}", reason));
    }
    disconnect_user(state, user_id).await;
}

pub async fn set_ban_ip(state: &AppState, ip: &str, hours: i64, reason: &str) {
    let expires = Local::now() + Duration::hours(hours.max(0));
    let escaped_ip = Database::stripslash(ip);
    let _ = state
        .db
        .run_query(&format!(
            "DELETE FROM users_bans WHERE ipaddress = '{}'",
            escaped_ip
        ))
        .await;
    let _ = state
        .db
        .run_query(&format!(
            "INSERT INTO users_bans (ipaddress,date_expire,descr) VALUES ('{}','{}','{}')",
            escaped_ip,
            expires.format("%Y-%m-%d %H:%M:%S"),
            Database::stripslash(reason)
        ))
        .await;

    let affected_ids = state
        .db
        .run_read_column_i64(&format!(
            "SELECT id FROM users WHERE ipaddress_last = '{}'",
            escaped_ip
        ))
        .await
        .unwrap_or_default();
    for affected_id in affected_ids {
        if let Some(user) = get_user(state, affected_id).await {
            let _ = user.sender.send(format!("@c{}", reason));
        }
        disconnect_user(state, affected_id).await;
    }
}

pub async fn generate_ban_report_for_user(state: &AppState, user_id: i64) -> String {
    let ban_details = state
        .db
        .run_read_row(&format!(
            "SELECT date_expire,descr,ipaddress FROM users_bans WHERE userid = '{}' LIMIT 1",
            user_id
        ))
        .await
        .unwrap_or_default();
    let user_details = state
        .db
        .run_read_row(&format!(
            "SELECT name,rank,ipaddress_last FROM users WHERE id = '{}' LIMIT 1",
            user_id
        ))
        .await
        .unwrap_or_default();
    if ban_details.len() < 3 || user_details.len() < 3 {
        return "holo.cast.banreport.null".to_string();
    }

    let mut note = "-".to_string();
    let mut ban_poster = "not available".to_string();
    let mut ban_posted = "not available".to_string();
    let log_entries = state
        .db
        .run_read_row(&format!(
            "SELECT userid,note,timestamp FROM system_stafflog WHERE action = 'ban' AND targetid = '{}' ORDER BY id ASC LIMIT 1",
            user_id
        ))
        .await
        .unwrap_or_default();
    if log_entries.len() >= 3 {
        if !log_entries[1].is_empty() {
            note = log_entries[1].clone();
        }
        ban_poster = state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT name FROM users WHERE id = '{}' LIMIT 1",
                log_entries[0]
            ))
            .await;
        ban_posted = log_entries[2].clone();
    }

    format!(
        "{} {}\r{}: {}\r{}: {}\r{}: {}\r{}: {}\r{}: {}\r{}: {}\r{}: {}\r{}: {}",
        "banreport_header",
        format!("{} [{}]", user_details[0], user_id),
        "common_userrank",
        user_details[1],
        "common_ip",
        user_details[2],
        "banreport_banner",
        ban_poster,
        "banreport_posted",
        ban_posted,
        "banreport_expires",
        ban_details[0],
        "banreport_reason",
        ban_details[1],
        "banreport_ipbanflag",
        (!ban_details[2].is_empty()).to_string().to_lowercase(),
        "banreport_staffnote",
        note
    )
}

pub async fn generate_ban_report_for_ip(state: &AppState, ip: &str) -> String {
    let escaped_ip = Database::stripslash(ip);
    let ban_details = state
        .db
        .run_read_row(&format!(
            "SELECT userid,date_expire,descr FROM users_bans WHERE ipaddress = '{}' LIMIT 1",
            escaped_ip
        ))
        .await
        .unwrap_or_default();
    if ban_details.len() < 3 {
        return "holo.cast.banreport.null".to_string();
    }

    let mut note = "-".to_string();
    let mut ban_poster = "not available".to_string();
    let mut ban_posted = "not available".to_string();
    let log_entries = state
        .db
        .run_read_row(&format!(
            "SELECT userid,note,timestamp FROM system_stafflog WHERE action = 'ban' AND targetid = '{}' ORDER BY id DESC LIMIT 1",
            ban_details[0]
        ))
        .await
        .unwrap_or_default();
    if log_entries.len() >= 3 {
        if !log_entries[1].is_empty() {
            note = log_entries[1].clone();
        }
        ban_poster = state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT name FROM users WHERE id = '{}' LIMIT 1",
                log_entries[0]
            ))
            .await;
        ban_posted = log_entries[2].clone();
    }
    let affected_usernames = state
        .db
        .run_read_column_string(&format!(
            "SELECT name FROM users WHERE ipaddress_last = '{}'",
            escaped_ip
        ))
        .await
        .unwrap_or_default();

    let mut report = format!(
        "{} {}\r{}: {}\r{}: {}\r{}: {}\r{}: {}\r{}: {}\r{}: {}\r\r{}:",
        "banreport_header",
        ip,
        "banreport_banner",
        ban_poster,
        "banreport_posted",
        ban_posted,
        "banreport_expires",
        ban_details[1],
        "banreport_reason",
        ban_details[2],
        "banreport_ipbanflag",
        "true",
        "banreport_staffnote",
        note,
        "banreport_affectedusernames"
    );
    for username in affected_usernames {
        report.push_str(&format!("\r - {}", username));
    }
    report
}

pub async fn generate_user_info(state: &AppState, user_id: i64, rank: u8) -> String {
    let user_details = state
        .db
        .run_read_row(&format!(
            "SELECT name,rank,mission,credits,tickets,email,birth,hbirth,ipaddress_last,lastvisit \
             FROM users WHERE id = '{}' AND rank <= '{}' LIMIT 1",
            user_id, rank
        ))
        .await
        .unwrap_or_default();
    if user_details.len() < 10 {
        return string_or_key(state, "userinfo_accesserror").await;
    }

    let mut info = format!("{}\r", string_or_key(state, "userinfo_header").await);
    info.push_str(&format!(
        "{}: {}\r",
        string_or_key(state, "common_userid").await,
        user_id
    ));
    info.push_str(&format!(
        "{}: {}\r",
        string_or_key(state, "common_username").await,
        user_details[0]
    ));
    info.push_str(&format!(
        "{}: {}\r",
        string_or_key(state, "common_userrank").await,
        user_details[1]
    ));
    info.push_str(&format!(
        "{}: {}\r",
        string_or_key(state, "common_usermission").await,
        user_details[2]
    ));
    info.push_str(&format!(
        "{}: {}\r",
        string_or_key(state, "common_credits").await,
        user_details[3]
    ));
    info.push_str(&format!(
        "{}: {}\r",
        string_or_key(state, "common_tickets").await,
        user_details[4]
    ));
    info.push_str(&format!(
        "{}: {}\r\r",
        string_or_key(state, "common_hbirth").await,
        user_details[7]
    ));
    info.push_str(&format!(
        "{}: {}\r",
        string_or_key(state, "common_birth").await,
        user_details[6]
    ));
    info.push_str(&format!(
        "{}: {}\r",
        string_or_key(state, "common_email").await,
        user_details[5]
    ));
    info.push_str(&format!(
        "{}: {}\r\r",
        string_or_key(state, "common_ip").await,
        user_details[8]
    ));

    if let Some(user) = get_user(state, user_id).await {
        let location = if user.room_id == 0 {
            string_or_key(state, "common_hotelview").await
        } else {
            let room_name = state
                .db
                .run_read_unsafe_string(&format!(
                    "SELECT name FROM rooms WHERE id = '{}' LIMIT 1",
                    user.room_id
                ))
                .await;
            let room_owner = state
                .db
                .run_read_unsafe_string(&format!(
                    "SELECT owner FROM rooms WHERE id = '{}' LIMIT 1",
                    user.room_id
                ))
                .await;
            format!(
                "{} '{}' [id: {}, {}: {}]",
                string_or_key(state, "common_room").await,
                room_name,
                user.room_id,
                string_or_key(state, "common_owner").await,
                room_owner
            )
        };
        info.push_str(&format!(
            "{}: {}",
            string_or_key(state, "common_location").await,
            location
        ));
    } else {
        info.push_str(&format!(
            "{}: {}",
            string_or_key(state, "common_lastaccess").await,
            user_details[9]
        ));
    }

    info
}

async fn string_or_key(state: &AppState, key: &str) -> String {
    crate::managers::string_manager::get_string(state, key)
        .await
        .unwrap_or_else(|_| key.to_string())
}

fn is_pending_ban(expires_at: &str) -> bool {
    chrono::NaiveDateTime::parse_from_str(expires_at, "%Y-%m-%d %H:%M:%S")
        .map(|expires| expires > Local::now().naive_local())
        .unwrap_or(false)
}
