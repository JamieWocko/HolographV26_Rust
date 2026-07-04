use anyhow::Result;
use chrono::Local;

use crate::core::state::AppState;
use crate::db::db::Database;

pub async fn add_staff_message(
    state: &AppState,
    action: &str,
    user_id: i64,
    target_id: i64,
    message: &str,
    note: &str,
) -> Result<()> {
    state
        .db
        .run_query(&format!(
            "INSERT INTO system_stafflog (action,userid,targetid,message,note,timestamp) VALUES ('{}','{}','{}','{}','{}','{}')",
            Database::stripslash(action),
            user_id,
            target_id,
            Database::stripslash(message),
            Database::stripslash(note),
            Local::now().format("%Y-%m-%d %H:%M:%S")
        ))
        .await
}
