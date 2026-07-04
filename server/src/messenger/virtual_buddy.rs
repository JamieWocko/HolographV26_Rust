use anyhow::Result;

use crate::core::state::AppState;
use crate::encoding::jeax_encoding::encode_vl64;
use crate::managers::user_manager;

pub async fn to_legacy_string(
    state: &AppState,
    buddy_user_id: i64,
    include_username: bool,
) -> Result<String> {
    let mut out = encode_vl64(buddy_user_id as i32);
    let online_user = user_manager::get_user(state, buddy_user_id).await;

    if include_username {
        let username = if let Some(user) = &online_user {
            user.username.clone()
        } else {
            user_manager::get_user_name(state, buddy_user_id).await
        };
        out.push_str(&username);
        out.push('\u{2}');
    }

    if let Some(user) = online_user {
        out.push_str("II");
        out.push(if user.in_room { 'I' } else { 'H' });
        out.push_str(&user.figure);
    } else {
        out.push_str("IHH");
    }

    out.push('\u{2}');
    out.push('H');
    Ok(out)
}
