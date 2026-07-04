use std::collections::HashMap;

use anyhow::Result;

use crate::core::state::AppState;
use crate::encoding::jeax_encoding::encode_vl64;
use crate::managers::user_manager;
use crate::messenger::virtual_buddy;

pub async fn friend_list(state: &AppState, user_id: i64) -> Result<String> {
    let user_ids = user_manager::get_user_friend_ids(state, user_id).await;
    let mut buddy_list = format!(
        "{}{}{}H{}",
        encode_vl64(200),
        encode_vl64(200),
        encode_vl64(600),
        encode_vl64(user_ids.len() as i32)
    );

    for buddy_id in user_ids {
        buddy_list.push_str(&virtual_buddy::to_legacy_string(state, buddy_id, true).await?);
    }

    buddy_list.push_str(&encode_vl64(200));
    buddy_list.push('H');
    Ok(buddy_list)
}

pub async fn friend_requests(state: &AppState, user_id: i64) -> Result<String> {
    let from_user_ids = state
        .db
        .run_read_column_i64(&format!(
            "SELECT userid_from FROM messenger_friendrequests WHERE userid_to = '{}' ORDER BY requestid ASC",
            user_id
        ))
        .await
        .unwrap_or_default();
    let request_ids = state
        .db
        .run_read_column_i64(&format!(
            "SELECT requestid FROM messenger_friendrequests WHERE userid_to = '{}' ORDER BY requestid ASC",
            user_id
        ))
        .await
        .unwrap_or_default();

    let mut requests = format!(
        "{}{}",
        encode_vl64(from_user_ids.len() as i32),
        encode_vl64(from_user_ids.len() as i32)
    );

    for (index, from_user_id) in from_user_ids.iter().enumerate() {
        requests.push_str(&encode_vl64(*request_ids.get(index).unwrap_or(&0) as i32));
        requests.push_str(&user_manager::get_user_name(state, *from_user_id).await);
        requests.push('\u{2}');
        requests.push_str(&from_user_id.to_string());
        requests.push('\u{2}');
    }

    Ok(requests)
}

pub async fn notify_online_friends_of_presence(state: &AppState, user_id: i64) -> Result<()> {
    let friend_ids = user_manager::get_user_friend_ids(state, user_id).await;
    let me = virtual_buddy::to_legacy_string(state, user_id, true).await?;

    for friend_id in friend_ids {
        if let Some(friend) = user_manager::get_user(state, friend_id).await {
            let _ = friend.sender.send(format!("@MHII{}", me));
        }
    }

    Ok(())
}

pub async fn notify_buddy_added(
    state: &AppState,
    recipient_user_id: i64,
    buddy_user_id: i64,
) -> Result<()> {
    if let Some(recipient) = user_manager::get_user(state, recipient_user_id).await {
        let buddy = virtual_buddy::to_legacy_string(state, buddy_user_id, true).await?;
        let _ = recipient.sender.send(format!("@MHII{}", buddy));
    }

    Ok(())
}

pub async fn notify_buddy_removed(state: &AppState, recipient_user_id: i64, buddy_user_id: i64) {
    if let Some(recipient) = user_manager::get_user(state, recipient_user_id).await {
        let _ = recipient
            .sender
            .send(format!("@MHIM{}", encode_vl64(buddy_user_id as i32)));
    }
}

pub async fn build_updates_packet(
    state: &AppState,
    user_id: i64,
    buddy_presence: &mut HashMap<i64, (bool, bool)>,
) -> Result<String> {
    let friend_ids = user_manager::get_user_friend_ids(state, user_id).await;
    let mut update_amount = 0_i32;
    let mut updates = String::new();

    for friend_id in friend_ids {
        let online_user = user_manager::get_user(state, friend_id).await;
        let current = if let Some(user) = &online_user {
            (true, user.in_room)
        } else {
            (false, false)
        };

        let changed = buddy_presence
            .get(&friend_id)
            .map(|previous| *previous != current)
            .unwrap_or(true);
        if changed {
            update_amount += 1;
            updates.push('H');
            updates.push_str(&virtual_buddy::to_legacy_string(state, friend_id, false).await?);
        }
        buddy_presence.insert(friend_id, current);
    }

    Ok(format!("H{}{}", encode_vl64(update_amount), updates))
}
