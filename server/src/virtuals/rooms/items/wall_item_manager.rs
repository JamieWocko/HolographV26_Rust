use anyhow::Result;

use crate::core::state::AppState;
use crate::virtuals::rooms::items::wall_item::WallItem;
use crate::virtuals::rooms::virtual_room::VirtualRoom;

pub async fn place_item(
    room: &mut VirtualRoom,
    state: &AppState,
    item_id: i64,
    template_id: i64,
    wall_position: String,
    var: String,
) -> Result<Option<String>> {
    if room.contains_wall_item(item_id) {
        return Ok(None);
    }

    let item = WallItem::new(state, item_id, template_id, wall_position.clone(), var).await;
    room.wall_items.push(item.clone());
    state
        .db
        .run_query(&format!(
            "UPDATE furniture SET roomid = '{}',wallpos = '{}' WHERE id = '{}' LIMIT 1",
            room.room_id, wall_position, item_id
        ))
        .await?;
    if state
        .db
        .check_exists(&format!(
            "SELECT id FROM furniture_moodlight WHERE id = '{}' LIMIT 1",
            item_id
        ))
        .await
    {
        state
            .db
            .run_query(&format!(
                "UPDATE furniture_moodlight SET roomid = '{}' WHERE id = '{}' LIMIT 1",
                room.room_id, item_id
            ))
            .await?;
    }

    Ok(Some(format!("AS{}", item.to_legacy_string(state).await)))
}

pub async fn remove_item(
    room: &mut VirtualRoom,
    state: &AppState,
    item_id: i64,
    owner_id: i64,
) -> Result<Option<String>> {
    let Some(index) = room.wall_items.iter().position(|item| item.id == item_id) else {
        return Ok(None);
    };

    room.wall_items.remove(index);
    if owner_id > 0 {
        state
            .db
            .run_query(&format!(
                "UPDATE furniture SET ownerid = '{}',roomid = '0',wallpos = '' WHERE id = '{}' LIMIT 1",
                owner_id, item_id
            ))
            .await?;
    } else {
        state
            .db
            .run_query(&format!(
                "DELETE FROM furniture WHERE id = '{}' LIMIT 1",
                item_id
            ))
            .await?;
    }

    Ok(Some(format!("AT{}", item_id)))
}

pub async fn toggle_item(
    room: &mut VirtualRoom,
    state: &AppState,
    item_id: i64,
    to_status: i32,
) -> Result<Option<String>> {
    let Some(item) = room.wall_items.iter_mut().find(|item| item.id == item_id) else {
        return Ok(None);
    };

    item.var = to_status.to_string();
    let sprite = item.sprite(state).await;
    state
        .db
        .run_query(&format!(
            "UPDATE furniture SET var = '{}' WHERE id = '{}' LIMIT 1",
            to_status, item_id
        ))
        .await?;

    Ok(Some(format!(
        "AU{}\t{}\t {}\t{}",
        item_id, sprite, item.wall_position, item.var
    )))
}
