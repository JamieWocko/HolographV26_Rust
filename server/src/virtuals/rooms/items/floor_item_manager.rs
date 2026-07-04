use std::cmp::Ordering;

use anyhow::Result;

use crate::core::state::AppState;
use crate::managers::catalogue_manager;
use crate::virtuals::rooms::items::floor_item::FloorItem;
use crate::virtuals::rooms::virtual_room::VirtualRoom;

pub async fn place_item(
    room: &mut VirtualRoom,
    state: &AppState,
    item_id: i64,
    template_id: i64,
    x: i32,
    y: i32,
    z: i32,
    var: String,
    max_stack_height: f64,
) -> Result<Vec<String>> {
    if room.contains_floor_item(item_id) {
        return Ok(Vec::new());
    }

    let template = catalogue_manager::get_template(state, template_id).await;
    if template.sprite.starts_with("sound_machine") && room.sound_machine_id > 0 {
        return Ok(Vec::new());
    }
    let Some(height) = room
        .compute_floor_item_height(state, &template, x, y, z as u8, max_stack_height)
        .await
    else {
        return Ok(Vec::new());
    };

    let item = FloorItem::new(item_id, template_id, x, y, z, height, var);
    let coords = room.item_footprint_coords(&template, x, y, z as u8);
    room.floor_items.push(item.clone());
    sort_items(&mut room.floor_items);
    if template.sprite.starts_with("sound_machine") {
        room.sound_machine_id = item_id;
    }

    state
        .db
        .run_query(&format!(
            "UPDATE furniture SET roomid = '{}',x = '{}',y = '{}',z = '{}',h = '{}' WHERE id = '{}' LIMIT 1",
            room.room_id,
            x,
            y,
            z,
            format_height(height),
            item_id
        ))
        .await?;

    room.rebuild_floor_item_map(state).await;

    let mut packets = vec![format!("A]{}", item.to_legacy_string(state).await)];
    packets.extend(room.refresh_coord_packets(coords));
    Ok(packets)
}

pub async fn remove_item(
    room: &mut VirtualRoom,
    state: &AppState,
    item_id: i64,
    owner_id: i64,
) -> Result<Vec<String>> {
    let Some(index) = room.floor_items.iter().position(|item| item.id == item_id) else {
        return Ok(Vec::new());
    };

    let item = room.floor_items.remove(index);
    let template = catalogue_manager::get_template(state, item.template_id).await;
    if room.sound_machine_id == 0 && template.sprite.starts_with("sound_machine") {
        room.sound_machine_id = 0;
    }
    let coords = room.item_footprint_coords(&template, item.x, item.y, item.z);

    if owner_id > 0 {
        state
            .db
            .run_query(&format!(
                "UPDATE furniture SET x = '0',y = '0',z = '0',h = '0',ownerid = '{}',roomid = '0' WHERE id = '{}' LIMIT 1",
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

    room.rebuild_floor_item_map(state).await;

    let mut packets = vec![format!("A^{}", item_id)];
    packets.extend(room.refresh_coord_packets(coords));
    Ok(packets)
}

pub async fn relocate_item(
    room: &mut VirtualRoom,
    state: &AppState,
    item_id: i64,
    x: i32,
    y: i32,
    z: i32,
    max_stack_height: f64,
) -> Result<Vec<String>> {
    let Some(index) = room.floor_items.iter().position(|item| item.id == item_id) else {
        return Ok(Vec::new());
    };

    let old_item = room.floor_items.remove(index);
    let template = catalogue_manager::get_template(state, old_item.template_id).await;
    let old_coords = room.item_footprint_coords(&template, old_item.x, old_item.y, old_item.z);
    room.rebuild_floor_item_map(state).await;

    let Some(height) = room
        .compute_floor_item_height(state, &template, x, y, z as u8, max_stack_height)
        .await
    else {
        room.floor_items.push(old_item);
        sort_items(&mut room.floor_items);
        room.rebuild_floor_item_map(state).await;
        return Ok(Vec::new());
    };

    let item = FloorItem::new(
        old_item.id,
        old_item.template_id,
        x,
        y,
        z,
        height,
        old_item.var,
    );
    let new_coords = room.item_footprint_coords(&template, x, y, z as u8);
    room.floor_items.push(item.clone());
    sort_items(&mut room.floor_items);

    state
        .db
        .run_query(&format!(
            "UPDATE furniture SET x = '{}',y = '{}',z = '{}',h = '{}' WHERE id = '{}' LIMIT 1",
            x,
            y,
            z,
            format_height(height),
            item_id
        ))
        .await?;

    room.rebuild_floor_item_map(state).await;

    let mut coords = old_coords;
    coords.extend(new_coords);
    let mut packets = vec![format!("A_{}", item.to_legacy_string(state).await)];
    packets.extend(room.refresh_coord_packets(coords));
    Ok(packets)
}

pub async fn toggle_item(
    room: &mut VirtualRoom,
    state: &AppState,
    item_id: i64,
    to_status: String,
    has_rights: bool,
) -> Result<Vec<String>> {
    let Some(index) = room.floor_items.iter().position(|item| item.id == item_id) else {
        return Ok(Vec::new());
    };

    let sprite = room.floor_items[index].sprite(state).await;
    if matches!(sprite.as_str(), "edice" | "edicehc")
        || sprite.starts_with("prizetrophy")
        || sprite.starts_with("greektrophy")
        || sprite.starts_with("present")
    {
        return Ok(Vec::new());
    }

    let template =
        catalogue_manager::get_template(state, room.floor_items[index].template_id).await;
    if matches!(to_status.to_lowercase().as_str(), "c" | "o") && template.is_door && !has_rights {
        return Ok(Vec::new());
    }

    let coords = room.item_footprint_coords(
        &template,
        room.floor_items[index].x,
        room.floor_items[index].y,
        room.floor_items[index].z,
    );
    room.floor_items[index].var = to_status.clone();
    state
        .db
        .run_query(&format!(
            "UPDATE furniture SET var = '{}' WHERE id = '{}' LIMIT 1",
            to_status, item_id
        ))
        .await?;
    room.rebuild_floor_item_map(state).await;

    let mut packets = vec![format!("AX{}\u{2}{}\u{2}", item_id, to_status)];
    packets.extend(room.refresh_coord_packets(coords));
    Ok(packets)
}

fn sort_items(items: &mut [FloorItem]) {
    items.sort_by(|left, right| {
        left.h
            .partial_cmp(&right.h)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.id.cmp(&right.id))
    });
}

fn format_height(value: f64) -> String {
    value.to_string().replace(',', ".")
}
