use anyhow::Result;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::time::{Duration, sleep};
use tracing::info;

use crate::core::state::AppState;
use crate::encoding::jeax_encoding::encode_vl64;
use crate::managers::user_manager;
use crate::virtuals::rooms::virtual_room::VirtualRoom;

pub async fn add_room(state: &AppState, room_id: i64, room: VirtualRoom) {
    let mut rooms = state.loaded_rooms.write().await;
    if rooms.contains_key(&room_id) {
        return;
    }

    let is_publicroom = room.is_publicroom;
    rooms.insert(room_id, room);
    let room_count = rooms.len() as i64;
    state.active_rooms.store(room_count, Ordering::Relaxed);
    state.peak_rooms.fetch_max(room_count, Ordering::Relaxed);
    drop(rooms);
    info!(room_id, is_publicroom, "room loaded");
    spawn_room_cycle_if_needed(Arc::new(state.clone()), room_id).await;
}

pub async fn save_room(state: &AppState, room: VirtualRoom) {
    let room_id = room.room_id;
    let mut rooms = state.loaded_rooms.write().await;
    rooms.insert(room_id, room);
    let room_count = rooms.len() as i64;
    state.active_rooms.store(room_count, Ordering::Relaxed);
    state.peak_rooms.fetch_max(room_count, Ordering::Relaxed);
}

pub async fn remove_room(state: &AppState, room_id: i64) -> Result<()> {
    let removed_room = state.loaded_rooms.write().await.remove(&room_id);
    if let Some(room) = removed_room {
        update_room_visitor_count(state, room_id, 0).await?;
        info!(
            room_id,
            is_publicroom = room.is_publicroom,
            "room destroyed"
        );
    }

    let room_count = state.loaded_rooms.read().await.len() as i64;
    state.active_rooms.store(room_count, Ordering::Relaxed);
    Ok(())
}

pub async fn contains_room(state: &AppState, room_id: i64) -> bool {
    state.loaded_rooms.read().await.contains_key(&room_id)
}

pub async fn room_count(state: &AppState) -> i64 {
    state.loaded_rooms.read().await.len() as i64
}

pub fn peak_room_count(state: &AppState) -> i64 {
    state.peak_rooms.load(Ordering::Relaxed)
}

fn should_decrement_swim_ticket(tickets: i64) -> bool {
    tickets != 33_333
}

pub async fn get_room(state: &AppState, room_id: i64) -> Option<VirtualRoom> {
    state.loaded_rooms.read().await.get(&room_id).cloned()
}

pub async fn load_room(state: &AppState, room_id: i64, is_publicroom: bool) -> Result<VirtualRoom> {
    if let Some(room) = get_room(state, room_id).await {
        return Ok(room);
    }

    let room = VirtualRoom::load(state, room_id, is_publicroom).await?;
    add_room(state, room_id, room.clone()).await;
    Ok(room)
}

pub async fn spawn_room_cycle_if_needed(state: Arc<AppState>, room_id: i64) {
    let mut active_cycles = state.active_room_cycles.lock().await;
    if !active_cycles.insert(room_id) {
        return;
    }
    drop(active_cycles);

    tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(410)).await;

            let Some(mut room) = get_room(&state, room_id).await else {
                break;
            };

            let outcome = room.process_status_cycle();
            if outcome.status_packet.is_none()
                && outcome.exited_users.is_empty()
                && outcome.room_packets.is_empty()
                && outcome.user_packets.is_empty()
                && outcome.ticket_decrements.is_empty()
            {
                continue;
            }

            let remaining_users = room
                .users
                .iter()
                .map(|entry| entry.user_id)
                .collect::<Vec<_>>();
            if !remaining_users.is_empty() {
                let _ =
                    update_room_visitor_count(&state, room_id, remaining_users.len() as i64).await;
                save_room(&state, room).await;

                if let Some(status_packet) = outcome.status_packet {
                    for user_id in &remaining_users {
                        if let Some(user) = user_manager::get_user(&state, *user_id).await {
                            let _ = user.sender.send(status_packet.clone());
                        }
                    }
                }

                for room_packet in outcome.room_packets {
                    for user_id in &remaining_users {
                        if let Some(user) = user_manager::get_user(&state, *user_id).await {
                            let _ = user.sender.send(room_packet.clone());
                        }
                    }
                }

                for (user_id, packet) in outcome.user_packets {
                    if let Some(user) = user_manager::get_user(&state, user_id).await {
                        let _ = user.sender.send(packet);
                    }
                }

                for user_id in outcome.ticket_decrements {
                    let tickets = state
                        .db
                        .run_read_unsafe_i64(&format!(
                            "SELECT tickets FROM users WHERE id = '{}' LIMIT 1",
                            user_id
                        ))
                        .await;
                    if !should_decrement_swim_ticket(tickets) {
                        continue;
                    }

                    let _ = state
                        .db
                        .run_query(&format!(
                            "UPDATE users SET tickets = tickets - 1 WHERE id = '{}' LIMIT 1",
                            user_id
                        ))
                        .await;
                    if let Some(user) = user_manager::get_user(&state, user_id).await {
                        let tickets = state
                            .db
                            .run_read_unsafe_i64(&format!(
                                "SELECT tickets FROM users WHERE id = '{}' LIMIT 1",
                                user_id
                            ))
                            .await;
                        let _ = user.sender.send(format!("A|{}", tickets));
                    }
                }

                for (room_uid, user_id) in outcome.exited_users {
                    for target_user_id in &remaining_users {
                        if let Some(user) = user_manager::get_user(&state, *target_user_id).await {
                            let _ = user.sender.send(format!("@]{room_uid}"));
                        }
                    }
                    state.clear_doorbell_access(user_id).await;
                    state.clear_doorbell_denied(user_id).await;
                    if let Some(mut user) = user_manager::get_user(&state, user_id).await {
                        // Original Holograph cleared the removed user's live room flags inside
                        // virtualRoom.removeUser(). Mirror that shared state here for cycle-based
                        // removals and let the owning Rust session reconcile its local fields.
                        user.in_room = false;
                        user.room_id = 0;
                        user.room_is_public = false;
                        state.online_users.write().await.insert(user_id, user);
                    }
                    if let Some(user) = user_manager::get_user(&state, user_id).await {
                        let _ = user.sender.send("@R".to_string());
                    }
                }
            } else {
                let _ = remove_room(&state, room_id).await;
                break;
            }
        }

        state.active_room_cycles.lock().await.remove(&room_id);
    });
}

pub async fn spawn_game_cycle_if_needed(state: Arc<AppState>, room_id: i64, game_id: i64) {
    let key = format!("{room_id}:{game_id}");
    let mut active_cycles = state.active_game_cycles.lock().await;
    if !active_cycles.insert(key.clone()) {
        return;
    }
    drop(active_cycles);

    tokio::spawn(async move {
        let countdown_steps = match get_room(&state, room_id).await {
            Some(room) => room
                .lobby
                .as_ref()
                .and_then(|lobby| lobby.games.iter().find(|entry| entry.id == game_id))
                .map(|game| game.countdown_seconds.max(0))
                .unwrap_or(0),
            None => 0,
        };

        for _ in 0..countdown_steps {
            let Some(mut room) = get_room(&state, room_id).await else {
                state.active_game_cycles.lock().await.remove(&key);
                return;
            };

            let mut found_game = false;
            if let Some(lobby) = room.lobby.as_mut()
                && let Some(game) = lobby.games.iter_mut().find(|entry| entry.id == game_id)
            {
                found_game = true;
                if game.left_countdown_seconds > 0 {
                    game.left_countdown_seconds -= 1;
                }
            }

            if !found_game {
                state.active_game_cycles.lock().await.remove(&key);
                return;
            }

            save_room(&state, room).await;
            sleep(Duration::from_secs(1)).await;
        }

        let Some(mut room) = get_room(&state, room_id).await else {
            state.active_game_cycles.lock().await.remove(&key);
            return;
        };

        let mut started_player_ids = Vec::new();
        let mut countdown_packet = None;
        if let Some(lobby) = room.lobby.as_mut()
            && let Some(game) = lobby.games.iter_mut().find(|entry| entry.id == game_id)
        {
            for team in &game.teams {
                for player in team {
                    started_player_ids.push(player.user_id);
                    let _ = state
                        .db
                        .run_query(&format!(
                            "UPDATE users SET tickets = tickets - 2 WHERE id = '{}' LIMIT 1",
                            player.user_id
                        ))
                        .await;
                }
            }
            countdown_packet = Some(format!("Cw{}", encode_vl64(game.total_time as i32)));
        }
        save_room(&state, room).await;

        if let Some(countdown_packet) = countdown_packet {
            for user_id in &started_player_ids {
                if let Some(user) = user_manager::get_user(&state, *user_id).await {
                    let tickets = state
                        .db
                        .run_read_unsafe_i64(&format!(
                            "SELECT tickets FROM users WHERE id = '{}' LIMIT 1",
                            user_id
                        ))
                        .await;
                    let _ = user.sender.send(format!("A|{}", tickets));
                    let _ = user.sender.send(countdown_packet.clone());
                }
            }
        }

        loop {
            sleep(Duration::from_millis(470)).await;

            let Some(mut room) = get_room(&state, room_id).await else {
                break;
            };

            let mut recipients = Vec::new();
            let update_packet;
            let mut end_packet = None;
            let mut should_stop = false;

            if let Some(lobby) = room.lobby.as_mut() {
                if let Some(game) = lobby.games.iter_mut().find(|entry| entry.id == game_id) {
                    for team in &game.teams {
                        recipients.extend(team.iter().map(|entry| entry.user_id));
                    }

                    update_packet = game.tick();
                    if game.left_time <= 0 {
                        end_packet = game.finish_game(&state).await.ok();
                        should_stop = true;
                    }
                } else {
                    break;
                }
            } else {
                break;
            }

            save_room(&state, room).await;

            if let Some(update_packet) = update_packet {
                for user_id in &recipients {
                    if let Some(user) = user_manager::get_user(&state, *user_id).await {
                        let _ = user.sender.send(update_packet.clone());
                        if let Some(ref end_packet) = end_packet {
                            let _ = user.sender.send(end_packet.clone());
                        }
                    }
                }
            }

            if should_stop {
                break;
            }
        }

        state.active_game_cycles.lock().await.remove(&key);
    });
}

pub async fn update_room_visitor_count(
    state: &AppState,
    room_id: i64,
    visitor_count: i64,
) -> Result<()> {
    state
        .db
        .run_query(&format!(
            "UPDATE rooms SET visitors_now = '{}' WHERE id = '{}' LIMIT 1",
            visitor_count, room_id
        ))
        .await
}

pub fn room_state_id(state: &str) -> i32 {
    match state {
        "closed" => 1,
        "password" => 2,
        _ => 0,
    }
}

pub fn room_state_name(state: i32) -> &'static str {
    match state {
        1 => "closed",
        2 => "password",
        _ => "open",
    }
}

pub async fn get_poll(state: &AppState, room_id: i64) -> String {
    let poll = match state
        .db
        .run_read_row(&format!(
            "SELECT pid,title,thanks FROM poll WHERE rid = '{}' LIMIT 1",
            room_id
        ))
        .await
    {
        Ok(row) => row,
        Err(_) => return String::new(),
    };
    if poll.len() < 3 {
        return String::new();
    }

    let poll_id = poll[0].parse::<i64>().unwrap_or(0);
    let question_rows = match state
        .db
        .run_read_table(&format!(
            "SELECT qid,question,type,min,max FROM poll_questions WHERE pid = '{}' ORDER BY qid ASC",
            poll_id
        ))
        .await
    {
        Ok(rows) => rows,
        Err(_) => return String::new(),
    };

    let mut packet = format!(
        "D}}{}{}\u{2}{}\u{2}{}",
        encode_vl64(poll_id as i32),
        poll[1],
        poll[2],
        encode_vl64(question_rows.len() as i32)
    );

    for (index, row) in question_rows.iter().enumerate() {
        if row.len() < 5 {
            continue;
        }

        let qid = row[0].parse::<i64>().unwrap_or(0);
        let answers = state
            .db
            .run_read_column_string(&format!(
                "SELECT answer FROM poll_answers WHERE qid = '{}' ORDER BY aid ASC",
                qid
            ))
            .await
            .unwrap_or_default();

        packet.push_str(&format!(
            "{}{}{}{}\u{2}{}{}{}",
            encode_vl64(qid as i32),
            encode_vl64((index + 1) as i32),
            encode_vl64(row[2].parse::<i32>().unwrap_or(0)),
            row[1],
            encode_vl64(answers.len() as i32),
            encode_vl64(row[3].parse::<i32>().unwrap_or(0)),
            encode_vl64(row[4].parse::<i32>().unwrap_or(0))
        ));
        for answer in answers {
            packet.push_str(&answer);
            packet.push('\u{2}');
        }
    }

    packet
}

pub fn refresh_wallitem_packet(
    item_id: i64,
    cct_name: &str,
    wall_position: &str,
    item_variable: &str,
) -> String {
    format!(
        "AU{}\t{}\t {}\t{}",
        item_id, cct_name, wall_position, item_variable
    )
}

pub mod moodlight {
    use anyhow::Result;

    use crate::core::state::AppState;
    use crate::db::db::Database;
    use crate::encoding::jeax_encoding::encode_vl64;
    use crate::managers::room_manager::refresh_wallitem_packet;

    pub async fn get_settings(state: &AppState, room_id: i64) -> Option<String> {
        let item_settings = state
            .db
            .run_read_row(&format!(
                "SELECT preset_cur,preset_1,preset_2,preset_3 FROM furniture_moodlight WHERE roomid = '{}' LIMIT 1",
                room_id
            ))
            .await
            .ok()?;
        if item_settings.len() < 4 {
            return None;
        }

        let mut settings = format!(
            "{}{}",
            encode_vl64(3),
            encode_vl64(item_settings[0].parse::<i32>().unwrap_or(1))
        );
        for (index, preset) in item_settings.iter().enumerate().skip(1).take(3) {
            let parts: Vec<&str> = preset.split(',').collect();
            if parts.len() < 3 {
                return None;
            }

            settings.push_str(&format!(
                "{}{}{}\u{2}{}",
                encode_vl64(index as i32),
                encode_vl64(parts[0].parse::<i32>().unwrap_or(0)),
                parts[1],
                encode_vl64(parts[2].parse::<i32>().unwrap_or(0))
            ));
        }

        Some(settings)
    }

    pub async fn set_settings(
        state: &AppState,
        room_id: i64,
        is_enabled: bool,
        preset_id: i64,
        bg_state: i64,
        preset_colour: &str,
        alpha_dark_f: i64,
    ) -> Result<Option<String>> {
        let item_id = state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT id FROM furniture_moodlight WHERE roomid = '{}' LIMIT 1",
                room_id
            ))
            .await;
        if item_id == 0 {
            return Ok(None);
        }

        let new_preset_value = if !is_enabled {
            let current_value = state
                .db
                .run_read_unsafe_string(&format!(
                    "SELECT var FROM furniture WHERE id = '{}' LIMIT 1",
                    item_id
                ))
                .await;
            if current_value.starts_with('2') {
                format!("1{}", current_value.get(1..).unwrap_or_default())
            } else {
                format!("2{}", current_value.get(1..).unwrap_or_default())
            }
        } else {
            format!(
                "2,{},{},{},{}",
                preset_id,
                bg_state,
                Database::stripslash(preset_colour),
                alpha_dark_f
            )
        };

        state
            .db
            .run_query(&format!(
                "UPDATE furniture SET var = '{}' WHERE id = '{}' LIMIT 1",
                Database::stripslash(&new_preset_value),
                item_id
            ))
            .await?;

        if is_enabled {
            state
                .db
                .run_query(&format!(
                    "UPDATE furniture_moodlight SET preset_cur = '{}',preset_{} = '{},{},{}' WHERE id = '{}' LIMIT 1",
                    preset_id,
                    preset_id,
                    bg_state,
                    Database::stripslash(preset_colour),
                    alpha_dark_f,
                    item_id
                ))
                .await?;
        }

        let wall_position = state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT wallpos FROM furniture WHERE id = '{}' LIMIT 1",
                item_id
            ))
            .await;

        Ok(Some(refresh_wallitem_packet(
            item_id,
            "roomdimmer",
            &wall_position,
            &new_preset_value,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::should_decrement_swim_ticket;

    #[test]
    fn preserves_unlimited_swim_ticket_value() {
        assert!(!should_decrement_swim_ticket(33_333));
        assert!(should_decrement_swim_ticket(2));
        assert!(should_decrement_swim_ticket(0));
    }
}
