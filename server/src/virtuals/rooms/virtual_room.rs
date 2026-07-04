use anyhow::Result;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::bots::virtual_bot::{ChatTrigger, VirtualBot};
use crate::core::state::AppState;
use crate::games::game_lobby::GameLobby;
use crate::managers::catalogue_manager;
use crate::managers::room_manager;
use crate::virtuals::rooms::items::floor_item::FloorItem;
use crate::virtuals::rooms::items::floor_item_manager;
use crate::virtuals::rooms::items::wall_item::WallItem;
use crate::virtuals::rooms::items::wall_item_manager;
use crate::virtuals::rooms::pathfinder::{pathfinder::Pathfinder, rotation};
use crate::virtuals::users::virtual_room_user::VirtualRoomUser;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SquareState {
    #[default]
    Open = 0,
    Blocked = 1,
    Seat = 2,
    Bed = 3,
    Rug = 4,
}

impl SquareState {
    fn from_i32(value: i32) -> Self {
        match value {
            1 => Self::Blocked,
            2 => Self::Seat,
            3 => Self::Bed,
            4 => Self::Rug,
            _ => Self::Open,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SquareTrigger {
    pub object_name: String,
    pub goal_x: i32,
    pub goal_y: i32,
    pub step_x: i32,
    pub step_y: i32,
    pub room_state: bool,
    pub room_id: i64,
}

#[derive(Debug, Clone, Default)]
pub struct VirtualRoom {
    pub room_id: i64,
    pub is_publicroom: bool,
    pub floor_items: Vec<FloorItem>,
    pub wall_items: Vec<WallItem>,
    pub sound_machine_id: i64,
    pub lobby: Option<GameLobby>,
    pub publicroom_items: String,
    pub heightmap: String,
    pub has_swimming_pool: bool,
    pub sq_base_state: Vec<Vec<SquareState>>,
    pub sq_state: Vec<Vec<SquareState>>,
    pub sq_item_rot: Vec<Vec<u8>>,
    pub sq_floor_height: Vec<Vec<u8>>,
    pub sq_item_height: Vec<Vec<f64>>,
    pub sq_stack: Vec<Vec<Vec<i64>>>,
    pub sq_unit: Vec<Vec<bool>>,
    pub sq_trigger: Vec<Vec<Option<SquareTrigger>>>,
    pub users: Vec<VirtualRoomUser>,
    pub bots: Vec<VirtualBot>,
    pub active_group_ids: HashSet<i64>,
    pub door_x: i32,
    pub door_y: i32,
    pub door_z: u8,
    pub door_h: i32,
    pub contains_poll: bool,
    pub poll_packet: String,
    pub specialcast_emitter: String,
    pub specialcast_interval_ms: i64,
    pub specialcast_rnd_min: i32,
    pub specialcast_rnd_max: i32,
    pub specialcast_elapsed_ms: i64,
    pub previous_special_cast: String,
    pub cycle_counter: u64,
}

#[derive(Debug, Default)]
pub struct RoomCycleOutcome {
    pub status_packet: Option<String>,
    pub exited_users: Vec<(i64, i64)>,
    pub room_packets: Vec<String>,
    pub user_packets: Vec<(i64, String)>,
    pub ticket_decrements: Vec<i64>,
}

#[derive(Debug, Default)]
pub struct RoomChatOutcome {
    pub recipient_ids: Vec<i64>,
    pub typing_packet: Option<String>,
    pub status_packet: Option<String>,
    pub chat_packet: String,
    pub bot_packets: Vec<String>,
}

#[derive(Debug, Default)]
pub struct RoomWhisperOutcome {
    pub source_user_id: i64,
    pub target_user_id: Option<i64>,
    pub typing_packet: Option<String>,
    pub whisper_packet: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoomKickTarget {
    pub user_id: i64,
    pub room_uid: i64,
}

#[derive(Debug, Default)]
pub struct RoomKickOutcome {
    pub targets: Vec<RoomKickTarget>,
}

#[derive(Debug, Default)]
pub struct RoomMuteOutcome {
    pub user_ids: Vec<i64>,
}

impl VirtualRoom {
    pub async fn load(state: &AppState, room_id: i64, is_publicroom: bool) -> Result<Self> {
        let room_model = state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT model FROM rooms WHERE id = '{}' LIMIT 1",
                room_id
            ))
            .await;
        let door_x = state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT door_x FROM room_modeldata WHERE model = '{}' LIMIT 1",
                room_model
            ))
            .await as i32;
        let door_y = state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT door_y FROM room_modeldata WHERE model = '{}' LIMIT 1",
                room_model
            ))
            .await as i32;
        let door_h = state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT door_h FROM room_modeldata WHERE model = '{}' LIMIT 1",
                room_model
            ))
            .await as i32;
        let door_z = state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT door_z FROM room_modeldata WHERE model = '{}' LIMIT 1",
                room_model
            ))
            .await as u8;
        let heightmap = state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT heightmap FROM room_modeldata WHERE model = '{}' LIMIT 1",
                room_model
            ))
            .await;

        let map_rows: Vec<&str> = heightmap
            .split('\r')
            .filter(|row| !row.is_empty())
            .collect();
        let col_x = map_rows.first().map(|row| row.len()).unwrap_or(0);
        let col_y = map_rows.len();

        let mut sq_state = vec![vec![SquareState::Open; col_y]; col_x];
        let mut sq_floor_height = vec![vec![0u8; col_y]; col_x];
        let mut sq_item_rot = vec![vec![0u8; col_y]; col_x];
        let mut sq_item_height = vec![vec![0.0f64; col_y]; col_x];
        let sq_stack = vec![vec![Vec::new(); col_y]; col_x];
        let mut sq_unit = vec![vec![false; col_y]; col_x];
        let mut sq_trigger = vec![vec![None; col_y]; col_x];

        for (y, row) in map_rows.iter().enumerate() {
            for (x, ch) in row.chars().enumerate() {
                let value = ch.to_string().trim().to_lowercase();
                if value == "x" {
                    sq_state[x][y] = SquareState::Blocked;
                } else {
                    sq_state[x][y] = SquareState::Open;
                    sq_floor_height[x][y] = value.parse::<u8>().unwrap_or(0);
                }
            }
        }

        let mut publicroom_items = String::new();
        let mut floor_items = Vec::new();
        let mut wall_items = Vec::new();
        let mut bots = Vec::new();
        let mut lobby = None;
        let mut has_swimming_pool = false;
        let mut specialcast_emitter = String::new();
        let mut specialcast_interval_ms = 0_i64;
        let mut specialcast_rnd_min = 0_i32;
        let mut specialcast_rnd_max = 0_i32;

        if is_publicroom {
            let public_items_raw = state
                .db
                .run_read_unsafe_string(&format!(
                    "SELECT publicroom_items FROM room_modeldata WHERE model = '{}' LIMIT 1",
                    room_model
                ))
                .await;
            for line in public_items_raw.lines() {
                let item_data: Vec<&str> = line.split_whitespace().collect();
                if item_data.len() < 7 {
                    continue;
                }

                let x = item_data[2].parse::<usize>().unwrap_or(0);
                let y = item_data[3].parse::<usize>().unwrap_or(0);
                let state_id = item_data[6].parse::<i32>().unwrap_or(0);
                if x < col_x && y < col_y {
                    sq_state[x][y] = SquareState::from_i32(state_id);
                    if sq_state[x][y] == SquareState::Seat {
                        sq_item_rot[x][y] = item_data[5].parse::<u8>().unwrap_or(0);
                        sq_item_height[x][y] = 1.0;
                    }
                }

                publicroom_items.push_str(&format!(
                    "{} {} {} {} {} {}\r",
                    item_data[0],
                    item_data[1],
                    item_data[2],
                    item_data[3],
                    item_data[4],
                    item_data[5]
                ));
            }

            let trigger_rows = state
                .db
                .run_read_table(&format!(
                    "SELECT object,x,y,goalx,goaly,stepx,stepy,roomid,state FROM room_modeldata_triggers WHERE model = '{}'",
                    room_model
                ))
                .await
                .unwrap_or_default();
            for row in trigger_rows {
                if row.len() < 9 {
                    continue;
                }
                let x = row[1].parse::<usize>().unwrap_or(0);
                let y = row[2].parse::<usize>().unwrap_or(0);
                if x >= col_x || y >= col_y {
                    continue;
                }
                sq_trigger[x][y] = Some(SquareTrigger {
                    object_name: row[0].clone(),
                    goal_x: row[3].parse::<i32>().unwrap_or(0),
                    goal_y: row[4].parse::<i32>().unwrap_or(0),
                    step_x: row[5].parse::<i32>().unwrap_or(0),
                    step_y: row[6].parse::<i32>().unwrap_or(0),
                    room_id: row[7].parse::<i64>().unwrap_or(0),
                    room_state: row[8] == "1",
                });
            }

            has_swimming_pool = state
                .db
                .check_exists(&format!(
                    "SELECT swimmingpool FROM room_modeldata WHERE model = '{}' AND swimmingpool = '1' LIMIT 1",
                    room_model
                ))
                .await;

            if state
                .db
                .check_exists(&format!(
                    "SELECT specialcast_interval FROM room_modeldata WHERE model = '{}' AND specialcast_interval > 0 LIMIT 1",
                    room_model
                ))
                .await
            {
                let specialcast_row = state
                    .db
                    .run_read_row(&format!(
                        "SELECT specialcast_emitter,specialcast_interval,specialcast_rnd_min,specialcast_rnd_max FROM room_modeldata WHERE model = '{}' LIMIT 1",
                        room_model
                    ))
                    .await?;
                if specialcast_row.len() >= 4 {
                    specialcast_emitter = specialcast_row[0].clone();
                    specialcast_interval_ms = specialcast_row[1].parse::<i64>().unwrap_or(0);
                    specialcast_rnd_min = specialcast_row[2].parse::<i32>().unwrap_or(0);
                    specialcast_rnd_max = specialcast_row[3].parse::<i32>().unwrap_or(0);
                }
            }

            if state
                .db
                .check_exists(&format!(
                    "SELECT id FROM games_lobbies WHERE id = '{}' LIMIT 1",
                    room_id
                ))
                .await
            {
                let settings = state
                    .db
                    .run_read_row(&format!(
                        "SELECT type,rank FROM games_lobbies WHERE id = '{}' LIMIT 1",
                        room_id
                    ))
                    .await?;
                if settings.len() >= 2 {
                    lobby = Some(
                        GameLobby::load(state, room_id, settings[0] == "bb", &settings[1]).await?,
                    );
                }
            }
        } else {
            let item_rows = state
                .db
                .run_read_table(&format!(
                    "SELECT id,tid,x,y,z,h,var,wallpos FROM furniture WHERE roomid = '{}' ORDER BY h ASC",
                    room_id
                ))
                .await?;
            for row in item_rows {
                if row.len() < 8 {
                    continue;
                }
                let item_id = row[0].parse::<i64>().unwrap_or(0);
                let template_id = row[1].parse::<i64>().unwrap_or(0);
                let wall_pos = row[7].clone();
                if wall_pos.is_empty() {
                    floor_items.push(FloorItem::new(
                        item_id,
                        template_id,
                        row[2].parse::<i32>().unwrap_or(0),
                        row[3].parse::<i32>().unwrap_or(0),
                        row[4].parse::<i32>().unwrap_or(0),
                        row[5].parse::<f64>().unwrap_or(0.0),
                        row[6].clone(),
                    ));
                } else {
                    wall_items.push(
                        WallItem::new(state, item_id, template_id, wall_pos, row[6].clone()).await,
                    );
                }
            }
        }

        if door_x >= 0 && door_y >= 0 && (door_x as usize) < col_x && (door_y as usize) < col_y {
            sq_state[door_x as usize][door_y as usize] = SquareState::Open;
        }

        let contains_poll = state
            .db
            .check_exists(&format!(
                "SELECT pid FROM poll WHERE rid = '{}' LIMIT 1",
                room_id
            ))
            .await;
        let poll_packet = if contains_poll {
            room_manager::get_poll(state, room_id).await
        } else {
            String::new()
        };

        let bot_ids = state
            .db
            .run_read_column_i64(&format!(
                "SELECT id FROM roombots WHERE roomid = '{}' ORDER BY id ASC",
                room_id
            ))
            .await
            .unwrap_or_default();
        let mut next_room_uid = 0_i64;
        for bot_id in bot_ids {
            let bot_row = state
                .db
                .run_read_row(&format!(
                    "SELECT name,mission,figure,x,y,z,freeroam,message_noshouting FROM roombots WHERE id = '{}' LIMIT 1",
                    bot_id
                ))
                .await?;
            if bot_row.len() < 8 {
                continue;
            }

            let sayings = state
                .db
                .run_read_column_string(&format!(
                    "SELECT text FROM roombots_texts WHERE id = '{}' AND type = 'say' ORDER BY id ASC",
                    bot_id
                ))
                .await
                .unwrap_or_default();
            let shouts = state
                .db
                .run_read_column_string(&format!(
                    "SELECT text FROM roombots_texts WHERE id = '{}' AND type = 'shout' ORDER BY id ASC",
                    bot_id
                ))
                .await
                .unwrap_or_default();
            let trigger_rows = state
                .db
                .run_read_table(&format!(
                    "SELECT words,replies,serve_replies,serve_item FROM roombots_texts_triggers WHERE id = '{}' ORDER BY id ASC",
                    bot_id
                ))
                .await
                .unwrap_or_default();
            let mut chat_triggers = Vec::new();
            for row in trigger_rows {
                if row.len() < 4 {
                    continue;
                }
                chat_triggers.push(ChatTrigger {
                    words: row[0]
                        .split('}')
                        .map(|entry| entry.trim().to_ascii_lowercase())
                        .filter(|entry| !entry.is_empty())
                        .collect(),
                    replies: row[1]
                        .split('}')
                        .map(|entry| entry.trim().to_string())
                        .filter(|entry| !entry.is_empty())
                        .collect(),
                    serve_replies: row[2]
                        .split('}')
                        .map(|entry| entry.trim().to_string())
                        .filter(|entry| !entry.is_empty())
                        .collect(),
                    serve_item: row[3].clone(),
                });
            }

            let coord_rows = state
                .db
                .run_read_table(&format!(
                    "SELECT x,y FROM roombots_coords WHERE id = '{}' ORDER BY id ASC",
                    bot_id
                ))
                .await
                .unwrap_or_default();
            let mut coords = coord_rows
                .into_iter()
                .filter_map(|row| {
                    if row.len() < 2 {
                        return None;
                    }
                    Some((
                        row[0].parse::<i32>().unwrap_or(0),
                        row[1].parse::<i32>().unwrap_or(0),
                    ))
                })
                .collect::<Vec<_>>();

            let x = bot_row[3].parse::<i32>().unwrap_or(0);
            let y = bot_row[4].parse::<i32>().unwrap_or(0);
            if !coords.iter().any(|coord| *coord == (x, y)) {
                coords.push((x, y));
            }

            let h = if x >= 0
                && y >= 0
                && (x as usize) < sq_floor_height.len()
                && (y as usize) < sq_floor_height[x as usize].len()
            {
                f64::from(sq_floor_height[x as usize][y as usize])
            } else {
                0.0
            };
            if x >= 0
                && y >= 0
                && (x as usize) < sq_unit.len()
                && (y as usize) < sq_unit[x as usize].len()
            {
                sq_unit[x as usize][y as usize] = true;
            }

            bots.push(VirtualBot {
                bot_id,
                room_id,
                room_uid: next_room_uid,
                name: bot_row[0].clone(),
                mission: bot_row[1].clone(),
                figure: bot_row[2].clone(),
                x,
                y,
                h,
                z1: bot_row[5].parse::<u8>().unwrap_or(0),
                z2: bot_row[5].parse::<u8>().unwrap_or(0),
                goal_x: -1,
                goal_y: -1,
                free_roam: bot_row[6] == "1",
                no_shouting_message: bot_row[7].clone(),
                sayings,
                shouts,
                coords,
                chat_triggers,
                ai_cycle_delay: 7 + (next_room_uid as i32 % 5),
                status_manager: crate::virtuals::users::virtual_room_user_status_manager::VirtualRoomUserStatusManager::new(-1, room_id),
                ..VirtualBot::default()
            });
            next_room_uid += 1;
        }

        let mut room = Self {
            room_id,
            is_publicroom,
            floor_items,
            wall_items,
            sound_machine_id: 0,
            lobby,
            publicroom_items,
            heightmap,
            has_swimming_pool,
            sq_base_state: sq_state.clone(),
            sq_state,
            sq_item_rot,
            sq_floor_height,
            sq_item_height,
            sq_stack,
            sq_unit,
            sq_trigger,
            users: Vec::new(),
            bots,
            active_group_ids: HashSet::new(),
            door_x,
            door_y,
            door_z,
            door_h,
            contains_poll,
            poll_packet,
            specialcast_emitter,
            specialcast_interval_ms,
            specialcast_rnd_min,
            specialcast_rnd_max,
            specialcast_elapsed_ms: 0,
            previous_special_cast: String::new(),
            cycle_counter: 0,
        };
        if !room.is_publicroom {
            room.rebuild_floor_item_map(state).await;
            for item in &room.floor_items {
                let template = catalogue_manager::get_template(state, item.template_id).await;
                if template.sprite.starts_with("sound_machine") {
                    room.sound_machine_id = item.id;
                }
            }
        }
        Ok(room)
    }

    pub async fn flooritems_legacy(&self, state: &AppState) -> String {
        if self.is_publicroom {
            return "H".to_string();
        }

        let mut out = String::new();
        for item in &self.floor_items {
            out.push_str(&item.to_legacy_string(state).await);
        }
        out
    }

    pub async fn wallitems_legacy(&self, state: &AppState) -> String {
        if self.is_publicroom {
            return String::new();
        }

        let mut out = String::new();
        for item in &self.wall_items {
            out.push_str(&item.to_legacy_string(state).await);
            out.push('\r');
        }
        out
    }

    pub fn get_free_room_identifier(&self) -> i64 {
        let mut room_uid = 0_i64;
        while self.users.iter().any(|user| user.room_uid == room_uid)
            || self.bots.iter().any(|bot| bot.room_uid == room_uid)
        {
            room_uid += 1;
        }
        room_uid
    }

    pub fn add_room_user(&mut self, mut user: VirtualRoomUser) -> i64 {
        if user.room_uid < 0 {
            user.room_uid = self.get_free_room_identifier();
        }
        let room_uid = user.room_uid;
        if user.x == 0 && user.y == 0 && user.h == 0.0 {
            user.x = self.door_x;
            user.y = self.door_y;
            user.z1 = self.door_z;
            user.z2 = self.door_z;
            user.h = self.door_h as f64;
        }
        if user.x >= 0
            && user.y >= 0
            && (user.x as usize) < self.sq_unit.len()
            && (user.y as usize) < self.sq_unit[user.x as usize].len()
        {
            self.sq_unit[user.x as usize][user.y as usize] = true;
        }
        self.users.push(user);
        room_uid
    }

    pub fn remove_room_user(&mut self, room_uid: i64) {
        if let Some(index) = self.users.iter().position(|user| user.room_uid == room_uid) {
            let user = self.users.remove(index);
            if user.x >= 0
                && user.y >= 0
                && (user.x as usize) < self.sq_unit.len()
                && (user.y as usize) < self.sq_unit[user.x as usize].len()
            {
                self.sq_unit[user.x as usize][user.y as usize] = false;
            }
            if user.group_id > 0
                && !self
                    .users
                    .iter()
                    .any(|entry| entry.group_id == user.group_id)
            {
                self.active_group_ids.remove(&user.group_id);
            }
        }
    }

    pub fn user_list_legacy(&self) -> String {
        let mut out = String::new();
        for user in &self.users {
            out.push_str(&user.details_string());
            out.push('\u{2}');
        }
        for bot in &self.bots {
            out.push_str(&bot.details_string());
            out.push('\u{2}');
        }
        out
    }

    pub fn dynamic_units(&self) -> String {
        self.user_list_legacy()
    }

    pub fn dynamic_statuses(&self) -> String {
        let mut out = String::new();
        for user in &self.users {
            out.push_str(&user.status_string());
            out.push('\r');
        }
        for bot in &self.bots {
            out.push_str(&bot.status_string());
            out.push('\r');
        }
        out
    }

    pub fn activate_group(&mut self, group_id: i64) -> bool {
        group_id > 0 && self.active_group_ids.insert(group_id)
    }

    pub async fn groups_legacy(&self, state: &AppState) -> String {
        let mut group_ids = self.active_group_ids.iter().copied().collect::<Vec<_>>();
        group_ids.sort_unstable();

        let mut out = crate::encoding::jeax_encoding::encode_vl64(group_ids.len() as i32);
        for group_id in group_ids {
            let badge = state
                .db
                .run_read_unsafe_string(&format!(
                    "SELECT badge FROM groups_details WHERE id = '{}' LIMIT 1",
                    group_id
                ))
                .await;
            out.push('I');
            out.push_str(&crate::encoding::jeax_encoding::encode_vl64(
                group_id as i32,
            ));
            out.push_str(&badge);
            out.push('\u{2}');
        }
        out
    }

    pub fn rotate_user(&mut self, user_id: i64, goal_x: i32, goal_y: i32) -> Option<String> {
        let user = self
            .users
            .iter_mut()
            .find(|entry| entry.user_id == user_id)?;
        if user.status_manager.contains_status("sit") || user.status_manager.contains_status("lay")
        {
            return None;
        }

        user.z1 = rotation::calculate(user.x, user.y, goal_x, goal_y) as u8;
        user.z2 = user.z1;
        Some(format!("@b{}\r", user.status_string()))
    }

    pub fn set_user_goal(&mut self, user_id: i64, goal_x: i32, goal_y: i32) {
        if let Some(user) = self.users.iter_mut().find(|entry| entry.user_id == user_id) {
            user.goal_x = goal_x;
            user.goal_y = goal_y;
        }
    }

    pub fn queue_user_single_step(
        &mut self,
        user_id: i64,
        to_x: i32,
        to_y: i32,
        second_refresh: bool,
    ) -> bool {
        let Some(index) = self.users.iter().position(|entry| entry.user_id == user_id) else {
            return false;
        };
        self.move_user_single_step(index, to_x, to_y, second_refresh);
        true
    }

    pub fn teleport_user_to_floor_item(&mut self, user_id: i64, floor_item: &FloorItem) -> bool {
        let Some(index) = self.users.iter().position(|entry| entry.user_id == user_id) else {
            return false;
        };

        if self.users[index].x >= 0
            && self.users[index].y >= 0
            && (self.users[index].x as usize) < self.sq_unit.len()
            && (self.users[index].y as usize) < self.sq_unit[self.users[index].x as usize].len()
        {
            self.sq_unit[self.users[index].x as usize][self.users[index].y as usize] = false;
        }

        if floor_item.x >= 0
            && floor_item.y >= 0
            && (floor_item.x as usize) < self.sq_unit.len()
            && (floor_item.y as usize) < self.sq_unit[floor_item.x as usize].len()
        {
            self.sq_unit[floor_item.x as usize][floor_item.y as usize] = true;
        }

        self.users[index].x = floor_item.x;
        self.users[index].y = floor_item.y;
        self.users[index].h = floor_item.h;
        self.users[index].z1 = floor_item.z;
        self.users[index].z2 = floor_item.z;
        self.users[index].goal_x = -1;
        self.users[index].goal_y = -1;
        self.users[index].walk_lock = false;
        true
    }

    pub fn teleport_user_to_tile(&mut self, user_id: i64, x: i32, y: i32) -> bool {
        let Some(index) = self.users.iter().position(|entry| entry.user_id == user_id) else {
            return false;
        };

        let (Ok(map_x), Ok(map_y)) = (usize::try_from(x), usize::try_from(y)) else {
            return false;
        };
        if map_x >= self.sq_state.len() || map_y >= self.sq_state[map_x].len() {
            return false;
        }
        if self.sq_state[map_x][map_y] == SquareState::Blocked {
            return false;
        }

        if self.users[index].x >= 0
            && self.users[index].y >= 0
            && (self.users[index].x as usize) < self.sq_unit.len()
            && (self.users[index].y as usize) < self.sq_unit[self.users[index].x as usize].len()
        {
            self.sq_unit[self.users[index].x as usize][self.users[index].y as usize] = false;
        }

        self.sq_unit[map_x][map_y] = true;
        self.users[index].x = x;
        self.users[index].y = y;
        self.users[index].h = if self.sq_item_height[map_x][map_y] > 0.0 {
            self.sq_item_height[map_x][map_y]
        } else {
            f64::from(self.sq_floor_height[map_x][map_y])
        };
        if matches!(
            self.sq_state[map_x][map_y],
            SquareState::Seat | SquareState::Bed
        ) {
            self.users[index].z1 = self.sq_item_rot[map_x][map_y];
            self.users[index].z2 = self.sq_item_rot[map_x][map_y];
        }
        self.users[index].goal_x = -1;
        self.users[index].goal_y = -1;
        self.users[index].walk_lock = false;
        self.users[index].special_teleportable = false;
        true
    }

    pub fn set_special_teleportable(&mut self, user_id: i64, enabled: bool) -> bool {
        let Some(user) = self.users.iter_mut().find(|entry| entry.user_id == user_id) else {
            return false;
        };
        user.special_teleportable = enabled;
        true
    }

    pub fn is_special_teleportable(&self, user_id: i64) -> bool {
        self.users
            .iter()
            .find(|entry| entry.user_id == user_id)
            .map(|entry| entry.special_teleportable)
            .unwrap_or(false)
    }

    pub fn set_user_door_goal(&mut self, user_id: i64) {
        if let Some(user) = self.users.iter_mut().find(|entry| entry.user_id == user_id) {
            user.walk_door = true;
            user.goal_x = self.door_x;
            user.goal_y = self.door_y;
        }
    }

    pub fn get_trigger(&self, x: i32, y: i32) -> Option<SquareTrigger> {
        let (Ok(x), Ok(y)) = (usize::try_from(x), usize::try_from(y)) else {
            return None;
        };

        self.sq_trigger
            .get(x)
            .and_then(|column| column.get(y))
            .and_then(|trigger| trigger.clone())
    }

    pub fn user_status_packet(&self, user_id: i64) -> Option<String> {
        self.users
            .iter()
            .find(|entry| entry.user_id == user_id)
            .map(|user| format!("@b{}\r", user.status_string()))
    }

    pub fn user_details_packet(&self, user_id: i64) -> Option<String> {
        self.users
            .iter()
            .find(|entry| entry.user_id == user_id)
            .map(|user| format!("@\\{}", user.details_string()))
    }

    pub fn mark_user_voted(&mut self, user_id: i64) {
        if let Some(user) = self.users.iter_mut().find(|entry| entry.user_id == user_id) {
            user.has_voted = true;
        }
    }

    pub fn voted_user_ids(&self) -> Vec<i64> {
        self.users
            .iter()
            .filter(|entry| entry.has_voted)
            .map(|entry| entry.user_id)
            .collect()
    }

    pub fn room_chat_say(
        &mut self,
        source_user_id: i64,
        message: &str,
        shout: bool,
    ) -> RoomChatOutcome {
        let Some(source_index) = self
            .users
            .iter()
            .position(|entry| entry.user_id == source_user_id)
        else {
            return RoomChatOutcome::default();
        };

        let source_room_uid = self.users[source_index].room_uid;
        let source_x = self.users[source_index].x;
        let source_y = self.users[source_index].y;
        let typing_packet = if self.users[source_index].is_typing {
            self.users[source_index].is_typing = false;
            Some(format!(
                "Ei{}H",
                crate::encoding::jeax_encoding::encode_vl64(source_room_uid as i32)
            ))
        } else {
            None
        };

        let status_packet = if let Some(gesture) = detect_emotion_gesture(message) {
            self.users[source_index]
                .status_manager
                .add_status("gest", gesture);
            self.user_status_packet(source_user_id)
        } else {
            None
        };

        let recipient_ids = if shout {
            self.users
                .iter()
                .map(|entry| entry.user_id)
                .collect::<Vec<_>>()
        } else {
            self.users
                .iter()
                .filter(|entry| (entry.x - source_x).abs() < 6 && (entry.y - source_y).abs() < 6)
                .map(|entry| entry.user_id)
                .collect::<Vec<_>>()
        };
        let chat_packet = format!(
            "{}{}{}\u{2}",
            if shout { "@Z" } else { "@X" },
            crate::encoding::jeax_encoding::encode_vl64(source_room_uid as i32),
            message
        );
        let bot_packets = self.process_bot_chat(source_user_id, message, shout);

        RoomChatOutcome {
            recipient_ids,
            typing_packet,
            status_packet,
            chat_packet,
            bot_packets,
        }
    }

    pub fn room_chat_whisper(
        &mut self,
        source_user_id: i64,
        receiver: &str,
        message: &str,
    ) -> RoomWhisperOutcome {
        let Some(source_index) = self
            .users
            .iter()
            .position(|entry| entry.user_id == source_user_id)
        else {
            return RoomWhisperOutcome::default();
        };

        let source_room_uid = self.users[source_index].room_uid;
        let typing_packet = if self.users[source_index].is_typing {
            self.users[source_index].is_typing = false;
            Some(format!(
                "Ei{}H",
                crate::encoding::jeax_encoding::encode_vl64(source_room_uid as i32)
            ))
        } else {
            None
        };
        let target_user_id = self
            .users
            .iter()
            .find(|entry| entry.username == receiver)
            .map(|entry| entry.user_id);
        let whisper_packet = if target_user_id.is_some() {
            format!(
                "@Y{}{}\u{2}",
                crate::encoding::jeax_encoding::encode_vl64(source_room_uid as i32),
                message
            )
        } else {
            String::new()
        };

        RoomWhisperOutcome {
            source_user_id,
            target_user_id,
            typing_packet,
            whisper_packet,
        }
    }

    pub fn kick_user(&mut self, target_user_id: i64) -> Option<RoomKickTarget> {
        let target = self
            .users
            .iter()
            .find(|entry| entry.user_id == target_user_id)
            .map(|entry| RoomKickTarget {
                user_id: entry.user_id,
                room_uid: entry.room_uid,
            })?;
        self.remove_room_user(target.room_uid);
        Some(target)
    }

    pub fn kick_users(&mut self, caster_user_id: i64, caster_rank: u8) -> RoomKickOutcome {
        let targets = self
            .users
            .iter()
            .filter(|entry| entry.user_id != caster_user_id && entry.rank < caster_rank)
            .map(|entry| RoomKickTarget {
                user_id: entry.user_id,
                room_uid: entry.room_uid,
            })
            .collect::<Vec<_>>();

        for target in &targets {
            self.remove_room_user(target.room_uid);
        }

        RoomKickOutcome { targets }
    }

    pub fn mute_users(&self, caster_user_id: i64, caster_rank: u8) -> RoomMuteOutcome {
        RoomMuteOutcome {
            user_ids: self
                .users
                .iter()
                .filter(|entry| entry.user_id != caster_user_id && entry.rank < caster_rank)
                .map(|entry| entry.user_id)
                .collect(),
        }
    }

    pub fn process_bot_chat(
        &mut self,
        source_user_id: i64,
        message: &str,
        shout: bool,
    ) -> Vec<String> {
        let Some(source_index) = self
            .users
            .iter()
            .position(|entry| entry.user_id == source_user_id)
        else {
            return Vec::new();
        };
        let source_x = self.users[source_index].x;
        let source_y = self.users[source_index].y;
        let mut packets = Vec::new();
        let mut status_updates = String::new();
        let seed = self.cycle_counter
            + SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|value| value.as_millis() as u64)
                .unwrap_or(0);

        for index in 0..self.bots.len() {
            if (self.bots[index].x - source_x).abs() >= 6
                || (self.bots[index].y - source_y).abs() >= 6
            {
                continue;
            }

            if shout {
                if !self.bots[index].no_shouting_message.is_empty()
                    && (seed.wrapping_add(index as u64) % 11 == 0)
                {
                    packets.push(format!(
                        "@X{}{}\u{2}",
                        crate::encoding::jeax_encoding::encode_vl64(
                            self.bots[index].room_uid as i32
                        ),
                        self.bots[index].no_shouting_message
                    ));
                }
                continue;
            }

            let words = message
                .split_whitespace()
                .map(|entry| entry.to_ascii_lowercase())
                .collect::<Vec<_>>();
            let trigger_index = self.bots[index]
                .chat_triggers
                .iter()
                .position(|trigger| words.iter().any(|word| trigger.contains_word(word)));
            let Some(trigger_index) = trigger_index else {
                continue;
            };

            self.bots[index].rotate_to(source_x, source_y);
            status_updates.push_str(&self.bots[index].status_string());
            status_updates.push('\r');

            let trigger = self.bots[index].chat_triggers[trigger_index].clone();
            if !trigger.serve_item.is_empty()
                && self.bots[index].customer_user_id.is_none()
                && let Some(coord) = self.bots[index].closest_walk_coord_to(source_x, source_y)
            {
                self.bots[index].status_manager.remove_status("dance");
                self.bots[index]
                    .status_manager
                    .add_status("carryd", &trigger.serve_item);
                self.bots[index].goal_x = coord.0;
                self.bots[index].goal_y = coord.1;
                self.bots[index].customer_user_id = Some(source_user_id);
                self.bots[index].customer_trigger_index = Some(trigger_index);
                status_updates.push_str(&self.bots[index].status_string());
                status_updates.push('\r');
            }

            let reply = trigger.reply(seed + index as u64);
            if !reply.is_empty() {
                packets.push(format!(
                    "@X{}{}\u{2}",
                    crate::encoding::jeax_encoding::encode_vl64(self.bots[index].room_uid as i32),
                    reply
                ));
            }
        }

        if !status_updates.is_empty() {
            packets.insert(0, format!("@b{}", status_updates));
        }
        packets
    }

    pub fn contains_floor_item(&self, item_id: i64) -> bool {
        self.floor_items.iter().any(|item| item.id == item_id)
    }

    pub fn contains_wall_item(&self, item_id: i64) -> bool {
        self.wall_items.iter().any(|item| item.id == item_id)
    }

    pub fn floor_item(&self, item_id: i64) -> Option<&FloorItem> {
        self.floor_items.iter().find(|item| item.id == item_id)
    }

    pub fn wall_item(&self, item_id: i64) -> Option<&WallItem> {
        self.wall_items.iter().find(|item| item.id == item_id)
    }

    pub fn refresh_coord(&mut self, x: i32, y: i32) -> Option<String> {
        let (Ok(x), Ok(y)) = (usize::try_from(x), usize::try_from(y)) else {
            return None;
        };
        let user = self
            .users
            .iter_mut()
            .find(|entry| entry.x == x as i32 && entry.y == y as i32)?;

        match self
            .sq_state
            .get(x)
            .and_then(|column| column.get(y))
            .copied()
            .unwrap_or(SquareState::Open)
        {
            SquareState::Seat => {
                if !user.status_manager.contains_status("sit") {
                    user.z1 = self.sq_item_rot[x][y];
                    user.z2 = user.z1;
                    user.status_manager
                        .add_status("sit", &format_height(self.sq_item_height[x][y]));
                    return Some(format!("@b{}\r", user.status_string()));
                }
            }
            SquareState::Bed => {
                if !user.status_manager.contains_status("lay") {
                    user.z1 = self.sq_item_rot[x][y];
                    user.z2 = user.z1;
                    user.status_manager
                        .add_status("lay", &format_height(self.sq_item_height[x][y]));
                    return Some(format!("@b{}\r", user.status_string()));
                }
            }
            _ => {
                user.status_manager.remove_status("sit");
                user.status_manager.remove_status("lay");
                user.h = f64::from(self.sq_floor_height[x][y]);
                return Some(format!("@b{}\r", user.status_string()));
            }
        }

        None
    }

    pub async fn place_wall_item(
        &mut self,
        state: &AppState,
        item_id: i64,
        template_id: i64,
        wall_position: String,
        var: String,
    ) -> Result<Option<String>> {
        wall_item_manager::place_item(self, state, item_id, template_id, wall_position, var).await
    }

    pub async fn remove_wall_item(
        &mut self,
        state: &AppState,
        item_id: i64,
        owner_id: i64,
    ) -> Result<Option<String>> {
        wall_item_manager::remove_item(self, state, item_id, owner_id).await
    }

    pub async fn toggle_wall_item(
        &mut self,
        state: &AppState,
        item_id: i64,
        to_status: i32,
    ) -> Result<Option<String>> {
        wall_item_manager::toggle_item(self, state, item_id, to_status).await
    }

    pub async fn place_floor_item(
        &mut self,
        state: &AppState,
        item_id: i64,
        template_id: i64,
        x: i32,
        y: i32,
        z: i32,
        var: String,
        max_stack_height: f64,
    ) -> Result<Vec<String>> {
        floor_item_manager::place_item(
            self,
            state,
            item_id,
            template_id,
            x,
            y,
            z,
            var,
            max_stack_height,
        )
        .await
    }

    pub async fn remove_floor_item(
        &mut self,
        state: &AppState,
        item_id: i64,
        owner_id: i64,
    ) -> Result<Vec<String>> {
        floor_item_manager::remove_item(self, state, item_id, owner_id).await
    }

    pub async fn relocate_floor_item(
        &mut self,
        state: &AppState,
        item_id: i64,
        x: i32,
        y: i32,
        z: i32,
        max_stack_height: f64,
    ) -> Result<Vec<String>> {
        floor_item_manager::relocate_item(self, state, item_id, x, y, z, max_stack_height).await
    }

    pub async fn toggle_floor_item(
        &mut self,
        state: &AppState,
        item_id: i64,
        to_status: String,
        has_rights: bool,
    ) -> Result<Vec<String>> {
        floor_item_manager::toggle_item(self, state, item_id, to_status, has_rights).await
    }

    pub fn process_status_cycle(&mut self) -> RoomCycleOutcome {
        self.cycle_counter = self.cycle_counter.wrapping_add(1);
        let mut status_updates = String::new();
        let mut exited_users = Vec::new();
        let mut room_packets = Vec::new();
        let mut user_packets = Vec::new();
        let mut ticket_decrements = Vec::new();

        for index in 0..self.users.len() {
            if self.users[index].pending_step_commit {
                let second_refresh = self.users[index].pending_step_second_refresh;
                self.users[index].x = self.users[index].pending_step_x;
                self.users[index].y = self.users[index].pending_step_y;
                self.users[index].h = self.users[index].pending_step_h;
                self.users[index].pending_step_x = -1;
                self.users[index].pending_step_y = -1;
                self.users[index].pending_step_h = 0.0;
                self.users[index].pending_step_commit = false;
                self.users[index].status_manager.remove_status("mv");

                if second_refresh
                    && self.sq_state[self.users[index].x as usize][self.users[index].y as usize]
                        == SquareState::Seat
                {
                    let user_x = self.users[index].x as usize;
                    let user_y = self.users[index].y as usize;
                    let sit_height = format_height(self.sq_item_height[user_x][user_y]);
                    self.users[index].status_manager.remove_status("dance");
                    self.users[index].z1 = self.sq_item_rot[user_x][user_y];
                    self.users[index].z2 = self.users[index].z1;
                    self.users[index]
                        .status_manager
                        .add_status("sit", &sit_height);
                }
                self.users[index].pending_step_second_refresh = false;
                if second_refresh {
                    status_updates.push_str(&self.users[index].status_string());
                    status_updates.push('\r');
                    continue;
                }
            }

            if self.users[index].goal_x == -1 {
                continue;
            }

            let goal_x = self.users[index].goal_x;
            let goal_y = self.users[index].goal_y;
            let mut state_map = self.sq_state.clone();

            if let (Ok(goal_x), Ok(goal_y)) = (usize::try_from(goal_x), usize::try_from(goal_y))
                && goal_x < state_map.len()
                && goal_y < state_map[goal_x].len()
            {
                if matches!(
                    state_map[goal_x][goal_y],
                    SquareState::Seat | SquareState::Bed
                ) {
                    state_map[goal_x][goal_y] = SquareState::Open;
                }
                if self.sq_unit[goal_x][goal_y] {
                    state_map[goal_x][goal_y] = SquareState::Blocked;
                }
            }

            let next_coords = Pathfinder::new(&state_map, &self.sq_floor_height, &self.sq_unit)
                .get_next(self.users[index].x, self.users[index].y, goal_x, goal_y);

            self.users[index].status_manager.remove_status("mv");
            if let Some((next_x, next_y)) = next_coords {
                let next_state = self
                    .sq_state
                    .get(next_x as usize)
                    .and_then(|column| column.get(next_y as usize))
                    .copied()
                    .unwrap_or(SquareState::Open);

                self.sq_unit[self.users[index].x as usize][self.users[index].y as usize] = false;
                self.sq_unit[next_x as usize][next_y as usize] = true;
                self.users[index].z1 =
                    rotation::calculate(self.users[index].x, self.users[index].y, next_x, next_y)
                        as u8;
                self.users[index].z2 = self.users[index].z1;
                self.users[index].status_manager.remove_status("sit");
                self.users[index].status_manager.remove_status("lay");

                let next_height = if next_state == SquareState::Rug {
                    self.sq_item_height[next_x as usize][next_y as usize]
                } else {
                    f64::from(self.sq_floor_height[next_x as usize][next_y as usize])
                };

                self.users[index].status_manager.add_status(
                    "mv",
                    &format!("{next_x},{next_y},{}", format_height(next_height)),
                );
                status_updates.push_str(&self.users[index].status_string());
                status_updates.push('\r');

                self.users[index].x = next_x;
                self.users[index].y = next_y;
                self.users[index].h = next_height;

                if next_state == SquareState::Seat {
                    self.users[index].status_manager.remove_status("dance");
                    self.users[index].z1 = self.sq_item_rot[next_x as usize][next_y as usize];
                    self.users[index].z2 = self.users[index].z1;
                    self.users[index].status_manager.add_status(
                        "sit",
                        &format_height(self.sq_item_height[next_x as usize][next_y as usize]),
                    );
                } else if next_state == SquareState::Bed {
                    self.users[index].status_manager.remove_status("dance");
                    self.users[index].z1 = self.sq_item_rot[next_x as usize][next_y as usize];
                    self.users[index].z2 = self.users[index].z1;
                    self.users[index].status_manager.add_status(
                        "lay",
                        &format_height(self.sq_item_height[next_x as usize][next_y as usize]),
                    );
                }
            } else {
                let walk_door = self.users[index].walk_door;
                self.users[index].goal_x = -1;
                self.users[index].goal_y = -1;
                if let Some(trigger) = self.get_trigger(self.users[index].x, self.users[index].y) {
                    if self.has_swimming_pool {
                        if trigger.object_name == "curtains1" || trigger.object_name == "curtains2"
                        {
                            self.users[index].walk_lock = true;
                            user_packets.push((self.users[index].user_id, "A`".to_string()));
                            room_packets.push(format!("AG{} close", trigger.object_name));
                        } else if !self.users[index].swim_outfit.is_empty() {
                            if trigger.object_name == "door" {
                                self.users[index].walk_lock = true;
                                self.users[index].goal_x = -1;
                                self.users[index].goal_y = 0;
                                room_packets.push("AGdoor close".to_string());
                                room_packets.push("A}".to_string());
                                ticket_decrements.push(self.users[index].user_id);
                                self.move_user_single_step(
                                    index,
                                    trigger.step_x,
                                    trigger.step_y,
                                    true,
                                );
                                status_updates.push_str(&self.users[index].status_string());
                                status_updates.push('\r');
                                continue;
                            } else if trigger.object_name.starts_with("Splash") {
                                room_packets.push(format!("AG{}", trigger.object_name));
                                self.users[index].status_manager.drop_carryd_item();
                                if trigger.object_name.get(8..) == Some("enter") {
                                    self.users[index].status_manager.add_status("swim", "");
                                } else {
                                    self.users[index].status_manager.remove_status("swim");
                                }
                                self.move_user_single_step(
                                    index,
                                    trigger.step_x,
                                    trigger.step_y,
                                    false,
                                );
                                self.users[index].goal_x = trigger.goal_x;
                                self.users[index].goal_y = trigger.goal_y;
                                status_updates.push_str(&self.users[index].status_string());
                                status_updates.push('\r');
                                continue;
                            }
                        }
                    }
                } else {
                    self.users[index].walk_door = false;
                    status_updates.push_str(&self.users[index].status_string());
                    status_updates.push('\r');

                    if walk_door {
                        exited_users.push((self.users[index].room_uid, self.users[index].user_id));
                    }
                    continue;
                }

                self.users[index].walk_door = false;
                status_updates.push_str(&self.users[index].status_string());
                status_updates.push('\r');
            }
        }

        let room_seed = self.cycle_counter
            + SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|value| value.as_millis() as u64)
                .unwrap_or(0);
        for index in 0..self.bots.len() {
            if self.bots[index].goal_x == -1 && self.bots[index].customer_user_id.is_none() {
                if self.bots[index].ai_cycle_delay > 0 {
                    self.bots[index].ai_cycle_delay -= 1;
                } else {
                    self.run_bot_ai_action(
                        index,
                        room_seed + index as u64,
                        &mut room_packets,
                        &mut status_updates,
                    );
                    self.bots[index].ai_cycle_delay = 7 + ((room_seed + index as u64) % 5) as i32;
                }
            }

            if self.bots[index].goal_x == -1 {
                continue;
            }

            let goal_x = self.bots[index].goal_x;
            let goal_y = self.bots[index].goal_y;
            let mut state_map = self.sq_state.clone();

            if let (Ok(goal_x), Ok(goal_y)) = (usize::try_from(goal_x), usize::try_from(goal_y))
                && goal_x < state_map.len()
                && goal_y < state_map[goal_x].len()
            {
                if state_map[goal_x][goal_y] == SquareState::Seat {
                    state_map[goal_x][goal_y] = SquareState::Open;
                }
                if self.sq_unit[goal_x][goal_y] {
                    state_map[goal_x][goal_y] = SquareState::Blocked;
                }
            }

            let next_coords = Pathfinder::new(&state_map, &self.sq_floor_height, &self.sq_unit)
                .get_next(self.bots[index].x, self.bots[index].y, goal_x, goal_y);

            self.bots[index].status_manager.remove_status("mv");
            if let Some((next_x, next_y)) = next_coords {
                let next_state = self
                    .sq_state
                    .get(next_x as usize)
                    .and_then(|column| column.get(next_y as usize))
                    .copied()
                    .unwrap_or(SquareState::Open);

                self.sq_unit[self.bots[index].x as usize][self.bots[index].y as usize] = false;
                self.sq_unit[next_x as usize][next_y as usize] = true;
                self.bots[index].z1 =
                    rotation::calculate(self.bots[index].x, self.bots[index].y, next_x, next_y)
                        as u8;
                self.bots[index].z2 = self.bots[index].z1;
                self.bots[index].status_manager.remove_status("sit");

                let next_height = if next_state == SquareState::Rug {
                    self.sq_item_height[next_x as usize][next_y as usize]
                } else {
                    f64::from(self.sq_floor_height[next_x as usize][next_y as usize])
                };

                self.bots[index].status_manager.add_status(
                    "mv",
                    &format!("{next_x},{next_y},{}", format_height(next_height)),
                );
                status_updates.push_str(&self.bots[index].status_string());
                status_updates.push('\r');

                self.bots[index].x = next_x;
                self.bots[index].y = next_y;
                self.bots[index].h = next_height;
                if next_state == SquareState::Seat {
                    self.bots[index].status_manager.remove_status("dance");
                    self.bots[index].z1 = self.sq_item_rot[next_x as usize][next_y as usize];
                    self.bots[index].z2 = self.bots[index].z1;
                    self.bots[index].status_manager.add_status(
                        "sit",
                        &format_height(self.sq_item_height[next_x as usize][next_y as usize]),
                    );
                }
            } else {
                if self.bots[index].x == goal_x && self.bots[index].y == goal_y {
                    self.finish_bot_order(
                        index,
                        room_seed + index as u64,
                        &mut room_packets,
                        &mut status_updates,
                    );
                }
                self.bots[index].goal_x = -1;
                self.bots[index].goal_y = -1;
                status_updates.push_str(&self.bots[index].status_string());
                status_updates.push('\r');
            }
        }

        for (room_uid, _) in &exited_users {
            self.remove_room_user(*room_uid);
        }

        if let Some(cast) = self.next_special_cast() {
            room_packets.push(format!("AG{} {}", self.specialcast_emitter, cast));
        }

        RoomCycleOutcome {
            status_packet: if status_updates.is_empty() {
                None
            } else {
                Some(format!("@b{status_updates}"))
            },
            exited_users,
            room_packets,
            user_packets,
            ticket_decrements,
        }
    }

    fn move_user_single_step(&mut self, index: usize, to_x: i32, to_y: i32, second_refresh: bool) {
        if to_x < 0
            || to_y < 0
            || to_x as usize >= self.sq_unit.len()
            || to_y as usize >= self.sq_unit[to_x as usize].len()
        {
            return;
        }

        self.sq_unit[self.users[index].x as usize][self.users[index].y as usize] = false;
        self.sq_unit[to_x as usize][to_y as usize] = true;
        self.users[index].z1 =
            rotation::calculate(self.users[index].x, self.users[index].y, to_x, to_y) as u8;
        self.users[index].z2 = self.users[index].z1;
        self.users[index].status_manager.remove_status("sit");

        let next_height = if self.sq_state[to_x as usize][to_y as usize] == SquareState::Rug {
            self.sq_item_height[to_x as usize][to_y as usize]
        } else {
            f64::from(self.sq_floor_height[to_x as usize][to_y as usize])
        };

        self.users[index].status_manager.add_status(
            "mv",
            &format!("{to_x},{to_y},{}", format_height(next_height)),
        );
        self.users[index].pending_step_x = to_x;
        self.users[index].pending_step_y = to_y;
        self.users[index].pending_step_h = next_height;
        self.users[index].pending_step_second_refresh = second_refresh;
        self.users[index].pending_step_commit = true;
    }

    pub async fn lobby_player_ranks(&self, state: &AppState) -> String {
        let Some(lobby) = &self.lobby else {
            return String::new();
        };

        let cache = state.rank_cache.read().await;
        let ranks = if lobby.is_battle_ball {
            &cache.game_ranks_bb
        } else {
            &cache.game_ranks_ss
        };

        let mut out = crate::encoding::jeax_encoding::encode_vl64(self.users.len() as i32);
        for user in &self.users {
            out.push_str(&crate::encoding::jeax_encoding::encode_vl64(
                user.room_uid as i32,
            ));
            out.push_str(&user.game_points.to_string());
            out.push('\u{2}');
            let title = ranks
                .iter()
                .find(|rank| {
                    user.game_points >= rank.min_points
                        && (rank.max_points == 0 || user.game_points <= rank.max_points)
                })
                .map(|rank| rank.title.clone())
                .unwrap_or_else(|| "holo.cast.gamerank.null".to_string());
            out.push_str(&title);
            out.push('\u{2}');
        }
        out
    }

    fn run_bot_ai_action(
        &mut self,
        index: usize,
        seed: u64,
        room_packets: &mut Vec<String>,
        status_updates: &mut String,
    ) {
        let action = (seed % 15) as i32;
        match action {
            1 => {
                if let Some((next_x, next_y)) = self.pick_bot_target(index, seed) {
                    if next_x == self.bots[index].x && next_y == self.bots[index].y {
                        let rotation = ((seed / 3) % 8) as u8;
                        if rotation != self.bots[index].z2 {
                            self.bots[index].z1 = rotation;
                            self.bots[index].z2 = rotation;
                            status_updates.push_str(&self.bots[index].status_string());
                            status_updates.push('\r');
                        }
                    } else {
                        self.bots[index].goal_x = next_x;
                        self.bots[index].goal_y = next_y;
                    }
                }
            }
            2 => {
                let mut rotation = ((seed / 5) % 8) as u8;
                if rotation == self.bots[index].z2 {
                    rotation = (rotation + 2) % 8;
                }
                if !self.bots[index].status_manager.contains_status("sit") {
                    self.bots[index].z1 = rotation;
                    self.bots[index].z2 = rotation;
                    status_updates.push_str(&self.bots[index].status_string());
                    status_updates.push('\r');
                }
            }
            3 => {
                if !self.bots[index].shouts.is_empty() {
                    let message = self.bots[index]
                        .shouts
                        .get(self.bots[index].seeded_index(self.bots[index].shouts.len(), seed))
                        .cloned()
                        .unwrap_or_default();
                    if !message.is_empty() {
                        room_packets.push(format!(
                            "@Z{}{}\u{2}",
                            crate::encoding::jeax_encoding::encode_vl64(
                                self.bots[index].room_uid as i32
                            ),
                            message
                        ));
                    }
                }
            }
            4 => {
                if !self.bots[index].sayings.is_empty() {
                    let message = self.bots[index]
                        .sayings
                        .get(self.bots[index].seeded_index(self.bots[index].sayings.len(), seed))
                        .cloned()
                        .unwrap_or_default();
                    if !message.is_empty() {
                        room_packets.push(format!(
                            "@X{}{}\u{2}",
                            crate::encoding::jeax_encoding::encode_vl64(
                                self.bots[index].room_uid as i32
                            ),
                            message
                        ));
                    }
                }
            }
            5 => {
                if seed.is_multiple_of(2) {
                    self.bots[index].status_manager.remove_status("dance");
                    self.bots[index].status_manager.add_status("wave", "");
                } else {
                    self.bots[index].status_manager.add_status("dance", "3");
                }
                status_updates.push_str(&self.bots[index].status_string());
                status_updates.push('\r');
            }
            _ => {}
        }
    }

    fn finish_bot_order(
        &mut self,
        index: usize,
        seed: u64,
        room_packets: &mut Vec<String>,
        status_updates: &mut String,
    ) {
        let Some(customer_user_id) = self.bots[index].customer_user_id else {
            return;
        };
        let Some(trigger_index) = self.bots[index].customer_trigger_index else {
            return;
        };
        let Some(customer_index) = self
            .users
            .iter()
            .position(|entry| entry.user_id == customer_user_id)
        else {
            self.bots[index].customer_user_id = None;
            self.bots[index].customer_trigger_index = None;
            self.bots[index].status_manager.drop_carryd_item();
            return;
        };

        let trigger = self.bots[index]
            .chat_triggers
            .get(trigger_index)
            .cloned()
            .unwrap_or_default();
        self.bots[index].rotate_to(self.users[customer_index].x, self.users[customer_index].y);
        self.bots[index].status_manager.drop_carryd_item();
        status_updates.push_str(&self.bots[index].status_string());
        status_updates.push('\r');

        if !self.users[customer_index]
            .status_manager
            .contains_status("sit")
        {
            self.users[customer_index].z1 = rotation::calculate(
                self.users[customer_index].x,
                self.users[customer_index].y,
                self.bots[index].x,
                self.bots[index].y,
            ) as u8;
            self.users[customer_index].z2 = self.users[customer_index].z1;
        }
        if !trigger.serve_item.is_empty() {
            self.users[customer_index]
                .status_manager
                .add_status("carryd", &trigger.serve_item);
        }
        status_updates.push_str(&self.users[customer_index].status_string());
        status_updates.push('\r');

        let serve_reply = trigger.serve_reply(seed);
        if !serve_reply.is_empty() {
            room_packets.push(format!(
                "@X{}{}\u{2}",
                crate::encoding::jeax_encoding::encode_vl64(self.bots[index].room_uid as i32),
                serve_reply
            ));
        }

        self.bots[index].customer_user_id = None;
        self.bots[index].customer_trigger_index = None;
    }

    fn pick_bot_target(&self, index: usize, seed: u64) -> Option<(i32, i32)> {
        if self.bots[index].free_roam {
            let max_x = self.sq_unit.len();
            let max_y = self.sq_unit.first().map(|column| column.len()).unwrap_or(0);
            for offset in 0..16_u64 {
                let next_x = ((seed + offset) as usize % max_x) as i32;
                let next_y = (((seed / 2) + offset) as usize % max_y) as i32;
                if self.bot_can_stand_at(next_x, next_y) {
                    return Some((next_x, next_y));
                }
            }
            None
        } else {
            let coords = &self.bots[index].coords;
            if coords.is_empty() {
                None
            } else {
                coords
                    .get(self.bots[index].seeded_index(coords.len(), seed))
                    .copied()
                    .filter(|(x, y)| self.bot_can_stand_at(*x, *y))
            }
        }
    }

    fn next_special_cast(&mut self) -> Option<String> {
        if self.specialcast_interval_ms <= 0 || self.specialcast_emitter.is_empty() {
            return None;
        }

        self.specialcast_elapsed_ms += 410;
        if self.specialcast_elapsed_ms < self.specialcast_interval_ms {
            return None;
        }
        self.specialcast_elapsed_ms = 0;

        let min = self.specialcast_rnd_min.min(self.specialcast_rnd_max);
        let max = self.specialcast_rnd_min.max(self.specialcast_rnd_max);
        if max <= 0 {
            return None;
        }

        for offset in 0..16_u64 {
            let span = (max - min + 1).max(1) as u64;
            let rnd = min + ((self.cycle_counter.wrapping_add(offset)) % span) as i32;
            let cast = match self.specialcast_emitter.as_str() {
                "cam1" => match rnd {
                    1 => self
                        .random_room_identifier(self.cycle_counter.wrapping_add(offset))
                        .map(|room_uid| format!("targetcamera {}", room_uid)),
                    2 => Some("setcamera 1".to_string()),
                    3 => Some("setcamera 2".to_string()),
                    _ => None,
                },
                "sf" => Some(rnd.to_string()),
                "lamp" => Some(format!("setlamp {}", rnd)),
                _ => None,
            };

            if let Some(cast) = cast
                && cast != self.previous_special_cast
            {
                self.previous_special_cast = cast.clone();
                return Some(cast);
            }
        }

        None
    }

    fn random_room_identifier(&self, seed: u64) -> Option<i64> {
        let mut ids = self.bots.iter().map(|bot| bot.room_uid).collect::<Vec<_>>();
        ids.extend(self.users.iter().map(|user| user.room_uid));
        if ids.is_empty() {
            None
        } else {
            Some(ids[seed as usize % ids.len()])
        }
    }

    fn bot_can_stand_at(&self, x: i32, y: i32) -> bool {
        let (Ok(map_x), Ok(map_y)) = (usize::try_from(x), usize::try_from(y)) else {
            return false;
        };
        if map_x >= self.sq_state.len() || map_y >= self.sq_state[map_x].len() {
            return false;
        }
        !matches!(
            self.sq_state[map_x][map_y],
            SquareState::Blocked | SquareState::Bed
        )
    }

    pub(crate) async fn rebuild_floor_item_map(&mut self, state: &AppState) {
        self.sq_state = self.sq_base_state.clone();
        self.sq_item_rot = vec![
            vec![0u8; self.sq_floor_height.first().map(Vec::len).unwrap_or(0)];
            self.sq_floor_height.len()
        ];
        self.sq_item_height =
            vec![
                vec![0.0f64; self.sq_floor_height.first().map(Vec::len).unwrap_or(0)];
                self.sq_floor_height.len()
            ];
        self.sq_stack =
            vec![
                vec![Vec::new(); self.sq_floor_height.first().map(Vec::len).unwrap_or(0)];
                self.sq_floor_height.len()
            ];

        self.floor_items.sort_by(|left, right| {
            left.h
                .partial_cmp(&right.h)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.id.cmp(&right.id))
        });

        for item in &self.floor_items {
            let template = catalogue_manager::get_template(state, item.template_id).await;
            let (length, width) = footprint_dimensions(&template, item.z);
            for tile_x in item.x..item.x + width {
                for tile_y in item.y..item.y + length {
                    let (Ok(map_x), Ok(map_y)) = (usize::try_from(tile_x), usize::try_from(tile_y))
                    else {
                        continue;
                    };
                    if map_x >= self.sq_state.len() || map_y >= self.sq_state[map_x].len() {
                        continue;
                    }

                    self.sq_stack[map_x][map_y].push(item.id);
                    self.sq_state[map_x][map_y] = SquareState::from_i32(template.type_id as i32);

                    if template.type_id == 2 || template.type_id == 3 {
                        self.sq_item_height[map_x][map_y] = item.h + template.top_h;
                        self.sq_item_rot[map_x][map_y] = item.z;
                    } else if template.type_id == 4 {
                        self.sq_item_height[map_x][map_y] = item.h;
                    }
                }
            }
        }

        if self.door_x >= 0
            && self.door_y >= 0
            && (self.door_x as usize) < self.sq_state.len()
            && (self.door_y as usize) < self.sq_state[self.door_x as usize].len()
        {
            self.sq_state[self.door_x as usize][self.door_y as usize] = SquareState::Open;
        }
    }

    pub(crate) async fn compute_floor_item_height(
        &self,
        state: &AppState,
        template: &crate::core::state::ItemTemplate,
        x: i32,
        y: i32,
        z: u8,
        max_stack_height: f64,
    ) -> Option<f64> {
        let coords = self.item_footprint_coords(template, x, y, z);
        if coords.is_empty() {
            return None;
        }

        let mut height = f64::from(*self.sq_floor_height.get(x as usize)?.get(y as usize)?);
        if let Some(top_item) = self.top_floor_item_at(x, y) {
            let top_template = catalogue_manager::get_template(state, top_item.template_id).await;
            height = top_item.h + top_template.top_h;
        }

        for (tile_x, tile_y) in &coords {
            if self.sq_unit[*tile_x as usize][*tile_y as usize] && template.type_id != 2 {
                return None;
            }

            match self.sq_state[*tile_x as usize][*tile_y as usize] {
                SquareState::Open => {}
                SquareState::Blocked => {
                    let Some(top_item) = self.top_floor_item_at(*tile_x, *tile_y) else {
                        return None;
                    };
                    let top_template =
                        catalogue_manager::get_template(state, top_item.template_id).await;
                    if top_template.top_h == 0.0
                        || top_template.type_id == 2
                        || top_template.type_id == 3
                    {
                        return None;
                    }
                    height = height.max(top_item.h + top_template.top_h);
                }
                SquareState::Rug => {
                    if let Some(top_item) = self.top_floor_item_at(*tile_x, *tile_y) {
                        height = height.max(top_item.h + 0.1);
                    }
                }
                SquareState::Seat | SquareState::Bed => return None,
            }
        }

        Some(height.min(max_stack_height))
    }

    pub(crate) fn top_floor_item_at(&self, x: i32, y: i32) -> Option<&FloorItem> {
        let (Ok(map_x), Ok(map_y)) = (usize::try_from(x), usize::try_from(y)) else {
            return None;
        };
        let item_id = *self.sq_stack.get(map_x)?.get(map_y)?.last()?;
        self.floor_item(item_id)
    }

    pub(crate) fn item_footprint_coords(
        &self,
        template: &crate::core::state::ItemTemplate,
        x: i32,
        y: i32,
        z: u8,
    ) -> Vec<(i32, i32)> {
        let (length, width) = footprint_dimensions(template, z);
        let mut coords = Vec::new();
        for tile_x in x..x + width {
            for tile_y in y..y + length {
                let (Ok(map_x), Ok(map_y)) = (usize::try_from(tile_x), usize::try_from(tile_y))
                else {
                    return Vec::new();
                };
                if map_x >= self.sq_state.len() || map_y >= self.sq_state[map_x].len() {
                    return Vec::new();
                }
                coords.push((tile_x, tile_y));
            }
        }
        coords
    }

    pub(crate) fn refresh_coord_packets(&mut self, coords: Vec<(i32, i32)>) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut payload = String::new();
        for (x, y) in coords {
            if !seen.insert((x, y)) {
                continue;
            }
            if let Some(packet) = self.refresh_coord(x, y) {
                payload.push_str(packet.trim_start_matches("@b"));
            }
        }
        if payload.is_empty() {
            Vec::new()
        } else {
            vec![format!("@b{}", payload)]
        }
    }
}

fn format_height(value: f64) -> String {
    value.to_string().replace(',', ".")
}

fn footprint_dimensions(template: &crate::core::state::ItemTemplate, z: u8) -> (i32, i32) {
    if z == 2 || z == 6 {
        (template.length as i32, template.width as i32)
    } else {
        (template.width as i32, template.length as i32)
    }
}

#[cfg(test)]
fn is_teleporter_sprite(sprite: &str) -> bool {
    matches!(
        sprite,
        "door" | "doorB" | "doorC" | "doorD" | "teleport_door" | "xmas08_telep" | "ads_cltele"
    )
}

fn detect_emotion_gesture(message: &str) -> Option<&'static str> {
    const GESTURES: [(&str, [&str; 4]); 4] = [
        ("sml", [":)", ":D", "", ""]),
        ("agr", [">:(", ":@", ":/", ""]),
        ("sad", [":(", ":'(", "", ""]),
        ("srp", [":o", ":O", ":0", ""]),
    ];

    for (gesture, patterns) in GESTURES {
        if patterns
            .iter()
            .filter(|pattern| !pattern.is_empty())
            .any(|pattern| message.contains(pattern))
        {
            return Some(gesture);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::{SquareState, VirtualRoom, is_teleporter_sprite};
    use crate::virtuals::users::virtual_room_user::VirtualRoomUser;

    fn build_test_room() -> VirtualRoom {
        VirtualRoom {
            sq_base_state: vec![vec![SquareState::Open; 3]; 3],
            sq_state: vec![vec![SquareState::Open; 3]; 3],
            sq_item_rot: vec![vec![0; 3]; 3],
            sq_floor_height: vec![vec![0; 3]; 3],
            sq_item_height: vec![vec![0.0; 3]; 3],
            sq_stack: vec![vec![Vec::new(); 3]; 3],
            sq_unit: vec![vec![false; 3]; 3],
            sq_trigger: vec![vec![None; 3]; 3],
            ..VirtualRoom::default()
        }
    }

    #[test]
    fn staged_single_step_move_waits_for_follow_up_cycle_before_committing_coords() {
        let mut room = build_test_room();
        let mut user = VirtualRoomUser::new(1, 1, 1);
        user.x = 1;
        user.y = 1;
        room.sq_unit[1][1] = true;
        room.users.push(user);

        room.move_user_single_step(0, 1, 2, true);

        assert_eq!(room.users[0].x, 1);
        assert_eq!(room.users[0].y, 1);
        assert!(room.users[0].pending_step_commit);
        assert!(room.users[0].status_manager.contains_status("mv"));

        let outcome = room.process_status_cycle();

        assert_eq!(room.users[0].x, 1);
        assert_eq!(room.users[0].y, 2);
        assert!(!room.users[0].pending_step_commit);
        assert!(!room.users[0].status_manager.contains_status("mv"));
        assert!(outcome.status_packet.is_some());
    }

    #[test]
    fn staged_non_refresh_step_can_resume_normal_walking_same_cycle() {
        let mut room = build_test_room();
        let mut user = VirtualRoomUser::new(1, 1, 1);
        user.x = 1;
        user.y = 1;
        user.goal_x = 2;
        user.goal_y = 2;
        room.sq_unit[1][1] = true;
        room.users.push(user);

        room.move_user_single_step(0, 1, 2, false);
        let outcome = room.process_status_cycle();

        assert_eq!(room.users[0].x, 2);
        assert_eq!(room.users[0].y, 2);
        assert!(!room.users[0].pending_step_commit);
        assert!(room.users[0].status_manager.contains_status("mv"));
        assert!(outcome.status_packet.is_some());
    }

    #[test]
    fn added_user_details_packet_reflects_door_spawn_coords() {
        let mut room = build_test_room();
        room.door_x = 2;
        room.door_y = 1;
        room.door_z = 4;
        room.door_h = 0;

        let mut user = VirtualRoomUser::new(1, 1, 1);
        user.username = "Jamie".to_string();
        user.figure = "hd-180-1".to_string();
        user.mission = "hello".to_string();
        user.sex = "M".to_string();

        room.add_room_user(user);
        let details_packet = room.user_details_packet(1).unwrap();

        assert!(details_packet.contains("l:2 1 0"));
    }

    #[test]
    fn recognizes_legacy_teleporter_sprites() {
        assert!(is_teleporter_sprite("door"));
        assert!(is_teleporter_sprite("doorB"));
        assert!(is_teleporter_sprite("doorC"));
        assert!(is_teleporter_sprite("doorD"));
        assert!(is_teleporter_sprite("teleport_door"));
        assert!(is_teleporter_sprite("xmas08_telep"));
        assert!(is_teleporter_sprite("ads_cltele"));
        assert!(!is_teleporter_sprite("chair_plasto"));
    }
}
