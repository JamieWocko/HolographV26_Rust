use crate::virtuals::users::virtual_room_user_status_manager::VirtualRoomUserStatusManager;

#[derive(Debug, Clone, Default)]
pub struct VirtualRoomUser {
    pub user_id: i64,
    pub room_id: i64,
    pub room_uid: i64,
    pub rank: u8,
    pub x: i32,
    pub y: i32,
    pub h: f64,
    pub z1: u8,
    pub z2: u8,
    pub goal_x: i32,
    pub goal_y: i32,
    pub pending_step_x: i32,
    pub pending_step_y: i32,
    pub pending_step_h: f64,
    pub pending_step_second_refresh: bool,
    pub pending_step_commit: bool,
    pub walk_lock: bool,
    pub walk_door: bool,
    pub has_voted: bool,
    pub swim_outfit: String,
    pub game_points: i64,
    pub username: String,
    pub figure: String,
    pub mission: String,
    pub sex: String,
    pub badges: [String; 5],
    pub group_id: i64,
    pub group_member_rank: i32,
    pub is_typing: bool,
    pub cat_transform: bool,
    pub croc_transform: bool,
    pub dog_transform: bool,
    pub special_teleportable: bool,
    pub status_manager: VirtualRoomUserStatusManager,
}

impl VirtualRoomUser {
    pub fn new(user_id: i64, room_id: i64, room_uid: i64) -> Self {
        Self {
            user_id,
            room_id,
            room_uid,
            goal_x: -1,
            goal_y: -1,
            pending_step_x: -1,
            pending_step_y: -1,
            sex: "M".to_string(),
            status_manager: VirtualRoomUserStatusManager::new(user_id, room_id),
            ..Self::default()
        }
    }

    pub fn details_string(&self) -> String {
        let mut figure = self.figure.clone();
        let mut transform_prefix = String::new();
        if self.cat_transform {
            transform_prefix = "7ui1\u{4}".to_string();
            figure = "1 006 D98961".to_string();
        } else if self.croc_transform {
            transform_prefix = "7ui1\u{4}".to_string();
            figure = "2 006 c8d71d".to_string();
        } else if self.dog_transform {
            transform_prefix = "7ui1\u{4}".to_string();
            figure = "0 005 B0C993".to_string();
        }

        let mut out = format!(
            "i:{}\ra:{}\rn:{}\rf:{}{}\rl:{} {} {}\rc:{}\rs:{}\rb:{}",
            self.room_uid,
            self.user_id,
            self.username,
            transform_prefix,
            figure,
            self.x,
            self.y,
            format_height(self.h),
            self.mission,
            self.sex,
            ""
        );
        if !self.badges[0].is_empty() {
            out.push_str(&format!("1:{}", self.badges[0]));
        }
        if !self.badges[1].is_empty() {
            out.push_str(&format!(",2:{}", self.badges[1]));
        }
        if !self.badges[2].is_empty() {
            out.push_str(&format!(",3:{}", self.badges[2]));
        }
        if !self.badges[3].is_empty() {
            out.push_str(&format!(",4:{}", self.badges[3]));
        }
        if !self.badges[4].is_empty() {
            out.push_str(&format!(",5:{}", self.badges[4]));
        }
        if !self.swim_outfit.is_empty() {
            out.push_str(&format!("\rp:{}", self.swim_outfit));
        }
        if self.group_id > 0 {
            out.push_str(&format!(
                "\rg:{}\rt:{}",
                self.group_id, self.group_member_rank
            ));
        }
        out
    }

    pub fn status_string(&self) -> String {
        format!(
            "{} {},{},{},{},{}/{}",
            self.room_uid,
            self.x,
            self.y,
            format_height(self.h),
            self.z1,
            self.z2,
            self.status_manager.to_legacy_string()
        )
    }
}

fn format_height(value: f64) -> String {
    value.to_string().replace(',', ".")
}
