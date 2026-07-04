use crate::virtuals::users::virtual_room_user_status_manager::VirtualRoomUserStatusManager;

#[derive(Debug, Clone, Default)]
pub struct ChatTrigger {
    pub words: Vec<String>,
    pub replies: Vec<String>,
    pub serve_replies: Vec<String>,
    pub serve_item: String,
}

impl ChatTrigger {
    pub fn contains_word(&self, word: &str) -> bool {
        let trimmed = word.trim_end_matches('?').to_ascii_lowercase();
        self.words.iter().any(|entry| entry == &trimmed)
    }

    pub fn reply(&self, seed: u64) -> String {
        choose_string(&self.replies, seed)
    }

    pub fn serve_reply(&self, seed: u64) -> String {
        choose_string(&self.serve_replies, seed)
    }
}

#[derive(Debug, Clone, Default)]
pub struct VirtualBot {
    pub bot_id: i64,
    pub room_id: i64,
    pub room_uid: i64,
    pub name: String,
    pub mission: String,
    pub figure: String,
    pub x: i32,
    pub y: i32,
    pub h: f64,
    pub z1: u8,
    pub z2: u8,
    pub goal_x: i32,
    pub goal_y: i32,
    pub free_roam: bool,
    pub no_shouting_message: String,
    pub sayings: Vec<String>,
    pub shouts: Vec<String>,
    pub coords: Vec<(i32, i32)>,
    pub chat_triggers: Vec<ChatTrigger>,
    pub customer_user_id: Option<i64>,
    pub customer_trigger_index: Option<usize>,
    pub last_message_id: i32,
    pub ai_cycle_delay: i32,
    pub status_manager: VirtualRoomUserStatusManager,
}

impl VirtualBot {
    pub fn details_string(&self) -> String {
        let mut out = format!(
            "i:{}\ra:-1\rn:{}\rf:{}\rl:{} {} {}\r",
            self.room_uid,
            self.name,
            self.figure,
            self.x,
            self.y,
            format_height(self.h)
        );
        if !self.mission.is_empty() {
            out.push_str(&format!("c:{}\r", self.mission));
        }
        out.push_str("[bot]\r");
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

    pub fn rotate_to(&mut self, x: i32, y: i32) {
        self.z1 =
            crate::virtuals::rooms::pathfinder::rotation::calculate(self.x, self.y, x, y) as u8;
        self.z2 = self.z1;
    }

    pub fn closest_walk_coord_to(&self, x: i32, y: i32) -> Option<(i32, i32)> {
        self.coords
            .iter()
            .min_by_key(|(coord_x, coord_y)| (coord_x - x).abs() + (coord_y - y).abs())
            .copied()
            .filter(|(coord_x, _)| *coord_x >= 0)
    }

    pub fn seeded_index(&self, count: usize, salt: u64) -> usize {
        if count == 0 {
            return 0;
        }
        ((self.bot_id as u64)
            .wrapping_mul(1103515245)
            .wrapping_add(self.room_uid as u64)
            .wrapping_add(salt)) as usize
            % count
    }
}

fn choose_string(values: &[String], seed: u64) -> String {
    if values.is_empty() {
        String::new()
    } else {
        values[seed as usize % values.len()].clone()
    }
}

fn format_height(value: f64) -> String {
    value.to_string().replace(',', ".")
}
