#[derive(Debug, Clone, Default)]
pub struct GamePlayer {
    pub user_id: i64,
    pub username: String,
    pub mission: String,
    pub figure: String,
    pub sex: String,
    pub room_uid: i64,
    pub team_id: i32,
    pub x: i32,
    pub y: i32,
    pub z: i32,
    pub h: i32,
    pub score: i32,
    pub goal_x: i32,
    pub goal_y: i32,
    pub entering_game: bool,
    pub bb_color_tile: bool,
}

impl GamePlayer {
    pub fn new(user_id: i64, username: String) -> Self {
        Self {
            user_id,
            username,
            sex: "M".to_string(),
            team_id: -1,
            goal_x: -1,
            goal_y: -1,
            ..Self::default()
        }
    }
}
