use anyhow::Result;

use crate::core::state::AppState;
use crate::db::db::Database;
use crate::encoding::jeax_encoding::encode_vl64;
use crate::games::game_player::GamePlayer;
use crate::virtuals::rooms::pathfinder::{game_pathfinder, rotation};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GameState {
    #[default]
    Waiting,
    Started,
    Ended,
}

#[derive(Debug, Clone, Default)]
pub struct Game {
    pub id: i64,
    pub name: String,
    pub map_id: i64,
    pub total_time: i64,
    pub left_time: i64,
    pub is_battle_ball: bool,
    pub state: GameState,
    pub enabled_powerups: Vec<i32>,
    pub owner: GamePlayer,
    pub subviewers: Vec<GamePlayer>,
    pub teams: Vec<Vec<GamePlayer>>,
    pub battle_ball_tiles: Vec<Vec<(i32, i32)>>,
    pub heightmap: String,
    pub left_countdown_seconds: i32,
    pub team_scores: Vec<i32>,
    pub blocked_tiles: Vec<Vec<bool>>,
    pub tick_phase: bool,
    pub countdown_seconds: i32,
    pub score_window_restart_game_seconds: i32,
}

impl GameState {
    pub fn as_i32(self) -> i32 {
        match self {
            Self::Waiting => 0,
            Self::Started => 1,
            Self::Ended => 2,
        }
    }
}

impl Game {
    pub fn new_battle_ball(
        id: i64,
        name: String,
        map_id: i64,
        team_amount: usize,
        enabled_powerups: Vec<i32>,
        owner: GamePlayer,
        total_time: i64,
        countdown_seconds: i32,
        score_window_restart_game_seconds: i32,
    ) -> Self {
        let mut game = Self {
            id,
            name,
            map_id,
            total_time,
            left_time: total_time,
            is_battle_ball: true,
            state: GameState::Waiting,
            enabled_powerups,
            owner,
            subviewers: Vec::new(),
            teams: vec![Vec::new(); team_amount],
            battle_ball_tiles: Vec::new(),
            heightmap: String::new(),
            left_countdown_seconds: countdown_seconds,
            team_scores: vec![0; team_amount],
            blocked_tiles: Vec::new(),
            tick_phase: false,
            countdown_seconds,
            score_window_restart_game_seconds,
        };
        game.move_player_by_user_id(game.owner.user_id, None, Some(0));
        game
    }

    pub fn new_snow_storm(
        id: i64,
        name: String,
        map_id: i64,
        team_amount: usize,
        total_time: i64,
        owner: GamePlayer,
        countdown_seconds: i32,
        score_window_restart_game_seconds: i32,
    ) -> Self {
        let mut game = Self {
            id,
            name,
            map_id,
            total_time,
            left_time: total_time,
            is_battle_ball: false,
            state: GameState::Waiting,
            enabled_powerups: Vec::new(),
            owner,
            subviewers: Vec::new(),
            teams: vec![Vec::new(); team_amount],
            battle_ball_tiles: Vec::new(),
            heightmap: String::new(),
            left_countdown_seconds: countdown_seconds,
            team_scores: vec![0; team_amount],
            blocked_tiles: Vec::new(),
            tick_phase: false,
            countdown_seconds,
            score_window_restart_game_seconds,
        };
        game.move_player_by_user_id(game.owner.user_id, None, Some(0));
        game
    }

    pub fn move_player_by_user_id(
        &mut self,
        user_id: i64,
        from_team_id: Option<usize>,
        to_team_id: Option<usize>,
    ) {
        if let Some(team_id) = from_team_id
            && team_id < self.teams.len()
        {
            self.teams[team_id].retain(|entry| entry.user_id != user_id);
        }

        let mut player = if self.owner.user_id == user_id {
            self.owner.clone()
        } else {
            self.remove_player(user_id, from_team_id)
                .unwrap_or_else(|| GamePlayer::new(user_id, String::new()))
        };

        if let Some(team_id) = to_team_id {
            if team_id < self.teams.len() {
                player.team_id = team_id as i32;
                if !self.teams[team_id]
                    .iter()
                    .any(|entry| entry.user_id == player.user_id)
                {
                    self.teams[team_id].push(player.clone());
                }
            }
        } else if self.owner.user_id == user_id {
            self.owner.team_id = -1;
        }

        if self.owner.user_id == user_id {
            self.owner = player;
        }
    }

    fn remove_player(&mut self, user_id: i64, from_team_id: Option<usize>) -> Option<GamePlayer> {
        if let Some(team_id) = from_team_id {
            if team_id < self.teams.len()
                && let Some(index) = self.teams[team_id]
                    .iter()
                    .position(|entry| entry.user_id == user_id)
            {
                return Some(self.teams[team_id].remove(index));
            }
        }

        for team in &mut self.teams {
            if let Some(index) = team.iter().position(|entry| entry.user_id == user_id) {
                return Some(team.remove(index));
            }
        }

        None
    }

    pub fn add_subviewer(&mut self, player: GamePlayer) {
        if !self
            .subviewers
            .iter()
            .any(|entry| entry.user_id == player.user_id)
        {
            self.subviewers.push(player);
        }
    }

    pub fn remove_subviewer_by_user_id(&mut self, user_id: i64) {
        self.subviewers.retain(|entry| entry.user_id != user_id);
    }

    pub fn launchable(&self) -> bool {
        self.teams.iter().filter(|team| !team.is_empty()).count() > 1
    }

    pub fn team_has_space(&self, team_id: usize) -> bool {
        let max_members = match self.teams.len() {
            2 => 6,
            3 => 4,
            4 => 3,
            _ => 0,
        };

        self.teams
            .get(team_id)
            .map(|team| team.len() < max_members)
            .unwrap_or(false)
    }

    pub fn sub_payload(&self) -> String {
        let mut entry = format!(
            "{}{}{}{}\u{2}",
            encode_vl64(self.state.as_i32()),
            if self.launchable() { "I" } else { "H" },
            self.name,
            '\u{2}'
        );
        entry.push_str(&encode_vl64(self.owner.room_uid as i32));
        entry.push_str(&self.owner.username);
        entry.push('\u{2}');

        if !self.is_battle_ball {
            entry.push_str(&encode_vl64(self.total_time as i32));
        }

        entry.push_str(&encode_vl64(self.map_id as i32));
        entry.push_str(&encode_vl64(0));
        entry.push_str(&encode_vl64(self.teams.len() as i32));

        for team in &self.teams {
            entry.push_str(&encode_vl64(team.len() as i32));
            for member in team {
                entry.push_str(&encode_vl64(member.room_uid as i32));
                entry.push_str(&member.username);
                entry.push('\u{2}');
            }
        }

        if self.is_battle_ball {
            if self.enabled_powerups.is_empty() {
                entry.push_str("9\u{2}");
            } else {
                for powerup in &self.enabled_powerups {
                    entry.push_str(&format!("{powerup},"));
                }
                entry.push_str("9\u{2}");
            }
        } else {
            entry.push_str(&encode_vl64(self.left_time as i32));
            entry.push_str(&encode_vl64(self.map_id as i32));
        }

        entry
    }

    pub fn get_map(&self, left_countdown_seconds: i32, countdown_seconds: i32) -> String {
        if !self.is_battle_ball {
            return "holo.cast.not_finished".to_string();
        }

        let height = self.battle_ball_tiles.len() as i32;
        let width = self
            .battle_ball_tiles
            .first()
            .map(|row| row.len() as i32)
            .unwrap_or(0);
        let mut setup = format!(
            "I{}{}H{}{}",
            encode_vl64(left_countdown_seconds),
            encode_vl64(countdown_seconds),
            encode_vl64(width),
            encode_vl64(height)
        );

        for row in &self.battle_ball_tiles {
            for (colour, state) in row {
                setup.push_str(&encode_vl64(*colour));
                setup.push_str(&encode_vl64(*state));
            }
        }
        setup.push_str("IH");
        setup
    }

    pub fn get_players(&self, left_countdown_seconds: i32, countdown_seconds: i32) -> String {
        if !self.is_battle_ball {
            return "holo.cast.not_finished".to_string();
        }

        let mut helper = String::new();
        let mut player_count = 0;
        for team in &self.teams {
            for player in team {
                helper.push_str(&format!(
                    "H{}{}{}{}{}HM{}\u{2}{}\u{2}{}\u{2}{}\u{2}{}{}",
                    encode_vl64(player_count),
                    encode_vl64(player.x),
                    encode_vl64(player.y),
                    encode_vl64(player.h),
                    encode_vl64(player.z),
                    player.username,
                    player.mission,
                    player.figure,
                    player.sex,
                    encode_vl64(player.team_id),
                    encode_vl64(player.room_uid as i32)
                ));
                player_count += 1;
            }
        }

        let height = self.battle_ball_tiles.len() as i32;
        let width = self
            .battle_ball_tiles
            .first()
            .map(|row| row.len() as i32)
            .unwrap_or(0);
        let mut setup = format!(
            "I{}{}{}{}{}",
            encode_vl64(left_countdown_seconds),
            encode_vl64(countdown_seconds),
            encode_vl64(player_count),
            helper,
            encode_vl64(width)
        );
        setup.push_str(&encode_vl64(height));
        for row in &self.battle_ball_tiles {
            for (colour, state) in row {
                setup.push_str(&encode_vl64(*colour));
                setup.push_str(&encode_vl64(*state));
            }
        }
        setup.push_str("IH");
        setup
    }

    pub async fn start_game(&mut self, state: &AppState) -> Result<()> {
        self.state = GameState::Started;
        self.left_countdown_seconds = self.countdown_seconds;
        self.tick_phase = false;
        self.team_scores = vec![0; self.teams.len()];

        let lobby_type = if self.is_battle_ball { "bb" } else { "ss" };
        self.heightmap = state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT heightmap FROM games_maps WHERE type = '{}' AND id = '{}' LIMIT 1",
                lobby_type, self.map_id
            ))
            .await;

        if !self.is_battle_ball {
            return Ok(());
        }

        let height_rows: Vec<&str> = self
            .heightmap
            .split('\r')
            .filter(|row| !row.is_empty())
            .collect();
        let tilemap = state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT bb_tilemap FROM games_maps WHERE type = 'bb' AND id = '{}' LIMIT 1",
                self.map_id
            ))
            .await;
        let tile_rows: Vec<&str> = tilemap.split('\r').filter(|row| !row.is_empty()).collect();

        self.battle_ball_tiles.clear();
        let mut blocked = vec![
            vec![false; height_rows.len()];
            height_rows.first().map(|row| row.len()).unwrap_or(0)
        ];
        for (y, row) in height_rows.iter().enumerate() {
            let mut tile_row = Vec::new();
            for (x, ch) in row.chars().enumerate() {
                if ch == 'x' {
                    blocked[x][y] = true;
                    tile_row.push((-2, 4));
                } else if tile_rows
                    .get(y)
                    .and_then(|tile_row| tile_row.chars().nth(x))
                    .unwrap_or('0')
                    == '1'
                {
                    tile_row.push((-1, 0));
                } else {
                    tile_row.push((-2, 4));
                }
            }
            self.battle_ball_tiles.push(tile_row);
        }

        let mut room_uid = 0_i64;
        for team_id in 0..self.teams.len() {
            if self.teams[team_id].is_empty() {
                continue;
            }

            let spawn = state
                .db
                .run_read_row(&format!(
                    "SELECT x,y,z FROM games_maps_playerspawns WHERE type = '{}' AND mapid = '{}' AND teamid = '{}' LIMIT 1",
                    lobby_type, self.map_id, team_id
                ))
                .await?;
            if spawn.len() < 3 {
                continue;
            }

            let mut spawn_x = spawn[0].parse::<i32>().unwrap_or(0);
            let mut spawn_y = spawn[1].parse::<i32>().unwrap_or(0);
            let spawn_z = spawn[2].parse::<i32>().unwrap_or(0);
            let mut flip = false;

            for player in &mut self.teams[team_id] {
                player.room_uid = room_uid;

                let mut safe_guard = 0;
                while spawn_x >= 0
                    && spawn_y >= 0
                    && (spawn_x as usize) < blocked.len()
                    && (spawn_y as usize) < blocked[spawn_x as usize].len()
                    && blocked[spawn_x as usize][spawn_y as usize]
                    && safe_guard < 50
                {
                    if spawn_z == 0 || spawn_z == 2 {
                        if flip {
                            spawn_x -= 1;
                        } else {
                            spawn_x += 1;
                        }
                    } else if spawn_z == 4 || spawn_z == 6 {
                        if flip {
                            spawn_y -= 1;
                        } else {
                            spawn_y += 1;
                        }
                    } else {
                        spawn_x += 1;
                    }
                    flip = !flip;
                    safe_guard += 1;
                }

                if spawn_x >= 0
                    && spawn_y >= 0
                    && (spawn_x as usize) < blocked.len()
                    && (spawn_y as usize) < blocked[spawn_x as usize].len()
                {
                    blocked[spawn_x as usize][spawn_y as usize] = true;
                    player.x = spawn_x;
                    player.y = spawn_y;
                    player.z = spawn_z;
                    player.h = height_rows
                        .get(spawn_y as usize)
                        .and_then(|row| row.chars().nth(spawn_x as usize))
                        .and_then(|value| value.to_digit(10))
                        .unwrap_or(0) as i32;
                }
                player.goal_x = -1;
                player.goal_y = -1;
                player.entering_game = true;
                room_uid += 1;
            }
        }

        self.blocked_tiles = blocked;

        Ok(())
    }

    pub fn tick(&mut self) -> Option<String> {
        if !self.is_battle_ball || self.state != GameState::Started {
            return None;
        }

        let team_amount = self.teams.len();
        let active_team_amount = self.teams.iter().filter(|team| !team.is_empty()).count() as i32;
        let mut amounts = [0_i32; 3];
        let mut players = String::new();
        let mut updated_tiles = String::new();
        let mut movements = String::new();

        for team_id in 0..team_amount {
            for player in &mut self.teams[team_id] {
                players.push_str(&format!(
                    "H{}{}{}{}{}HM",
                    encode_vl64(player.room_uid as i32),
                    encode_vl64(player.x),
                    encode_vl64(player.y),
                    encode_vl64(player.h),
                    encode_vl64(player.z)
                ));

                if player.bb_color_tile {
                    player.bb_color_tile = false;
                    if let Some((colour, state)) = self
                        .battle_ball_tiles
                        .get(player.y as usize)
                        .and_then(|row| row.get(player.x as usize))
                        .copied()
                    {
                        updated_tiles.push_str(&format!(
                            "{}{}{}{}",
                            encode_vl64(player.x),
                            encode_vl64(player.y),
                            encode_vl64(colour),
                            encode_vl64(state)
                        ));
                        amounts[1] += 1;
                    }
                }

                if player.goal_x != -1 {
                    let next = game_pathfinder::get_next_step(
                        player.x,
                        player.y,
                        player.goal_x,
                        player.goal_y,
                    );
                    let next_x = next.x;
                    let next_y = next.y;
                    let can_move = next_x >= 0
                        && next_y >= 0
                        && (next_x as usize) < self.blocked_tiles.len()
                        && (next_y as usize) < self.blocked_tiles[next_x as usize].len()
                        && !self.blocked_tiles[next_x as usize][next_y as usize];

                    if can_move {
                        amounts[2] += 1;
                        movements.push_str(&format!(
                            "J{}{}{}",
                            encode_vl64(player.room_uid as i32),
                            encode_vl64(next_x),
                            encode_vl64(next_y)
                        ));
                        if next_x == player.goal_x && next_y == player.goal_y {
                            player.goal_x = -1;
                            player.goal_y = -1;
                        }

                        self.blocked_tiles[player.x as usize][player.y as usize] = false;
                        self.blocked_tiles[next_x as usize][next_y as usize] = true;
                        player.z = rotation::calculate(player.x, player.y, next_x, next_y);
                        player.x = next_x;
                        player.y = next_y;
                        player.h = self
                            .heightmap
                            .split('\r')
                            .filter(|row| !row.is_empty())
                            .nth(next_y as usize)
                            .and_then(|row| row.chars().nth(next_x as usize))
                            .and_then(|value| value.to_digit(10))
                            .unwrap_or(0) as i32;

                        if let Some(tile) = self
                            .battle_ball_tiles
                            .get_mut(next_y as usize)
                            .and_then(|row| row.get_mut(next_x as usize))
                        {
                            if tile.1 != 4 {
                                if tile.0 == team_id as i32 {
                                    tile.1 += 1;
                                } else {
                                    tile.1 = 1;
                                    tile.0 = team_id as i32;
                                }

                                let extra_points = match tile.1 {
                                    1 => active_team_amount,
                                    2 => active_team_amount * 3,
                                    3 => active_team_amount * 5,
                                    4 => active_team_amount * 7,
                                    _ => 0,
                                };
                                self.team_scores[team_id] += extra_points;
                                player.score += extra_points;
                                player.bb_color_tile = true;
                            }
                        }
                    } else {
                        player.goal_x = -1;
                        player.goal_y = -1;
                    }
                }

                amounts[0] += 1;
            }
        }

        let mut score_string = format!("H{}", encode_vl64(team_amount as i32));
        for score in &self.team_scores {
            score_string.push_str(&encode_vl64(*score));
        }

        if self.tick_phase {
            self.left_time -= 1;
        }
        self.tick_phase = !self.tick_phase;

        Some(format!(
            "Ct{}{}{}{}{}I{}{}",
            encode_vl64(amounts[0]),
            players,
            encode_vl64(amounts[1]),
            updated_tiles,
            score_string,
            encode_vl64(amounts[2]),
            movements
        ))
    }

    pub fn end_scoreboard(&mut self) -> String {
        self.state = GameState::Ended;
        let team_amount = self.teams.len();
        let mut scores = format!(
            "Cx{}{}",
            encode_vl64(self.score_window_restart_game_seconds),
            encode_vl64(team_amount as i32)
        );

        for team_id in 0..team_amount {
            let members = &self.teams[team_id];
            if members.is_empty() {
                scores.push('M');
                continue;
            }

            scores.push_str(&encode_vl64(members.len() as i32));
            for player in members {
                scores.push_str(&encode_vl64(player.room_uid as i32));
                if player.username.is_empty() {
                    scores.push('M');
                } else {
                    scores.push_str(&player.username);
                    scores.push('\u{2}');
                }
                scores.push_str(&encode_vl64(player.score));
            }
            scores.push_str(&encode_vl64(*self.team_scores.get(team_id).unwrap_or(&0)));
        }

        scores
    }

    pub async fn finish_game(&mut self, state: &AppState) -> Result<String> {
        if self.is_battle_ball {
            for team in &self.teams {
                for player in team {
                    state
                        .db
                        .run_query(&format!(
                            "UPDATE users SET bb_playedgames = bb_playedgames + 1,bb_totalpoints = bb_totalpoints + {} WHERE id = '{}' LIMIT 1",
                            player.score,
                            Database::stripslash(&player.user_id.to_string())
                        ))
                        .await?;
                }
            }
        }

        Ok(self.end_scoreboard())
    }
}

#[cfg(test)]
mod tests {
    use super::{Game, GameState};
    use crate::games::game_player::GamePlayer;

    #[test]
    fn game_launchable_requires_two_non_empty_teams() {
        let owner = GamePlayer {
            user_id: 1,
            username: "owner".to_string(),
            room_uid: 5,
            ..GamePlayer::new(1, "owner".to_string())
        };
        let mut game =
            Game::new_battle_ball(1, "bb".to_string(), 2, 2, vec![1, 2], owner, 120, 6, 5);
        assert!(!game.launchable());

        let mut second = GamePlayer::new(2, "guest".to_string());
        second.room_uid = 6;
        game.teams[1].push(second);
        assert!(game.launchable());
        assert_eq!(game.state, GameState::Waiting);
    }

    #[test]
    fn battle_ball_uses_runtime_timing_values() {
        let owner = GamePlayer::new(1, "owner".to_string());
        let game = Game::new_battle_ball(1, "bb".to_string(), 2, 2, vec![1], owner, 180, 9, 7);

        assert_eq!(game.left_countdown_seconds, 9);
        assert_eq!(game.countdown_seconds, 9);
        assert_eq!(game.score_window_restart_game_seconds, 7);
        assert_eq!(game.total_time, 180);
    }

    #[test]
    fn moving_owner_between_teams_removes_previous_membership() {
        let owner = GamePlayer {
            user_id: 1,
            username: "owner".to_string(),
            room_uid: 5,
            ..GamePlayer::new(1, "owner".to_string())
        };
        let mut game =
            Game::new_battle_ball(1, "bb".to_string(), 2, 2, vec![1, 2], owner, 120, 6, 5);

        game.move_player_by_user_id(1, Some(0), Some(1));

        assert!(game.teams[0].iter().all(|entry| entry.user_id != 1));
        assert_eq!(
            game.teams[1]
                .iter()
                .filter(|entry| entry.user_id == 1)
                .count(),
            1
        );
        assert_eq!(game.owner.team_id, 1);
    }
}
