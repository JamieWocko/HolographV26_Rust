use anyhow::Result;

use crate::core::state::{AppState, GameRank};
use crate::encoding::jeax_encoding::encode_vl64;
use crate::games::game::Game;
use crate::games::game_player::GamePlayer;
use crate::managers::rank_manager;

#[derive(Debug, Clone, Default)]
pub struct GameLobby {
    pub room_id: i64,
    pub is_battle_ball: bool,
    pub rank_title: String,
    pub rank: GameRank,
    pub games: Vec<Game>,
    pub allowed_powerups: Vec<i32>,
}

impl GameLobby {
    pub async fn load(
        state: &AppState,
        room_id: i64,
        is_battle_ball: bool,
        rank_title: &str,
    ) -> Result<Self> {
        let rank = rank_manager::get_game_rank(state, is_battle_ball, rank_title).await;
        let allowed_powerups = if is_battle_ball {
            let powerups = state
                .db
                .run_read_unsafe_string(&format!(
                    "SELECT bb_allowedpowerups FROM games_lobbies WHERE id = '{}' LIMIT 1",
                    room_id
                ))
                .await;

            powerups
                .split(',')
                .filter_map(|entry| entry.parse::<i32>().ok())
                .collect()
        } else {
            Vec::new()
        };

        Ok(Self {
            room_id,
            is_battle_ball,
            rank_title: rank_title.to_string(),
            rank,
            games: Vec::new(),
            allowed_powerups,
        })
    }

    pub fn game_type(&self) -> &'static str {
        if self.is_battle_ball { "bb" } else { "ss" }
    }

    pub fn valid_gamerank(&self, points: i64) -> bool {
        points >= self.rank.min_points
            && (self.rank.max_points == 0 || points <= self.rank.max_points)
    }

    pub fn game_list(&self) -> String {
        let mut amounts = [0_i32; 3];
        let mut list = String::new();

        for game in &self.games {
            list.push_str(&encode_vl64(game.id as i32));
            list.push_str(&game.name);
            list.push('\u{2}');
            list.push_str(&encode_vl64(game.owner.room_uid as i32));
            list.push_str(&game.owner.username);
            list.push('\u{2}');
            if !self.is_battle_ball {
                list.push_str(&encode_vl64(game.left_time as i32));
            }
            list.push_str(&encode_vl64(game.map_id as i32));
            amounts[game.state.as_i32() as usize] += 1;
        }

        if amounts[1] > 0 || amounts[2] > 0 {
            format!(
                "{}{}{}{}",
                encode_vl64(amounts[0]),
                encode_vl64(amounts[1]),
                encode_vl64(amounts[2]),
                list
            )
        } else {
            format!("{}{}", encode_vl64(amounts[0]), list)
        }
    }

    pub fn get_create_game_settings(&self) -> String {
        if !self.is_battle_ball {
            return "#".to_string();
        }

        let mut settings = format!("{}fieldType\u{2}HJIII{}", encode_vl64(4), encode_vl64(5));
        settings.push_str(&format!("numTeams\u{2}HJJIJI{}", encode_vl64(4)));
        settings.push_str("allowedPowerups\u{2}IJ");
        if self.allowed_powerups.is_empty() {
            settings.push('9');
        } else {
            for (index, powerup) in self.allowed_powerups.iter().enumerate() {
                if index > 0 {
                    settings.push(',');
                }
                settings.push_str(&powerup.to_string());
            }
        }
        settings.push_str("\u{2}Hname\u{2}IJ\u{2}H");
        settings
    }

    pub fn allows_powerup(&self, id: i32) -> bool {
        self.allowed_powerups.contains(&id)
    }

    pub fn create_game(
        &mut self,
        owner: GamePlayer,
        name: String,
        map_id: i64,
        team_amount: usize,
        enabled_powerups: Vec<i32>,
        total_time: i64,
        countdown_seconds: i32,
        score_window_restart_game_seconds: i32,
    ) -> i64 {
        let mut game_id = 0_i64;
        while self.games.iter().any(|entry| entry.id == game_id) {
            game_id += 1;
        }

        let game = if self.is_battle_ball {
            Game::new_battle_ball(
                game_id,
                name,
                map_id,
                team_amount,
                enabled_powerups,
                owner,
                total_time,
                countdown_seconds,
                score_window_restart_game_seconds,
            )
        } else {
            Game::new_snow_storm(
                game_id,
                name,
                map_id,
                team_amount,
                total_time,
                owner,
                countdown_seconds,
                score_window_restart_game_seconds,
            )
        };
        self.games.push(game);
        game_id
    }
}

#[cfg(test)]
mod tests {
    use super::GameLobby;
    use crate::core::state::GameRank;
    use crate::games::game::GameState;
    use crate::games::game_player::GamePlayer;

    #[test]
    fn battle_ball_settings_match_legacy_shape() {
        let lobby = GameLobby {
            is_battle_ball: true,
            allowed_powerups: vec![1, 4, 9],
            ..GameLobby::default()
        };
        let settings = lobby.get_create_game_settings();
        assert!(settings.contains("fieldType"));
        assert!(settings.contains("numTeams"));
        assert!(settings.contains("allowedPowerups"));
        assert!(settings.contains("1,4,9"));
    }

    #[test]
    fn game_list_includes_waiting_counts() {
        let mut lobby = GameLobby {
            is_battle_ball: true,
            rank: GameRank {
                title: "A".to_string(),
                min_points: 0,
                max_points: 100,
            },
            ..GameLobby::default()
        };
        let mut owner = GamePlayer::new(1, "owner".to_string());
        owner.room_uid = 4;
        lobby.create_game(owner, "test".to_string(), 2, 2, vec![1], 120, 6, 5);
        let list = lobby.game_list();
        assert!(list.contains("test"));
        assert_eq!(lobby.games[0].state, GameState::Waiting);
    }
}
