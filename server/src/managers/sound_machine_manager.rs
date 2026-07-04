use chrono::Datelike;

use crate::core::state::AppState;
use crate::db::db::Database;
use crate::encoding::jeax_encoding::encode_vl64;

pub async fn get_hand_soundsets(state: &AppState, user_id: i64) -> String {
    let item_ids = state
        .db
        .run_read_column_i64(&format!(
            "SELECT id FROM furniture WHERE ownerid = '{}' AND roomid = '0' AND soundmachine_soundset > 0 ORDER BY id ASC",
            user_id
        ))
        .await
        .unwrap_or_default();
    let mut soundsets = encode_vl64(item_ids.len() as i32);
    if !item_ids.is_empty() {
        let set_ids = state
            .db
            .run_read_column_i64(&format!(
                "SELECT soundmachine_soundset FROM furniture WHERE ownerid = '{}' AND roomid = '0' AND soundmachine_soundset > 0 ORDER BY id ASC",
                user_id
            ))
            .await
            .unwrap_or_default();
        for soundset_id in set_ids {
            soundsets.push_str(&encode_vl64(soundset_id as i32));
        }
    }
    soundsets
}

pub fn calculate_song_length(data: &str) -> i32 {
    let mut song_length = 0_i32;
    let track = data.split(':').collect::<Vec<_>>();
    for i in (1..8).step_by(3) {
        let Some(samples_raw) = track.get(i) else {
            return -1;
        };
        let mut track_length = 0_i32;
        for sample in samples_raw.split(';') {
            let Some((_, length)) = sample.rsplit_once(',') else {
                return -1;
            };
            let Ok(length) = length.parse::<i32>() else {
                return -1;
            };
            track_length += length;
        }
        if track_length > song_length {
            song_length = track_length;
        }
    }
    song_length
}

pub async fn get_machine_song_list(state: &AppState, machine_id: i64) -> String {
    let ids = state
        .db
        .run_read_column_i64(&format!(
            "SELECT id FROM soundmachine_songs WHERE machineid = '{}' ORDER BY id ASC",
            machine_id
        ))
        .await
        .unwrap_or_default();
    let mut songs = encode_vl64(ids.len() as i32);
    if !ids.is_empty() {
        let lengths = state
            .db
            .run_read_column_i64(&format!(
                "SELECT length FROM soundmachine_songs WHERE machineid = '{}' ORDER BY id ASC",
                machine_id
            ))
            .await
            .unwrap_or_default();
        let titles = state
            .db
            .run_read_column_string(&format!(
                "SELECT title FROM soundmachine_songs WHERE machineid = '{}' ORDER BY id ASC",
                machine_id
            ))
            .await
            .unwrap_or_default();
        let burnt_flags = state
            .db
            .run_read_column_string(&format!(
                "SELECT burnt FROM soundmachine_songs WHERE machineid = '{}' ORDER BY id ASC",
                machine_id
            ))
            .await
            .unwrap_or_default();

        for i in 0..ids.len() {
            songs.push_str(&encode_vl64(ids[i] as i32));
            songs.push_str(&encode_vl64(
                lengths.get(i).copied().unwrap_or_default() as i32
            ));
            songs.push_str(titles.get(i).map(String::as_str).unwrap_or_default());
            songs.push('\u{2}');
            songs.push(if burnt_flags.get(i).map(String::as_str) == Some("1") {
                'I'
            } else {
                'H'
            });
        }
    }
    songs
}

pub async fn get_machine_playlist(state: &AppState, machine_id: i64) -> String {
    let ids = state
        .db
        .run_read_column_i64(&format!(
            "SELECT songid FROM soundmachine_playlists WHERE machineid = '{}' ORDER BY pos ASC",
            machine_id
        ))
        .await
        .unwrap_or_default();
    let mut playlist = format!("H{}", encode_vl64(ids.len() as i32));
    for (index, song_id) in ids.iter().enumerate() {
        let title = state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT title FROM soundmachine_songs WHERE id = '{}' LIMIT 1",
                song_id
            ))
            .await;
        let creator_user_id = state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT userid FROM soundmachine_songs WHERE id = '{}' LIMIT 1",
                song_id
            ))
            .await;
        let creator = state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT name FROM users WHERE id = '{}' LIMIT 1",
                creator_user_id
            ))
            .await;
        playlist.push_str(&encode_vl64(*song_id as i32));
        playlist.push_str(&encode_vl64((index + 1) as i32));
        playlist.push_str(&title);
        playlist.push('\u{2}');
        playlist.push_str(&creator);
        playlist.push('\u{2}');
    }
    playlist
}

pub async fn get_song(state: &AppState, song_id: i64) -> String {
    let song_data = state
        .db
        .run_read_row(&format!(
            "SELECT title,data FROM soundmachine_songs WHERE id = '{}' LIMIT 1",
            song_id
        ))
        .await
        .unwrap_or_default();
    if song_data.len() >= 2 {
        format!(
            "{}{}\u{2}{}\u{2}",
            encode_vl64(song_id as i32),
            song_data[0],
            song_data[1]
        )
    } else {
        "holo.cast.soundmachine.song.unknown".to_string()
    }
}

pub fn build_burned_disk_status(song_id: i64, username: &str, length: i64, title: &str) -> String {
    let today = chrono::Local::now().date_naive();
    format!(
        "{}{}\n{}\n{}\n{}\n{}\n{}",
        encode_vl64(song_id as i32),
        Database::stripslash(username),
        today.day(),
        today.month(),
        today.year(),
        length,
        Database::stripslash(title)
    )
}

#[cfg(test)]
mod tests {
    use super::calculate_song_length;

    #[test]
    fn calculate_song_length_matches_legacy_tracks() {
        let data = "0:1,2;2,3:0:0:0,1;1,1:0:0:5,6;1,4";
        assert_eq!(calculate_song_length(data), 10);
    }

    #[test]
    fn calculate_song_length_returns_minus_one_on_invalid_shape() {
        assert_eq!(calculate_song_length("broken"), -1);
    }
}
