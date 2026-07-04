use crate::core::state::AppState;
use crate::encoding::jeax_encoding::encode_vl64;

#[derive(Debug, Clone)]
pub struct VirtualSongEditor {
    pub machine_id: i64,
    pub user_id: i64,
    pub slot: [i64; 4],
}

impl VirtualSongEditor {
    pub fn new(machine_id: i64, user_id: i64) -> Self {
        Self {
            machine_id,
            user_id,
            slot: [0; 4],
        }
    }

    pub async fn load_soundsets(&mut self, state: &AppState) {
        for slot_id in 0..4 {
            self.slot[slot_id] = state
                .db
                .run_read_unsafe_i64(&format!(
                    "SELECT soundmachine_soundset FROM furniture WHERE soundmachine_machineid = '{}' AND soundmachine_slot = '{}' ORDER BY id ASC",
                    self.machine_id,
                    slot_id + 1
                ))
                .await;
        }
    }

    pub async fn add_soundset(&mut self, state: &AppState, sound_set_id: i64, slot_id: i64) {
        self.slot[(slot_id - 1) as usize] = sound_set_id;
        let _ = state
            .db
            .run_query(&format!(
                "UPDATE furniture SET roomid = '-3',soundmachine_machineid = '{}',soundmachine_slot = '{}' WHERE ownerid = '{}' AND soundmachine_soundset = '{}' ORDER BY id ASC LIMIT 1",
                self.machine_id,
                slot_id,
                self.user_id,
                sound_set_id
            ))
            .await;
    }

    pub async fn remove_soundset(&mut self, state: &AppState, slot_id: i64) {
        self.slot[(slot_id - 1) as usize] = 0;
        let _ = state
            .db
            .run_query(&format!(
                "UPDATE furniture SET roomid = '0',soundmachine_machineid = NULL,soundmachine_slot = NULL WHERE ownerid = '{}' AND soundmachine_machineid = '{}' AND soundmachine_slot = '{}' LIMIT 1",
                self.user_id,
                self.machine_id,
                slot_id
            ))
            .await;
    }

    pub fn slot_free(&self, slot_id: i64) -> bool {
        self.slot[(slot_id - 1) as usize] == 0
    }

    pub fn get_soundsets(&self) -> String {
        let mut amount = 0_i32;
        let mut soundsets = String::new();
        for slot_id in 0..4 {
            let sound_set = self.slot[slot_id];
            if sound_set > 0 {
                soundsets.push_str(&encode_vl64((slot_id + 1) as i32));
                soundsets.push_str(&encode_vl64(sound_set as i32));
                soundsets.push_str("QB");
                let mut sample_id = (sound_set * 9) - 8;
                while sample_id <= (sound_set * 9) {
                    soundsets.push_str(&encode_vl64(sample_id as i32));
                    sample_id += 1;
                }
                amount += 1;
            }
        }
        format!("PA{}{}", encode_vl64(amount), soundsets)
    }
}
