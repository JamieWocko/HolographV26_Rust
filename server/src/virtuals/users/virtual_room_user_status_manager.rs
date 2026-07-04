use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct VirtualRoomUserStatusManager {
    pub user_id: i64,
    pub room_id: i64,
    pub statuses: HashMap<String, String>,
}

impl VirtualRoomUserStatusManager {
    pub fn new(user_id: i64, room_id: i64) -> Self {
        Self {
            user_id,
            room_id,
            statuses: HashMap::new(),
        }
    }

    pub fn add_status(&mut self, key: &str, value: &str) {
        self.statuses.insert(key.to_string(), value.to_string());
    }

    pub fn remove_status(&mut self, key: &str) {
        self.statuses.remove(key);
    }

    pub fn clear(&mut self) {
        self.statuses.clear();
    }

    pub fn drop_carryd_item(&mut self) {
        self.remove_status("carryd");
        self.remove_status("drink");
    }

    pub fn contains_status(&self, key: &str) -> bool {
        self.statuses.contains_key(key)
    }

    pub fn to_legacy_string(&self) -> String {
        let mut output = String::new();
        for (key, value) in &self.statuses {
            output.push_str(key);
            if !value.is_empty() {
                output.push(' ');
                output.push_str(value);
            }
            output.push('/');
        }
        output
    }
}
