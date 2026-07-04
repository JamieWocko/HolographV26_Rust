use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicUsize, Ordering};

use tokio::sync::{Mutex, RwLock, mpsc, watch};

use crate::core::config::AppConfig;
use crate::db::db::Database;
use crate::virtuals::rooms::virtual_room::VirtualRoom;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub db: Database,
    pub runtime_config: Arc<RwLock<RuntimeConfig>>,
    pub string_cache: Arc<RwLock<StringCache>>,
    pub rank_cache: Arc<RwLock<RankCache>>,
    pub catalogue_cache: Arc<RwLock<CatalogueCache>>,
    pub recycler_cache: Arc<RwLock<RecyclerCache>>,
    pub loaded_rooms: Arc<RwLock<HashMap<i64, VirtualRoom>>>,
    pub online_users: Arc<RwLock<HashMap<i64, OnlineUser>>>,
    pub pending_doorbell_access: Arc<RwLock<HashMap<i64, i64>>>,
    pub denied_doorbell_access: Arc<RwLock<HashMap<i64, i64>>>,
    pub active_trades: Arc<RwLock<HashMap<i64, TradeState>>>,
    pub active_room_cycles: Arc<Mutex<HashSet<i64>>>,
    pub active_game_cycles: Arc<Mutex<HashSet<String>>>,
    pub active_connection_ids: Arc<Mutex<HashSet<usize>>>,
    pub accepted_connections: Arc<AtomicUsize>,
    pub peak_online_users: Arc<AtomicUsize>,
    pub active_rooms: Arc<AtomicI64>,
    pub peak_rooms: Arc<AtomicI64>,
}

impl AppState {
    pub fn new(config: AppConfig, db: Database) -> Self {
        Self {
            config: Arc::new(config),
            db,
            runtime_config: Arc::new(RwLock::new(RuntimeConfig::default())),
            string_cache: Arc::new(RwLock::new(StringCache::default())),
            rank_cache: Arc::new(RwLock::new(RankCache::default())),
            catalogue_cache: Arc::new(RwLock::new(CatalogueCache::default())),
            recycler_cache: Arc::new(RwLock::new(RecyclerCache::default())),
            loaded_rooms: Arc::new(RwLock::new(HashMap::new())),
            online_users: Arc::new(RwLock::new(HashMap::new())),
            pending_doorbell_access: Arc::new(RwLock::new(HashMap::new())),
            denied_doorbell_access: Arc::new(RwLock::new(HashMap::new())),
            active_trades: Arc::new(RwLock::new(HashMap::new())),
            active_room_cycles: Arc::new(Mutex::new(HashSet::new())),
            active_game_cycles: Arc::new(Mutex::new(HashSet::new())),
            active_connection_ids: Arc::new(Mutex::new(HashSet::new())),
            accepted_connections: Arc::new(AtomicUsize::new(0)),
            peak_online_users: Arc::new(AtomicUsize::new(0)),
            active_rooms: Arc::new(AtomicI64::new(0)),
            peak_rooms: Arc::new(AtomicI64::new(0)),
        }
    }

    pub async fn allocate_connection_id(&self) -> Option<usize> {
        let mut ids = self.active_connection_ids.lock().await;
        let max_connections = self.runtime_config.read().await.game_max_connections;
        for id in 1..max_connections {
            if ids.insert(id) {
                self.accepted_connections.fetch_add(1, Ordering::Relaxed);
                return Some(id);
            }
        }

        None
    }

    pub async fn free_connection_id(&self, id: usize) {
        self.active_connection_ids.lock().await.remove(&id);
    }

    pub async fn add_online_user(&self, user_id: i64, user: OnlineUser) {
        let mut users = self.online_users.write().await;
        users.insert(user_id, user);
        let online_count = users.len();
        self.peak_online_users
            .fetch_max(online_count, Ordering::Relaxed);
    }

    pub async fn remove_online_user(&self, user_id: i64) {
        self.online_users.write().await.remove(&user_id);
    }

    pub async fn online_count(&self) -> usize {
        self.online_users.read().await.len()
    }

    pub async fn allow_doorbell_access(&self, user_id: i64, room_id: i64) {
        self.pending_doorbell_access
            .write()
            .await
            .insert(user_id, room_id);
    }

    pub async fn consume_doorbell_access(&self, user_id: i64, room_id: i64) -> bool {
        let mut access = self.pending_doorbell_access.write().await;
        if access.get(&user_id) == Some(&room_id) {
            access.remove(&user_id);
            true
        } else {
            false
        }
    }

    pub async fn clear_doorbell_access(&self, user_id: i64) {
        self.pending_doorbell_access.write().await.remove(&user_id);
    }

    pub async fn mark_doorbell_denied(&self, user_id: i64, room_id: i64) {
        self.denied_doorbell_access
            .write()
            .await
            .insert(user_id, room_id);
    }

    pub async fn consume_doorbell_denied(&self, user_id: i64, room_id: i64) -> bool {
        let mut denied = self.denied_doorbell_access.write().await;
        if denied.get(&user_id) == Some(&room_id) {
            denied.remove(&user_id);
            true
        } else {
            false
        }
    }

    pub async fn clear_doorbell_denied(&self, user_id: i64) {
        self.denied_doorbell_access.write().await.remove(&user_id);
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub game_port: u16,
    pub game_max_connections: usize,
    pub mus_port: Option<u16>,
    pub mus_host: Option<String>,
    pub lang: String,
    pub rooms_loadadvertisement_img: String,
    pub rooms_loadadvertisement_uri: String,
    pub game_countdown_seconds: i32,
    pub game_score_window_restart_game_seconds: i32,
    pub game_battle_ball_game_length_seconds: i64,
    pub enable_trading: bool,
    pub enable_chat_anims: bool,
    pub enable_welcome_message: bool,
    pub enable_word_filter: bool,
    pub filter_censor: String,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            game_port: 30000,
            game_max_connections: 2_000,
            mus_port: Some(30001),
            mus_host: Some("127.0.0.1".to_string()),
            lang: "en".to_string(),
            rooms_loadadvertisement_img: String::new(),
            rooms_loadadvertisement_uri: String::new(),
            game_countdown_seconds: 6,
            game_score_window_restart_game_seconds: 5,
            game_battle_ball_game_length_seconds: 120,
            enable_trading: true,
            enable_chat_anims: false,
            enable_welcome_message: true,
            enable_word_filter: false,
            filter_censor: String::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct StringCache {
    pub entries: HashMap<String, String>,
    pub swear_words: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RankCache {
    pub user_ranks: HashMap<u8, Vec<String>>,
    pub game_ranks_bb: Vec<GameRank>,
    pub game_ranks_ss: Vec<GameRank>,
}

#[derive(Debug, Clone, Default)]
pub struct CatalogueCache {
    pub pages: HashMap<String, CataloguePage>,
    pub item_templates: HashMap<i64, ItemTemplate>,
}

#[derive(Debug, Clone, Default)]
pub struct RecyclerCache {
    pub enabled: bool,
    pub session_length: i64,
    pub session_expire_length: i64,
    pub item_min_ownership_length: i64,
    pub session_rewards: HashMap<i64, i64>,
    pub setup_string: String,
}

#[derive(Debug, Clone, Default)]
pub struct CataloguePage {
    pub display_name: String,
    pub page_data: String,
    pub min_rank: u8,
}

#[derive(Debug, Clone)]
pub struct ItemTemplate {
    pub type_id: u8,
    pub sprite: String,
    pub colour: String,
    pub length: i64,
    pub width: i64,
    pub top_h: f64,
    pub is_door: bool,
    pub is_tradeable: bool,
    pub is_recycleable: bool,
}

impl Default for ItemTemplate {
    fn default() -> Self {
        Self {
            type_id: 1,
            sprite: String::new(),
            colour: "null".to_string(),
            length: 1,
            width: 1,
            top_h: 0.0,
            is_door: false,
            is_tradeable: false,
            is_recycleable: false,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct GameRank {
    pub title: String,
    pub min_points: i64,
    pub max_points: i64,
}

#[derive(Clone)]
pub struct OnlineUser {
    pub connection_id: usize,
    pub user_id: i64,
    pub username: String,
    pub figure: String,
    pub rank: u8,
    pub in_room: bool,
    pub room_id: i64,
    pub room_is_public: bool,
    pub hand_page: Arc<AtomicI32>,
    pub ping_ok: Arc<AtomicBool>,
    pub is_muted: Arc<AtomicBool>,
    pub sender: mpsc::UnboundedSender<String>,
    pub disconnect: watch::Sender<bool>,
}

#[derive(Debug, Clone, Default)]
pub struct TradeState {
    pub partner_user_id: i64,
    pub partner_room_uid: i64,
    pub accepted: bool,
    pub item_ids: Vec<i64>,
}
