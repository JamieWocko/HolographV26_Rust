use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub emulator: EmulatorConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub game_port: u16,
    #[serde(default = "default_game_max_connections")]
    pub game_max_connections: usize,
    #[serde(default)]
    pub mus_port: Option<u16>,
    #[serde(default)]
    pub mus_host: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub host: String,
    #[serde(default = "default_mysql_port")]
    pub port: u16,
    pub name: String,
    pub user: String,
    pub password: String,
    #[serde(default = "default_pool_size")]
    pub max_connections: u32,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct EmulatorConfig {
    #[serde(default = "default_lang_key")]
    pub default_lang: String,
    #[serde(default = "default_welcome_enabled")]
    pub enable_welcome_message: bool,
    #[serde(default = "default_trading_enabled")]
    pub enable_trading: bool,
}

#[derive(Debug, Deserialize)]
struct RawAppConfig {
    #[serde(default)]
    server: Option<ServerConfig>,
    #[serde(default)]
    database: Option<RawDatabaseConfig>,
    #[serde(default)]
    mysql: Option<RawDatabaseConfig>,
    #[serde(default)]
    emulator: EmulatorConfig,
}

#[derive(Debug, Deserialize)]
struct RawDatabaseConfig {
    host: String,
    #[serde(default = "default_mysql_port")]
    port: u16,
    #[serde(alias = "database")]
    name: String,
    #[serde(alias = "username")]
    user: String,
    #[serde(default)]
    password: String,
    #[serde(default = "default_pool_size")]
    max_connections: u32,
}

impl AppConfig {
    pub fn load(workdir: &Path) -> Result<Self> {
        let toml_path = workdir.join("config").join("holograph.toml");
        if toml_path.exists() {
            return Self::load_toml(&toml_path);
        }

        let legacy_path = workdir.join("bin").join("mysql.ini");
        if legacy_path.exists() {
            return Self::load_legacy_ini(&legacy_path);
        }

        bail!(
            "no config found. Expected {} or {}",
            toml_path.display(),
            legacy_path.display()
        );
    }

    fn load_toml(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {}", path.display()))?;
        Self::parse_toml(&raw)
            .with_context(|| format!("failed to parse TOML config {}", path.display()))
    }

    fn load_legacy_ini(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read legacy config file {}", path.display()))?;
        let ini = parse_ini(&raw);
        let mysql = ini
            .get("mysql")
            .context("legacy mysql.ini is missing [mysql] section")?;

        Ok(Self {
            server: ServerConfig {
                game_port: 30000,
                game_max_connections: default_game_max_connections(),
                mus_port: Some(30001),
                mus_host: Some("127.0.0.1".to_string()),
            },
            database: DatabaseConfig {
                host: mysql
                    .get("host")
                    .cloned()
                    .context("legacy mysql.ini missing mysql.host")?,
                port: mysql
                    .get("port")
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(default_mysql_port()),
                name: mysql
                    .get("database")
                    .cloned()
                    .context("legacy mysql.ini missing mysql.database")?,
                user: mysql
                    .get("username")
                    .cloned()
                    .context("legacy mysql.ini missing mysql.username")?,
                password: mysql.get("password").cloned().unwrap_or_default(),
                max_connections: default_pool_size(),
            },
            emulator: EmulatorConfig::default(),
        })
    }

    pub fn default_config_path(workdir: &Path) -> PathBuf {
        workdir.join("config").join("holograph.toml")
    }

    fn parse_toml(raw: &str) -> Result<Self> {
        let parsed: RawAppConfig = toml::from_str(raw)?;
        let server = parsed.server.context(
            "TOML config is missing [server] section required for Holograph-compatible startup",
        )?;
        let database = parsed
            .database
            .or(parsed.mysql)
            .context("TOML config is missing [database] or legacy [mysql] section")?;

        Ok(Self {
            server,
            database: DatabaseConfig {
                host: database.host,
                port: database.port,
                name: database.name,
                user: database.user,
                password: database.password,
                max_connections: database.max_connections,
            },
            emulator: parsed.emulator,
        })
    }
}

fn parse_ini(raw: &str) -> HashMap<String, HashMap<String, String>> {
    let mut data = HashMap::new();
    let mut current = String::new();

    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with(';') || line.starts_with('#') {
            continue;
        }

        if line.starts_with('[') && line.ends_with(']') {
            current = line[1..line.len() - 1].trim().to_string();
            data.entry(current.clone()).or_insert_with(HashMap::new);
            continue;
        }

        if let Some((key, value)) = line.split_once('=') {
            let section = data.entry(current.clone()).or_insert_with(HashMap::new);
            section.insert(key.trim().to_string(), value.trim().to_string());
        }
    }

    data
}

const fn default_mysql_port() -> u16 {
    3306
}

const fn default_game_max_connections() -> usize {
    2_000
}

const fn default_pool_size() -> u32 {
    20
}

fn default_lang_key() -> String {
    "en".to_string()
}

const fn default_welcome_enabled() -> bool {
    true
}

const fn default_trading_enabled() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::AppConfig;

    #[test]
    fn parses_modern_database_section() {
        let config = AppConfig::parse_toml(
            r#"
[server]
game_port = 30000

[database]
host = "127.0.0.1"
port = 3306
name = "holodb"
user = "root"
password = "secret"
max_connections = 15
"#,
        )
        .expect("modern config should parse");

        assert_eq!(config.database.host, "127.0.0.1");
        assert_eq!(config.database.name, "holodb");
        assert_eq!(config.database.user, "root");
        assert_eq!(config.database.password, "secret");
        assert_eq!(config.database.max_connections, 15);
    }

    #[test]
    fn parses_legacy_mysql_section_and_aliases() {
        let config = AppConfig::parse_toml(
            r#"
[server]
game_port = 30000

[mysql]
host = "localhost"
database = "holodb"
username = "holo"
password = "legacy-pass"
"#,
        )
        .expect("legacy-compatible config should parse");

        assert_eq!(config.database.host, "localhost");
        assert_eq!(config.database.port, 3306);
        assert_eq!(config.database.name, "holodb");
        assert_eq!(config.database.user, "holo");
        assert_eq!(config.database.password, "legacy-pass");
        assert_eq!(config.database.max_connections, 20);
    }

    #[test]
    fn defaults_empty_password_when_omitted() {
        let config = AppConfig::parse_toml(
            r#"
[server]
game_port = 30000

[mysql]
host = "localhost"
database = "holodb"
username = "holo"
"#,
        )
        .expect("password should default to empty string");

        assert!(config.database.password.is_empty());
    }
}
