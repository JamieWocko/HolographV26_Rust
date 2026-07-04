use anyhow::Result;
use tracing::info;

use crate::core::state::AppState;
use crate::db::db::Database;

pub async fn init(state: &AppState, lang_extension: &str) -> Result<()> {
    info!(lang_extension, "initializing strings from system_strings");
    let lang_keys = state
        .db
        .run_read_column_string("SELECT stringid FROM system_strings ORDER BY id ASC")
        .await?;
    let lang_vars = state
        .db
        .run_read_column_string(&format!(
            "SELECT var_{} FROM system_strings ORDER BY id ASC",
            lang_extension
        ))
        .await?;

    let mut entries = std::collections::HashMap::new();
    for (index, key) in lang_keys.iter().enumerate() {
        if key.is_empty() {
            continue;
        }
        let value = lang_vars
            .get(index)
            .cloned()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| key.clone());
        entries.insert(key.clone(), value);
    }

    let mut cache = state.string_cache.write().await;
    cache.entries = entries;
    info!(
        string_count = cache.entries.len(),
        "loaded strings from system_strings"
    );
    Ok(())
}

pub async fn init_filter(state: &AppState) -> Result<()> {
    let enable_filter = get_table_entry(state, "wordfilter_enable").await? == "1";
    let censor = get_table_entry(state, "wordfilter_censor").await?;
    let swear_words = if enable_filter {
        state
            .db
            .run_read_column_string("SELECT word FROM wordfilter")
            .await?
    } else {
        Vec::new()
    };

    {
        let mut cache = state.string_cache.write().await;
        cache.swear_words = swear_words.clone();
    }

    let mut runtime = state.runtime_config.write().await;
    runtime.enable_word_filter = enable_filter && !swear_words.is_empty() && !censor.is_empty();
    runtime.filter_censor = censor;
    if enable_filter {
        info!("initializing word filter");
        if runtime.enable_word_filter {
            info!(
                swear_word_count = swear_words.len(),
                censor = %runtime.filter_censor,
                "word filter enabled"
            );
        } else {
            info!(
                "word filter was preferred as enabled but no words and/or replacement found, wordfilter disabled"
            );
        }
    } else {
        info!("word filter disabled");
    }
    Ok(())
}

pub async fn get_string(state: &AppState, string_id: &str) -> Result<String> {
    let cache = state.string_cache.read().await;
    Ok(cache
        .entries
        .get(string_id)
        .cloned()
        .unwrap_or_else(|| string_id.to_string()))
}

pub async fn get_table_entry(state: &AppState, key: &str) -> Result<String> {
    let query = format!(
        "SELECT sval FROM system_config WHERE skey = '{}' LIMIT 1",
        Database::stripslash(key)
    );
    Ok(state.db.run_read_unsafe_string(&query).await)
}

pub async fn welcome_message_enabled(state: &AppState) -> Result<bool> {
    Ok(state.runtime_config.read().await.enable_welcome_message)
}

pub fn get_string_part(input: &str, start_index: usize, length: usize) -> String {
    input.chars().skip(start_index).take(length).collect()
}

pub fn wrap_parameters(params: &[String], start_index: usize) -> String {
    params
        .iter()
        .skip(start_index)
        .cloned()
        .collect::<Vec<_>>()
        .join(" ")
}

pub async fn filter_swearwords(state: &AppState, text: &str) -> String {
    let runtime = state.runtime_config.read().await;
    if !runtime.enable_word_filter {
        return text.to_string();
    }
    let censor = runtime.filter_censor.clone();
    drop(runtime);

    let cache = state.string_cache.read().await;
    let mut filtered = text.to_string();
    for swear_word in &cache.swear_words {
        filtered = replace_case_insensitive(&filtered, swear_word, &censor);
    }
    filtered
}

fn replace_case_insensitive(input: &str, needle: &str, replacement: &str) -> String {
    if needle.is_empty() {
        return input.to_string();
    }

    let lower_input = input.to_lowercase();
    let lower_needle = needle.to_lowercase();
    let mut output = String::new();
    let mut cursor = 0usize;

    while let Some(found) = lower_input[cursor..].find(&lower_needle) {
        let start = cursor + found;
        let end = start + needle.len();
        output.push_str(&input[cursor..start]);
        output.push_str(replacement);
        cursor = end;
    }

    output.push_str(&input[cursor..]);
    output
}
