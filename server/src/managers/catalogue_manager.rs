use anyhow::Result;
use tracing::info;

use crate::core::state::{AppState, CataloguePage, ItemTemplate};
use crate::db::db::Database;
use crate::managers::string_manager;

pub async fn init(state: &AppState) -> Result<()> {
    info!("starting caching of catalogue + items");
    let page_ids = state
        .db
        .run_read_column_i64("SELECT indexid FROM catalogue_pages ORDER BY indexid")
        .await
        .unwrap_or_default();

    let mut pages = std::collections::HashMap::new();
    let mut templates = std::collections::HashMap::new();

    for page_id in page_ids {
        cache_page(state, page_id, &mut pages, &mut templates).await?;
    }
    cache_page(state, -1, &mut pages, &mut templates).await?;

    let mut cache = state.catalogue_cache.write().await;
    cache.pages = pages;
    cache.item_templates = templates;
    info!(
        catalogue_pages = cache.pages.len(),
        item_templates = cache.item_templates.len(),
        "successfully cached catalogue pages and item templates"
    );
    Ok(())
}

pub async fn get_template(state: &AppState, template_id: i64) -> ItemTemplate {
    state
        .catalogue_cache
        .read()
        .await
        .item_templates
        .get(&template_id)
        .cloned()
        .unwrap_or_default()
}

pub async fn get_page_index(state: &AppState, user_rank: u8) -> String {
    let page_names = state
        .db
        .run_read_column_string(&format!(
            "SELECT indexname FROM catalogue_pages WHERE minrank <= '{}' ORDER BY indexid ASC",
            user_rank
        ))
        .await
        .unwrap_or_default();

    let cache = state.catalogue_cache.read().await;
    let mut out = String::new();
    for page_name in page_names {
        if let Some(page) = cache.pages.get(&page_name) {
            out.push_str(&page_name);
            out.push('\t');
            out.push_str(&page.display_name);
            out.push('\r');
        }
    }
    out
}

pub async fn get_page(state: &AppState, page_name: &str, user_rank: u8) -> String {
    let cache = state.catalogue_cache.read().await;
    match cache.pages.get(page_name) {
        Some(page) if user_rank >= page.min_rank => page.page_data.clone(),
        Some(_) => "holo.cast.catalogue.access_denied".to_string(),
        None => "cast_catalogue.access_denied".to_string(),
    }
}

pub async fn page_exists(state: &AppState, page_name: &str) -> bool {
    state
        .catalogue_cache
        .read()
        .await
        .pages
        .contains_key(page_name)
}

pub async fn last_item_id(state: &AppState) -> i64 {
    state
        .db
        .run_read_unsafe_i64("SELECT MAX(id) FROM furniture LIMIT 1")
        .await
}

pub async fn handle_purchase(
    state: &AppState,
    template_id: i64,
    receiver_id: i64,
    room_id: i64,
    decor_id: &str,
    present_box_id: i64,
) -> Result<()> {
    let template = get_template(state, template_id).await;
    let sprite = template.sprite.as_str();
    let mut handle_present_box = true;

    match sprite {
        "landscape" | "wallpaper" | "floor" => {
            let item_id = last_item_id(state).await;
            state
                .db
                .run_query(&format!(
                    "UPDATE furniture SET var = '{}' WHERE id = '{}' LIMIT 1",
                    Database::stripslash(decor_id),
                    item_id
                ))
                .await?;
        }
        "roomdimmer" => {
            let item_id = last_item_id(state).await;
            let default_preset = "1,#000000,155";
            let default_set_preset = "1,1,1,#000000,155";
            state
                .db
                .run_query(&format!(
                    "INSERT INTO furniture_moodlight(id,roomid,preset_cur,preset_1,preset_2,preset_3) VALUES ('{}','0','1','{}','{}','{}')",
                    item_id, default_preset, default_preset, default_preset
                ))
                .await?;
            state
                .db
                .run_query(&format!(
                    "UPDATE furniture SET var = '{}' WHERE id = '{}' LIMIT 1",
                    default_set_preset, item_id
                ))
                .await?;
        }
        "door" | "doorB" | "doorC" | "doorD" | "teleport_door" | "xmas08_telep" | "ads_cltele" => {
            let item_id_1 = last_item_id(state).await;
            state
                .db
                .run_query(&format!(
                    "INSERT INTO furniture(tid,ownerid,roomid,teleportid) VALUES ('{}','{}','{}','{}')",
                    template_id, receiver_id, room_id, item_id_1
                ))
                .await?;
            let item_id_2 = last_item_id(state).await;
            state
                .db
                .run_query(&format!(
                    "UPDATE furniture SET teleportid = '{}' WHERE id = '{}' LIMIT 1",
                    item_id_2, item_id_1
                ))
                .await?;
            if present_box_id > 0 {
                state
                    .db
                    .run_query(&format!(
                        "INSERT INTO furniture_presents(id,itemid) VALUES ('{}','{}')",
                        present_box_id, item_id_1
                    ))
                    .await?;
                state
                    .db
                    .run_query(&format!(
                        "INSERT INTO furniture_presents(id,itemid) VALUES ('{}','{}')",
                        present_box_id, item_id_2
                    ))
                    .await?;
            }
            handle_present_box = false;
        }
        "post.it" | "post.it.vd" => {
            let item_id = last_item_id(state).await;
            state
                .db
                .run_query(&format!(
                    "UPDATE furniture SET var = '20' WHERE id = '{}' LIMIT 1",
                    item_id
                ))
                .await?;
        }
        _ => {
            if string_manager::get_string_part(sprite, 0, 10) == "sound_set_" {
                let item_id = last_item_id(state).await;
                let sound_set = sprite.get(10..).unwrap_or("0").parse::<i64>().unwrap_or(0);
                state
                    .db
                    .run_query(&format!(
                        "UPDATE furniture SET soundmachine_soundset = '{}' WHERE id = '{}' LIMIT 1",
                        sound_set, item_id
                    ))
                    .await?;
            }
        }
    }

    if present_box_id > 0 && handle_present_box {
        state
            .db
            .run_query(&format!(
                "INSERT INTO furniture_presents(id,itemid) VALUES ('{}','{}')",
                present_box_id,
                last_item_id(state).await
            ))
            .await?;
    }

    Ok(())
}

pub async fn trade_item_list(state: &AppState, item_ids: &[i64]) -> String {
    let sep = '\u{1e}';
    let mut out = String::new();
    for (index, item_id) in item_ids.iter().enumerate() {
        if *item_id == 0 {
            continue;
        }

        let template_id = state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT tid FROM furniture WHERE id = '{}' LIMIT 1",
                item_id
            ))
            .await;
        let template = get_template(state, template_id).await;
        out.push_str(&format!("SI{item_id}{sep}{index}{sep}"));
        out.push(if template.type_id > 0 { 'S' } else { 'I' });
        out.push_str(&format!("{sep}{item_id}{sep}{}{sep}", template.sprite));
        if template.type_id > 0 {
            let var = state
                .db
                .run_read_unsafe_string(&format!(
                    "SELECT var FROM furniture WHERE id = '{}' LIMIT 1",
                    item_id
                ))
                .await;
            out.push_str(&format!(
                "{}{sep}{}{sep}{}{sep}",
                template.length, template.width, var
            ));
        }
        out.push_str(&format!("{}{sep}{}{sep}/", template.colour, index));
    }
    out
}

pub fn wall_position_ok(wall_position: &str) -> String {
    let pos: Vec<&str> = wall_position.split(' ').collect();
    if pos.len() != 3 {
        return String::new();
    }
    if pos[2] != "l" && pos[2] != "r" {
        return String::new();
    }

    let Some(width_part) = pos[0].strip_prefix(":w=") else {
        return String::new();
    };
    let Some(length_part) = pos[1].strip_prefix("l=") else {
        return String::new();
    };

    let width: Vec<&str> = width_part.split(',').collect();
    let length: Vec<&str> = length_part.split(',').collect();
    if width.len() != 2 || length.len() != 2 {
        return String::new();
    }

    let width_x = width[0].parse::<i32>().ok();
    let width_y = width[1].parse::<i32>().ok();
    let length_x = length[0].parse::<i32>().ok();
    let length_y = length[1].parse::<i32>().ok();

    let Some(width_x) = width_x else {
        return String::new();
    };
    let Some(width_y) = width_y else {
        return String::new();
    };
    let Some(length_x) = length_x else {
        return String::new();
    };
    let Some(length_y) = length_y else {
        return String::new();
    };

    if !(0..=200).contains(&width_x)
        || !(0..=200).contains(&width_y)
        || !(0..=200).contains(&length_x)
        || !(0..=200).contains(&length_y)
    {
        return String::new();
    }

    format!(
        ":w={},{} l={},{} {}",
        width_x, width_y, length_x, length_y, pos[2]
    )
}

async fn cache_page(
    state: &AppState,
    page_id: i64,
    pages: &mut std::collections::HashMap<String, CataloguePage>,
    templates: &mut std::collections::HashMap<i64, ItemTemplate>,
) -> Result<()> {
    let page_data = state
        .db
        .run_read_row(&format!(
            "SELECT indexname,minrank,displayname,style_layout,img_header,img_side,label_description,label_misc,label_moredetails FROM catalogue_pages WHERE indexid = '{}'",
            page_id
        ))
        .await?;
    if page_id > 0 && page_data.is_empty() {
        return Ok(());
    }

    let mut page_index_name = String::new();
    let mut page_builder = String::new();
    let mut page = CataloguePage::default();

    if page_id > 0 && page_data.len() >= 9 {
        page_index_name = page_data[0].clone();
        page.display_name = page_data[2].clone();
        page.min_rank = page_data[1].parse::<u8>().unwrap_or(1);

        page_builder.push_str(&format!(
            "i:{}\rn:{}\rl:{}\r",
            page_index_name, page_data[2], page_data[3]
        ));
        if !page_data[4].is_empty() {
            page_builder.push_str(&format!("g:{}\r", page_data[4]));
        }
        if !page_data[5].is_empty() {
            page_builder.push_str(&format!("e:{}\r", page_data[5]));
        }
        if !page_data[6].is_empty() {
            page_builder.push_str(&format!("h:{}\r", page_data[6]));
        }
        if !page_data[8].is_empty() {
            page_builder.push_str(&format!("w:{}\r", page_data[8]));
        }
        if !page_data[7].is_empty() {
            for misc in page_data[7].split('\r') {
                page_builder.push_str(misc);
                page_builder.push('\r');
            }
        }
    }

    let item_rows = state
        .db
        .run_read_table(&format!(
            "SELECT tid,typeid,length,width,catalogue_cost,door,tradeable,recycleable,catalogue_name,catalogue_description,name_cct,colour,top \
             FROM catalogue_items WHERE catalogue_id_page = '{}' ORDER BY catalogue_id_index ASC",
            page_id
        ))
        .await?;

    for row in item_rows {
        if row.len() < 13 {
            continue;
        }

        let template_id = row[0].parse::<i64>().unwrap_or(0);
        let item_type_id = row[1].parse::<u8>().unwrap_or(1);
        let item_length = row[2].parse::<i64>().unwrap_or(1);
        let item_width = row[3].parse::<i64>().unwrap_or(1);
        let item_cost = row[4].parse::<i64>().unwrap_or(0);
        let item_door = row[5] == "1";
        let item_tradeable = row[6] == "1";
        let item_recycleable = row[7] == "1";
        let item_name = row[8].clone();
        let item_desc = row[9].clone();
        let item_cct = row[10].clone();
        let item_colour = row[11].clone();
        let item_top_h = row[12].parse::<f64>().unwrap_or(0.0);

        if !string_manager::get_string_part(&item_cct, 0, 4).eq("deal") {
            templates.entry(template_id).or_insert_with(|| {
                if item_cct.contains(' ') {
                    let mut parts = item_cct.splitn(2, ' ');
                    ItemTemplate {
                        sprite: parts.next().unwrap_or_default().to_string(),
                        colour: parts.next().unwrap_or_default().to_string(),
                        type_id: item_type_id,
                        length: item_length,
                        width: item_width,
                        top_h: item_top_h,
                        is_door: item_door,
                        is_tradeable: item_tradeable,
                        is_recycleable: item_recycleable,
                    }
                } else {
                    ItemTemplate {
                        sprite: item_cct.clone(),
                        colour: item_colour.clone(),
                        type_id: item_type_id,
                        length: item_length,
                        width: item_width,
                        top_h: item_top_h,
                        is_door: item_door,
                        is_tradeable: item_tradeable,
                        is_recycleable: item_recycleable,
                    }
                }
            });

            if page_id == -1 {
                continue;
            }

            page_builder.push_str(&format!(
                "p:{}\t{}\t{}\t\t{}\t{}\t",
                item_name,
                item_desc,
                item_cost,
                if item_type_id == 0 { "i" } else { "s" },
                item_cct
            ));

            if item_type_id == 0 {
                page_builder.push('\t');
            } else {
                page_builder.push_str("0\t");
            }

            if item_type_id == 0 {
                page_builder.push('\t');
            } else {
                page_builder.push_str(&format!("{},{}\t", item_length, item_width));
            }

            page_builder.push_str(&item_cct);
            page_builder.push('\t');
            if item_type_id > 0 {
                page_builder.push_str(&item_colour);
            }
            page_builder.push('\r');
        } else if page_id != -1 {
            let deal_id = item_cct.get(4..).unwrap_or("0").parse::<i64>().unwrap_or(0);
            let deal_item_ids = state
                .db
                .run_read_column_i64(&format!(
                    "SELECT tid FROM catalogue_deals WHERE id = '{}' ORDER BY tid ASC",
                    deal_id
                ))
                .await
                .unwrap_or_default();
            let deal_item_amounts = state
                .db
                .run_read_column_i64(&format!(
                    "SELECT amount FROM catalogue_deals WHERE id = '{}' ORDER BY tid ASC",
                    deal_id
                ))
                .await
                .unwrap_or_default();

            page_builder.push_str(&format!(
                "p:{}\t{}\t{}\t\td\t\t\t\tdeal{}\t\t{}\t",
                item_name,
                item_desc,
                item_cost,
                deal_id,
                deal_item_ids.len()
            ));
            for (index, deal_item_id) in deal_item_ids.iter().enumerate() {
                let item_cct = state
                    .db
                    .run_read_unsafe_string(&format!(
                        "SELECT name_cct FROM catalogue_items WHERE tid = '{}' LIMIT 1",
                        deal_item_id
                    ))
                    .await;
                let item_colour = state
                    .db
                    .run_read_unsafe_string(&format!(
                        "SELECT colour FROM catalogue_items WHERE tid = '{}' LIMIT 1",
                        deal_item_id
                    ))
                    .await;
                page_builder.push_str(&format!(
                    "{}\t{}\t{}\t",
                    item_cct,
                    deal_item_amounts.get(index).copied().unwrap_or(1),
                    item_colour
                ));
            }
        }
    }

    if page_id != -1 {
        page.page_data = page_builder;
        pages.insert(page_index_name, page);
    }

    Ok(())
}
